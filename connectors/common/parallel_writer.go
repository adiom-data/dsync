/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package common

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/adiom-data/dsync/protocol/iface"
	"golang.org/x/exp/rand"
)

type ParallelWriterConnector interface {
	HandleBarrierMessage(iface.DataMessage) error
	ProcessDataMessages([]iface.DataMessage) error
}

// A parallelized writes processor
// Preserves sequence of operations for a given _id
// Supports barriers
// Automatically winds down when the context is done
type ParallelWriter struct {
	// Context for execution
	ctx context.Context

	// MongoConnector
	connector ParallelWriterConnector

	// Number of parallel workers
	numWorkers   int
	maxBatchSize int

	// Array of workers
	workers []writerWorker

	// Map of task barriers that were scheduled but haven't been cleared yet
	// Maps task ID to the number of workers that processed the barrier yet (countdown to 0)
	// A barrier is cleared out by the last worker that processes it
	taskBarrierMap map[uint]uint
	// mutex for taskBarrierMap
	taskBarrierMapMutex sync.Mutex

	// CDC resume token barrier countdown
	// We don't admit new CDC barriers until the countdown reaches 0
	resumeTokenBarrierWorkersCountdown atomic.Int32

	blockBarrier chan struct{}

	done chan struct{}
}

// NewParallelWriter creates a new ParallelWriter
func NewParallelWriter(ctx context.Context, connector ParallelWriterConnector, numWorkers int, maxBatchSize int) *ParallelWriter {
	return &ParallelWriter{
		ctx:            ctx,
		connector:      connector,
		numWorkers:     numWorkers,
		maxBatchSize:   maxBatchSize,
		taskBarrierMap: make(map[uint]uint),
		blockBarrier:   make(chan struct{}),
		done:           make(chan struct{}),
	}
}

func (bwa *ParallelWriter) Start() {
	// create and start the workers
	bwa.workers = make([]writerWorker, bwa.numWorkers)
	for i := 0; i < bwa.numWorkers; i++ {
		bwa.workers[i] = newWriterWorker(bwa, i, 10) //XXX: should we make the queue size configurable? WARNING: these could be batches and they could be big
		go func() {
			bwa.workers[i].run()
			bwa.done <- struct{}{}
		}()
	}
}

func (bwa *ParallelWriter) StopAndWait() {
	for _, worker := range bwa.workers {
		close(worker.queue)
	}
	for i := 0; i < bwa.numWorkers; i++ {
		<-bwa.done
	}
}

func hashDataMsgId(dataMsg iface.DataMessage) int {
	hash := fnv.New32a()
	for _, d := range dataMsg.Id {
		hash.Write(d.GetData())
	}
	return int(hash.Sum32())
}

// Processes a data message
func (bwa *ParallelWriter) ScheduleDataMessage(dataMsg iface.DataMessage) error {
	if len(bwa.workers) == 0 {
		return fmt.Errorf("ParallelWriter not started")
	}

	var workerId int

	if dataMsg.MutationType == iface.MutationType_InsertBatch { // batch inserts don't have ids in the data message
		workerId = rand.Intn(bwa.numWorkers) // let's just randomly assign them to workers //XXX: how safe is this?
	} else {
		// hash the _id to determine the correct worker
		workerId = hashDataMsgId(dataMsg) % bwa.numWorkers
	}
	// add the message to the worker's queue
	bwa.workers[workerId].addMessage(dataMsg)

	return nil
}

// Processes a barrier
func (bwa *ParallelWriter) ScheduleBarrier(barrierMsg iface.DataMessage) error {
	// For task barriers, initialize the countdown for the task based on task ID
	// Error out if the barrier is already present in the map
	// For CDC barriers, warn if the countdown is not 0
	// Otherwise, set the countdown to the number of workers
	// Lastly, broadcast the barrier to all workers
	switch barrierMsg.BarrierType {
	case iface.BarrierType_TaskComplete:
		bwa.taskBarrierMapMutex.Lock()
		if _, ok := bwa.taskBarrierMap[barrierMsg.BarrierTaskId]; ok {
			bwa.taskBarrierMapMutex.Unlock()
			return fmt.Errorf("task barrier for task %v already present in the map", barrierMsg.BarrierTaskId)
		}
		bwa.taskBarrierMap[barrierMsg.BarrierTaskId] = uint(bwa.numWorkers)
		bwa.taskBarrierMapMutex.Unlock()
	case iface.BarrierType_CdcResumeTokenUpdate:
		countdown := bwa.resumeTokenBarrierWorkersCountdown.Load()
		if countdown != 0 {
			return fmt.Errorf("another CDC resume token barrier is already being processed (countdown is %v instead of 0)", countdown)
		}
		bwa.resumeTokenBarrierWorkersCountdown.Store(int32(bwa.numWorkers))
	}

	bwa.BroadcastMessage(barrierMsg)

	if barrierMsg.BarrierType == iface.BarrierType_Block {
		slog.Debug("Blocking barrier encountered.")
		for i := 0; i < bwa.numWorkers; i++ {
			<-bwa.blockBarrier
		}
		slog.Debug("Blocking barrier unblocked.")
	}

	return nil
}

// Broadcasts message to all workers
func (bwa *ParallelWriter) BroadcastMessage(dataMsg iface.DataMessage) {
	for i := 0; i < bwa.numWorkers; i++ {
		bwa.workers[i].addMessage(dataMsg)
	}
}

// ----------------
// Worker business
// ----------------

type writerWorker struct {
	// Assembly
	parallelWriter *ParallelWriter

	// Worker ID
	id int
	// Worker's queue
	queue chan iface.DataMessage
}

// newWriterWorker creates a new writerWorker
func newWriterWorker(parallelWriter *ParallelWriter, id int, queueSize int) writerWorker {
	return writerWorker{parallelWriter, id, make(chan iface.DataMessage, queueSize)}
}

// Worker's main loop - processes messages from the queue
func (ww *writerWorker) run() {
	var batch []iface.DataMessage
	for {
		select {
		case <-ww.parallelWriter.ctx.Done():
			return
		case msg, ok := <-ww.queue:
			if !ok {
				return
			}
			// if it's a barrier message, check that we're the last worker to see it before handling
			if msg.MutationType == iface.MutationType_Barrier {
				isLastWorker := false

				// process existing batch prior to barrier
				if len(batch) > 0 {
					err := ww.parallelWriter.connector.ProcessDataMessages(batch)
					if err != nil {
						slog.Error(fmt.Sprintf("Worker %v failed to process data messages: %v", ww.id, err))
					}
					batch = nil
				}

				if msg.BarrierType == iface.BarrierType_Block {
					ww.parallelWriter.blockBarrier <- struct{}{}
				}

				// if it's a task barrier, decrement the countdown for the task
				if msg.BarrierType == iface.BarrierType_TaskComplete {
					ww.parallelWriter.taskBarrierMapMutex.Lock()
					ww.parallelWriter.taskBarrierMap[msg.BarrierTaskId]--
					if ww.parallelWriter.taskBarrierMap[msg.BarrierTaskId] == 0 {
						delete(ww.parallelWriter.taskBarrierMap, msg.BarrierTaskId)
						isLastWorker = true
					}
					ww.parallelWriter.taskBarrierMapMutex.Unlock()
				}

				// if it's a CDC barrier, decrement the countdown
				if msg.BarrierType == iface.BarrierType_CdcResumeTokenUpdate {
					countdown := ww.parallelWriter.resumeTokenBarrierWorkersCountdown.Add(-1)
					if countdown == 0 {
						isLastWorker = true
					}
				}

				if isLastWorker {
					err := ww.parallelWriter.connector.HandleBarrierMessage(msg)
					if err != nil {
						slog.Error(fmt.Sprintf("Worker %v failed to handle barrier message: %v", ww.id, err))
					}
				}

				continue
			}

			// Check to see if we should process a batch now
			if len(batch) > 0 && msg.Loc != batch[0].Loc {
				err := ww.parallelWriter.connector.ProcessDataMessages(batch)
				if err != nil {
					slog.Error(fmt.Sprintf("Worker %v failed to process data messages: %v", ww.id, err))
				}
				batch = nil
			}

			batch = append(batch, msg)
			// Process right away if we've reached the size or have to process an embedded batch or the queue is currently empty
			if (ww.parallelWriter.maxBatchSize > 0 && len(batch) >= ww.parallelWriter.maxBatchSize) || msg.MutationType == iface.MutationType_InsertBatch || len(ww.queue) == 0 {
				err := ww.parallelWriter.connector.ProcessDataMessages(batch)
				if err != nil {
					if msg.MutationType == iface.MutationType_InsertBatch {
						slog.Error(fmt.Sprintf("Worker %v failed to process data messages: %v", ww.id, err), "insertbatch", len(*batch[0].Data))
					} else {
						slog.Error(fmt.Sprintf("Worker %v failed to process data messages: %v", ww.id, err), "batch", len(batch), "first", batch[0])
					}
				}
				batch = nil
			}
		}
	}
}

// Adds a message to the worker's queue
func (ww *writerWorker) addMessage(msg iface.DataMessage) {
	ww.queue <- msg
}
