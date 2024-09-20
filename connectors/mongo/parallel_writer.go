/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package mongo

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
	ProcessDataMessage(iface.DataMessage) error
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
	numWorkers int

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
}

// NewParallelWriter creates a new ParallelWriter
func NewParallelWriter(ctx context.Context, connector ParallelWriterConnector, numWorkers int) *ParallelWriter {
	return &ParallelWriter{
		ctx:            ctx,
		connector:      connector,
		numWorkers:     numWorkers,
		taskBarrierMap: make(map[uint]uint),
	}
}

func (bwa *ParallelWriter) Start() {
	// create and start the workers
	bwa.workers = make([]writerWorker, bwa.numWorkers)
	for i := 0; i < bwa.numWorkers; i++ {
		bwa.workers[i] = newWriterWorker(bwa, i, 10) //XXX: should we make the queue size configurable? WARNING: these could be batches and they could be big
		go bwa.workers[i].run()
	}
}

func hashDataMsgId(dataMsg iface.DataMessage) int {
	hash := fnv.New32a()
	hash.Write(*dataMsg.Id)

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
	for {
		select {
		case <-ww.parallelWriter.ctx.Done():
			return
		case msg := <-ww.queue:
			// if it's a barrier message, check that we're the last worker to see it before handling
			if msg.MutationType == iface.MutationType_Barrier {
				isLastWorker := false

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

			// process the data message
			err := ww.parallelWriter.connector.ProcessDataMessage(msg)
			if err != nil {
				slog.Error(fmt.Sprintf("Worker %v failed to process data message: %v", ww.id, err))
			}
		}
	}
}

// Adds a message to the worker's queue
func (ww *writerWorker) addMessage(msg iface.DataMessage) {
	ww.queue <- msg
}
