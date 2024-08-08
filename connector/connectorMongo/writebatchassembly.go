/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorMongo

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"golang.org/x/exp/rand"
)

// A parallelized assembly for writes batching
// Preserves sequence of operations for a given _id
// Supports barriers
// Automatically winds down when the context is done
type BatchWriteAssembly struct {
	// Context for execution
	ctx context.Context

	// MongoConnector
	connector *MongoConnector

	// Number of parallel workers
	numWorkers int
	// Maximum number of operations in a batch for the assembly
	maxBatchSizeOps int
	// Maximum time to wait for a batch to fill up - after this time the flush will be forced anyway
	maxBatchFillTimeMs int

	// Array of workers
	workers []writerWorker
}

// NewBatchWriteAssembly creates a new BatchWriteAssembly
func NewBatchWriteAssembly(ctx context.Context, connector *MongoConnector, numWorkers int, maxBatchSizeOps int, maxBatchFillTimeMs int) *BatchWriteAssembly {
	return &BatchWriteAssembly{
		ctx:                ctx,
		connector:          connector,
		numWorkers:         numWorkers,
		maxBatchSizeOps:    maxBatchSizeOps,
		maxBatchFillTimeMs: maxBatchFillTimeMs,
	}
}

func (bwa *BatchWriteAssembly) Start() {
	// create and start the workers
	bwa.workers = make([]writerWorker, bwa.numWorkers)
	for i := 0; i < bwa.numWorkers; i++ {
		bwa.workers[i] = newWriterWorker(bwa, i, 1000) //XXX: should we make the queue size configurable?
		go bwa.workers[i].run()
	}
}

func hashDataMsgId(dataMsg iface.DataMessage) int {
	hash := fnv.New32a()
	hash.Write(*dataMsg.Id)

	return int(hash.Sum32())
}

// Processes a data message
func (bwa *BatchWriteAssembly) ScheduleDataMessage(dataMsg iface.DataMessage) error {
	if len(bwa.workers) == 0 {
		return fmt.Errorf("BatchWriteAssembly not started")
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
func (bwa *BatchWriteAssembly) ScheduleBarrier(barrierMsg iface.DataMessage) error {
	bwa.connector.handleBarrierMessage(barrierMsg)
	return nil
}

// ----------------
// Worker business
// ----------------

type writerWorker struct {
	// Assembly
	batchWriteAssembly *BatchWriteAssembly

	// Worker ID
	id int
	// Worker's queue
	queue chan iface.DataMessage
}

// newWriterWorker creates a new writerWorker
func newWriterWorker(batchWriteAssembly *BatchWriteAssembly, id int, queueSize int) writerWorker {
	return writerWorker{batchWriteAssembly, id, make(chan iface.DataMessage, queueSize)}
}

// Worker's main loop - processes messages from the queue
func (ww *writerWorker) run() {
	for {
		select {
		case <-ww.batchWriteAssembly.ctx.Done():
			return
		case msg := <-ww.queue:
			// process the message
			err := ww.batchWriteAssembly.connector.processDataMessage(msg)
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
