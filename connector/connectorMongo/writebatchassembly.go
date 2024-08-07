/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorMongo

import (
	"context"

	"github.com/adiom-data/dsync/protocol/iface"
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
	// Assembly flush interval in milliseconds
	flushIntervalMs int
}

// NewBatchWriteAssembly creates a new BatchWriteAssembly
func NewBatchWriteAssembly(ctx context.Context, connector *MongoConnector, numWorkers int, maxBatchSizeOps int, flushIntervalMs int) *BatchWriteAssembly {
	return &BatchWriteAssembly{
		ctx:             ctx,
		connector:       connector,
		numWorkers:      numWorkers,
		maxBatchSizeOps: maxBatchSizeOps,
		flushIntervalMs: flushIntervalMs,
	}
}

// Processes a data message
func (bwa *BatchWriteAssembly) ScheduleDataMessage(dataMsg iface.DataMessage) error {
	return nil
}

// Processes a barrier
func (bwa *BatchWriteAssembly) ScheduleBarrier(barrierMsg iface.DataMessage) error {
	bwa.connector.handleBarrierMessage(barrierMsg)
	return nil
}
