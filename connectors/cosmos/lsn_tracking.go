/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package cosmos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

// Creates parallel change streams for each namespaces, and processes the events concurrently to increment global LSN
func (cc *Connector) startGlobalLsnWorkers(ctx context.Context, namespaces []iface.Namespace, readPlanStartAt int64) error {
	var wg sync.WaitGroup
	lsnTracker := NewMultiNsLSNTracker()
	// iterate over all namespaces and start a change stream for each
	for _, ns := range namespaces {
		wg.Add(1)
		go func(ns iface.Namespace) {
			defer wg.Done()
			//get task location and retrieve resume token
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			slog.Info(fmt.Sprintf("Connector %s is starting to track LSN for flow %s at namespace %s.%s", cc.ID, cc.FlowId, loc.Database, loc.Collection))

			token, err := cc.flowCDCResumeTokenMap.GetToken(loc)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get resume token for location %v: %v", loc, err))
			}
			var opts *moptions.ChangeStreamOptions
			if token != nil {
				//set the change stream options to start from the resume token
				opts = moptions.ChangeStream().SetResumeAfter(token).SetFullDocument(moptions.UpdateLookup)
			} else { //we need to start from the read plan creation time to be safe
				// create timestamp from read plan start time
				ts := primitive.Timestamp{T: uint32(readPlanStartAt)}
				slog.Debug(fmt.Sprintf("Starting change stream for %v at timestamp %v", ns, ts))
				opts = moptions.ChangeStream().SetStartAtOperationTime(&ts).SetFullDocument(moptions.UpdateLookup)
			}
			changeStream, err := createChangeStream(ctx, cc.Client, loc, opts)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					slog.Debug(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v, but the context was cancelled", loc.Database, loc.Collection, err))
				} else {
					slog.Error(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v", loc.Database, loc.Collection, err))
				}
				return
			}
			defer changeStream.Close(ctx)

			useCosmosContinuationToken := true

			// process the change stream events for this change stream
			for changeStream.Next(ctx) {
				var change bson.M
				if err := changeStream.Decode(&change); err != nil {
					slog.Error(fmt.Sprintf("Failed to decode change stream event: %v", err))
					continue
				}

				// extract the continuation value from the change stream event
				// if we fail to extract the continuation value, we will stop using the Cosmos continuation token for this change stream
				if useCosmosContinuationToken {
					continuation, err := getChangeStreamContinuationValue(change)
					if err != nil {
						slog.Warn(fmt.Sprintf("Error extracting continuation value for namespace %v from change event: %v. Replication lag might be inaccurate", ns, err))
						useCosmosContinuationToken = false //don't try to extract the continuation value anymore
						lsnTracker.IncrementLSN(ns)        //increment the LSN
					} else {
						// store the continuation value in the lsn tracker
						lsnTracker.SetLSN(ns, int64(continuation))
					}
				} else {
					lsnTracker.IncrementLSN(ns)
				}

				// update the global LSN
				atomic.StoreInt64(&cc.Status.WriteLSN, lsnTracker.GetGlobalLSN())
			}

			if err := changeStream.Err(); err != nil {
				if ctx.Err() == context.Canceled {
					slog.Debug(fmt.Sprintf("Change stream error: %v, but the context was cancelled", err))
				} else {
					slog.Error(fmt.Sprintf("Change stream error: %v", err))
				}
			}

		}(ns)
	}
	wg.Wait()
	return nil
}

// MultiNsLSNTracker is a thread-safe tracker for Last Sequence Numbers (LSN) across multiple namespaces.
type MultiNsLSNTracker struct {
	mu         sync.Mutex
	namespaces map[iface.Namespace]int64 // Stores the LSNs for each namespace.
}

// NewMultiNsLSNTracker creates a new LSNTracker instance.
func NewMultiNsLSNTracker() *MultiNsLSNTracker {
	return &MultiNsLSNTracker{
		namespaces: make(map[iface.Namespace]int64),
	}
}

// SetLSN sets the LSN for a specific namespace.
func (l *MultiNsLSNTracker) SetLSN(namespace iface.Namespace, lsn int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.namespaces[namespace] = lsn
}

// GetLSN gets the LSN for a specific namespace.
func (l *MultiNsLSNTracker) GetLSN(namespace iface.Namespace) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.namespaces[namespace]
}

// GetGlobalLSN returns the sum of all LSNs across all namespaces.
func (l *MultiNsLSNTracker) GetGlobalLSN() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	var globalLSN int64
	for _, lsn := range l.namespaces {
		globalLSN += lsn
	}
	return globalLSN
}

// IncrementLSN increments the LSN for a specific namespace.
func (l *MultiNsLSNTracker) IncrementLSN(namespace iface.Namespace) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.namespaces[namespace]++
}
