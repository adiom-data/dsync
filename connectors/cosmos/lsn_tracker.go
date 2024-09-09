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
	"go.mongodb.org/mongo-driver/bson/primitive"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

// LsnTracker is a helper method to track the LSN of the CosmosDB change feed
// It's simply the overall number of changes seen so far
// It's used for replication lag calculation
// We just run parallel changestream listeners for each namespace

// Creates parallel change streams for each namespaces, and processes the events concurrently to increment global LSN
func (cc *Connector) startGlobalLsnWorkers(ctx context.Context, namespaces []namespace, readPlanStartAt int64) error {
	var wg sync.WaitGroup
	// iterate over all namespaces and start a change stream for each
	for _, ns := range namespaces {
		wg.Add(1)
		go func(ns namespace) {
			defer wg.Done()
			//get task location and retrieve resume token
			loc := iface.Location{Database: ns.db, Collection: ns.col}
			slog.Info(fmt.Sprintf("Connector %s is starting to read change stream for flow %s at namespace %s.%s", cc.id, cc.flowId, loc.Database, loc.Collection))

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
			changeStream, err := cc.createChangeStream(ctx, loc, opts)
			if err != nil {
				if errors.Is(context.Canceled, ctx.Err()) {
					slog.Debug(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v, but the context was cancelled", loc.Database, loc.Collection, err))
				} else {
					slog.Error(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v", loc.Database, loc.Collection, err))
				}
				return
			}
			defer changeStream.Close(ctx)

			//process the change stream events for this change stream
			for changeStream.Next(ctx) {
				//increment WriteLSN atomically
				atomic.AddInt64(&cc.status.WriteLSN, 1)
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
