/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorMongo

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/adiom-data/dsync/protocol/iface"
)

// doIntegrityCheck performs a data integrity check on the underlying data store
// _sync is a synchronous version of this function
func (mc *MongoConnector) doIntegrityCheck_sync(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan) {
	// var res iface.ConnectorDataIntegrityCheckResult
	// db := "test"
	// col := "test"
	// collection := mc.client.Database(db).Collection(col)
	// count, err := collection.CountDocuments(mc.ctx, bson.D{})
	// if err != nil {
	// 	res = iface.ConnectorDataIntegrityCheckResult{Success: false}
	// } else {
	// 	res = iface.ConnectorDataIntegrityCheckResult{Count: count, Success: true}
	// }
	// mc.coord.PostDataIntegrityCheckResult(flowId, mc.id, res)

	var res iface.ConnectorDataIntegrityCheckResult
	tasks := readPlan.Tasks

	// create a channel to distribute tasks
	taskChannel := make(chan iface.ReadPlanTask)
	// create a wait group to wait for all copiers to finish
	var wg sync.WaitGroup
	wg.Add(mc.settings.numParallelIntegrityCheckTasks)

	// start 4 copiers
	for i := 0; i < mc.settings.numParallelIntegrityCheckTasks; i++ {
		go func() {
			defer wg.Done()
			for task := range taskChannel {
				slog.Debug(fmt.Sprintf("Processing integrity check task: %v", task))
				db := task.Def.Db
				col := task.Def.Col
				collection := mc.client.Database(db).Collection(col)
				count, err := collection.EstimatedDocumentCount(mc.ctx)
				if err != nil {
					if mc.flowCtx.Err() == context.Canceled {
						slog.Debug(fmt.Sprintf("Count error: %v, but the context was cancelled", err))
						return
					} else {
						slog.Error(fmt.Sprintf("Failed to count documents: %v", err))
					}
					res.Success = false
					mc.coord.PostDataIntegrityCheckResult(flowId, mc.id, res)
					return
				}
				// atomically increment the counter
				atomic.AddInt64(&res.Count, count)
			}
		}()
	}

	// iterate over all the tasks and distribute them to checkers
	for _, task := range tasks {
		taskChannel <- task
	}
	// close the task channel to signal checkers that there are no more tasks
	close(taskChannel)

	// wait for all tasks to finish
	wg.Wait()

	// post the result
	res.Success = true
	mc.coord.PostDataIntegrityCheckResult(flowId, mc.id, res)
}
