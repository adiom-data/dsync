/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorCosmos

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	// System databases that we don't want to copy
	ExcludedDBListForIC = []string{"local", "config", "admin", "adiom-internal"}
	// System collections that we don't want to copy (regex pattern)
	ExcludedSystemCollPattern = "^system[.]"
)

func (cc *CosmosConnector) createInitialCopyTasks(namespaces []string) ([]iface.ReadPlanTask, error) {
	var dbsToResolve []string //database names that we need to resolve

	var tasks []iface.ReadPlanTask
	taskId := iface.ReadPlanTaskID(1)

	if namespaces == nil {
		var err error
		dbsToResolve, err = cc.getAllDatabases()
		if err != nil {
			return nil, err
		}
	} else {
		// iterate over provided namespaces
		// if it has a dot, then it is a fully qualified namespace
		// otherwise, it is a database name to resolve
		for _, ns := range namespaces {
			db, col, isFQN := strings.Cut(ns, ".")
			if isFQN {
				task := iface.ReadPlanTask{Id: taskId}
				task.Def.Db = db
				task.Def.Col = col

				tasks = append(tasks, task)
				taskId++
			} else {
				dbsToResolve = append(dbsToResolve, ns)
			}
		}
	}

	slog.Debug(fmt.Sprintf("Databases to resolve: %v", dbsToResolve))

	//iterate over unresolved databases and get all collections
	for _, db := range dbsToResolve {
		colls, err := cc.getAllCollections(db)
		if err != nil {
			return nil, err
		}
		//create tasks for these
		for _, coll := range colls {
			task := iface.ReadPlanTask{Id: taskId}
			task.Def.Db = db
			task.Def.Col = coll

			tasks = append(tasks, task)
			taskId++
		}
	}

	if len(tasks) > cc.settings.maxNumNamespaces {
		return nil, fmt.Errorf("too many namespaces to copy: %d, max %d", len(tasks), cc.settings.maxNumNamespaces)
	}

	return cc.partitionTasksIfNecessary(tasks)
}

// partitionTasksIfNecessary checks all the namespace tasks and partitions them if necessary
func (cc *CosmosConnector) partitionTasksIfNecessary(namespaceTasks []iface.ReadPlanTask) ([]iface.ReadPlanTask, error) {
	countCheckChannel := make(chan iface.ReadPlanTask, len(namespaceTasks))
	approxSplitPointsCalcChannel := make(chan iface.ReadPlanTask, len(namespaceTasks))

	finalTasksChannel := make(chan iface.ReadPlanTask, len(namespaceTasks))
	finalTasks := make([]iface.ReadPlanTask, 0, len(namespaceTasks))

	// add namespace tasks to the queue to check their counts
	go func() {
		for _, task := range namespaceTasks {
			countCheckChannel <- task
		}
		close(countCheckChannel)
	}()

	// do parallel counting (async) and generate approximate boundaries
	// do parallel task generation based on approximate boundaries (skip those with matching boundaries - empty tasks)

	for task := range finalTasksChannel {
		finalTasks = append(finalTasks, task)
	}
	return finalTasks, nil
}

// In parallel checks the count of the namespace and segregates them into two channels
func (cc *CosmosConnector) parallelNamespaceTaskCountChecker(countCheckChannel <-chan iface.ReadPlanTask, finalTasksChannel chan<- iface.ReadPlanTask, approxSplitPointsCalcChannel chan<- iface.ReadPlanTask) {
	//define workgroup
	wg := sync.WaitGroup{}
	wg.Add(cc.settings.numParallelPartitionWorkers)
	// create workers to do the counting
	for i := 0; i < cc.settings.numParallelPartitionWorkers; i++ {
		go func() {
			for nsTask := range countCheckChannel {
				collection := cc.client.Database(nsTask.Def.Db).Collection(nsTask.Def.Col)
				count, err := collection.EstimatedDocumentCount(cc.ctx)
				if err != nil {
					if cc.ctx.Err() == context.Canceled {
						slog.Debug(fmt.Sprintf("Count error: %v, but the context was cancelled", err))
					} else {
						slog.Error(fmt.Sprintf("Failed to count documents: %v", err))
					}
				}

				if count < cc.settings.targetDocCountPerPartition*2 { //not worth doing anything
					finalTasksChannel <- nsTask
				} else {
					//get top and bottom bounds
					top, bottom, err := cc.getTopAndBottom(cc.ctx, nsTask, cc.settings.partitionKey)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to get top and bottom boundaries for a task %v so not splitting: %v", nsTask, err))
						finalTasksChannel <- nsTask
					} else {

					}
					//if can't partition (different type, not objectid or number), just add the task as a whole
					//send top and bottom tasks to finalized channel
					//do a split in floor (count / cc.settings.targetDocCountPerPartition) parts
					//generate new approx boundaries
					//send the tasks with approx bounaries downstream
				}
			}
			wg.Done()
		}()
	}

	// wait for all workers to finish and close the idsToCheck channel
	wg.Wait()
	close(approxSplitPointsCalcChannel)
}

// check if two bounaries of a range are of the same and a "partitionable" type (ObjectId or Number)
func canPartitionRange(value1 bson.RawValue, value2 bson.RawValue) bool {
	if value1.Type != value2.Type {
		return false
	}

	if value1.Type == bson.TypeObjectID /* || value1.Type == bson.TypeInt32 || value1.Type == bson.TypeInt64 */ {
		return true
	}

	return false
}

// get top and bottom boundaries for a namespace task
func (cc *CosmosConnector) getTopAndBottom(ctx context.Context, task iface.ReadPlanTask, partitionKey string) (bson.RawValue, bson.RawValue, error) {
	collection := cc.client.Database(task.Def.Db).Collection(task.Def.Col)
	optsTop := options.Find().SetProjection(bson.D{{partitionKey, 1}}).SetLimit(1).SetSort(bson.D{{partitionKey, -1}})
	optsBottom := options.Find().SetProjection(bson.D{{partitionKey, 1}}).SetLimit(1).SetSort(bson.D{{partitionKey, 1}})

	//get the top and bottom boundaries
	topCursor, err := collection.Find(ctx, bson.M{}, optsTop)
	defer topCursor.Close(ctx)
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, err
	}

	bottomCursor, err := collection.Find(ctx, bson.M{}, optsBottom)
	defer bottomCursor.Close(ctx)
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, err
	}

	topId := topCursor.Current.Lookup(partitionKey)
	bottomId := bottomCursor.Current.Lookup(partitionKey)

	return topId, bottomId, nil
}

// get all database names except system databases
func (cc *CosmosConnector) getAllDatabases() ([]string, error) {
	dbNames, err := cc.client.ListDatabaseNames(cc.ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	dbs := slices.DeleteFunc(dbNames, func(d string) bool {
		return slices.Contains(ExcludedDBListForIC, d)
	})

	return dbs, nil
}

// get all collections in a database except system collections
func (cc *CosmosConnector) getAllCollections(dbName string) ([]string, error) {
	collectionsAll, err := cc.client.Database(dbName).ListCollectionNames(cc.ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	//remove all system collections that match the pattern
	r, _ := regexp.Compile(ExcludedSystemCollPattern)
	collections := slices.DeleteFunc(collectionsAll, func(n string) bool {
		return r.Match([]byte(n))
	})

	return collections, nil
}
