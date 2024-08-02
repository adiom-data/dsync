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
	approxTasksChannel := make(chan iface.ReadPlanTask, len(namespaceTasks))

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
	go cc.parallelNamespaceTaskPreparer(countCheckChannel, finalTasksChannel, approxTasksChannel)
	// do parallel task generation based on approximate boundaries (skip those with matching boundaries - empty tasks)

	for task := range finalTasksChannel {
		finalTasks = append(finalTasks, task)
	}
	return finalTasks, nil
}

// In parallel checks the count of the namespace and segregates them into two channels
// One for finalized tasks, and another for partitioned tasks with approximate bounds that need to be clarified
func (cc *CosmosConnector) parallelNamespaceTaskPreparer(countCheckChannel <-chan iface.ReadPlanTask, finalTasksChannel chan<- iface.ReadPlanTask, approxTasksChannel chan<- iface.ReadPlanTask) {
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
						slog.Warn(fmt.Sprintf("Failed to count documents, not splitting: %v", err))
						finalTasksChannel <- nsTask
						continue
					}
				}

				if count < cc.settings.targetDocCountPerPartition*2 { //not worth doing anything
					finalTasksChannel <- nsTask
				} else { //we need to split it
					slog.Debug(fmt.Sprintf("Need to split task %v with count %v", nsTask, count))
					//get min and max bounds
					min, max, err := cc.getMinAndMax(cc.ctx, nsTask, cc.settings.partitionKey)
					if err != nil {
						slog.Warn(fmt.Sprintf("Failed to get min and max boundaries for a task %v so not splitting: %v", nsTask, err))
						finalTasksChannel <- nsTask
						continue
					}
					slog.Debug(fmt.Sprintf("Min and max boundaries for task %v: %v, %v", nsTask, min, max))
					// find approximate split points
					numParts := int(count / cc.settings.targetDocCountPerPartition)
					approxBounds, err := splitRange(min, max, numParts)
					if err != nil {
						slog.Warn(fmt.Sprintf("Failed to split range for task %v so not splitting: %v", nsTask, err))
						finalTasksChannel <- nsTask
						continue
					}
					slog.Debug(fmt.Sprintf("Number of approximate split points for task %v: %v", nsTask, len(approxBounds)))

					//send min and max tasks to finalized channel
					minTask := iface.ReadPlanTask{}
					minTask.Def = nsTask.Def
					minTask.Def.PartitionKey = cc.settings.partitionKey
					minTask.Def.High = min //only setting the high boundary indicating that we want (-INF, min)

					maxTask := iface.ReadPlanTask{}
					maxTask.Def = nsTask.Def
					maxTask.Def.PartitionKey = cc.settings.partitionKey
					maxTask.Def.Low = max //only setting the low boundary indicating that we want (max, INF)

					finalTasksChannel <- minTask
					finalTasksChannel <- maxTask
					//send the tasks with approx bounaries downstream
				}
			}
			wg.Done()
		}()
	}

	// wait for all workers to finish and close the idsToCheck channel
	wg.Wait()
	close(finalTasksChannel)
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
