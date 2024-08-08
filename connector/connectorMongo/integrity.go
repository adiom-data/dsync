/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorMongo

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
)

// XXX: should this be somewhere else?
type namespace struct {
	db  string
	col string
}

func (ns namespace) String() string {
	return ns.db + "." + ns.col
}

type nsCountResult struct {
	ns    namespace
	count int64
	err   error
}

// doIntegrityCheck performs a data integrity check on the underlying data store
// _sync is a synchronous version of this function
func (mc *MongoConnector) doIntegrityCheck_sync(flowId iface.FlowID, options iface.ConnectorOptions) error {
	//XXX: should we use/create flowContext here in case it becomes a part of the flow and we want to have ability to interrupt?

	var res iface.ConnectorDataIntegrityCheckResult
	res.Success = false

	// 1. Get the list of fully qualified namespaces
	namespaces, err := mc.getFQNamespaceList(options.Namespace)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get fully qualified namespace list: %v", err))
		err := mc.coord.PostDataIntegrityCheckResult(flowId, mc.id, res)
		if err != nil {
			return err
		}
	}

	// Quick exit if there are no namespaces
	if len(namespaces) == 0 {
		res.Success = true
		res.Count = 0
		res.Digest = ""
		if err := mc.coord.PostDataIntegrityCheckResult(flowId, mc.id, res); err != nil {
			return err
		}
		return nil
	}

	slog.Debug(fmt.Sprintf("Namespaces for validation: %v", namespaces))

	// create a map to store the results
	namespacesCountMap := make(map[string]int64)

	// 2. Calculate the total number of documents for each namespace - we will parallelize this operation

	// create a channel to distribute tasks
	taskChannel := make(chan namespace, len(namespaces))
	// create a channel to collect the results
	resultChannel := make(chan nsCountResult, len(namespaces))

	// start 4 copiers
	for i := 0; i < mc.settings.numParallelIntegrityCheckTasks; i++ {
		go func() {
			for ns := range taskChannel {
				collection := mc.client.Database(ns.db).Collection(ns.col)
				count, err := collection.EstimatedDocumentCount(mc.ctx)
				if err != nil {
					if errors.Is(context.Canceled, mc.ctx.Err()) {
						slog.Debug(fmt.Sprintf("Count error: %v, but the context was cancelled", err))
					} else {
						slog.Error(fmt.Sprintf("Failed to count documents: %v", err))
					}
				}
				resultChannel <- nsCountResult{ns: ns, count: count, err: err}
			}
		}()
	}

	// iterate over all the namespaces and distribute them to workers
	for _, task := range namespaces {
		taskChannel <- task
	}
	// close the task channel to signal workers that there are no more tasks
	close(taskChannel)

	// wait for all tasks to finish and collect the results
	for i := 0; i < len(namespaces); i++ {
		result := <-resultChannel //XXX: should there be a timeout here?
		if result.err != nil {
			slog.Error(fmt.Sprintf("Failed to count documents for namespace %s: %v", result.ns.String(), result.err))
			if err := mc.coord.PostDataIntegrityCheckResult(flowId, mc.id, res); err != nil {
				return err
			}
			return nil
		} else {
			namespacesCountMap[result.ns.String()] = result.count
		}
	}

	// 3. Arrange the namespaces in a lexicographical order
	sortedNamespaces := make([]string, 0, len(namespacesCountMap))
	for ns := range namespacesCountMap {
		sortedNamespaces = append(sortedNamespaces, ns)
	}
	sort.Strings(sortedNamespaces)

	// 4. Create a string, concatenating the namespaces and respective counts in the form of "namespace:count" and using "," to join them
	// also calculate the total number of documents across all the namespaces in the same loop
	var concatenatedString string
	totalCount := int64(0)
	for _, ns := range sortedNamespaces {
		count := namespacesCountMap[ns]
		totalCount += count
		concatenatedString += ns + ":" + fmt.Sprintf("%d", count) + ","
	}
	// remove the trailing comma
	concatenatedString = concatenatedString[:len(concatenatedString)-1]

	// 5. Calculate the SHA256 hash of the string
	hash := sha256.Sum256([]byte(concatenatedString))

	// post the result
	res.Success = true
	res.Count = totalCount
	res.Digest = fmt.Sprintf("%x", hash)

	if err := mc.coord.PostDataIntegrityCheckResult(flowId, mc.id, res); err != nil {
		return err
	}
	return nil
}

// Returns a list of fully qualified namespaces
func (mc *MongoConnector) getFQNamespaceList(namespacesFilter []string) ([]namespace, error) {
	var dbsToResolve []string //database names that we need to resolve

	var namespaces []namespace

	if namespacesFilter == nil {
		var err error
		dbsToResolve, err = mc.getAllDatabases()
		if err != nil {
			return nil, err
		}
	} else {
		// iterate over provided namespaces
		// if it has a dot, then it is a fully qualified namespace
		// otherwise, it is a database name to resolve
		for _, ns := range namespacesFilter {
			db, col, isFQN := strings.Cut(ns, ".")
			if isFQN {
				namespaces = append(namespaces, namespace{db: db, col: col})
			} else {
				dbsToResolve = append(dbsToResolve, ns)
			}
		}
	}

	//iterate over unresolved databases and get all collections
	for _, db := range dbsToResolve {
		colls, err := mc.getAllCollections(db)
		if err != nil {
			return nil, err
		}
		//create tasks for these
		for _, coll := range colls {
			namespaces = append(namespaces, namespace{db: db, col: coll})
		}
	}

	return namespaces, nil
}
