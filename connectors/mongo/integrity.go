/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package mongo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"

	// TODO: Check how to add a new package properly
	"github.com/holiman/uint256"
)

func NamespaceString(ns iface.Namespace) string {
	return ns.Db + "." + ns.Col
}

type nsCountResult struct {
	ns    iface.Namespace
	count int64
	err   error
}

type nsHashResult struct {
	ns   iface.Namespace
	hash *uint256.Int
	err  error
}

// doIntegrityCheck performs a data integrity check on the underlying data store
// _sync is a synchronous version of this function
func (mc *BaseMongoConnector) doIntegrityCheck_sync(flowId iface.FlowID, options iface.ConnectorOptions) error {
	//XXX: should we use/create flowContext here in case it becomes a part of the flow, and we want to have ability to interrupt?

	var res iface.ConnectorDataIntegrityCheckResult
	res.Success = false

	// 1. Get the list of fully qualified namespaces
	namespaces, err := mc.getFQNamespaceList(options.Namespace)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get fully qualified namespace list: %v", err))
		err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res)
		if err != nil {
			return err
		}
	}

	// Quick exit if there are no namespaces
	if len(namespaces) == 0 {
		res.Success = true
		res.Count = 0
		res.Digest = ""
		if err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res); err != nil {
			return err
		}
		return nil
	}

	slog.Debug(fmt.Sprintf("Namespaces for validation: %v", namespaces))

	// create a map to store the results
	namespacesCountMap := make(map[string]int64)

	// 2. Calculate the total number of documents for each namespace - we will parallelize this operation

	// create a channel to distribute tasks
	taskChannel := make(chan iface.Namespace, len(namespaces))
	// create a channel to collect the results
	resultChannel := make(chan nsCountResult, len(namespaces))

	// start 4 copiers
	for i := 0; i < mc.Settings.NumParallelIntegrityCheckTasks; i++ {
		go func() {
			for ns := range taskChannel {
				collection := mc.Client.Database(ns.Db).Collection(ns.Col)
				count, err := collection.EstimatedDocumentCount(mc.Ctx)
				if err != nil {
					if errors.Is(context.Canceled, mc.Ctx.Err()) {
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
			slog.Error(fmt.Sprintf("Failed to count documents for namespace %s: %v", NamespaceString(result.ns), result.err))
			if err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res); err != nil {
				return err
			}
			return nil
		} else {
			namespacesCountMap[NamespaceString(result.ns)] = result.count
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

	if err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res); err != nil {
		return err
	}
	return nil
}

func (mc *BaseMongoConnector) doFullIntegrityCheck_sync(flowId iface.FlowID, options iface.ConnectorOptions) error {
	var res iface.ConnectorDataIntegrityCheckResult
	res.Success = false

	// 1. Get the list of fully qualified namespaces
	namespaces, err := mc.getFQNamespaceList(options.Namespace)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get fully qualified namespace list: %v", err))
		err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res)
		if err != nil {
			return err
		}
	}

	// Quick exit if there are no namespaces
	if len(namespaces) == 0 {
		res.Success = true
		res.Count = 0
		res.Digest = ""
		if err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res); err != nil {
			return err
		}
		return nil
	}

	slog.Debug(fmt.Sprintf("Namespaces for validation: %v", namespaces))

	// create a map to store the results
	namespacesSha256Map := make(map[string]string)

	// 2. Calculate the total number of documents for each namespace - we will parallelize this operation

	// create a channel to distribute tasks
	taskChannel := make(chan iface.Namespace, len(namespaces))
	// create a channel to collect the results
	resultChannel := make(chan nsHashResult, len(namespaces))

	var wg sync.WaitGroup
	wg.Add(mc.Settings.NumParallelIntegrityCheckTasks)

	// start hashers
	for i := 0; i < mc.Settings.NumParallelIntegrityCheckTasks; i++ {
		go func() {
			defer wg.Done()
			completeHash := uint256.NewInt(1)
			for ns := range taskChannel {
				slog.Debug(fmt.Sprintf("Processing namespace: %v", ns))
				collection := mc.Client.Database(ns.Db).Collection(ns.Col)
				cursor, err := collection.Find(mc.FlowCtx, bson.D{})
				if err != nil {
					if errors.Is(context.Canceled, mc.FlowCtx.Err()) {
						slog.Debug(fmt.Sprintf("Find error: %v, but the context was cancelled", err))
					} else {
						slog.Error(fmt.Sprintf("Failed to find documents in collection: %v", err))
					}
					continue
				}

				for cursor.Next(mc.FlowCtx) {
					rawData := cursor.Current
					data := []byte(rawData)

					hash := sha256.Sum256(data)

					// TODO: Conversation here is slow and ugly. Find more fitting package for this.
					hashStr := strings.TrimLeft(hex.EncodeToString(hash[:]), "0")
					hashInt := uint256.MustFromHex("0x" + hashStr)

					// Multiplication is order independent, so it does not matter the order we are going to get the
					// values in, we will still get the same hash as the result, if all the hashes were the same.
					completeHash.Mul(completeHash, hashInt)

					if cursor.RemainingBatchLength() == 0 { //no more left in the batch
						resultChannel <- nsHashResult{ns: ns, hash: completeHash, err: err}
						completeHash = uint256.NewInt(1)
					}
				}
				if err := cursor.Err(); err != nil {
					if errors.Is(context.Canceled, mc.FlowCtx.Err()) {
						slog.Debug(fmt.Sprintf("Cursor error: %v, but the context was cancelled", err))
					} else {
						slog.Error(fmt.Sprintf("Cursor error: %v", err))
					}
				} else {
					_ = cursor.Close(mc.FlowCtx)
					slog.Debug(fmt.Sprintf("Done hashing namespace: %v", ns))
				}
			}
		}()
	}

	// iterate over all the namespaces and distribute them to workers
	for _, task := range namespaces {
		taskChannel <- task
	}

	// close the task channel to signal workers that there are no more tasks
	close(taskChannel)

	//wait for all hashers to finish
	wg.Wait()

	close(resultChannel)

	for result := range resultChannel {
		if result.err != nil {
			slog.Error(fmt.Sprintf("Failed to hash documents for namespace %s: %v", NamespaceString(result.ns), result.err))
			if err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res); err != nil {
				return err
			}
			return nil
		} else {
			namespacesSha256Map[NamespaceString(result.ns)] = result.hash.Hex()
		}
	}

	// 3. Arrange the namespaces in a lexicographical order
	sortedNamespaces := make([]string, 0, len(namespacesSha256Map))
	for ns := range namespacesSha256Map {
		sortedNamespaces = append(sortedNamespaces, ns)
	}
	sort.Strings(sortedNamespaces)

	// 4. Create a string, concatenating the namespaces and respective hash in the form of "namespace:hash" and using "," to join them
	var concatenatedString string
	for _, ns := range sortedNamespaces {
		hash := namespacesSha256Map[ns]
		concatenatedString += ns + ":" + hash + ","
	}
	// remove the trailing comma
	concatenatedString = concatenatedString[:len(concatenatedString)-1]

	// 5. Calculate the SHA256 hash of the string
	hash := sha256.Sum256([]byte(concatenatedString))

	// post the result
	res.Success = true
	res.Count = 0
	res.Digest = hex.EncodeToString(hash[:])

	if err := mc.Coord.PostDataIntegrityCheckResult(flowId, mc.ID, res); err != nil {
		return err
	}
	return nil
}

// Returns a list of fully qualified namespaces
func (mc *BaseMongoConnector) getFQNamespaceList(namespacesFilter []string) ([]iface.Namespace, error) {
	var dbsToResolve []string //database names that we need to resolve

	var namespaces []iface.Namespace

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
				namespaces = append(namespaces, iface.Namespace{Db: db, Col: col})
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
			namespaces = append(namespaces, iface.Namespace{Db: db, Col: coll})
		}
	}

	return namespaces, nil
}
