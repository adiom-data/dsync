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
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// batch size for ids to check
	idsBatchSize = 1000
	// number of parallel tasks to scan the witness index
	numParallelWitnessScanTasks = 4
	// number of parallel tasks to scan the source index
	numParallelSourceScanTasks = 4
)

/**
 * Check for deletes in Cosmos
 *
 * This function is used to simulate deletes in Cosmos since Cosmos does not support deletes in changestream.
 * We use an external "Witness" index to track which documents we have already read from the source.
 * At any point in time, any document that is not present in the Witness index but is present in the source is considered deleted.
 *
 * For simplicity, we are using the destination's index directly right now because we can.
 */
//TODO: Could there be a race condition here when a doc with the same _id is recreated?
func (cc *CosmosConnector) CheckForDeletesSync(flowId iface.FlowID, options iface.ConnectorOptions, flowDataChannel chan<- iface.DataMessage) {
	if !cc.settings.EmulateDeletes {
		//do nothing
		return
	}

	// Preparations
	mismatchedNamespaces := make(chan namespace)
	idsToCheck := make(chan idsWithLocation)  //channel to post ids to check
	idsToDelete := make(chan idsWithLocation) //channel to post ids to delete

	// 1. Get namespaces list from the Witness
	namespaces, err := getFQNamespaceListWitness(cc.flowCtx, cc.witnessMongoClient, options.Namespace)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get fully qualified namespace list from the witness: %v", err))
		return
	}
	// 2. Compare the doc count on both sides (asynchoronously so that we can proceed with the next steps here)
	go cc.compareDocCountWithWitness(cc.witnessMongoClient, namespaces, mismatchedNamespaces)

	// 3. For mismatches, use Witness index to find out what has been deleted (async so that we can proceed with the next steps here)
	go cc.parallelScanWitnessNamespaces(cc.witnessMongoClient, mismatchedNamespaces, idsToCheck)
	go cc.checkSourceIdsAndGenerateDeletes(idsToCheck, idsToDelete)

	// 4. Generate delete events
	cc.generateDeleteMessages(idsToDelete, flowDataChannel)
}

// Compares the document count between us and the Witness for given namespaces, and writes mismatches to the channel
// The channel is closed at the end to signal that all comparisons are done
func (cc *CosmosConnector) compareDocCountWithWitness(witnessClient *mongo.Client, namespaces []namespace, mismatchedNamespaces chan<- namespace) {
	for _, ns := range namespaces {
		// get the count from the source
		sourceCount, err := cc.client.Database(ns.db).Collection(ns.col).CountDocuments(cc.flowCtx, bson.M{})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get count from source for %v: %v", ns, err))
			continue
		}

		// get the count from the witness
		witnessCount, err := witnessClient.Database(ns.db).Collection(ns.col).CountDocuments(cc.flowCtx, bson.M{})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get count from witness for %v: %v", ns, err))
			continue
		}

		if sourceCount != witnessCount {
			slog.Debug(fmt.Sprintf("Mismatched namespace: %v, source count: %v, witness count: %v", ns, sourceCount, witnessCount))
			if witnessCount != 0 {
				mismatchedNamespaces <- ns
			} else {
				slog.Debug(fmt.Sprintf("Witness count is 0 for %v, skipping", ns)) //XXX: is this possible?
			}
		}
	}
	close(mismatchedNamespaces)
	slog.Debug("All comparisons done")
}

// Reads namespaces from the channel with 4 workers and scans the Witness index in parallel
// Sends the ids to the channel for checking against the source
// Exits when the channel is closed
func (cc *CosmosConnector) parallelScanWitnessNamespaces(witnessClient *mongo.Client, mismatchedNamespaces <-chan namespace, idsToCheck chan<- idsWithLocation) {
	//define workgroup
	wg := sync.WaitGroup{}
	wg.Add(numParallelWitnessScanTasks)
	// create workers to scan the witness
	for i := 0; i < numParallelWitnessScanTasks; i++ {
		go func() {
			for ns := range mismatchedNamespaces {
				cc.scanWitnessNamespace(witnessClient, ns, idsToCheck)
			}
			wg.Done()
		}()
	}

	// wait for all workers to finish and close the idsToCheck channel
	wg.Wait()
	close(idsToCheck)
}

// Scans the Witness index for the given namespace and sends the ids to the channel (in batches for efficiency)
func (cc *CosmosConnector) scanWitnessNamespace(witnessClient *mongo.Client, ns namespace, idsToCheck chan<- idsWithLocation) {
	slog.Debug(fmt.Sprintf("Scanning witness index namespace %v", ns))
	// get all ids from the source
	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cur, err := witnessClient.Database(ns.db).Collection(ns.col).Find(cc.flowCtx, bson.M{}, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get all ids from source for %v: %v", ns, err))
		return
	}
	defer cur.Close(cc.flowCtx)

	var ids []interface{}
	count := 0
	for cur.Next(cc.flowCtx) {
		var idDoc bson.M
		err := cur.Decode(&idDoc)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to decode id: %v", err))
			continue
		}
		// Check if the _id field exists in the result
		if _, ok := idDoc["_id"]; !ok {
			slog.Error("Missing _id field in query result")
			continue
		}
		ids = append(ids, idDoc["_id"])
		count++
		if count == idsBatchSize {
			idsToCheck <- idsWithLocation{loc: iface.Location{Database: ns.db, Collection: ns.col}, ids: ids}
			ids = nil
			count = 0
		}
	}

	if count > 0 {
		idsToCheck <- idsWithLocation{loc: iface.Location{Database: ns.db, Collection: ns.col}, ids: ids}
	}
}

// Reads ids to check from the channel, checks if they exist in the source, and sends those don't to the delete channel
// Uses 4 workers to parallelize the checks
// Exits when the channel is closed
func (cc *CosmosConnector) checkSourceIdsAndGenerateDeletes(idsToCheck <-chan idsWithLocation, idsToDelete chan<- idsWithLocation) {
	//define workgroup
	wg := sync.WaitGroup{}
	wg.Add(numParallelSourceScanTasks)
	// create workers to scan the source
	for i := 0; i < numParallelSourceScanTasks; i++ {
		go func() {
			for idsWithLoc := range idsToCheck {
				cc.checkSourceIdsAndGenerateDeletesWorker(idsWithLoc, idsToDelete)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(idsToDelete)
}

// Checks if the ids exist in the source and sends those that don't to the delete channel
func (cc *CosmosConnector) checkSourceIdsAndGenerateDeletesWorker(idsWithLoc idsWithLocation, idsToDelete chan<- idsWithLocation) {
	slog.Debug(fmt.Sprintf("Checking source for ids: %+v (first: %v, last: %v, n: %v)", idsWithLoc.loc, idsWithLoc.ids[0], idsWithLoc.ids[len(idsWithLoc.ids)-1], len(idsWithLoc.ids)))
	// we will take advatage of the $setDifference MongoDB aggregation operator to find the ids that are not in the source
	// it does work in Cosmos, which is great!
	// db.col.aggregate([ {$match:{_id:{$in: ID'S}}},{ "$group": { "_id": null, "ids": { "$addToSet": "$_id" } } }, { "$project": { "missingIds": { "$setDifference": [ID'S, "$ids"] }, "_id": 0 } }] )

	// create a pipeline to find the missing ids
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"_id", bson.D{{"$in", idsWithLoc.ids}}}}}},
		{{"$group", bson.D{{"_id", nil}, {"ids", bson.D{{"$addToSet", "$_id"}}}}}},
		{{"$project", bson.D{
			{"missingIds", bson.D{{"$setDifference", bson.A{idsWithLoc.ids, "$ids"}}}},
			{"_id", 0},
		}}},
	}

	cur, err := cc.client.Database(idsWithLoc.loc.Database).Collection(idsWithLoc.loc.Collection).Aggregate(cc.flowCtx, pipeline)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to aggregate: %v", err))
		return
	}
	defer cur.Close(cc.flowCtx)
	// extract the missing ids
	var res bson.M
	if cur.Next(cc.flowCtx) {
		err := cur.Decode(&res)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to decode missing ids: %v", err))
			return
		}
	} else {
		slog.Debug(fmt.Sprintf("Missing ids source query returned nothing for %v", idsWithLoc.loc))
		return
	}

	if res["missingIds"] == nil {
		slog.Debug(fmt.Sprintf("No missing ids found on the source for %v", idsWithLoc.loc))
		return
	}

	// convert result to array
	missingIds := []interface{}(res["missingIds"].(primitive.A))

	if len(missingIds) > 0 {
		slog.Debug(fmt.Sprintf("Missing ids for %v: %v", idsWithLoc.loc, missingIds))
		idsToDelete <- idsWithLocation{loc: idsWithLoc.loc, ids: missingIds}
	}
}

// Reads ids from one channel and sends delete event messages to the other
// Exits when the channel is closed
func (cc *CosmosConnector) generateDeleteMessages(idsToDelete <-chan idsWithLocation, dataChannel chan<- iface.DataMessage) {
	for idWithLoc := range idsToDelete {
		for i := 0; i < len(idWithLoc.ids); i++ {
			// convert id to raw bson
			idType, idVal, err := bson.MarshalValue(idWithLoc.ids[i])
			if err != nil {
				slog.Error(fmt.Sprintf("failed to marshal _id: %v", err))
			}
			dataChannel <- iface.DataMessage{Loc: idWithLoc.loc, Id: &idVal, IdType: byte(idType), MutationType: iface.MutationType_Delete}
		}
	}
}

/****************************************************
 * Helper functions
 ****************************************************/

// tuple to store location and a set of id
type idsWithLocation struct {
	loc iface.Location
	ids []interface{}
}

// get all database names except system databases
func getAllDatabasesWitness(ctx context.Context, witnessClient *mongo.Client) ([]string, error) {
	dbNames, err := witnessClient.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	dbs := slices.DeleteFunc(dbNames, func(d string) bool {
		return slices.Contains(ExcludedDBListForIC, d)
	})

	return dbs, nil
}

// get all collections in a database except system collections
func getAllCollectionsWitness(ctx context.Context, witnessClient *mongo.Client, dbName string) ([]string, error) {
	collectionsAll, err := witnessClient.Database(dbName).ListCollectionNames(ctx, bson.M{})
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

// Returns a list of fully qualified namespaces
func getFQNamespaceListWitness(ctx context.Context, witnessClient *mongo.Client, namespacesFilter []string) ([]namespace, error) {
	var dbsToResolve []string //database names that we need to resolve

	var namespaces []namespace

	if namespacesFilter == nil {
		var err error
		dbsToResolve, err = getAllDatabasesWitness(ctx, witnessClient)
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
		colls, err := getAllCollectionsWitness(ctx, witnessClient, db)
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
