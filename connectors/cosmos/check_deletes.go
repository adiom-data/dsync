/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

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

// Compares the document count between us and the Witness for given namespaces, and writes mismatches to the channel
// The channel is closed at the end to signal that all comparisons are done
func compareDocCountWithWitness(ctx context.Context, client *mongo.Client, witnessClient *mongo.Client, namespaces []iface.Namespace, mismatchedNamespaces chan<- iface.Namespace) {
	for _, ns := range namespaces {
		// get the count from the source
		sourceCount, err := client.Database(ns.Db).Collection(ns.Col).EstimatedDocumentCount(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get count from source for %v: %v", ns, err))
			continue
		}

		// get the count from the witness
		witnessCount, err := witnessClient.Database(ns.Db).Collection(ns.Col).EstimatedDocumentCount(ctx)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get count from witness for %v: %v", ns, err))
			continue
		}

		if sourceCount != witnessCount { //there's something we haven't deleted yet //XXX: is there a better heuristic here? This will not work for some workloads
			slog.Debug(fmt.Sprintf("Mismatched namespace: %v, source count: %v, witness count: %v", ns, sourceCount, witnessCount))
			mismatchedNamespaces <- ns
		}
	}
	close(mismatchedNamespaces)
	slog.Debug("All comparisons done")
}

// Reads namespaces from the channel with 4 workers and scans the Witness index in parallel
// Sends the ids to the channel for checking against the source
// Exits when the channel is closed
func parallelScanWitnessNamespaces(ctx context.Context, witnessClient *mongo.Client, mismatchedNamespaces <-chan iface.Namespace, idsToCheck chan<- idsWithLocation) {
	//define workgroup
	wg := sync.WaitGroup{}
	wg.Add(numParallelWitnessScanTasks)
	// create workers to scan the witness
	for i := 0; i < numParallelWitnessScanTasks; i++ {
		go func() {
			for ns := range mismatchedNamespaces {
				scanWitnessNamespace(ctx, witnessClient, ns, idsToCheck)
			}
			wg.Done()
		}()
	}

	// wait for all workers to finish and close the idsToCheck channel
	wg.Wait()
	close(idsToCheck)
}

// Scans the Witness index for the given namespace and sends the ids to the channel (in batches for efficiency)
func scanWitnessNamespace(ctx context.Context, witnessClient *mongo.Client, ns iface.Namespace, idsToCheck chan<- idsWithLocation) {
	slog.Debug(fmt.Sprintf("Scanning witness index namespace %v", ns))
	// get all ids from the source
	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cur, err := witnessClient.Database(ns.Db).Collection(ns.Col).Find(ctx, bson.M{}, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get all ids from source for %v: %v", ns, err))
		return
	}
	defer cur.Close(ctx)

	var ids []interface{}
	count := 0
	for cur.Next(ctx) {
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
			idsToCheck <- idsWithLocation{loc: iface.Location{Database: ns.Db, Collection: ns.Col}, ids: ids}
			ids = nil
			count = 0
		}
	}

	if count > 0 {
		idsToCheck <- idsWithLocation{loc: iface.Location{Database: ns.Db, Collection: ns.Col}, ids: ids}
	}
}

// Reads ids to check from the channel, checks if they exist in the source, and sends those don't to the delete channel
// Uses 4 workers to parallelize the checks
// Exits when the channel is closed
func checkSourceIdsAndGenerateDeletes(ctx context.Context, client *mongo.Client, idsToCheck <-chan idsWithLocation, idsToDelete chan<- idsWithLocation) {
	//define workgroup
	wg := sync.WaitGroup{}
	wg.Add(numParallelSourceScanTasks)
	// create workers to scan the source
	for i := 0; i < numParallelSourceScanTasks; i++ {
		go func() {
			for idsWithLoc := range idsToCheck {
				checkSourceIdsAndGenerateDeletesWorker(ctx, client, idsWithLoc, idsToDelete)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(idsToDelete)
}

// Checks if the ids exist in the source and sends those that don't to the delete channel
func checkSourceIdsAndGenerateDeletesWorker(ctx context.Context, client *mongo.Client, idsWithLoc idsWithLocation, idsToDelete chan<- idsWithLocation) {
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

	cur, err := client.Database(idsWithLoc.loc.Database).Collection(idsWithLoc.loc.Collection).Aggregate(ctx, pipeline)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to aggregate: %v", err))
		return
	}
	defer cur.Close(ctx)
	// extract the missing ids
	var missingIds []interface{}
	var res bson.M
	if cur.Next(ctx) {
		err := cur.Decode(&res)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to decode missing ids: %v", err))
			return
		}

		if res["missingIds"] == nil {
			slog.Debug(fmt.Sprintf("No missing ids found on the source for %v", idsWithLoc.loc))
			return
		}

		// convert result to array
		missingIds = []interface{}(res["missingIds"].(primitive.A))
	} else {
		slog.Debug(fmt.Sprintf("Missing ids source query returned nothing for %v", idsWithLoc.loc))
		//this means all are missing on the source!
		missingIds = idsWithLoc.ids
	}

	if len(missingIds) > 0 {
		slog.Debug(fmt.Sprintf("Found %v missing ids for %v", len(missingIds), idsWithLoc.loc))
		idsToDelete <- idsWithLocation{loc: idsWithLoc.loc, ids: missingIds}
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
func getFQNamespaceListWitness(ctx context.Context, witnessClient *mongo.Client, namespacesFilter []string) ([]iface.Namespace, error) {
	var dbsToResolve []string //database names that we need to resolve

	var namespaces []iface.Namespace

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
				namespaces = append(namespaces, iface.Namespace{Db: db, Col: col})
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
			namespaces = append(namespaces, iface.Namespace{Db: db, Col: coll})
		}
	}

	return namespaces, nil
}
