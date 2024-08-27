/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	// System databases that we don't want to copy
	ExcludedDBListForIC = []string{"local", "config", "admin", "adiom-internal", dummyDB}
	// System collections that we don't want to copy (regex pattern)
	ExcludedSystemCollPattern = "^system[.]"

	//XXX (AK, 6/2024): these need to use negative lookahead to make change stream work with shared tier Mongo (although it seems they rewrite the whole thing in a funny way that doesn't work)
	//XXX (AK, 6/2024): dummyDB business is not excluded as a hack to get the resume token from the change stream
	ExcludedDBPatternCS         = `^(?!local$|config$|admin$|adiom-internal$)`
	ExcludedSystemCollPatternCS = `^(?!system.)`
)

func (mc *MongoConnector) createInitialCopyTasks(namespaces []string) ([]iface.ReadPlanTask, error) {
	var dbsToResolve []string //database names that we need to resolve

	var tasks []iface.ReadPlanTask
	taskId := iface.ReadPlanTaskID(1)

	if namespaces == nil {
		var err error
		dbsToResolve, err = mc.getAllDatabases()
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
		colls, err := mc.getAllCollections(db)
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

	return tasks, nil
}

// get all database names except system databases
func (mc *MongoConnector) getAllDatabases() ([]string, error) {
	dbNames, err := mc.client.ListDatabaseNames(mc.ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	dbs := slices.DeleteFunc(dbNames, func(d string) bool {
		return slices.Contains(ExcludedDBListForIC, d)
	})

	return dbs, nil
}

// get all collections in a database except system collections
func (mc *MongoConnector) getAllCollections(dbName string) ([]string, error) {
	collectionsAll, err := mc.client.Database(dbName).ListCollectionNames(mc.ctx, bson.M{})
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

// creates a filter for the change stream to include only the specified namespaces
func createChangeStreamNamespaceFilterFromTasks(tasks []iface.ReadPlanTask) bson.D {
	var filters []bson.D
	for _, task := range tasks {
		filters = append(filters, bson.D{{"ns.db", task.Def.Db}, {"ns.coll", task.Def.Col}})
	}
	// add dummyDB and dummyCol to the filter so that we can track the changes in the dummy collection to get the cluster time (otherwise we can't use the resume token)
	filters = append(filters, bson.D{{"ns.db", dummyDB}, {"ns.coll", dummyCol}})
	return bson.D{{"$or", filters}}
}

// create a change stream filter that covers all namespaces except system
func createChangeStreamNamespaceFilter() bson.D {
	return bson.D{{"$and", []bson.D{
		{{"ns.db", bson.D{{"$regex", primitive.Regex{Pattern: ExcludedDBPatternCS}}}}},
		{{"ns.coll", bson.D{{"$regex", primitive.Regex{Pattern: ExcludedSystemCollPatternCS}}}}},
	}}}
}
