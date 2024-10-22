/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
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
