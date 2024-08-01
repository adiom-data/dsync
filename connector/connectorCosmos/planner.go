/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorCosmos

import (
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	MaxNumTasks = 8
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

	if len(tasks) > MaxNumTasks {
		return nil, fmt.Errorf("too many tasks to copy: %d, max %d", len(tasks), MaxNumTasks)
	}
	return tasks, nil
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
