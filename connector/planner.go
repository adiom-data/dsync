package connector

import (
	"log/slog"
	"regexp"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

var (
	// System databases that we don't want to copy
	ExcludedDBList = []string{"local", "config", "admin", dummyDB}
	// System collections that we don't want to copy (regex pattern)
	ExcludedSystemCollPattern = "^system[.]"
)

type DataCopyTask struct {
	Db  string
	Col string
}

func (mc *MongoConnector) createInitialCopyTasks(namespaces []string) ([]DataCopyTask, error) {
	var dbsToResolve []string //database names that we need to resolve

	var tasks []DataCopyTask

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
				tasks = append(tasks, DataCopyTask{Db: db, Col: col})
			} else {
				dbsToResolve = append(dbsToResolve, ns)
			}
		}
	}

	slog.Debug("Databases to resolve: ", dbsToResolve)

	//iterate over unresolved databases and get all collections
	for _, db := range dbsToResolve {
		colls, err := mc.getAllCollections(db)
		if err != nil {
			return nil, err
		}
		//create tasks for these
		for _, coll := range colls {
			tasks = append(tasks, DataCopyTask{Db: db, Col: coll})
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
		return slices.Contains(ExcludedDBList, d)
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
func createChangeStreamNamespaceFilterFromTasks(tasks []DataCopyTask) bson.D {
	var filters []bson.D
	for _, task := range tasks {
		filters = append(filters, bson.D{{"ns.db", task.Db}, {"ns.coll", task.Col}})
	}
	// add dummyDB and dummyCol to the filter so that we can track the changes in the dummy collection to get the cluster time (otherwise we can't use the resume token)
	//TODO: reevaluate the approach
	filters = append(filters, bson.D{{"ns.db", dummyDB}, {"ns.coll", dummyCol}})
	return bson.D{{"$or", filters}}
}
