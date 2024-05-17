package connector

import (
	"log/slog"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

var (
	// System databases that we don't want to copy
	ExcludedDBList = []string{"local", "config", "admin", dummyDB}
)

type DataCopyTask struct {
	Namespace string
}

func (mc *MongoConnector) createInitialCopyTasks(namespaces []string) ([]DataCopyTask, error) {
	var dbsToResolve []string //database names that we need to resolve
	var fqns []string         //fully qualified namespaces ('db.collection')

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
			if strings.Contains(ns, ".") {
				fqns = append(fqns, ns)
			} else {
				dbsToResolve = append(dbsToResolve, ns)
			}
		}
	}

	slog.Debug("Databases to resolve: ", dbsToResolve)
	slog.Debug("Fully qualified namespaces: ", fqns)

	return nil, nil
}

func (mc *MongoConnector) getAllDatabases() ([]string, error) {
	var response bson.Raw
	request := bson.D{{"listDatabases", 1}}

	err := mc.client.Database("admin").RunCommand(mc.ctx, request).Decode(&response)
	if err != nil {
		return nil, err
	}

	dbsRaw, lookupErr := response.LookupErr("databases")
	if lookupErr != nil {
		return nil, lookupErr
	}

	// we need this one to deserialize the response from the listDatabases command
	type dbSpec struct {
		Name       string
		SizeOnDisk int64
		Empty      bool
	}
	var dbSpecs []*dbSpec
	if err := dbsRaw.Unmarshal(&dbSpecs); err != nil {
		return nil, err
	}

	var dbs []string
	for _, dbSpec := range dbSpecs {
		// Do not copy the excluded system databases
		if !slices.Contains(ExcludedDBList, dbSpec.Name) {
			dbs = append(dbs, dbSpec.Name)
		}
	}

	return dbs, nil
}
