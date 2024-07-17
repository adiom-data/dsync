package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/mitchellh/hashstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	dummyDB  string = "adiom-internal-dummy" //note that this must be different from the metadata DB - that one is excluded from copying, while this one isn't
	dummyCol string = "dummy"
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

// Generates static connector ID based on connection string
// XXX: is this the best place to do this? - move to overall connector util file
func GenerateConnectorID(connectionString string) iface.ConnectorID {
	id, err := hashstructure.Hash(connectionString, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.ConnectorID(strconv.FormatUint(id, 16))
}

func insertDummyRecord(ctx context.Context, client *mongo.Client) error {
	//set id to a string client address (to make it random)
	id := fmt.Sprintf("%v", client)
	col := client.Database(dummyDB).Collection(dummyCol)
	_, err := col.InsertOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("failed to insert dummy record: %v", err)
	}
	//delete the record
	_, err = col.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("failed to delete dummy record: %v", err)
	}

	return nil
}

func GetLatestResumeToken(ctx context.Context, client *mongo.Client) (bson.Raw, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}

	slog.Debug("Getting latest resume token...")
	//will need to initialize dummy collection
	db := client.Database(dummyDB)
	err := db.CreateCollection(ctx, dummyCol)
	if err != nil {
		slog.Error("failed to create collection: %v", err)
	}

	changeStream, err := createDummyChangeStream(client, ctx) //TODO (AK, 6/2024): We should limit this to just the dummy collection or we can catch something that we don't want :)
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	// we need ANY event to get the resume token that we can use to extract the cluster time
	go func() {
		if err := insertDummyRecord(ctx, client); err != nil {
			slog.Error(fmt.Sprintf("Error inserting dummy record: %v", err.Error()))
		}
	}()

	changeStream.Next(ctx)
	resumeToken := changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}

	return resumeToken, nil
}

func createDummyChangeStream(client *mongo.Client, ctx context.Context) (*mongo.ChangeStream, error) {
	db := dummyDB
	col := dummyCol
	collection := client.Database(db).Collection(col)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace"}}}}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}, {Key: "fullDocument", Value: 1}, {Key: "ns", Value: 1}, {Key: "documentKey", Value: 1}}}},
	}

	opts := options.ChangeStream().SetFullDocument("updateLookup")
	slog.Info(fmt.Sprintf("pipeline: %v, opts: %v", pipeline, opts))
	changeStream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("%v", err))
	}
	slog.Info("Opened change stream for %v\n", collection)
	return changeStream, nil
}

func CreateInitialCopyTasks(namespaces []string, client *mongo.Client, ctx context.Context) ([]iface.ReadPlanTask, error) {
	var dbsToResolve []string //database names that we need to resolve

	var tasks []iface.ReadPlanTask
	taskId := iface.ReadPlanTaskID(1)

	if namespaces == nil {
		var err error
		dbsToResolve, err = getAllDatabases(client, ctx)
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
		colls, err := getAllCollections(db, client, ctx)
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
func getAllDatabases(client *mongo.Client, ctx context.Context) ([]string, error) {
	dbNames, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	dbs := slices.DeleteFunc(dbNames, func(d string) bool {
		return slices.Contains(ExcludedDBListForIC, d)
	})

	return dbs, nil
}

// get all collections in a database except system collections
func getAllCollections(dbName string, client *mongo.Client, ctx context.Context) ([]string, error) {
	collectionsAll, err := client.Database(dbName).ListCollectionNames(ctx, bson.M{})
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
func CreateChangeStreamNamespaceFilterFromTasks(tasks []iface.ReadPlanTask) bson.D {
	var filters []bson.D
	for _, task := range tasks {
		filters = append(filters, bson.D{{"ns.db", task.Def.Db}, {"ns.coll", task.Def.Col}})
	}
	// add dummyDB and dummyCol to the filter so that we can track the changes in the dummy collection to get the cluster time (otherwise we can't use the resume token)
	filters = append(filters, bson.D{{"ns.db", dummyDB}, {"ns.coll", dummyCol}})
	return bson.D{{"$or", filters}}
}

// create a change stream filter that covers all namespaces except system
func CreateChangeStreamNamespaceFilter() bson.D {
	return bson.D{{"$and", []bson.D{
		{{"ns.db", bson.D{{"$regex", primitive.Regex{Pattern: ExcludedDBPatternCS}}}}},
		{{"ns.coll", bson.D{{"$regex", primitive.Regex{Pattern: ExcludedSystemCollPatternCS}}}}},
	}}}
}

func ShouldIgnoreChangeStreamEvent(change bson.M) bool {
	db := change["ns"].(bson.M)["db"].(string)
	col := change["ns"].(bson.M)["coll"].(string)

	//We need to filter out the dummy collection
	//TODO (AK, 6/2024): is it the best way to do it?
	if (db == dummyDB) && (col == dummyCol) {
		return true
	}

	return false
}

func ConvertChangeStreamEventToDataMessage(change bson.M) (iface.DataMessage, error) {
	slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

	db := change["ns"].(bson.M)["db"].(string)
	col := change["ns"].(bson.M)["coll"].(string)
	optype := change["operationType"].(string)

	loc := iface.Location{Database: db, Collection: col}
	var dataMsg iface.DataMessage

	switch optype {
	case "insert":
		fullDocument := change["fullDocument"].(bson.M)
		// convert fulldocument to BSON.Raw
		fullDocumentRaw, err := bson.Marshal(fullDocument)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal full document: %v", err)
		}
		dataMsg = iface.DataMessage{Loc: loc, Data: &fullDocumentRaw, MutationType: iface.MutationType_Insert}
	case "update":
		// get the id of the document that was changed
		id := change["documentKey"].(bson.M)["_id"]
		// convert id to raw bson
		idType, idVal, err := bson.MarshalValue(id)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal _id: %v", err)
		}
		// get the full state of the document after the change
		if change["fullDocument"] == nil {
			//TODO (AK, 6/2024): find a better way to report that we need to ignore this event
			return iface.DataMessage{MutationType: iface.MutationType_Reserved}, nil // no full document, nothing to do (probably got deleted before we got to the event in the change stream)
		}
		fullDocument := change["fullDocument"].(bson.M)
		// convert fulldocument to BSON.Raw
		fullDocumentRaw, err := bson.Marshal(fullDocument)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal full document: %v", err)
		}
		dataMsg = iface.DataMessage{Loc: loc, Id: &idVal, IdType: byte(idType), Data: &fullDocumentRaw, MutationType: iface.MutationType_Update}
	case "delete":
		// get the id of the document that was deleted
		id := change["documentKey"].(bson.M)["_id"]
		// convert id to raw bson
		idType, idVal, err := bson.MarshalValue(id)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal _id: %v", err)
		}
		dataMsg = iface.DataMessage{Loc: loc, Id: &idVal, IdType: byte(idType), MutationType: iface.MutationType_Delete}
	default:
		return iface.DataMessage{}, fmt.Errorf("unsupported change event operation type: %v", optype)
	}

	return dataMsg, nil
}
