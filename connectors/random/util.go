/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package random

import (
	"fmt"
	"log/slog"
	"math/rand"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/brianvoe/gofakeit/v7"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	batch_size = 100 // used for change stream batch inserts
)

// checks if the location key exists in the map, if not creates a new IndexMap for the given location to store ids
func (rc *conn) getOrCreateLocIndexMap(loc iface.Location) *IndexMap {
	rc.docMapMutex.Lock()
	defer rc.docMapMutex.Unlock()

	docMap, exists := rc.docMap[loc]
	if !exists {
		rc.docMap[loc] = NewIndexMap()
		docMap = rc.docMap[loc]
	}
	return docMap
}

/*
	 	function processes a data generation task for the initial data generation.
		It generates random documents for the specified collection in the task and sends them in a batch data messages to the channel
*/
func (rc *conn) ProcessDataGenerationTaskBatch(partition *adiomv1.Partition) (*adiomv1.ListDataResponse, error) {
	loc := iface.Location{Database: partition.GetNamespace().GetDb(), Collection: partition.GetNamespace().GetCol()}
	var docs []map[string]interface{}
	//create slice of random documents
	for i := 0; i < rc.settings.numInitialDocumentsPerCollection; i++ { //Batch size is just number of docs per collection here
		doc := rc.generateRandomDocument()
		docs = append(docs, doc)
	}
	//generate batch insert data message with the slice of docs
	dataBatch, err := rc.BatchInsertDataMessage(loc, docs)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to generate data message: %v", err))
		return nil, err
	}
	return &adiomv1.ListDataResponse{
		Data: dataBatch,
		Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}, nil
}

/*
	 	function simplifies initial data generation by organizing into tasks and distributing them to multiple threads
		a single task is generated for each collection in each database, number of tasks = numDatabases * numCollectionsPerDatabase
		returns slice of DataCopyTasks
*/
func (rc *conn) CreateInitialGenerationTasks() []*adiomv1.Partition {
	var tasks []*adiomv1.Partition

	for i := 1; i <= rc.settings.numDatabases; i++ {
		for j := 1; j <= rc.settings.numCollectionsPerDatabase; j++ {
			task := &adiomv1.Partition{
				Namespace: &adiomv1.Namespace{
					Db:  fmt.Sprintf("db%d", i),
					Col: fmt.Sprintf("col%d", j),
				},
				EstimatedCount: uint64(rc.settings.maxDocsPerCollection),
			}
			tasks = append(tasks, task)
		}
	}
	return tasks
}

/*
	 	function generates random document with the specified number of fields in rc.settings and values consisting of random strings of length docSize using gofakeit
		XXX: Possible improvement: allow user to specify field names and types in settings for more customization, maybe through a template
*/
func (rc *conn) generateRandomDocument() map[string]interface{} {
	doc := make(map[string]interface{})
	for i := 1; i <= rc.settings.numFields; i++ {
		doc[fmt.Sprintf("field%d", i)] = gofakeit.LetterN(rc.settings.docSize)
	}
	return doc
}

/*
	 	function generates a single insert data message with a random document for the specified location
		generates an id for the document and updates the given location's IndexMap with the id
*/
func (rc *conn) SingleInsertDataMessage(loc iface.Location, doc map[string]interface{}) (*adiomv1.StreamUpdatesResponse, error) {
	//checks if the location key exists in the map, if not creates a new IndexMap for the given location to store ids
	docMap := rc.getOrCreateLocIndexMap(loc)
	id := docMap.AddRandomID() //generate random id and add to IndexMap
	doc["_id"] = id            //add id field to document
	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal map to bson: %v", err)
	}
	idType, idVal, _ := bson.MarshalValue(id)
	return &adiomv1.StreamUpdatesResponse{
		Updates: []*adiomv1.Update{{
			Id: &adiomv1.BsonValue{
				Data: idVal,
				Type: uint32(idType),
			},
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: data,
		}},
		Namespace:  &adiomv1.Namespace{Db: loc.Database, Col: loc.Collection},
		Type:       adiomv1.DataType_DATA_TYPE_MONGO_BSON,
		NextCursor: []byte{},
	}, nil
}

/*
function generates a batch insert data message with a slice of random documents for the specified location
generates an id for each document and updates the given location's IndexMap with the ids
sends all of the documents in a single batch insert data message to optimize performance
*/
func (rc *conn) BatchInsertDataMessage(loc iface.Location, docs []map[string]interface{}) ([][]byte, error) {
	var dataBatch [][]byte
	var err error
	dataBatch = make([][]byte, len(docs)) //create slice of byte slices to store marshaled documents for data message

	docMap := rc.getOrCreateLocIndexMap(loc)

	//prepare the dataBatch with marshaled documents
	for i, doc := range docs {
		id := docMap.AddRandomID()            //generate random id for each document and add to IndexMap,
		doc["_id"] = id                       //add id field to each document in the slice
		dataBatch[i], err = bson.Marshal(doc) //marshal each document to bson and add to the dataBatch
		if err != nil {
			return nil, fmt.Errorf("failed to marshal map to bson: %v", err)
		}
	}
	return dataBatch, nil
}

/* Generates a random change stream event based on the given operation
 */
func (rc *conn) generateChangeStreamEvent(operation adiomv1.UpdateType) (*adiomv1.StreamUpdatesResponse, error) {
	//generate random location
	db := gofakeit.Number(1, rc.settings.numDatabases)
	col := gofakeit.Number(1, rc.settings.numCollectionsPerDatabase)
	loc := iface.Location{Database: fmt.Sprintf("db%d", db), Collection: fmt.Sprintf("col%d", col)}

	docMap := rc.getOrCreateLocIndexMap(loc)
	if docMap.GetNumDocs() == 0 && (operation == adiomv1.UpdateType_UPDATE_TYPE_DELETE || operation == adiomv1.UpdateType_UPDATE_TYPE_UPDATE) {
		//do an insert instead
		operation = adiomv1.UpdateType_UPDATE_TYPE_INSERT
	}

	switch operation { //switch between single inserts, batch inserts, updates, and deletes, depends on the operation generated

	//insert case generates a single insert data message with a random document
	case adiomv1.UpdateType_UPDATE_TYPE_INSERT:
		if docMap.GetNumDocs() >= rc.settings.maxDocsPerCollection { //if the collection is full, no op and return
			slog.Debug(fmt.Sprintf("%d docs in collection, cannot insert more", docMap.GetNumDocs()))
			return nil, nil
		}
		doc := rc.generateRandomDocument()
		slog.Debug(fmt.Sprintf("Generated insert change stream event in collection %v.%v", loc.Database, loc.Collection))
		return rc.SingleInsertDataMessage(loc, doc)

	//update case generates an update data message with a random document by generating a random id from the IndexMap
	case adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
		id := docMap.GetRandomKey() //get random id from the IndexMap to update
		doc := rc.generateRandomDocument()
		doc["_id"] = id //generate new document for the given id and return update data message
		data, _ := bson.Marshal(doc)
		idType, idVal, _ := bson.MarshalValue(id)
		slog.Debug(fmt.Sprintf("Generated update change stream event with id %d in collection %v.%v", id, loc.Database, loc.Collection))
		return &adiomv1.StreamUpdatesResponse{
			Updates: []*adiomv1.Update{{
				Id: &adiomv1.BsonValue{
					Data: idVal,
					Type: uint32(idType),
				},
				Type: operation,
				Data: data,
			}},
			Namespace: &adiomv1.Namespace{
				Db:  loc.Database,
				Col: loc.Collection,
			},
			Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
		}, nil

	//delete case generates a delete data message with a random id from the IndexMap
	case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
		id := docMap.DeleteRandomKey()
		idType, idVal, _ := bson.MarshalValue(id)
		slog.Debug(fmt.Sprintf("Generated delete change stream event with id %d in collection %v.%v", id, loc.Database, loc.Collection))
		return &adiomv1.StreamUpdatesResponse{
			Updates: []*adiomv1.Update{{
				Id: &adiomv1.BsonValue{
					Data: idVal,
					Type: uint32(idType),
				},
				Type: operation,
			}},
			Namespace: &adiomv1.Namespace{
				Db:  loc.Database,
				Col: loc.Collection,
			},
			Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
		}, nil
	}
	return nil, fmt.Errorf("failed to generate change stream event")
}

/*
Randomly generates a database operation [insert, update, delete] based on probabilities given in the settings
Errors if given probabilities do not sum to 1.0
*/
func (rc *conn) generateOperation() (adiomv1.UpdateType, error) {
	//generate random operation based on probabilities
	options := []adiomv1.UpdateType{adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE, adiomv1.UpdateType_UPDATE_TYPE_DELETE}
	//check if probabilities sum to 1.0, if not error
	totalProb := 0.0
	for _, prob := range rc.settings.probabilities {
		totalProb += prob
	}
	if totalProb != 1.0 {
		fmt.Println("Error: Probabilities must sum to 1.0")
		return 0, fmt.Errorf("probabilities must sum to 1.0, sum is %f", totalProb)
	}
	//generate random float and choose operation based on probabilities
	r := rand.Float64()
	sum := 0.0
	for i, prob := range rc.settings.probabilities {
		sum += prob
		if r < sum {
			return options[i], nil
		}
	}
	return 0, fmt.Errorf("failed to generate operation")
}
