/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorRandom

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/brianvoe/gofakeit/v7"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	progressReportingIntervalSec = 10
	batch_size                   = 100 // used for change stream batch inserts
)

type ReaderProgress struct {
	initialSyncDocs    atomic.Uint64
	changeStreamEvents uint64
	tasksTotal         uint64
	tasksCompleted     atomic.Uint64
}

/*
	 	function processes a data generation task for the initial generation.
		It generates random documents for the specified collection in the task and sends them as single data messages to the channel
*/
func (rc *RandomReadConnector) ProcessDataGenerationTask(task iface.ReadPlanTask, channel chan iface.DataMessage, readerProgress *ReaderProgress) {
	loc := iface.Location{Database: task.Def.Db, Collection: task.Def.Col}
	//task is to generate the specified number of documents in rc.settings for the given collection
	for i := 0; i < rc.settings.numInitialDocumentsPerCollection; i++ {
		//generate random document and send as single insert data message
		doc := rc.generateRandomDocument()
		dataMsg, err := rc.SingleInsertDataMessage(loc, doc)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to generate data message: %v", err))
			continue
		}
		channel <- dataMsg
		readerProgress.initialSyncDocs.Add(1)
	}
	readerProgress.tasksCompleted.Add(1) //Atomic add here for thread safety
	slog.Debug(fmt.Sprintf("Done processing task: %v", task))
}

/*
	 	function processes a data generation task for the initial data generation.
		It generates random documents for the specified collection in the task and sends them in a batch data messages to the channel
*/
func (rc *RandomReadConnector) ProcessDataGenerationTaskBatch(task iface.ReadPlanTask, channel chan iface.DataMessage, readerProgress *ReaderProgress) {
	loc := iface.Location{Database: task.Def.Db, Collection: task.Def.Col}
	var docs []map[string]interface{}
	//create slice of random documents
	for i := 0; i < rc.settings.numInitialDocumentsPerCollection; i++ { //Batch size is just number of docs per collection here
		doc := rc.generateRandomDocument()
		docs = append(docs, doc)
		readerProgress.initialSyncDocs.Add(1)
	}
	//generate batch insert data message with the slice of docs
	dataMsg, err := rc.BatchInsertDataMessage(loc, docs)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to generate data message: %v", err))
	}
	channel <- dataMsg                   //Send the batch insert data message to channel
	readerProgress.tasksCompleted.Add(1) //Atomic add for thread safety
	slog.Debug(fmt.Sprintf("Done processing task: %v", task))
}

/*
	 	function simplifies initial data generation by organizing into tasks and distributing them to multiple threads
		a single task is generated for each collection in each database, number of tasks = numDatabases * numCollectionsPerDatabase
		returns slice of DataCopyTasks
*/
func (rc *RandomReadConnector) CreateInitialGenerationTasks() []iface.ReadPlanTask {
	var tasks []iface.ReadPlanTask

	for i := 1; i <= rc.settings.numDatabases; i++ {
		for j := 1; j <= rc.settings.numCollectionsPerDatabase; j++ {
			task := iface.ReadPlanTask{}
			task.Def.Db = fmt.Sprintf("db%d", i)
			task.Def.Col = fmt.Sprintf("col%d", j)

			tasks = append(tasks, task)
		}
	}
	return tasks
}

/*
	 	function generates random document with the specified number of fields in rc.settings and values consisting of random strings of length docSize using gofakeit
		XXX: Possible improvement: allow user to specify field names and types in settings for more customization, maybe through a template
*/
func (rc *RandomReadConnector) generateRandomDocument() map[string]interface{} {
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
func (rc *RandomReadConnector) SingleInsertDataMessage(loc iface.Location, doc map[string]interface{}) (iface.DataMessage, error) {
	//checks if the location key exists in the map, if not creates a new IndexMap for the given location to store ids
	docMap, exists := rc.docMap[loc]
	if !exists {
		rc.docMap[loc] = NewIndexMap()
		docMap = rc.docMap[loc]
	}
	id := docMap.AddRandomID() //generate random id and add to IndexMap
	doc["_id"] = id            //add id field to document
	data, err := bson.Marshal(doc)
	if err != nil {
		return iface.DataMessage{}, fmt.Errorf("failed to marshal map to bson: %v", err)
	}
	return iface.DataMessage{
		Loc:          loc,
		Data:         &data,
		MutationType: iface.MutationType_Insert,
	}, nil
}

/*
function generates a batch insert data message with a slice of random documents for the specified location
generates an id for each document and updates the given location's IndexMap with the ids
sends all of the documents in a single batch insert data message to optimize performance
*/
func (rc *RandomReadConnector) BatchInsertDataMessage(loc iface.Location, docs []map[string]interface{}) (iface.DataMessage, error) {
	var dataBatch [][]byte
	var err error
	dataBatch = make([][]byte, len(docs)) //create slice of byte slices to store marshaled documents for data message

	//Check if index map exists for the location, if not create a new one
	//XXX: Should we make this atomic? With current implementation only one thread is writing to a given Index Map for a particular location, so no current concurrency issues
	docMap, exists := rc.docMap[loc]
	if !exists {
		rc.docMap[loc] = NewIndexMap()
		docMap = rc.docMap[loc]
	}

	//prepare the dataBatch with marshaled documents
	for i, doc := range docs {
		id := docMap.AddRandomID()            //generate random id for each document and add to IndexMap,
		doc["_id"] = id                       //add id field to each document in the slice
		dataBatch[i], err = bson.Marshal(doc) //marshal each document to bson and add to the dataBatch
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal map to bson: %v", err)
		}
	}
	return iface.DataMessage{
		Loc:          loc,
		DataBatch:    dataBatch,
		MutationType: iface.MutationType_InsertBatch,
	}, nil
}

/* Generates a random change stream event based on the given operation, updates the ReaderProgress and lsn
 */
func (rc *RandomReadConnector) generateChangeStreamEvent(operation string, readerProgress *ReaderProgress, lsn *int64) (iface.DataMessage, error) {
	//generate random location
	db := gofakeit.Number(1, rc.settings.numDatabases)
	col := gofakeit.Number(1, rc.settings.numCollectionsPerDatabase)
	loc := iface.Location{Database: fmt.Sprintf("db%d", db), Collection: fmt.Sprintf("col%d", col)}

	switch operation { //switch between single inserts, batch inserts, updates, and deletes, depends on the operation generated

	//insert case generates a single insert data message with a random document
	case "insert":
		docMap := rc.docMap[loc]                                     //get the IndexMap for the generated collection
		if docMap.GetNumDocs() >= rc.settings.maxDocsPerCollection { //if the collection is full, no op and return
			slog.Debug(fmt.Sprintf("%d docs in collection, cannot insert more, lsn %d", docMap.GetNumDocs(), *lsn))
			return iface.DataMessage{}, nil
		}
		doc := rc.generateRandomDocument()
		rc.incrementProgress(readerProgress, lsn)
		slog.Debug(fmt.Sprintf("Generated insert change stream event in collection %v.%v", loc.Database, loc.Collection))
		return rc.SingleInsertDataMessage(loc, doc)

	//insertBatch case generates a batch insert data message with a slice of random documents
	case "insertBatch":
		docMap := rc.docMap[loc]                                               //get the IndexMap for the generated collection
		if docMap.GetNumDocs() > rc.settings.maxDocsPerCollection-batch_size { //if the collection is almost full and cannot fit a batch, no op and return
			slog.Debug(fmt.Sprintf("%d docs in collection, cannot insert %d docs max reached, lsn %d", docMap.GetNumDocs(), batch_size, *lsn))
			return iface.DataMessage{}, nil
		}
		var docs []map[string]interface{}
		for i := 0; i < batch_size; i++ { //generate a slice of random documents
			docs = append(docs, rc.generateRandomDocument())
			rc.incrementProgress(readerProgress, lsn)
		}
		slog.Debug(fmt.Sprintf("Generated batch insert change stream event (%d inserts) in collection %v.%v", batch_size, loc.Database, loc.Collection))
		return rc.BatchInsertDataMessage(loc, docs)

	//update case generates an update data message with a random document by generating a random id from the IndexMap
	case "update":
		docMap := rc.docMap[loc]
		id := docMap.GetRandomKey() //get random id from the IndexMap to update
		doc := rc.generateRandomDocument()
		doc["_id"] = id //generate new document for the given id and return update data message
		data, _ := bson.Marshal(doc)
		idType, idVal, _ := bson.MarshalValue(id)
		rc.incrementProgress(readerProgress, lsn)
		slog.Debug(fmt.Sprintf("Generated update change stream event with id %d in collection %v.%v", id, loc.Database, loc.Collection))
		return iface.DataMessage{Loc: loc, Data: &data, Id: &idVal, IdType: byte(idType), MutationType: iface.MutationType_Update}, nil

	//delete case generates a delete data message with a random id from the IndexMap
	case "delete":
		docMap := rc.docMap[loc]
		id := docMap.DeleteRandomKey()
		idType, idVal, _ := bson.MarshalValue(id)
		rc.incrementProgress(readerProgress, lsn)
		slog.Debug(fmt.Sprintf("Generated delete change stream event with id %d in collection %v.%v", id, loc.Database, loc.Collection))
		return iface.DataMessage{Loc: loc, Id: &idVal, IdType: byte(idType), MutationType: iface.MutationType_Delete}, nil
	}
	return iface.DataMessage{}, fmt.Errorf("failed to generate change stream event, lsn %d", *lsn)
}

/*
Randomly generates a database operation [insert, insertBatch, update, delete] based on probabilities given in the settings
Errors if given probabilities do not sum to 1.0
*/
func (rc *RandomReadConnector) generateOperation() (string, error) {
	//generate random operation based on probabilities
	options := []string{"insert", "insertBatch", "update", "delete"}
	//check if probabilities sum to 1.0, if not error
	totalProb := 0.0
	for _, prob := range rc.settings.probabilities {
		totalProb += prob
	}
	if totalProb != 1.0 {
		fmt.Println("Error: Probabilities must sum to 1.0")
		return "", fmt.Errorf("probabilities must sum to 1.0, sum is %f", totalProb)
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
	return "", fmt.Errorf("failed to generate operation")
}

// increment progress counters for change stream events and lsn, cleans up the change stream generation code
func (rc *RandomReadConnector) incrementProgress(reader *ReaderProgress, lsn *int64) {
	reader.changeStreamEvents++
	(*lsn)++
	rc.status.WriteLSN++
}
