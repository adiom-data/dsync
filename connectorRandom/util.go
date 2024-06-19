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

type DataCopyTask struct {
	Db  string
	Col string
}

type ReaderProgress struct {
	initialSyncDocs    atomic.Uint64
	changeStreamEvents uint64
	tasksTotal         uint64
	tasksCompleted     atomic.Uint64
}

func (rc *NullReadConnector) ProcessDataGenerationTask(task DataCopyTask, channel chan iface.DataMessage, readerProgress *ReaderProgress) {
	loc := iface.Location{Database: task.Db, Collection: task.Col}
	for i := 0; i < rc.settings.numInitialDocumentsPerCollection; i++ {
		doc := rc.generateRandomDocument()
		dataMsg, err := rc.SingleInsertDataMessage(loc, doc)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to generate data message: %v", err))
			continue
		}
		channel <- dataMsg
		readerProgress.initialSyncDocs.Add(1)
	}
	readerProgress.tasksCompleted.Add(1) //XXX Should we do atomic add here as well, shared variable multiple threads
	slog.Debug(fmt.Sprintf("Done processing task: %v", task))
}

func (rc *NullReadConnector) ProcessDataGenerationTaskBatch(task DataCopyTask, channel chan iface.DataMessage, readerProgress *ReaderProgress) {
	loc := iface.Location{Database: task.Db, Collection: task.Col}
	var docs []map[string]interface{}
	//create array of random documents
	for i := 0; i < rc.settings.numInitialDocumentsPerCollection; i++ { //Batch size is just number of docs per collection here
		doc := rc.generateRandomDocument()
		docs = append(docs, doc)
		readerProgress.initialSyncDocs.Add(1)
	}
	//generate data message batch with array of docs
	dataMsg, err := rc.BatchInsertDataMessage(loc, docs)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to generate data message: %v", err))
	}
	channel <- dataMsg
	readerProgress.tasksCompleted.Add(1) //XXX Should we do atomic add here as well, shared variable multiple threads
	slog.Debug(fmt.Sprintf("Done processing task: %v", task))
}

func (rc *NullReadConnector) CreateInitialGenerationTasks() []DataCopyTask {
	var tasks []DataCopyTask

	for i := 1; i <= rc.settings.numDatabases; i++ {
		for j := 1; j <= rc.settings.numCollectionsPerDatabase; j++ {
			task := DataCopyTask{
				Db:  fmt.Sprintf("db%d", i),
				Col: fmt.Sprintf("col%d", j),
			}
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (rc *NullReadConnector) generateRandomDocument() map[string]interface{} {
	doc := make(map[string]interface{})
	for i := 1; i <= rc.settings.numFields; i++ {
		doc[fmt.Sprintf("field%d", i)] = gofakeit.LetterN(rc.settings.docSize)
	}
	return doc
}

func (rc *NullReadConnector) SingleInsertDataMessage(loc iface.Location, doc map[string]interface{}) (iface.DataMessage, error) {
	docMap, exists := rc.docMap[loc]
	if !exists {
		rc.docMap[loc] = NewIndexMap()
		docMap = rc.docMap[loc]
	}
	id := docMap.AddRandomID()
	doc["_id"] = id
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

func (rc *NullReadConnector) BatchInsertDataMessage(loc iface.Location, docs []map[string]interface{}) (iface.DataMessage, error) {
	var dataBatch [][]byte
	var err error
	dataBatch = make([][]byte, len(docs))

	docMap, exists := rc.docMap[loc] //Does this part need to be atomic? Current implementation only one thread works
	if !exists {                     //with a particular location
		rc.docMap[loc] = NewIndexMap()
		docMap = rc.docMap[loc]
	}
	for i, doc := range docs {
		id := docMap.AddRandomID()
		doc["_id"] = id
		dataBatch[i], err = bson.Marshal(doc)
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

func (rc *NullReadConnector) generateChangeStreamEvent(operation string, readerProgress *ReaderProgress, lsn *int64) (iface.DataMessage, error) {
	//generate random location
	db := gofakeit.Number(1, rc.settings.numDatabases)
	col := gofakeit.Number(1, rc.settings.numCollectionsPerDatabase)
	loc := iface.Location{Database: fmt.Sprintf("db%d", db), Collection: fmt.Sprintf("col%d", col)}

	switch operation {

	case "insert":
		docMap := rc.docMap[loc]
		if docMap.GetNumDocs() >= rc.settings.maxDocsPerCollection {
			slog.Debug(fmt.Sprintf("%d docs in collection, cannot insert more, lsn %d", docMap.GetNumDocs(), *lsn))
			return iface.DataMessage{}, nil
		}
		doc := rc.generateRandomDocument()
		rc.incrementProgress(readerProgress, lsn)
		slog.Debug(fmt.Sprintf("Generated insert change stream event in collection %v.%v", loc.Database, loc.Collection))
		return rc.SingleInsertDataMessage(loc, doc)

	case "insertBatch":
		docMap := rc.docMap[loc]
		if docMap.GetNumDocs() > rc.settings.maxDocsPerCollection-batch_size {
			slog.Debug(fmt.Sprintf("%d docs in collection, cannot insert %d docs max reached, lsn %d", docMap.GetNumDocs(), batch_size, *lsn))
			return iface.DataMessage{}, nil
		}
		var docs []map[string]interface{}
		for i := 0; i < batch_size; i++ {
			docs = append(docs, rc.generateRandomDocument())
			rc.incrementProgress(readerProgress, lsn)
		}
		return rc.BatchInsertDataMessage(loc, docs)

	case "update":
		docMap := rc.docMap[loc]
		id := docMap.GetRandomKey()
		doc := rc.generateRandomDocument()
		doc["_id"] = id
		data, _ := bson.Marshal(doc)
		idType, idVal, _ := bson.MarshalValue(id)
		rc.incrementProgress(readerProgress, lsn)
		slog.Debug(fmt.Sprintf("Generated update change stream event with id %d in collection %v.%v", id, loc.Database, loc.Collection))
		return iface.DataMessage{Loc: loc, Data: &data, Id: &idVal, IdType: byte(idType), MutationType: iface.MutationType_Update}, nil

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

func (rc *NullReadConnector) generateOperation() (string, error) {
	//generate random operation based on probabilities
	options := []string{"insert", "insertBatch", "update", "delete"}
	totalProb := 0.0
	for _, prob := range rc.settings.probabilities {
		totalProb += prob
	}
	if totalProb != 1.0 {
		fmt.Println("Error: Probabilities must sum to 1.0")
		return "", fmt.Errorf("probabilities must sum to 1.0, sum is %f", totalProb)
	}
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

func (rc *NullReadConnector) incrementProgress(reader *ReaderProgress, lsn *int64) {
	reader.changeStreamEvents++
	(*lsn)++
	rc.status.WriteLSN++
}
