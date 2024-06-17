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
		dataMsg, err := rc.generateDataMessage(loc, doc)
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
	dataMsg, err := rc.generateDataMessageBatch(loc, docs)
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

func (rc *NullReadConnector) generateDataMessage(loc iface.Location, doc map[string]interface{}) (iface.DataMessage, error) {
	if rc.settings.docMap[loc] == nil {
		rc.settings.docMap[loc] = make(map[int]bool)
	}
	docMap := rc.settings.docMap[loc]
	id := len(docMap)
	doc["_id"] = id
	docMap[id] = true
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

func (rc *NullReadConnector) generateDataMessageBatch(loc iface.Location, docs []map[string]interface{}) (iface.DataMessage, error) {
	var dataBatch [][]byte
	var err error
	dataBatch = make([][]byte, len(docs))
	if rc.settings.docMap[loc] == nil {
		rc.settings.docMap[loc] = make(map[int]bool)
	}
	docMap := rc.settings.docMap[loc]
	currId := len(docMap)
	for i, doc := range docs {
		doc["_id"] = currId + i
		docMap[currId+i] = true
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
	db := gofakeit.Number(1, rc.settings.numDatabases)
	col := gofakeit.Number(1, rc.settings.numCollectionsPerDatabase)
	loc := iface.Location{Database: fmt.Sprintf("db%d", db), Collection: fmt.Sprintf("col%d", col)}
	slog.Debug(fmt.Sprintf("number of docs in col: %d", len(rc.settings.docMap[loc])))
	switch operation {
	case "insert":
		//generate namespace
		doc := rc.generateRandomDocument()
		readerProgress.changeStreamEvents++
		(*lsn)++
		rc.status.WriteLSN++
		slog.Debug(fmt.Sprintf("Generated insert change stream event: %v, with location %v", doc, loc))
		return rc.generateDataMessage(loc, doc)
	case "insertBatch":
		//insert batch change
		//generate namespace
		var docs []map[string]interface{}
		for i := 0; i < batch_size; i++ {
			docs = append(docs, rc.generateRandomDocument())
			readerProgress.changeStreamEvents++
			(*lsn)++
			rc.status.WriteLSN++
		}
		return rc.generateDataMessageBatch(loc, docs)

	case "update":
		//update change may need to change ids to be unique per collection
		docMap := rc.settings.docMap[loc]
		id := gofakeit.Number(0, len(docMap)-1)
		doc := rc.generateRandomDocument()
		doc["_id"] = id
		//make sure id is not already deleted, is there a better way to do this?
		for !docMap[id] {
			id = gofakeit.Number(0, len(docMap)-1)
		}
		data, _ := bson.Marshal(doc)
		idType, idVal, _ := bson.MarshalValue(id)
		readerProgress.changeStreamEvents++
		(*lsn)++
		rc.status.WriteLSN++
		slog.Debug(fmt.Sprintf("Generated update change stream event: %v, with location %v", doc, loc))
		return iface.DataMessage{Loc: loc, Data: &data, Id: &idVal, IdType: byte(idType), MutationType: iface.MutationType_Update}, nil
	case "delete":
		//delete change
		docMap := rc.settings.docMap[loc]
		id := gofakeit.Number(0, len(docMap)-1)
		//again making sure id is not already deleted, maybe limit how many times we try then move on?
		//other option could be to make the doc map a map of slices
		for !docMap[id] {
			id = gofakeit.Number(0, len(docMap)-1)
		}
		docMap[id] = false
		idType, idVal, _ := bson.MarshalValue(id)
		readerProgress.changeStreamEvents++
		(*lsn)++
		rc.status.WriteLSN++
		slog.Debug(fmt.Sprintf("Generated delete change stream event: %v, with location %v", id, loc))
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
