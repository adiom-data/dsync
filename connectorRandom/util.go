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
		id := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents //XXX: Possible overflow eventually
		doc := rc.generateRandomDocument(id)
		dataMsg, err := generateDataMessage(loc, doc)
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
		id := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents //XXX: Possible overflow eventually
		doc := rc.generateRandomDocument(id)
		docs = append(docs, doc)
		readerProgress.initialSyncDocs.Add(1)
	}
	//generate data message batch with array of docs
	dataMsg, err := generateDataMessageBatch(loc, docs)
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

func (rc *NullReadConnector) generateRandomDocument(id uint64) map[string]interface{} {
	doc := make(map[string]interface{})
	doc["_id"] = id
	for i := 1; i <= rc.settings.numFields; i++ {
		doc[fmt.Sprintf("field%d", i)] = gofakeit.LetterN(rc.settings.docSize)
	}
	return doc
}

func generateDataMessage(loc iface.Location, doc map[string]interface{}) (iface.DataMessage, error) {
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

func generateDataMessageBatch(loc iface.Location, docs []map[string]interface{}) (iface.DataMessage, error) {
	var dataBatch [][]byte
	var err error
	dataBatch = make([][]byte, len(docs))
	for i, doc := range docs {
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
	switch operation {
	case "insert":
		//generate namespace
		id := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
		doc := rc.generateRandomDocument(id)
		readerProgress.changeStreamEvents++
		(*lsn)++
		rc.status.WriteLSN++
		slog.Debug(fmt.Sprintf("Generated insert change stream event: %v, with location %v", doc, loc))
		return generateDataMessage(loc, doc)
	case "insertBatch":
		//insert batch change
		//generate namespace
		var docs []map[string]interface{}
		for i := 0; i < batch_size; i++ {
			id := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
			docs = append(docs, rc.generateRandomDocument(id))
			readerProgress.changeStreamEvents++
			(*lsn)++
			rc.status.WriteLSN++
		}
		return generateDataMessageBatch(loc, docs)

	case "update":
	//update change may need to change ids to be unique per collection
	case "delete":
		//delete change
	}
	return iface.DataMessage{}, fmt.Errorf("Failed to generate change stream event")
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
		return "", fmt.Errorf("Probabilities must sum to 1.0")
	}
	r := rand.Float64()
	sum := 0.0
	for i, prob := range rc.settings.probabilities {
		sum += prob
		if r < sum {
			return options[i], nil
		}
	}
	return "", fmt.Errorf("Failed to generate operation")
}
