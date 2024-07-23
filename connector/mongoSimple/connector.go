package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	batchSize           = 10000
	maxGoroutines       = 8
	reportInterval      = 10 * time.Second
	resumeTokenInterval = 60 * time.Second
	sampleSize          = 1000
)

type TransferStats struct {
	docsProcessed  atomic.Int64
	bytesProcessed atomic.Int64
	changeEvents   atomic.Int64
}

type MongoConnector struct {
	sourceClient *mongo.Client
	destClient   *mongo.Client
	sourceDB     string
	destDB       string
	resumeToken  bson.Raw
	stats        TransferStats
	dataChannel  chan DataMessage
}

type DataMessage struct {
	MutationType string
	Data         bson.Raw
	Namespace    string
	Checksum     string
}

const (
	resumeTokenCollection = "resumeTokens"
	resumeTokenID         = "currentToken"
)

func NewMongoConnector(ctx context.Context, sourceURI, destURI, sourceDB, destDB string) (*MongoConnector, error) {
	sourceClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sourceURI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source: %v", err)
	}

	destClient, err := mongo.Connect(ctx, options.Client().ApplyURI(destURI))
	if err != nil {
		sourceClient.Disconnect(ctx)
		return nil, fmt.Errorf("failed to connect to destination: %v", err)
	}

	return &MongoConnector{
		sourceClient: sourceClient,
		destClient:   destClient,
		sourceDB:     sourceDB,
		destDB:       destDB,
		dataChannel:  make(chan DataMessage, batchSize),
	}, nil
}

func (mc *MongoConnector) Transfer(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	// Get the latest resume token
	latestToken, err := mc.getLatestResumeToken(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest resume token: %v", err)
	}

	if latestToken == nil {
		// No resume token found, start initial sync
		go func() {
			defer wg.Done()
			if err := mc.initialSync(ctx); err != nil {
				log.Printf("Initial sync error: %v", err)
			}
		}()
	} else {
		// Resume token found, skip initial sync
		log.Println("Resuming from previous session...")
		wg.Done() // Reduce wait group count since we're skipping initial sync
	}

	// Start change stream
	go func() {
		defer wg.Done()
		if err := mc.changeStream(ctx); err != nil {
			log.Printf("Change stream error: %v", err)
		}
	}()

	// Start writer goroutines
	for i := 0; i < maxGoroutines; i++ {
		go mc.writer(ctx)
	}

	// Start progress reporter
	go mc.reportProgress(ctx)

	wg.Wait()
	close(mc.dataChannel)

	// Perform final validation
	if err := mc.performValidation(ctx); err != nil {
		log.Printf("Final validation error: %v", err)
	}

	return nil
}

func (mc *MongoConnector) initialSync(ctx context.Context) error {
	collections, err := mc.sourceClient.Database(mc.sourceDB).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to list collections: %v", err)
	}

	for _, coll := range collections {
		if err := mc.syncCollection(ctx, coll); err != nil {
			return fmt.Errorf("failed to sync collection %s: %v", coll, err)
		}
	}
	return nil
}

func (mc *MongoConnector) syncCollection(ctx context.Context, collName string) error {
	coll := mc.sourceClient.Database(mc.sourceDB).Collection(collName)
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to create cursor: %v", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.Raw
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("failed to decode document: %v", err)
		}
		checksum := mc.calculateChecksum(doc)
		mc.dataChannel <- DataMessage{
			MutationType: "insert",
			Data:         doc,
			Namespace:    fmt.Sprintf("%s.%s", mc.sourceDB, collName),
			Checksum:     checksum,
		}
		mc.stats.docsProcessed.Add(1)
		mc.stats.bytesProcessed.Add(int64(len(doc)))
	}
	return cursor.Err()
}

func (mc *MongoConnector) changeStream(ctx context.Context) error {
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if mc.resumeToken != nil {
		opts.SetResumeAfter(mc.resumeToken)
	}

	stream, err := mc.sourceClient.Database(mc.sourceDB).Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return fmt.Errorf("failed to start change stream: %v", err)
	}
	defer stream.Close(ctx)

	resumeTokenTicker := time.NewTicker(resumeTokenInterval)
	defer resumeTokenTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resumeTokenTicker.C:
			if err := mc.updateResumeToken(ctx, mc.resumeToken); err != nil {
				log.Printf("Failed to update resume token: %v", err)
			}
		default:
			if stream.Next(ctx) {
				var changeEvent bson.M
				if err := stream.Decode(&changeEvent); err != nil {
					return fmt.Errorf("failed to decode change event: %v", err)
				}
				mc.processChangeEvent(changeEvent)
				mc.resumeToken = stream.ResumeToken()
			} else if stream.Err() != nil {
				return fmt.Errorf("change stream error: %v", stream.Err())
			}
		}
	}
}

func (mc *MongoConnector) processChangeEvent(changeEvent bson.M) {
	operationType, _ := changeEvent["operationType"].(string)
	documentKey, _ := changeEvent["documentKey"].(bson.M)
	fullDocument, _ := changeEvent["fullDocument"].(bson.Raw)
	ns, _ := changeEvent["ns"].(bson.M)
	db, _ := ns["db"].(string)
	coll, _ := ns["coll"].(string)

	data := bson.Raw{}
	switch operationType {
	case "insert", "replace", "update":
		data = fullDocument
	case "delete":
		data, _ = bson.Marshal(documentKey)
	default:
		return // Ignore other operation types
	}

	checksum := mc.calculateChecksum(data)
	mc.dataChannel <- DataMessage{
		MutationType: operationType,
		Data:         data,
		Namespace:    fmt.Sprintf("%s.%s", db, coll),
		Checksum:     checksum,
	}
	mc.stats.changeEvents.Add(1)
	mc.stats.bytesProcessed.Add(int64(len(data)))
}

func (mc *MongoConnector) writer(ctx context.Context) {
	batch := make([]DataMessage, 0, batchSize)
	for msg := range mc.dataChannel {
		batch = append(batch, msg)
		if len(batch) >= batchSize {
			if err := mc.writeBatch(ctx, batch); err != nil {
				log.Printf("Failed to write batch: %v", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := mc.writeBatch(ctx, batch); err != nil {
			log.Printf("Failed to write final batch: %v", err)
		}
	}
}

func (mc *MongoConnector) writeBatch(ctx context.Context, batch []DataMessage) error {
	// Group operations by collection
	operations := make(map[string][]mongo.WriteModel)
	for _, msg := range batch {
		if msg.MutationType == "resumeToken" {
			// Handle resume token update
			if err := mc.updateResumeToken(ctx, msg.Data); err != nil {
				return fmt.Errorf("failed to update resume token: %v", err)
			}
			continue
		}

		var operation mongo.WriteModel
		switch msg.MutationType {
		case "insert":
			operation = mongo.NewInsertOneModel().SetDocument(msg.Data)
		case "update", "replace":
			var document bson.M
			bson.Unmarshal(msg.Data, &document)
			operation = mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": document["_id"]}).SetReplacement(document).SetUpsert(true)
		case "delete":
			var filter bson.M
			bson.Unmarshal(msg.Data, &filter)
			operation = mongo.NewDeleteOneModel().SetFilter(filter)
		default:
			continue
		}
		operations[msg.Namespace] = append(operations[msg.Namespace], operation)
	}

	// Execute bulk writes for each collection
	for namespace, ops := range operations {
		parts := strings.Split(namespace, ".")
		if len(parts) != 2 {
			return fmt.Errorf("invalid namespace: %s", namespace)
		}
		db, coll := parts[0], parts[1]
		_, err := mc.destClient.Database(db).Collection(coll).BulkWrite(ctx, ops)
		if err != nil {
			return fmt.Errorf("bulk write failed for %s: %v", namespace, err)
		}
	}
	return nil
}

func (mc *MongoConnector) updateResumeToken(ctx context.Context, token bson.Raw) error {
	collection := mc.destClient.Database(mc.destDB).Collection(resumeTokenCollection)
	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": resumeTokenID},
		bson.M{"$set": bson.M{"token": token}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("failed to update resume token: %v", err)
	}
	return nil
}

func (mc *MongoConnector) getLatestResumeToken(ctx context.Context) (bson.Raw, error) {
	collection := mc.destClient.Database(mc.destDB).Collection(resumeTokenCollection)
	var result struct {
		Token bson.Raw `bson:"token"`
	}
	err := collection.FindOne(ctx, bson.M{"_id": resumeTokenID}).Decode(&result)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil // No token found, which is fine for initial run
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get resume token: %v", err)
	}
	return result.Token, nil
}

func (mc *MongoConnector) calculateChecksum(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

func (mc *MongoConnector) reportProgress(ctx context.Context) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	var lastDocs, lastBytes, lastEvents int64
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentDocs := mc.stats.docsProcessed.Load()
			currentBytes := mc.stats.bytesProcessed.Load()
			currentEvents := mc.stats.changeEvents.Load()

			docRate := float64(currentDocs-lastDocs) / reportInterval.Seconds()
			byteRate := float64(currentBytes-lastBytes) / reportInterval.Seconds() / 1024 / 1024 // MB/s
			eventRate := float64(currentEvents-lastEvents) / reportInterval.Seconds()

			elapsedTime := time.Since(startTime)
			log.Printf("Progress: Docs: %d (%.2f/s), Data: %.2f MB (%.2f MB/s), Change Events: %d (%.2f/s), Elapsed Time: %v",
				currentDocs, docRate, float64(currentBytes)/1024/1024, byteRate, currentEvents, eventRate, elapsedTime)

			lastDocs = currentDocs
			lastBytes = currentBytes
			lastEvents = currentEvents
		}
	}
}

func (mc *MongoConnector) performValidation(ctx context.Context) error {
	log.Println("Starting final validation...")

	sourceColl := mc.sourceClient.Database(mc.sourceDB).Collection("your_collection")
	destColl := mc.destClient.Database(mc.destDB).Collection("your_collection")

	sourceCount, err := sourceColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to count source documents: %v", err)
	}

	destCount, err := destColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to count destination documents: %v", err)
	}

	log.Printf("Source documents: %d, Destination documents: %d", sourceCount, destCount)

	if sourceCount != destCount {
		return fmt.Errorf("document count mismatch: source %d, destination %d", sourceCount, destCount)
	}

	// Perform sample validation
	pipeline := mongo.Pipeline{
		{{"$sample", bson.D{{"size", sampleSize}}}},
	}

	sourceCursor, err := sourceColl.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to create source cursor: %v", err)
	}
	defer sourceCursor.Close(ctx)

	var mismatches int
	for sourceCursor.Next(ctx) {
		var sourceDoc bson.M
		if err := sourceCursor.Decode(&sourceDoc); err != nil {
			return fmt.Errorf("failed to decode source document: %v", err)
		}

		var destDoc bson.M
		err := destColl.FindOne(ctx, bson.M{"_id": sourceDoc["_id"]}).Decode(&destDoc)
		if err != nil {
			mismatches++
			log.Printf("Document not found in destination: %v", sourceDoc["_id"])
			continue
		}

		sourceChecksum := mc.calculateChecksum(mc.toBson(sourceDoc))
		destChecksum := mc.calculateChecksum(mc.toBson(destDoc))
		if sourceChecksum != destChecksum {
			mismatches++
			log.Printf("Checksum mismatch for document: %v", sourceDoc["_id"])
		}
	}

	log.Printf("Validation complete. Mismatches found: %d out of %d sampled", mismatches, sampleSize)

	if mismatches > 0 {
		return fmt.Errorf("validation failed with %d mismatches", mismatches)
	}

	return nil
}

func (mc *MongoConnector) toBson(m bson.M) []byte {
	data, _ := bson.Marshal(m)
	return data
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connector, err := NewMongoConnector(ctx, "mongodb://source:27017", "mongodb://dest:27017", "sourceDB", "destDB")
	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}

	if err := connector.Transfer(ctx); err != nil {
		log.Fatalf("Transfer error: %v", err)
	}

	connector.sourceClient.Disconnect(ctx)
	connector.destClient.Disconnect(ctx)

	log.Println("Transfer complete")
}
