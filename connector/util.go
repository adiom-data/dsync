package connector

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

//XXX: this is not going to work on anything but a dedicated Mongo cluster
/*
func getLastOpTime(ctx context.Context, client *mongo.Client) (*primitive.Timestamp, error) {
	appendOplogNoteCmd := bson.D{
		{"appendOplogNote", 1},
		{"data", bson.D{{"adiom-connector", "lastOpTime"}}},
	}
	res := client.Database("admin").RunCommand(ctx, appendOplogNoteCmd)

	var responseRaw bson.Raw
	var err error
	if responseRaw, err = res.Raw(); err != nil {
		return nil, fmt.Errorf("failed to append oplog note: %v", err)
	}

	opTimeRaw, lookupErr := responseRaw.LookupErr("operationTime")
	if lookupErr != nil {
		return nil, fmt.Errorf("failed to get operationTime from appendOplogNote response: %v", lookupErr)
	}

	t, i := opTimeRaw.Timestamp()
	return &primitive.Timestamp{T: t, I: i}, nil
}
*/

const (
	dummyDB                      string = "adiom-internal"
	dummyCol                     string = "dummy"
	progressReportingIntervalSec        = 5
)

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

func getLatestResumeToken(ctx context.Context, client *mongo.Client) (bson.Raw, error) {
	slog.Debug("Getting latest resume token...")
	changeStream, err := client.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	// we need ANY event to get the resume token that we can use to extract the cluster time
	go insertDummyRecord(ctx, client)

	changeStream.Next(ctx)
	resumeToken := changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}

	return resumeToken, nil
}
