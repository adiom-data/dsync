/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package cosmos

import (
	"context"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

// Creates a single changestream compatible with CosmosDB with the provided options
func createChangeStream(ctx context.Context, client *mongo.Client, namespace iface.Location, opts *moptions.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	db := namespace.Database
	col := namespace.Collection
	collection := client.Database(db).Collection(col)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace"}}}}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}, {Key: "fullDocument", Value: 1}, {Key: "ns", Value: 1}, {Key: "documentKey", Value: 1}}}}}

	changeStream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, err
	}
	return changeStream, nil
}
