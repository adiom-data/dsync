/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/adiom-data/dsync/protocol/iface"
)

// split the range between two boundaries into equal parts
// returns an array of boundaries including the top and bottom boundaries
func splitRange(value1 bson.RawValue, value2 bson.RawValue, numParts int) ([]bson.RawValue, error) {
	if !canPartitionRange(value1, value2) {
		return nil, fmt.Errorf("cannot partition range between %v and %v", value1, value2)
	}

	switch value1.Type {
	case bson.TypeObjectID:
		return splitRangeObjectId(value1, value2, numParts)
	}

	return nil, fmt.Errorf("unsupported type %v", value1.Type)
}

// check if two boundaries of a range are of the same and a "partitionable" type (ObjectId or Number)
func canPartitionRange(value1 bson.RawValue, value2 bson.RawValue) bool {
	if value1.Type != value2.Type {
		return false
	}

	if value1.Type == bson.TypeObjectID /* || value1.Type == bson.TypeInt32 || value1.Type == bson.TypeInt64 */ {
		return true
	}

	return false
}

// splits the range between two objectIds into equal parts based on timestamps
// returns an array of boundaries including the top and bottom boundaries
// value1 is the bottom boundary, value2 is the top boundary (i.e. value1 < value2)
func splitRangeObjectId(value1 bson.RawValue, value2 bson.RawValue, numParts int) ([]bson.RawValue, error) {
	minT := value1.ObjectID().Timestamp()
	maxT := value2.ObjectID().Timestamp()

	if minT.After(maxT) {
		return nil, fmt.Errorf("min value is greater than max value")
	}

	// calculate the time difference between the two boundaries
	diff := maxT.Sub(minT)
	partSize := diff / time.Duration(numParts)

	// create the boundaries (except the min and max)
	boundaries := make([]bson.RawValue, numParts+1)
	for i := 1; i < numParts; i++ {
		t := minT.Add(time.Duration(i) * partSize)
		val := primitive.NewObjectIDFromTimestamp(t)
		boundaries[i] = bson.RawValue{
			Type:  bson.TypeObjectID,
			Value: val[:],
		}

	}

	// add the mina nd max boundaries to be precise
	boundaries[0] = value1
	boundaries[numParts] = value2

	return boundaries, nil
}

// get min and max boundaries for a namespace task
func (cc *CosmosConnector) getMinAndMax(ctx context.Context, ns iface.Namespace, partitionKey string) (bson.RawValue, bson.RawValue, error) {
	collection := cc.client.Database(ns.Db).Collection(ns.Col)
	optsMax := options.Find().SetProjection(bson.D{{partitionKey, 1}}).SetLimit(1).SetSort(bson.D{{partitionKey, -1}})
	optsMin := options.Find().SetProjection(bson.D{{partitionKey, 1}}).SetLimit(1).SetSort(bson.D{{partitionKey, 1}})

	// get the top and bottom boundaries
	topCursor, err := collection.Find(ctx, bson.M{}, optsMax)
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, err
	}
	defer topCursor.Close(ctx)

	bottomCursor, err := collection.Find(ctx, bson.M{}, optsMin)
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, err
	}
	defer bottomCursor.Close(ctx)

	if !topCursor.Next(ctx) {
		return bson.RawValue{}, bson.RawValue{}, fmt.Errorf("failed to get top boundary")
	}

	if !bottomCursor.Next(ctx) {
		return bson.RawValue{}, bson.RawValue{}, fmt.Errorf("failed to get bottom boundary")
	}

	topId := topCursor.Current.Lookup(partitionKey)
	bottomId := bottomCursor.Current.Lookup(partitionKey)

	return bottomId, topId, nil
}

// function to find the closest (lower or equal) value in the collection
func findClosestLowerValue(ctx context.Context, collection *mongo.Collection, partitionKey string, value interface{}) (bson.RawValue, error) {
	opts := options.FindOne().SetSort(bson.D{{partitionKey, -1}})
	filter := bson.D{{partitionKey, bson.D{{"$lte", value}}}}
	resRaw, err := collection.FindOne(ctx, filter, opts).Raw()
	if err != nil {
		return bson.RawValue{}, err
	}
	if resRaw == nil {
		return bson.RawValue{}, fmt.Errorf("failed to find the closest value to %v in %v", value, collection.Name())
	}
	return resRaw.Lookup(partitionKey), nil
}
