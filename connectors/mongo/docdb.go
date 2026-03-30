package mongo

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"
)

var supportedIDTypes = map[bson.Type]bool{
	bson.TypeObjectID: true,
	bson.TypeString:   true,
	bson.TypeInt32:    true,
	bson.TypeInt64:    true,
	bson.TypeBinary:   true,
}

func compareBSONRawValues(a, b bson.RawValue) int {
	switch a.Type {
	case bson.TypeObjectID:
		return bytes.Compare(a.Value, b.Value)
	case bson.TypeString:
		return bytes.Compare(a.Value[4:len(a.Value)-1], b.Value[4:len(b.Value)-1])
	case bson.TypeInt32:
		ai := int32(binary.LittleEndian.Uint32(a.Value))
		bi := int32(binary.LittleEndian.Uint32(b.Value))
		return cmp.Compare(ai, bi)
	case bson.TypeInt64:
		ai := int64(binary.LittleEndian.Uint64(a.Value))
		bi := int64(binary.LittleEndian.Uint64(b.Value))
		return cmp.Compare(ai, bi)
	case bson.TypeBinary:
		return bytes.Compare(a.Value[5:], b.Value[5:])
	}
	panic("compareBSONRawValues called with unsupported type")
}

func (c *conn) sampleIDs(ctx context.Context, col *mongo.Collection, numSamples int64) ([]bson.RawValue, error) {
	if c.flavor != FlavorDocumentDB {
		res, err := col.Aggregate(ctx, mongo.Pipeline{
			{{"$sample", bson.D{{"size", numSamples}}}},
			{{"$project", bson.D{{"_id", 1}}}},
			{{"$sort", bson.D{{"_id", 1}}}},
		})
		if err != nil {
			return nil, fmt.Errorf("error getting %v samples: %w", numSamples, err)
		}
		defer res.Close(ctx)
		var ids []bson.RawValue
		for res.Next(ctx) {
			ids = append(ids, res.Current.Lookup("_id"))
		}
		if res.Err() != nil {
			return nil, fmt.Errorf("err iterating through samples: %w", res.Err())
		}
		return ids, nil
	}

	type sampleResult struct {
		id  bson.RawValue
		key string
	}

	results := make([]sampleResult, numSamples)
	eg, ctx := errgroup.WithContext(ctx)
	for i := int64(0); i < numSamples; i++ {
		i := i
		eg.Go(func() error {
			res, err := col.Aggregate(ctx, mongo.Pipeline{
				{{"$sample", bson.D{{"size", 1}}}},
				{{"$project", bson.D{{"_id", 1}}}},
			})
			if err != nil {
				return fmt.Errorf("error getting sample %v: %w", i, err)
			}
			defer res.Close(ctx)
			if res.Next(ctx) {
				id := res.Current.Lookup("_id")
				results[i] = sampleResult{id: id, key: hex.EncodeToString(id.Value)}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	var ids []bson.RawValue
	for _, r := range results {
		if r.key == "" {
			continue
		}
		if _, dup := seen[r.key]; !dup {
			seen[r.key] = struct{}{}
			ids = append(ids, r.id)
		}
	}

	if len(ids) > 0 {
		t := ids[0].Type
		for _, id := range ids[1:] {
			if id.Type != t {
				return nil, fmt.Errorf("mixed _id types not supported for DocumentDB sampling")
			}
		}
		if !supportedIDTypes[t] {
			return nil, fmt.Errorf("unsupported _id type for DocumentDB sampling: %v", t)
		}
		sort.Slice(ids, func(i, j int) bool {
			return compareBSONRawValues(ids[i], ids[j]) < 0
		})
	}

	return ids, nil
}
