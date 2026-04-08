/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package firestore

import (
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBsonTypeConversion(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected map[string]any
	}{
		{
			name: "ObjectID field",
			input: map[string]any{
				"_id":    bson.ObjectID{0x66, 0xf4, 0xc6, 0x92, 0x91, 0xab, 0x1a, 0x55, 0x33, 0x94, 0x5e, 0x37},
				"field1": "value1",
			},
			expected: map[string]any{
				"_id":    "66f4c69291ab1a5533945e37",
				"field1": "value1",
			},
		},
		{
			name: "DateTime field",
			input: map[string]any{
				"_id":       "doc1",
				"createdAt": bson.DateTime(1704067200000), // 2024-01-01 00:00:00 UTC
			},
			expected: map[string]any{
				"_id":       "doc1",
				"createdAt": time.UnixMilli(1704067200000).UTC(),
			},
		},
		{
			name: "Binary field",
			input: map[string]any{
				"_id":  "doc1",
				"data": bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{0x01, 0x02, 0x03}},
			},
			expected: map[string]any{
				"_id":  "doc1",
				"data": []byte{0x01, 0x02, 0x03},
			},
		},
		{
			name: "Decimal128 field",
			input: map[string]any{
				"_id":   "doc1",
				"price": mustParseDecimal128("123.45"),
			},
			expected: map[string]any{
				"_id":   "doc1",
				"price": "123.45",
			},
		},
		{
			name: "Nested document with BSON types",
			input: map[string]any{
				"_id": "doc1",
				"nested": bson.M{
					"objectId": bson.ObjectID{0x66, 0xf4, 0xc6, 0x92, 0x91, 0xab, 0x1a, 0x55, 0x33, 0x94, 0x5e, 0x37},
					"date":     bson.DateTime(1704067200000),
				},
			},
			expected: map[string]any{
				"_id": "doc1",
				"nested": map[string]any{
					"objectId": "66f4c69291ab1a5533945e37",
					"date":     time.UnixMilli(1704067200000).UTC(),
				},
			},
		},
		{
			name: "Array with BSON types",
			input: map[string]any{
				"_id": "doc1",
				"ids": bson.A{
					bson.ObjectID{0x66, 0xf4, 0xc6, 0x92, 0x91, 0xab, 0x1a, 0x55, 0x33, 0x94, 0x5e, 0x37},
					bson.ObjectID{0x66, 0xf4, 0xc6, 0x92, 0x91, 0xab, 0x1a, 0x55, 0x33, 0x94, 0x5e, 0x38},
				},
			},
			expected: map[string]any{
				"_id": "doc1",
				"ids": []any{
					"66f4c69291ab1a5533945e37",
					"66f4c69291ab1a5533945e38",
				},
			},
		},
		{
			name: "bson.D document",
			input: map[string]any{
				"_id": "doc1",
				"doc": bson.D{
					{Key: "name", Value: "test"},
					{Key: "id", Value: bson.ObjectID{0x66, 0xf4, 0xc6, 0x92, 0x91, 0xab, 0x1a, 0x55, 0x33, 0x94, 0x5e, 0x37}},
				},
			},
			expected: map[string]any{
				"_id": "doc1",
				"doc": map[string]any{
					"name": "test",
					"id":   "66f4c69291ab1a5533945e37",
				},
			},
		},
		{
			name: "Primitive types unchanged",
			input: map[string]any{
				"_id":     "doc1",
				"str":     "hello",
				"int32":   int32(42),
				"int64":   int64(9999999999),
				"float64": 3.14,
				"bool":    true,
				"null":    nil,
			},
			expected: map[string]any{
				"_id":     "doc1",
				"str":     "hello",
				"int32":   int32(42),
				"int64":   int64(9999999999),
				"float64": 3.14,
				"bool":    true,
				"null":    nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertBsonTypes(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRawToMapWithBsonTypes(t *testing.T) {
	// Create a BSON document with various types
	doc := bson.D{
		{Key: "_id", Value: bson.ObjectID{0x66, 0xf4, 0xc6, 0x92, 0x91, 0xab, 0x1a, 0x55, 0x33, 0x94, 0x5e, 0x37}},
		{Key: "name", Value: "test"},
		{Key: "createdAt", Value: bson.DateTime(1704067200000)},
		{Key: "nested", Value: bson.D{
			{Key: "ref", Value: bson.ObjectID{0x66, 0xf4, 0xc6, 0x92, 0x91, 0xab, 0x1a, 0x55, 0x33, 0x94, 0x5e, 0x38}},
		}},
	}

	raw, err := bson.Marshal(doc)
	require.NoError(t, err)

	result, err := rawToMap(raw, adiomv1.DataType_DATA_TYPE_MONGO_BSON)
	require.NoError(t, err)

	// Check that ObjectID was converted to string
	assert.Equal(t, "66f4c69291ab1a5533945e37", result["_id"])
	assert.Equal(t, "test", result["name"])

	// Check DateTime was converted to time.Time
	createdAt, ok := result["createdAt"].(time.Time)
	assert.True(t, ok, "createdAt should be time.Time")
	assert.Equal(t, int64(1704067200000), createdAt.UnixMilli())

	// Check nested ObjectID was converted
	nested, ok := result["nested"].(map[string]any)
	assert.True(t, ok, "nested should be map[string]any")
	assert.Equal(t, "66f4c69291ab1a5533945e38", nested["ref"])
}

func mustParseDecimal128(s string) bson.Decimal128 {
	d, err := bson.ParseDecimal128(s)
	if err != nil {
		panic(err)
	}
	return d
}
