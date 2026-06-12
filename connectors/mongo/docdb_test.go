package mongo

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func bsonRawValue(t *testing.T, v any) bson.RawValue {
	t.Helper()
	typ, data, err := bson.MarshalValue(v)
	require.NoError(t, err)
	return bson.RawValue{Type: typ, Value: data}
}

func TestSupportedIDTypesIncludesEmbeddedDocument(t *testing.T) {
	assert.True(t, supportedIDTypes[bson.TypeEmbeddedDocument])
}

func TestCompareBSONRawValuesEmbeddedDocumentSortsCompositeIDs(t *testing.T) {
	ids := []bson.RawValue{
		bsonRawValue(t, bson.D{{"tenant", "b"}, {"seq", int32(1)}}),
		bsonRawValue(t, bson.D{{"tenant", "a"}, {"seq", int32(2)}}),
		bsonRawValue(t, bson.D{{"tenant", "a"}, {"seq", int32(1)}}),
	}

	sort.Slice(ids, func(i, j int) bool {
		return compareBSONRawValues(ids[i], ids[j]) < 0
	})

	assert.True(t, ids[0].Equal(bsonRawValue(t, bson.D{{"tenant", "a"}, {"seq", int32(1)}})))
	assert.True(t, ids[1].Equal(bsonRawValue(t, bson.D{{"tenant", "a"}, {"seq", int32(2)}})))
	assert.True(t, ids[2].Equal(bsonRawValue(t, bson.D{{"tenant", "b"}, {"seq", int32(1)}})))
}

func TestCompareBSONRawValuesEmbeddedDocumentSortsNestedDocuments(t *testing.T) {
	low := bsonRawValue(t, bson.D{
		{"tenant", "a"},
		{"bucket", bson.D{{"region", "us"}, {"part", int32(1)}}},
	})
	high := bsonRawValue(t, bson.D{
		{"tenant", "a"},
		{"bucket", bson.D{{"region", "us"}, {"part", int32(2)}}},
	})

	assert.Negative(t, compareBSONRawValues(low, high))
	assert.Positive(t, compareBSONRawValues(high, low))
	assert.Zero(t, compareBSONRawValues(low, low))
}

func TestCompareBSONRawValuesEmbeddedDocumentComparesMixedNumericTypes(t *testing.T) {
	oneInt32 := bsonRawValue(t, bson.D{{"seq", int32(1)}})
	oneInt64 := bsonRawValue(t, bson.D{{"seq", int64(1)}})
	twoInt32 := bsonRawValue(t, bson.D{{"seq", int32(2)}})
	tenInt64 := bsonRawValue(t, bson.D{{"seq", int64(10)}})
	decimal, err := bson.ParseDecimal128("2.5")
	require.NoError(t, err)
	twoPointFiveDecimal := bsonRawValue(t, bson.D{{"seq", decimal}})

	assert.Zero(t, compareBSONRawValues(oneInt32, oneInt64))
	assert.Negative(t, compareBSONRawValues(twoInt32, tenInt64))
	assert.Positive(t, compareBSONRawValues(twoPointFiveDecimal, twoInt32))
}

func TestCompareBSONRawValuesEmbeddedDocumentComparesNonFiniteNumbers(t *testing.T) {
	nan := bsonRawValue(t, bson.D{{"seq", math.NaN()}})
	negInf := bsonRawValue(t, bson.D{{"seq", math.Inf(-1)}})
	one := bsonRawValue(t, bson.D{{"seq", int32(1)}})
	posInf := bsonRawValue(t, bson.D{{"seq", math.Inf(1)}})

	assert.Negative(t, compareBSONRawValues(nan, negInf))
	assert.Negative(t, compareBSONRawValues(negInf, one))
	assert.Negative(t, compareBSONRawValues(one, posInf))
}

func TestCompareBSONRawValuesBinaryUsesMongoOrdering(t *testing.T) {
	shortZ := bsonRawValue(t, bson.Binary{Subtype: 0x00, Data: []byte("z")})
	longAA := bsonRawValue(t, bson.Binary{Subtype: 0x00, Data: []byte("aa")})
	generic := bsonRawValue(t, bson.Binary{Subtype: 0x00, Data: []byte("a")})
	uuid := bsonRawValue(t, bson.Binary{Subtype: 0x04, Data: []byte("a")})

	assert.Negative(t, compareBSONRawValues(shortZ, longAA))
	assert.Negative(t, compareBSONRawValues(generic, uuid))
}

func TestCompareBSONRawValuesEmbeddedDocumentComparesFieldTypeBeforeKey(t *testing.T) {
	nullWithEarlierKey := bsonRawValue(t, bson.D{{"a", nil}})
	minKeyWithLaterKey := bsonRawValue(t, bson.D{{"b", bson.MinKey{}}})
	aKey := bsonRawValue(t, bson.D{{"a", int32(1)}})
	bKey := bsonRawValue(t, bson.D{{"b", int32(1)}})

	assert.Negative(t, compareBSONRawValues(minKeyWithLaterKey, nullWithEarlierKey))
	assert.Negative(t, compareBSONRawValues(aKey, bKey))
}
