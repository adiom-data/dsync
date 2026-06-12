package mongo

import (
	"context"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoServerEmbeddedIDSortMatchesLocalComparator(t *testing.T) {
	uri := os.Getenv("MONGO_SORT_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_SORT_TEST_URI to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	require.NoError(t, client.Ping(ctx, nil))

	col := client.Database("dsync_sort_test").Collection("embedded_ids_" + bson.NewObjectID().Hex())
	defer func() {
		require.NoError(t, col.Drop(context.Background()))
	}()

	decimal25, err := bson.ParseDecimal128("2.5")
	require.NoError(t, err)
	decimal300, err := bson.ParseDecimal128("300")
	require.NoError(t, err)

	type sample struct {
		label string
		id    bson.D
	}
	samples := []sample{
		{"minkey-later-key", bson.D{{"z", bson.MinKey{}}}},
		{"null-earlier-key", bson.D{{"a", nil}}},
		{"double-neg-inf", bson.D{{"n", math.Inf(-1)}}},
		{"int32-2", bson.D{{"n", int32(2)}}},
		{"decimal-2.5", bson.D{{"n", decimal25}}},
		{"int64-10", bson.D{{"n", int64(10)}}},
		{"double-20.25", bson.D{{"n", 20.25}}},
		{"decimal-300", bson.D{{"n", decimal300}}},
		{"double-pos-inf", bson.D{{"n", math.Inf(1)}}},
		{"double-nan", bson.D{{"n", math.NaN()}}},
		{"string-a", bson.D{{"s", "a"}}},
		{"nested-a1", bson.D{{"tenant", "a"}, {"bucket", bson.D{{"region", "us"}, {"part", int32(1)}}}}},
		{"nested-a2", bson.D{{"tenant", "a"}, {"bucket", bson.D{{"region", "us"}, {"part", int32(2)}}}}},
		{"array-lex-1-2", bson.D{{"arr", bson.A{int32(1), int32(2)}}}},
		{"array-lex-1-3", bson.D{{"arr", bson.A{int32(1), int32(3)}}}},
		{"binary-short-z", bson.D{{"bin", bson.Binary{Subtype: 0x00, Data: []byte("z")}}}},
		{"binary-long-aa", bson.D{{"bin", bson.Binary{Subtype: 0x00, Data: []byte("aa")}}}},
		{"binary-subtype-4", bson.D{{"bin", bson.Binary{Subtype: 0x04, Data: []byte("a")}}}},
		{"objectid", bson.D{{"oid", bson.NewObjectID()}}},
		{"bool-false", bson.D{{"b", false}}},
		{"date", bson.D{{"d", time.Unix(100, 0)}}},
		{"timestamp", bson.D{{"ts", bson.Timestamp{T: 100, I: 1}}}},
		{"regex", bson.D{{"r", bson.Regex{Pattern: "a", Options: "i"}}}},
		{"maxkey", bson.D{{"m", bson.MaxKey{}}}},
	}

	rawIDs := make(map[string]bson.RawValue, len(samples))
	for _, s := range samples {
		_, err := col.InsertOne(ctx, bson.D{{"_id", s.id}, {"label", s.label}})
		require.NoError(t, err, s.label)
		rawIDs[s.label] = bsonRawValue(t, s.id)
	}

	cursor, err := col.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{"_id", 1}}))
	require.NoError(t, err)
	defer cursor.Close(ctx)

	var serverOrder []string
	for cursor.Next(ctx) {
		serverOrder = append(serverOrder, cursor.Current.Lookup("label").StringValue())
	}
	require.NoError(t, cursor.Err())

	var localOrder []string
	for i := len(samples) - 1; i >= 0; i-- {
		localOrder = append(localOrder, samples[i].label)
	}
	sort.Slice(localOrder, func(i, j int) bool {
		return compareBSONRawValues(rawIDs[localOrder[i]], rawIDs[localOrder[j]]) < 0
	})

	require.Equal(t, serverOrder, localOrder)
	t.Logf("server/local order: %v", serverOrder)

	serverRank := make(map[string]int, len(serverOrder))
	for i, label := range serverOrder {
		serverRank[label] = i
	}
	for _, a := range samples {
		for _, b := range samples {
			want := cmpSign(serverRank[a.label] - serverRank[b.label])
			got := cmpSign(compareBSONRawValues(rawIDs[a.label], rawIDs[b.label]))
			require.Equal(t, want, got, "%s vs %s", a.label, b.label)
		}
	}
}

func cmpSign(i int) int {
	switch {
	case i < 0:
		return -1
	case i > 0:
		return 1
	default:
		return 0
	}
}
