package postgres

import (
	"net/netip"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBSONToMapConvertsBinaryValues(t *testing.T) {
	uuidBytes := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	doc, err := bson.Marshal(bson.D{
		{Key: "bytes", Value: bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte("data")}},
		{Key: "uuid", Value: bson.Binary{Subtype: bson.TypeBinaryUUID, Data: uuidBytes}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := bsonToMap(NewBSONRegistry(), doc, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got["bytes"], []byte("data")) {
		t.Fatalf("bytes = %#v", got["bytes"])
	}
	wantUUID := pgtype.UUID{Valid: true}
	copy(wantUUID.Bytes[:], uuidBytes)
	if !reflect.DeepEqual(got["uuid"], wantUUID) {
		t.Fatalf("uuid = %#v, want %#v", got["uuid"], wantUUID)
	}
}

func TestBSONToMapRejectsLegacyUUID(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{
		Key: "uuid",
		Value: bson.Binary{
			Subtype: bson.TypeBinaryUUIDOld,
			Data:    []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bsonToMap(NewBSONRegistry(), doc, false); err == nil {
		t.Fatal("expected legacy UUID subtype to be rejected")
	}
}

func TestBSONToMapPreservesIDForDestinationColumn(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "source-id"}, {Key: "value", Value: "data"}})
	if err != nil {
		t.Fatal(err)
	}

	got, err := bsonToMap(NewBSONRegistry(), doc, true)
	if err != nil {
		t.Fatal(err)
	}
	if got["_id"] != "source-id" {
		t.Fatalf("_id = %#v", got["_id"])
	}
}

func TestMarshalWithRegistryNormalizesPostgresNetworkTypes(t *testing.T) {
	prefix := netip.MustParsePrefix("192.0.2.0/24")
	doc, err := marshalWithRegistry(NewBSONRegistry(), map[string]interface{}{"network": prefix})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := bson.Unmarshal(doc, &got); err != nil {
		t.Fatal(err)
	}
	if got["network"] != prefix.String() {
		t.Fatalf("network = %#v, want %q", got["network"], prefix.String())
	}
}

func TestMarshalValueWithRegistryNormalizesPostgresNetworkTypes(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "address", value: netip.MustParseAddr("192.0.2.1"), want: "192.0.2.1"},
		{name: "prefix", value: netip.MustParsePrefix("192.0.2.0/24"), want: "192.0.2.0/24"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typ, data, err := marshalValueWithRegistry(NewBSONRegistry(), test.value)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			if err := bson.UnmarshalValue(typ, data, &got); err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("network ID = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBSONRegistryEncodesPostgresUUIDAsUUID(t *testing.T) {
	uuid := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	typ, data, err := marshalValueWithRegistry(NewBSONRegistry(), uuid)
	if err != nil {
		t.Fatal(err)
	}
	var got bson.Binary
	if err := bson.UnmarshalValue(typ, data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Subtype != bson.TypeBinaryUUID || !reflect.DeepEqual(got.Data, uuid[:]) {
		t.Fatalf("binary = %#v", got)
	}
}
