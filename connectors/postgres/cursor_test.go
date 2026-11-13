package postgres

import (
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestPartitionCursorRoundTripDatabaseTypes(t *testing.T) {
	when := time.Date(2026, 10, 14, 12, 30, 0, 0, time.UTC)
	uuid := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	number := decimal.RequireFromString("1234567890.0123456789")
	want := &partitionCursor{Low: []interface{}{uuid, when, number}}

	encoded, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePartitionCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cursor = %#v, want %#v", got, want)
	}
}

func TestCompositeMongoIDRoundTripDatabaseTypes(t *testing.T) {
	uuid := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	when := time.Date(2026, 10, 14, 12, 30, 0, 0, time.UTC)
	number := decimal.RequireFromString("1234567890.0123456789")
	want := []interface{}{uuid, when, number}

	id, _, err := toMongoID(NewBSONRegistry(), want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fromMongoID(NewBSONRegistry(), id, []string{"first", "second", "third"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ID = %#v, want %#v", got, want)
	}
}

func TestScalarByteaMongoIDRoundTrip(t *testing.T) {
	want := []byte("primary-key")
	id, _, err := toMongoID(NewBSONRegistry(), []interface{}{want})
	if err != nil {
		t.Fatal(err)
	}
	got, err := fromMongoID(NewBSONRegistry(), id, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []interface{}{want}) {
		t.Fatalf("ID = %#v, want %#v", got, []interface{}{want})
	}
}
