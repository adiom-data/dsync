package postgres

import (
	"reflect"
	"testing"
)

func TestIDColumnMetadataChoosesUnusedAliasForNonPrimaryID(t *testing.T) {
	columns := map[string]map[string]struct{}{
		"public.items": {"item_id": {}, "_id": {}, "_id2": {}},
	}
	hasID, aliases := idColumnMetadata(columns, map[string][]string{"public.items": {"item_id"}})
	if !hasID["public.items"] {
		t.Fatal("expected _id column")
	}
	if aliases["public.items"] != "_id3" {
		t.Fatalf("alias = %q, want _id3", aliases["public.items"])
	}
}

func TestIDColumnMetadataDoesNotAliasPrimaryID(t *testing.T) {
	columns := map[string]map[string]struct{}{
		"public.items": {"tenant": {}, "_id": {}},
	}
	hasID, aliases := idColumnMetadata(columns, map[string][]string{"public.items": {"tenant", "_id"}})
	if !hasID["public.items"] {
		t.Fatal("expected _id column")
	}
	if aliases["public.items"] != "" {
		t.Fatalf("unexpected alias %q", aliases["public.items"])
	}
}

func TestSourceIDUsesNamedPrimaryKeyPartsWithIDLast(t *testing.T) {
	c := &conn{
		pkeys:        map[string][]string{"public.items": {"_id", "tenant"}},
		hasIDColumn:  map[string]bool{"public.items": true},
		bsonRegistry: NewBSONRegistry(),
	}
	id, err := c.sourceUpdateID("public.items", map[string]interface{}{"_id": "item", "tenant": "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{id[0].GetName(), id[1].GetName()}; !reflect.DeepEqual(got, []string{"tenant", "_id"}) {
		t.Fatalf("ID order = %v", got)
	}
	values, err := fromMongoID(NewBSONRegistry(), id, []string{"_id", "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(values, []interface{}{"item", "acme"}) {
		t.Fatalf("values = %#v", values)
	}
	document := map[string]interface{}{"_id": "item", "tenant": "acme"}
	if err := c.prepareSourceDocument("public.items", document); err != nil {
		t.Fatal(err)
	}
	if document["_id"] != "item" {
		t.Fatalf("document _id = %#v", document["_id"])
	}
}

func TestPrepareSourceDocumentAliasesNonPrimaryID(t *testing.T) {
	c := &conn{
		pkeys:        map[string][]string{"public.items": {"item_id"}},
		hasIDColumn:  map[string]bool{"public.items": true},
		idAliases:    map[string]string{"public.items": "_id2"},
		bsonRegistry: NewBSONRegistry(),
	}
	document := map[string]interface{}{"item_id": int32(7), "_id": "original"}
	if err := c.prepareSourceDocument("public.items", document); err != nil {
		t.Fatal(err)
	}
	if document["_id2"] != "original" {
		t.Fatalf("alias = %#v", document["_id2"])
	}
	if document["_id"] != int32(7) {
		t.Fatalf("synthetic _id = %#v", document["_id"])
	}
}
