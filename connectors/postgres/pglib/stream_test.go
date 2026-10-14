package pglib

import (
	"reflect"
	"testing"
)

func TestFillUnchangedValues(t *testing.T) {
	newValues := map[string]interface{}{"id": int32(1), "name": "new", "toasted": unchangedToastValue{}}
	oldValues := map[string]interface{}{"id": int32(1), "name": "old", "toasted": "large value"}

	fillUnchangedValues(newValues, oldValues)

	want := map[string]interface{}{"id": int32(1), "name": "new", "toasted": "large value"}
	if !reflect.DeepEqual(newValues, want) {
		t.Fatalf("values = %#v, want %#v", newValues, want)
	}
}

func TestUnchangedToastColumns(t *testing.T) {
	values := map[string]interface{}{"id": int32(1), "toasted": unchangedToastValue{}}
	got := unchangedToastColumns(values)
	want := []string{"toasted"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
}
