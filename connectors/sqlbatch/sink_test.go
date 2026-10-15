package sqlbatch

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/adiom-data/dsync/connectors/sqlbatch/dialects"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/jackc/pgx/v5/pgtype"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDecodeJSONDocumentPreservesIntegerAndDecimalText(t *testing.T) {
	columns := map[string]dialects.ColumnMetadata{
		"id":     {Name: "id", DatabaseType: "int8"},
		"amount": {Name: "amount", DatabaseType: "numeric"},
		"nested": {Name: "nested", DatabaseType: "jsonb"},
	}
	document, err := decodeJSONDocument([]byte(`{"id":9007199254740993,"amount":1234567890.0123456789,"nested":{"ok":true}}`), columns, dialects.Postgres{})
	if err != nil {
		t.Fatal(err)
	}
	if document["id"] != int64(9007199254740993) {
		t.Fatalf("id = %#v", document["id"])
	}
	if document["amount"] != "1234567890.0123456789" {
		t.Fatalf("amount = %#v", document["amount"])
	}
	if document["nested"] != `{"ok":true}` {
		t.Fatalf("nested = %#v", document["nested"])
	}
}

func TestUpsertOperationUsesKeysThenPhysicalColumnOrder(t *testing.T) {
	document := map[string]any{"name": "value", "id": int64(2), "tenant": "a", "active": true}
	metadata := dialects.TableMetadata{
		Columns: []dialects.ColumnMetadata{
			{Name: "id"},
			{Name: "tenant"},
			{Name: "name", Nullable: true},
			{Name: "active", Nullable: true},
		},
		PrimaryKey: []string{"tenant", "id"},
	}
	operation, err := upsertOperation(metadata, document)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tenant", "id", "name", "active"}
	if !reflect.DeepEqual(operation.columns, want) {
		t.Fatalf("columns = %#v, want %#v", operation.columns, want)
	}
}

func TestUpsertOperationAppliesDefaultAndNullForOmittedColumns(t *testing.T) {
	metadata := dialects.TableMetadata{
		Columns: []dialects.ColumnMetadata{
			{Name: "id"},
			{Name: "status", HasDefault: true},
			{Name: "note", Nullable: true},
		},
		PrimaryKey: []string{"id"},
	}
	operation, err := upsertOperation(metadata, map[string]any{"id": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(operation.columns, []string{"id", "note"}) {
		t.Fatalf("columns = %#v", operation.columns)
	}
	if !reflect.DeepEqual(operation.defaultColumns, []string{"status"}) {
		t.Fatalf("default columns = %#v", operation.defaultColumns)
	}
	if !reflect.DeepEqual(operation.values, []any{int64(1), nil}) {
		t.Fatalf("values = %#v", operation.values)
	}
}

func TestUpdateKeyValuesUsesConfiguredOrder(t *testing.T) {
	value := func(name string, value any) *adiomv1.BsonValue {
		t.Helper()
		typ, data, err := bson.MarshalValue(value)
		if err != nil {
			t.Fatal(err)
		}
		return &adiomv1.BsonValue{Name: name, Type: uint32(typ), Data: data}
	}
	update := &adiomv1.Update{Id: []*adiomv1.BsonValue{value("id", int32(7)), value("tenant", "a")}}
	metadata := dialects.TableMetadata{
		Columns:    []dialects.ColumnMetadata{{Name: "tenant"}, {Name: "id"}},
		PrimaryKey: []string{"tenant", "id"},
	}
	values, err := updateKeyValues(update, metadata, dialects.Postgres{})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"a", int32(7)}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %#v, want %#v", values, want)
	}
}

func TestUpdateKeyValuesRejectsMissingKey(t *testing.T) {
	metadata := dialects.TableMetadata{
		Columns:    []dialects.ColumnMetadata{{Name: "id"}},
		PrimaryKey: []string{"id"},
	}
	_, err := updateKeyValues(&adiomv1.Update{}, metadata, dialects.Postgres{})
	if err == nil {
		t.Fatal("expected missing-key error")
	}
}

func TestUpdateKeyValuesConvertsPostgresUUID(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	typ, encoded, err := bson.MarshalValue(bson.Binary{Subtype: bson.TypeBinaryUUID, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	metadata := dialects.TableMetadata{
		Columns:    []dialects.ColumnMetadata{{Name: "id", DatabaseType: "uuid"}},
		PrimaryKey: []string{"id"},
	}
	values, err := updateKeyValues(&adiomv1.Update{Id: []*adiomv1.BsonValue{{
		Name: "id", Type: uint32(typ), Data: encoded,
	}}}, metadata, dialects.Postgres{})
	if err != nil {
		t.Fatal(err)
	}
	want := pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}
	if !reflect.DeepEqual(values, []any{want}) {
		t.Fatalf("values = %#v, want %#v", values, []any{want})
	}
}

func TestUpdateKeyValuesRejectsLegacyUUID(t *testing.T) {
	typ, encoded, err := bson.MarshalValue(bson.Binary{
		Subtype: bson.TypeBinaryUUIDOld,
		Data:    []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := dialects.TableMetadata{
		Columns:    []dialects.ColumnMetadata{{Name: "id", DatabaseType: "uuid"}},
		PrimaryKey: []string{"id"},
	}
	_, err = updateKeyValues(&adiomv1.Update{Id: []*adiomv1.BsonValue{{
		Name: "id", Type: uint32(typ), Data: encoded,
	}}}, metadata, dialects.Postgres{})
	if err == nil {
		t.Fatal("expected legacy UUID subtype to be rejected")
	}
}

func TestUpsertOperationRequiresCompleteGeneratedKey(t *testing.T) {
	metadata := dialects.TableMetadata{
		Columns: []dialects.ColumnMetadata{{
			Name:               "id",
			Identity:           true,
			IdentityGeneration: "ALWAYS",
		}},
		PrimaryKey: []string{"id"},
	}
	_, err := upsertOperation(metadata, map[string]any{})
	if err == nil {
		t.Fatal("expected missing-key error")
	}
	operation, err := upsertOperation(metadata, map[string]any{"id": int64(7)})
	if err != nil {
		t.Fatal(err)
	}
	if !operation.overrideIdentity {
		t.Fatal("expected identity override")
	}
}

func TestUpsertOperationRejectsComputedPrimaryKey(t *testing.T) {
	metadata := dialects.TableMetadata{
		Columns:    []dialects.ColumnMetadata{{Name: "id", Generated: true}},
		PrimaryKey: []string{"id"},
	}
	_, err := upsertOperation(metadata, map[string]any{"id": int64(7)})
	if err == nil {
		t.Fatal("expected generated-key error")
	}
}

func TestUpsertOperationRejectsSuppliedGeneratedNonKey(t *testing.T) {
	for _, column := range []dialects.ColumnMetadata{
		{Name: "computed", Generated: true},
		{Name: "generated_id", Identity: true},
	} {
		metadata := dialects.TableMetadata{
			Columns:    []dialects.ColumnMetadata{{Name: "id"}, column},
			PrimaryKey: []string{"id"},
		}
		_, err := upsertOperation(metadata, map[string]any{
			"id":        int64(1),
			column.Name: "source value",
		})
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf(`column %q is generated by the destination`, column.Name)) {
			t.Fatalf("expected generated-column error for %q, got %v", column.Name, err)
		}
	}
}

func TestUpsertOperationAllowsOmittedGeneratedNonKey(t *testing.T) {
	metadata := dialects.TableMetadata{
		Columns: []dialects.ColumnMetadata{
			{Name: "id"},
			{Name: "generated_id", Identity: true},
		},
		PrimaryKey: []string{"id"},
	}
	if _, err := upsertOperation(metadata, map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("unexpected error for omitted generated column: %v", err)
	}
}

func TestWriteBatchSizeUsesDefaultAndDestinationCap(t *testing.T) {
	tests := []struct {
		name             string
		configured       int
		maxParameters    int
		parametersPerRow int
		want             int
	}{
		{name: "default", maxParameters: 65535, parametersPerRow: 3, want: DefaultWriteBatchSize},
		{name: "configured", configured: 25, maxParameters: 65535, parametersPerRow: 3, want: 25},
		{name: "default destination cap", maxParameters: 2000, parametersPerRow: 30, want: 66},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := writeBatchSize(test.configured, test.maxParameters, test.parametersPerRow)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("batch size = %d, want %d", got, test.want)
			}
		})
	}
	if _, err := writeBatchSize(100, 2000, 30); err == nil {
		t.Fatal("expected configured batch size above destination limit to fail")
	}
}
