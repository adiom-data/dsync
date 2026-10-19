package sqlbatch

import (
	"encoding/json"
	"reflect"
	"slices"
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

func TestDecodeJSONDocumentNormalizesBatchedDecimalValues(t *testing.T) {
	tests := []struct {
		name       string
		dialect    SQLDialect
		columnType string
	}{
		{name: "SQL Server", dialect: dialects.SQLServer{}, columnType: "decimal"},
		{name: "Oracle", dialect: dialects.Oracle{}, columnType: "NUMBER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			columns := map[string]dialects.ColumnMetadata{
				"amount": {Name: "amount", DatabaseType: test.columnType},
			}
			var valueType reflect.Type
			for _, item := range []struct {
				input string
				want  string
			}{
				{input: `{"amount":1}`, want: "1"},
				{input: `{"amount":1.5}`, want: "1.5"},
			} {
				document, err := decodeJSONDocument([]byte(item.input), columns, test.dialect)
				if err != nil {
					t.Fatal(err)
				}
				value := document["amount"]
				if valueType == nil {
					valueType = reflect.TypeOf(value)
				} else if reflect.TypeOf(value) != valueType {
					t.Fatalf("amount type = %T, want %v", value, valueType)
				}
				var text string
				switch value := value.(type) {
				case string:
					text = value
				case interface{ String() (string, error) }:
					text, err = value.String()
					if err != nil {
						t.Fatal(err)
					}
				default:
					t.Fatalf("amount = %#v (%T), want exact decimal representation", value, value)
				}
				if text != item.want {
					t.Fatalf("amount = %q, want %q", text, item.want)
				}
			}
		})
	}
}

func TestDecodeJSONDocumentDoesNotSynthesizeGeneratedOrIdentityColumns(t *testing.T) {
	columns := map[string]dialects.ColumnMetadata{
		"id":       {Name: "id", DatabaseType: "bigint"},
		"computed": {Name: "computed", DatabaseType: "bigint", Nullable: true, Generated: true},
		"sequence": {Name: "sequence", DatabaseType: "bigint", Identity: true},
	}
	document, err := decodeJSONDocument([]byte(`{"id":1}`), columns, dialects.SQLServer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := document["computed"]; exists {
		t.Fatal("omitted generated column was synthesized")
	}
	if _, exists := document["sequence"]; exists {
		t.Fatal("omitted identity column was synthesized")
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
		Columns: []dialects.ColumnMetadata{
			{Name: "tenant", DatabaseType: "text"},
			{Name: "id", DatabaseType: "int8"},
		},
		PrimaryKey: []string{"tenant", "id"},
	}
	values, err := updateKeyValues(update, metadata, dialects.Postgres{})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"a", int64(7)}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %#v, want %#v", values, want)
	}
}

func TestUpdateKeyValuesRejectsMissingKey(t *testing.T) {
	metadata := dialects.TableMetadata{Columns: []dialects.ColumnMetadata{{Name: "id", DatabaseType: "int8"}}, PrimaryKey: []string{"id"}}
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

func TestUpdateKeyValuesNormalizesNumericRepresentations(t *testing.T) {
	bsonValue := func(value any) *adiomv1.BsonValue {
		t.Helper()
		typ, data, err := bson.MarshalValue(value)
		if err != nil {
			t.Fatal(err)
		}
		return &adiomv1.BsonValue{Name: "id", Type: uint32(typ), Data: data}
	}
	decimalValue, err := bson.ParseDecimal128("1.5")
	if err != nil {
		t.Fatal(err)
	}
	updates := []*adiomv1.Update{
		{Id: []*adiomv1.BsonValue{bsonValue(int64(1))}},
		{Id: []*adiomv1.BsonValue{bsonValue(decimalValue)}},
	}
	tests := []struct {
		name       string
		dialect    SQLDialect
		columnType string
	}{
		{name: "SQL Server", dialect: dialects.SQLServer{}, columnType: "decimal"},
		{name: "Oracle", dialect: dialects.Oracle{}, columnType: "NUMBER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := dialects.TableMetadata{Columns: []dialects.ColumnMetadata{{Name: "id", DatabaseType: test.columnType}}, PrimaryKey: []string{"id"}}
			var valueType reflect.Type
			for _, update := range updates {
				values, err := updateKeyValues(update, metadata, test.dialect)
				if err != nil {
					t.Fatal(err)
				}
				if valueType == nil {
					valueType = reflect.TypeOf(values[0])
				} else if reflect.TypeOf(values[0]) != valueType {
					t.Fatalf("key type = %T, want %v", values[0], valueType)
				}
			}
		})
	}
}

func TestOperationKeyDistinguishesOracleNumbers(t *testing.T) {
	dialect := dialects.Oracle{}
	column := dialects.ColumnMetadata{Name: "id", DatabaseType: "NUMBER"}
	one, err := dialect.ConvertJSONValue(column, json.Number("1"))
	if err != nil {
		t.Fatal(err)
	}
	two, err := dialect.ConvertJSONValue(column, json.Number("2"))
	if err != nil {
		t.Fatal(err)
	}
	columns := []dialects.ColumnMetadata{{Name: "id", DatabaseType: "NUMBER"}}
	if operationKey([]any{one}, columns) == operationKey([]any{two}, columns) {
		t.Fatal("distinct Oracle numbers produced the same operation key")
	}
}

func TestOperationKeyCanonicalizesNumericRepresentationsOnly(t *testing.T) {
	numeric := []dialects.ColumnMetadata{{Name: "id", DatabaseType: "decimal"}}
	if operationKey([]any{"1"}, numeric) != operationKey([]any{"1.0"}, numeric) {
		t.Fatal("equivalent decimal keys should produce the same operation key")
	}
	if operationKey([]any{"1"}, numeric) == operationKey([]any{"2"}, numeric) {
		t.Fatal("distinct decimal keys should produce different operation keys")
	}
	text := []dialects.ColumnMetadata{{Name: "id", DatabaseType: "nvarchar"}}
	if operationKey([]any{"A"}, text) == operationKey([]any{"a"}, text) {
		t.Fatal("text keys should retain exact connector-side comparison")
	}
}

func TestUpsertOperationRejectsSuppliedGeneratedColumn(t *testing.T) {
	metadata := dialects.TableMetadata{
		Columns: []dialects.ColumnMetadata{
			{Name: "id"},
			{Name: "computed", Generated: true},
		},
		PrimaryKey: []string{"id"},
	}
	if _, err := upsertOperation(metadata, map[string]any{"id": int64(1), "computed": int64(2)}); err == nil {
		t.Fatal("expected supplied generated column to be rejected")
	}
	operation, err := upsertOperation(metadata, map[string]any{"id": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(operation.columns, "computed") {
		t.Fatal("omitted generated column should not be written")
	}
}

func TestUpsertOperationIncludesSuppliedNonKeyIdentity(t *testing.T) {
	metadata := dialects.TableMetadata{
		Columns: []dialects.ColumnMetadata{
			{Name: "id"},
			{Name: "sequence", Identity: true, IdentityGeneration: "ALWAYS"},
		},
		PrimaryKey: []string{"id"},
	}
	operation, err := upsertOperation(metadata, map[string]any{"id": int64(1), "sequence": int64(42)})
	if err != nil {
		t.Fatal(err)
	}
	if !operation.overrideIdentity || !reflect.DeepEqual(operation.identityColumns, []string{"sequence"}) {
		t.Fatalf("identity operation = %#v", operation)
	}
	if !reflect.DeepEqual(operation.columns, []string{"id", "sequence"}) {
		t.Fatalf("columns = %#v", operation.columns)
	}
}

func TestValidateIdentityOverride(t *testing.T) {
	operation := writeOperation{overrideIdentity: true, identityColumns: []string{"id"}}
	if err := validateIdentityOverride(dialects.Oracle{}, operation); err == nil {
		t.Fatal("expected Oracle identity override to be rejected")
	}
	if err := validateIdentityOverride(dialects.Postgres{}, operation); err != nil {
		t.Fatalf("Postgres identity override: %v", err)
	}
	if err := validateIdentityOverride(dialects.SQLServer{}, operation); err != nil {
		t.Fatalf("SQL Server identity override: %v", err)
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
