package sqlbatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/sqlbatch/dialects"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const DefaultWriteBatchSize = 100

func decodeJSONDocument(data []byte, columns map[string]dialects.ColumnMetadata, dialect SQLDialect) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if document == nil {
		return nil, fmt.Errorf("expected a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("expected one JSON object")
		}
		return nil, err
	}
	for key, value := range document {
		column, ok := columns[key]
		if !ok {
			return nil, fmt.Errorf("destination table has no column %q", key)
		}
		converted, err := dialect.ConvertJSONValue(column, value)
		if err != nil {
			return nil, err
		}
		document[key] = converted
	}
	for key, column := range columns {
		if _, exists := document[key]; exists || !column.Nullable || column.HasDefault || column.Generated || column.Identity {
			continue
		}
		converted, err := dialect.ConvertJSONValue(column, nil)
		if err != nil {
			return nil, err
		}
		document[key] = converted
	}
	return document, nil
}

func sqlValue(value any) any {
	switch value := value.(type) {
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return integer
		}
		return value.String()
	case bson.Binary:
		return value.Data
	case bson.Decimal128:
		return value.String()
	case bson.DateTime:
		return time.UnixMilli(int64(value))
	case bson.ObjectID:
		return value.Hex()
	case map[string]any, []any:
		encoded, err := json.Marshal(value)
		if err == nil {
			return string(encoded)
		}
	}
	return value
}

func numericJSONValue(value any) (json.Number, bool) {
	switch value := value.(type) {
	case int32:
		return json.Number(strconv.FormatInt(int64(value), 10)), true
	case int64:
		return json.Number(strconv.FormatInt(value, 10)), true
	case float64:
		return json.Number(strconv.FormatFloat(value, 'g', -1, 64)), true
	case bson.Decimal128:
		return json.Number(value.String()), true
	default:
		return "", false
	}
}

func documentValues(document map[string]any, columns []string) []any {
	values := make([]any, len(columns))
	for i, column := range columns {
		values[i] = document[column]
	}
	return values
}

func updateKeyValues(update *adiomv1.Update, metadata dialects.TableMetadata, dialect SQLDialect) ([]any, error) {
	keys := metadata.PrimaryKey
	columns := columnMetadataByName(metadata)
	parts := make(map[string]*adiomv1.BsonValue, len(update.GetId()))
	for _, part := range update.GetId() {
		if part.GetName() != "" {
			parts[part.GetName()] = part
		}
	}
	values := make([]any, len(keys))
	for i, key := range keys {
		column, exists := columns[key]
		if !exists {
			return nil, fmt.Errorf("primary key field %q is not present in table metadata", key)
		}
		part, exists := parts[key]
		if !exists {
			return nil, fmt.Errorf("update ID is missing key field %q", key)
		}
		var value any
		if err := bson.UnmarshalValue(bson.Type(part.GetType()), part.GetData(), &value); err != nil {
			return nil, fmt.Errorf("decode key field %q: %w", key, err)
		}
		if binary, ok := value.(bson.Binary); ok && binary.Subtype == bson.TypeBinaryUUIDOld &&
			(strings.EqualFold(column.DatabaseType, "uuid") || strings.EqualFold(column.DatabaseType, "uniqueidentifier")) {
			return nil, fmt.Errorf("legacy BSON UUID subtype 3 is not supported for key field %q", key)
		}
		if number, ok := numericJSONValue(value); ok {
			converted, err := dialect.ConvertJSONValue(column, number)
			if err != nil {
				return nil, fmt.Errorf("convert key field %q: %w", key, err)
			}
			value = converted
		} else {
			value = sqlValue(value)
			converted, err := dialect.ConvertJSONValue(column, value)
			if err != nil {
				return nil, fmt.Errorf("convert key field %q: %w", key, err)
			}
			value = converted
		}
		if value == nil {
			return nil, fmt.Errorf("update ID has null key field %q", key)
		}
		values[i] = value
	}
	return values, nil
}

func (c *conn) tableMetadata(ctx context.Context, namespace string) (dialects.TableMetadata, error) {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	if metadata, ok := c.sinkMetadata[namespace]; ok {
		return metadata, nil
	}
	metadata, err := c.dialect.InspectTable(ctx, c.c, namespace)
	if err != nil {
		return dialects.TableMetadata{}, fmt.Errorf("inspect destination table %q: %w", namespace, err)
	}
	if len(metadata.PrimaryKey) == 0 {
		return dialects.TableMetadata{}, fmt.Errorf("destination table %q has no primary key", namespace)
	}
	if c.sinkMetadata == nil {
		c.sinkMetadata = map[string]dialects.TableMetadata{}
	}
	c.sinkMetadata[namespace] = metadata
	return metadata, nil
}

func columnMetadataByName(metadata dialects.TableMetadata) map[string]dialects.ColumnMetadata {
	columns := make(map[string]dialects.ColumnMetadata, len(metadata.Columns))
	for _, column := range metadata.Columns {
		columns[column.Name] = column
	}
	return columns
}

type writeOperation struct {
	delete           bool
	overrideIdentity bool
	columns          []string
	columnTypes      []string
	defaultColumns   []string
	identityColumns  []string
	values           []any
	key              string
}

func numericDatabaseType(databaseType string) bool {
	switch strings.ToLower(databaseType) {
	case "tinyint", "smallint", "int", "integer", "bigint",
		"int2", "int4", "int8", "numeric", "decimal", "money", "smallmoney",
		"real", "float", "float4", "float8", "double precision",
		"number", "binary_float", "binary_double":
		return true
	default:
		return false
	}
}

func canonicalNumericKey(value any) (string, bool) {
	var text string
	switch value := value.(type) {
	case string:
		text = value
	case int:
		text = strconv.FormatInt(int64(value), 10)
	case int32:
		text = strconv.FormatInt(int64(value), 10)
	case int64:
		text = strconv.FormatInt(value, 10)
	case float32:
		text = strconv.FormatFloat(float64(value), 'g', -1, 32)
	case float64:
		text = strconv.FormatFloat(value, 'g', -1, 64)
	case json.Number:
		text = value.String()
	case interface{ String() (string, error) }:
		var err error
		text, err = value.String()
		if err != nil {
			return "", false
		}
	default:
		return "", false
	}
	if number, ok := new(big.Rat).SetString(text); ok {
		return number.RatString(), true
	}
	return "", false
}

func operationKey(values []any, columns []dialects.ColumnMetadata) string {
	normalized := make([]any, len(values))
	for i, value := range values {
		normalized[i] = value
		if i < len(columns) && numericDatabaseType(columns[i].DatabaseType) {
			if number, ok := canonicalNumericKey(value); ok {
				normalized[i] = []string{"numeric", number}
			}
		}
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return fmt.Sprint(normalized)
	}
	return string(encoded)
}

func upsertOperation(metadata dialects.TableMetadata, document map[string]any) (writeOperation, error) {
	keys := metadata.PrimaryKey
	columns := make([]string, 0, len(metadata.Columns))
	columnTypes := make([]string, 0, len(metadata.Columns))
	defaultColumns := make([]string, 0, len(metadata.Columns))
	identityColumns := make([]string, 0, len(metadata.Columns))
	keyValues := make([]any, len(keys))
	keyColumns := make([]dialects.ColumnMetadata, len(keys))
	overrideIdentity := false
	for i, key := range keys {
		var keyMetadata *dialects.ColumnMetadata
		for columnIndex := range metadata.Columns {
			if metadata.Columns[columnIndex].Name == key {
				keyMetadata = &metadata.Columns[columnIndex]
				break
			}
		}
		if keyMetadata == nil {
			return writeOperation{}, fmt.Errorf("primary key field %q is not present in table metadata", key)
		}
		if keyMetadata.Generated && !keyMetadata.Identity {
			return writeOperation{}, fmt.Errorf("generated primary key field %q cannot be written explicitly", key)
		}
		value, exists := document[key]
		if !exists || value == nil {
			return writeOperation{}, fmt.Errorf("document is missing non-null key field %q", key)
		}
		columns = append(columns, key)
		columnTypes = append(columnTypes, keyMetadata.DatabaseType)
		keyValues[i] = value
		keyColumns[i] = *keyMetadata
		if keyMetadata.Identity {
			identityColumns = append(identityColumns, key)
			overrideIdentity = overrideIdentity || strings.EqualFold(keyMetadata.IdentityGeneration, "ALWAYS")
		}
	}
	for _, column := range metadata.Columns {
		if slices.Contains(keys, column.Name) {
			continue
		}
		value, exists := document[column.Name]
		if column.Generated && !column.Identity {
			if exists {
				return writeOperation{}, fmt.Errorf("generated column %q cannot be written explicitly", column.Name)
			}
			continue
		}
		if column.Identity {
			if !exists {
				continue
			}
			if value == nil {
				return writeOperation{}, fmt.Errorf("identity column %q cannot be null", column.Name)
			}
			columns = append(columns, column.Name)
			columnTypes = append(columnTypes, column.DatabaseType)
			identityColumns = append(identityColumns, column.Name)
			overrideIdentity = overrideIdentity || strings.EqualFold(column.IdentityGeneration, "ALWAYS")
			continue
		}
		if exists {
			if value == nil && !column.Nullable {
				return writeOperation{}, fmt.Errorf("column %q is required and cannot be null", column.Name)
			}
			columns = append(columns, column.Name)
			columnTypes = append(columnTypes, column.DatabaseType)
			continue
		}
		if column.HasDefault {
			defaultColumns = append(defaultColumns, column.Name)
			continue
		}
		if !column.Nullable {
			return writeOperation{}, fmt.Errorf("document is missing required column %q", column.Name)
		}
		document[column.Name] = nil
		columns = append(columns, column.Name)
		columnTypes = append(columnTypes, column.DatabaseType)
	}
	return writeOperation{
		overrideIdentity: overrideIdentity,
		columns:          columns,
		columnTypes:      columnTypes,
		defaultColumns:   defaultColumns,
		identityColumns:  identityColumns,
		values:           documentValues(document, columns),
		key:              operationKey(keyValues, keyColumns),
	}, nil
}

func validateIdentityOverride(dialect SQLDialect, operation writeOperation) error {
	if operation.overrideIdentity && !dialect.SupportsIdentityOverride() {
		return fmt.Errorf("destination does not support explicitly writing GENERATED ALWAYS identity columns %v", operation.identityColumns)
	}
	return nil
}

func writeBatchSize(configured, maxParameters, parametersPerRow int) (int, error) {
	if configured < 0 {
		return 0, fmt.Errorf("configured write batch size cannot be negative: %d", configured)
	}
	batchSize := configured
	usingDefault := batchSize == 0
	if usingDefault {
		batchSize = DefaultWriteBatchSize
	}
	if maxParameters < 1 || parametersPerRow < 1 {
		return batchSize, nil
	}
	maxRows := maxParameters / parametersPerRow
	if maxRows < 1 {
		return 0, fmt.Errorf("write requires %d parameters per row, exceeding the destination limit of %d", parametersPerRow, maxParameters)
	}
	if batchSize > maxRows {
		if !usingDefault {
			return 0, fmt.Errorf("configured write batch size %d exceeds the destination maximum of %d rows for writes with %d parameters per row", batchSize, maxRows, parametersPerRow)
		}
		batchSize = maxRows
	}
	return batchSize, nil
}

func (c *conn) executeWriteOperations(ctx context.Context, namespace string, keys []string, operations []writeOperation) error {
	// Flushes are independent database statements. If a later flush fails, an
	// error is returned but earlier flushes may already have committed.
	var pending []writeOperation
	seen := map[string]struct{}{}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		var query string
		if pending[0].delete {
			query = c.dialect.DeleteQuery(namespace, keys, len(pending))
		} else {
			var err error
			query, err = c.dialect.UpsertQuery(namespace, keys, pending[0].columns, pending[0].columnTypes, pending[0].defaultColumns, pending[0].identityColumns, len(pending), pending[0].overrideIdentity)
			if err != nil {
				return err
			}
		}
		var values []any
		for _, operation := range pending {
			values = append(values, operation.values...)
		}
		if _, err := c.c.ExecContext(ctx, query, values...); err != nil {
			return err
		}
		pending = nil
		seen = map[string]struct{}{}
		return nil
	}
	for _, operation := range operations {
		batchSize, err := writeBatchSize(c.config.Sink.writeBatchSize(namespace), c.dialect.MaxQueryParameters(), len(operation.values))
		if err != nil {
			return err
		}
		_, duplicate := seen[operation.key]
		incompatible := len(pending) > 0 && (pending[0].delete != operation.delete ||
			pending[0].overrideIdentity != operation.overrideIdentity ||
			!slices.Equal(pending[0].columns, operation.columns) ||
			!slices.Equal(pending[0].columnTypes, operation.columnTypes) ||
			!slices.Equal(pending[0].defaultColumns, operation.defaultColumns) ||
			!slices.Equal(pending[0].identityColumns, operation.identityColumns))
		if len(pending) >= batchSize || incompatible || duplicate {
			if err := flush(); err != nil {
				return err
			}
		}
		pending = append(pending, operation)
		seen[operation.key] = struct{}{}
	}
	return flush()
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	if r.Msg.GetPartialWriteResults() {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partial write results are not supported by sqlbatch"))
	}
	namespace := r.Msg.GetNamespace()
	metadata, err := c.tableMetadata(ctx, namespace)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	columns := columnMetadataByName(metadata)
	operations := make([]writeOperation, 0, len(r.Msg.GetData()))
	for i, data := range r.Msg.GetData() {
		document, err := decodeJSONDocument(data, columns, c.dialect)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("decode document %d: %w", i, err))
		}
		operation, err := upsertOperation(metadata, document)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("document %d: %w", i, err))
		}
		if err := validateIdentityOverride(c.dialect, operation); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("document %d: %w", i, err))
		}
		operations = append(operations, operation)
	}
	if err := c.executeWriteOperations(ctx, namespace, metadata.PrimaryKey, operations); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	if r.Msg.GetPartialWriteResults() {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partial write results are not supported by sqlbatch"))
	}
	namespace := r.Msg.GetNamespace()
	metadata, err := c.tableMetadata(ctx, namespace)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	columns := columnMetadataByName(metadata)
	keyColumns := make([]dialects.ColumnMetadata, len(metadata.PrimaryKey))
	for i, key := range metadata.PrimaryKey {
		keyColumns[i] = columns[key]
	}
	for _, update := range r.Msg.GetUpdates() {
		if update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partial updates are not supported by sqlbatch"))
		}
	}

	operations := make([]writeOperation, 0, len(r.Msg.GetUpdates()))
	for i, update := range r.Msg.GetUpdates() {
		if update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_UNKNOWN {
			continue
		}
		keyValues, err := updateKeyValues(update, metadata, c.dialect)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: %w", i, err))
		}
		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			document, err := decodeJSONDocument(update.GetData(), columns, c.dialect)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("decode update %d: %w", i, err))
			}
			for keyIndex, key := range metadata.PrimaryKey {
				document[key] = keyValues[keyIndex]
			}
			operation, err := upsertOperation(metadata, document)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: %w", i, err))
			}
			if err := validateIdentityOverride(c.dialect, operation); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: %w", i, err))
			}
			operations = append(operations, operation)
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			operations = append(operations, writeOperation{delete: true, values: keyValues, key: operationKey(keyValues, keyColumns)})
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unsupported update type %v", update.GetType()))
		}
	}
	if err := c.executeWriteOperations(ctx, namespace, metadata.PrimaryKey, operations); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}
