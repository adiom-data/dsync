/*
 * Copyright (C) 2025 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package file

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This file holds the value encoding used by the CSV sink and the matching
// type inference used by the CSV source. The two are designed together so
// that an export followed by an import restores the original values for every
// type with a natural CSV form. Known lossy cases:
//   - null, missing and empty string all read back as missing;
//   - strings that look like numbers, booleans, ObjectIDs, dates or JSON come
//     back typed when inference is on (including a string _id such as "42");
//   - int64 values that fit in int32 come back as int32, at the top level and
//     inside nested documents (relaxed Extended JSON);
//   - "\r\n" inside a value reads back as "\n" (encoding/csv normalizes it);
//   - an embedded document whose keys are Extended JSON keywords (e.g.
//     {"$numberLong":"5"}) is read back as the value it spells, or as a
//     string if it is not valid Extended JSON.
//
// Writer (cell encoding):
//   string               as is
//   int32 / int64        decimal
//   double               shortest round-trip form, integral values keep ".0";
//                        NaN, Infinity, -Infinity
//   bool                 true / false
//   null                 empty cell
//   ObjectID             24-char lowercase hex
//   date                 ISO-8601 UTC with milliseconds (years 0000-9999)
//   document / array     relaxed Extended JSON (BSON) or compact JSON (JSON)
//   everything else      relaxed Extended JSON, e.g. {"$numberDecimal":"1.5"}
//
// Reader (inference, when enabled): the inverse of the above.

const (
	idField      = "_id" // identifier in MONGO_BSON documents
	jsonIDField  = "id"  // identifier in JSON_ID documents
	dateLayout   = "2006-01-02T15:04:05.000Z07:00"
	extJSONField = "v"
)

var (
	intPattern      = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
	floatPattern    = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
	objectIDPattern = regexp.MustCompile(`^[0-9a-f]{24}$`)
	// legacyObjectIDPattern matches ObjectID("...") as written by older versions of this connector.
	legacyObjectIDPattern = regexp.MustCompile(`^ObjectI[Dd]\("([0-9a-fA-F]{24})"\)$`)
	datePattern           = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$`)
)

// idColumnFor returns the CSV column that holds the document identifier for a data type.
func idColumnFor(dataType adiomv1.DataType) string {
	if dataType == adiomv1.DataType_DATA_TYPE_MONGO_BSON {
		return idField
	}
	return jsonIDField
}

// encodedDoc is a document converted to CSV cells, keyed by column name.
// A key with an empty cell is still a column (e.g. a null value).
type encodedDoc map[string]string

// encodeDocument converts a source document into CSV cells.
func encodeDocument(data []byte, dataType adiomv1.DataType) (encodedDoc, error) {
	switch dataType {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		return encodeBSONDocument(data)
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		return encodeJSONDocument(data)
	default:
		return nil, fmt.Errorf("%w: got %v", ErrUnsupportedType, dataType)
	}
}

func encodeBSONDocument(data []byte) (encodedDoc, error) {
	raw := bson.Raw(data)
	elems, err := raw.Elements()
	if err != nil {
		return nil, fmt.Errorf("failed to parse input data as BSON: %w", err)
	}
	doc := make(encodedDoc, len(elems))
	for _, elem := range elems {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, fmt.Errorf("failed to parse input data as BSON: %w", err)
		}
		val, err := elem.ValueErr()
		if err != nil {
			return nil, fmt.Errorf("failed to parse BSON value of field %q: %w", key, err)
		}
		cell, err := encodeBSONValue(val)
		if err != nil {
			return nil, fmt.Errorf("failed to encode field %q: %w", key, err)
		}
		doc[key] = cell
	}
	return doc, nil
}

func encodeBSONValue(val bson.RawValue) (string, error) {
	switch val.Type {
	case bson.TypeString:
		return val.StringValue(), nil
	case bson.TypeInt32:
		return strconv.FormatInt(int64(val.Int32()), 10), nil
	case bson.TypeInt64:
		return strconv.FormatInt(val.Int64(), 10), nil
	case bson.TypeDouble:
		return formatDouble(val.Double()), nil
	case bson.TypeBoolean:
		return strconv.FormatBool(val.Boolean()), nil
	case bson.TypeNull:
		return "", nil
	case bson.TypeObjectID:
		return val.ObjectID().Hex(), nil
	case bson.TypeDateTime:
		t := time.UnixMilli(val.DateTime()).UTC()
		if t.Year() >= 0 && t.Year() <= 9999 {
			return t.Format(dateLayout), nil
		}
		// Not representable as ISO-8601 with a 4-digit year; keep it lossless.
		return extJSONValue(val)
	default:
		// Documents, arrays and BSON-specific types (Decimal128, Binary, Timestamp,
		// Regex, MinKey/MaxKey, JavaScript, Undefined, ...).
		return extJSONValue(val)
	}
}

// extJSONValue renders a single BSON value as relaxed Extended JSON.
func extJSONValue(val bson.RawValue) (string, error) {
	out, err := bson.MarshalExtJSON(bson.D{{Key: extJSONField, Value: val}}, false, false)
	if err != nil {
		return "", err
	}
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(out, &wrapper); err != nil {
		return "", err
	}
	return string(wrapper[extJSONField]), nil
}

func formatDouble(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0" // keep doubles distinguishable from integers
	}
	return s
}

func encodeJSONDocument(data []byte) (encodedDoc, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("failed to parse input data as JSON: %w", err)
	}
	doc := make(encodedDoc, len(fields))
	for key, raw := range fields {
		cell, err := encodeJSONValue(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to encode field %q: %w", key, err)
		}
		doc[key] = cell
	}
	return doc, nil
}

func encodeJSONValue(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return "", nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	case 'n':
		return "", nil
	case '{', '[':
		var buf bytes.Buffer
		if err := json.Compact(&buf, raw); err != nil {
			return "", err
		}
		return buf.String(), nil
	default:
		// numbers (kept verbatim, so precision is preserved) and booleans
		return string(raw), nil
	}
}

// rowDecoder turns CSV rows into documents of the requested type.
type rowDecoder struct {
	header      []string
	dataType    adiomv1.DataType
	inferTypes  bool
	idColumn    int // index of the column that becomes the identifier, -1 if none
	outputNames []string
}

func newRowDecoder(header []string, dataType adiomv1.DataType, inferTypes bool) (*rowDecoder, error) {
	if dataType != adiomv1.DataType_DATA_TYPE_MONGO_BSON && dataType != adiomv1.DataType_DATA_TYPE_JSON_ID {
		return nil, fmt.Errorf("%w: requested type %v", ErrUnsupportedType, dataType)
	}
	seen := make(map[string]bool, len(header))
	for _, col := range header {
		if seen[col] {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, col)
		}
		seen[col] = true
	}

	// The identifier column is the one named for the requested type; if the file
	// only has the other name, that one is used instead. When both exist the
	// other one stays an ordinary field, so nothing is lost.
	idName := idColumnFor(dataType)
	otherName := jsonIDField
	if idName == jsonIDField {
		otherName = idField
	}
	d := &rowDecoder{
		header:      header,
		dataType:    dataType,
		inferTypes:  inferTypes,
		idColumn:    -1,
		outputNames: append([]string(nil), header...),
	}
	for i, col := range header {
		if col == idName {
			d.idColumn = i
		}
	}
	if d.idColumn == -1 {
		for i, col := range header {
			if col == otherName {
				d.idColumn = i
				d.outputNames[i] = idName
			}
		}
	}
	return d, nil
}

func (d *rowDecoder) decode(row []string) ([]byte, error) {
	if d.dataType == adiomv1.DataType_DATA_TYPE_MONGO_BSON {
		return d.decodeBSON(row)
	}
	return d.decodeJSON(row)
}

func (d *rowDecoder) decodeBSON(row []string) ([]byte, error) {
	doc := make(bson.D, 0, len(d.header))
	if d.idColumn >= 0 && d.idColumn < len(row) && row[d.idColumn] != "" {
		v, err := d.inferBSON(row[d.idColumn])
		if err != nil {
			return nil, err
		}
		doc = append(doc, bson.E{Key: idField, Value: v})
	}
	for i, cell := range row {
		if i >= len(d.header) || i == d.idColumn || cell == "" {
			continue
		}
		v, err := d.inferBSON(cell)
		if err != nil {
			return nil, err
		}
		doc = append(doc, bson.E{Key: d.outputNames[i], Value: v})
	}
	return bson.Marshal(doc)
}

func (d *rowDecoder) inferBSON(cell string) (any, error) {
	if !d.inferTypes {
		return cell, nil
	}
	switch cell {
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "NaN":
		return math.NaN(), nil
	case "Infinity":
		return math.Inf(1), nil
	case "-Infinity":
		return math.Inf(-1), nil
	}
	if intPattern.MatchString(cell) {
		n, err := strconv.ParseInt(cell, 10, 64)
		if err != nil {
			return cell, nil // out of int64 range: keep as string
		}
		if n >= math.MinInt32 && n <= math.MaxInt32 {
			return int32(n), nil
		}
		return n, nil
	}
	if floatPattern.MatchString(cell) {
		if f, err := strconv.ParseFloat(cell, 64); err == nil {
			return f, nil
		}
		return cell, nil
	}
	if objectIDPattern.MatchString(cell) {
		if oid, err := bson.ObjectIDFromHex(cell); err == nil {
			return oid, nil
		}
	}
	if m := legacyObjectIDPattern.FindStringSubmatch(cell); m != nil {
		if oid, err := bson.ObjectIDFromHex(m[1]); err == nil {
			return oid, nil
		}
	}
	if datePattern.MatchString(cell) {
		if t, err := time.Parse(time.RFC3339Nano, cell); err == nil {
			return bson.NewDateTimeFromTime(t), nil
		}
	}
	if cell[0] == '{' || cell[0] == '[' {
		var wrapper bson.Raw
		if err := bson.UnmarshalExtJSON([]byte(`{"`+extJSONField+`":`+cell+`}`), false, &wrapper); err == nil {
			return wrapper.Lookup(extJSONField), nil
		}
	}
	return cell, nil
}

func (d *rowDecoder) decodeJSON(row []string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	write := func(name, cell string) error {
		key, err := json.Marshal(name)
		if err != nil {
			return err
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(d.inferJSON(cell))
		return nil
	}
	if d.idColumn >= 0 && d.idColumn < len(row) && row[d.idColumn] != "" {
		if err := write(jsonIDField, row[d.idColumn]); err != nil {
			return nil, err
		}
	}
	for i, cell := range row {
		if i >= len(d.header) || i == d.idColumn || cell == "" {
			continue
		}
		if err := write(d.outputNames[i], cell); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// inferJSON returns the JSON encoding of a cell.
func (d *rowDecoder) inferJSON(cell string) []byte {
	if d.inferTypes {
		switch {
		case cell == "true" || cell == "false":
			return []byte(cell)
		case intPattern.MatchString(cell):
			return []byte(cell) // JSON numbers have no range limit
		case floatPattern.MatchString(cell):
			if _, err := strconv.ParseFloat(cell, 64); err == nil {
				return []byte(cell)
			}
		case cell[0] == '{' || cell[0] == '[':
			if json.Valid([]byte(cell)) {
				var buf bytes.Buffer
				if json.Compact(&buf, []byte(cell)) == nil {
					return buf.Bytes()
				}
			}
		}
	}
	out, _ := json.Marshal(cell) // marshalling a string cannot fail
	return out
}
