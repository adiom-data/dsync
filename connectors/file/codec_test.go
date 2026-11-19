/*
 * Copyright (C) 2025 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package file

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// --- helpers ---

func newSink(t *testing.T) (adiomv1connect.ConnectorServiceHandler, string) {
	t.Helper()
	dir := t.TempDir()
	conn, err := NewConn(ConnectorSettings{Uri: "file://" + dir})
	require.NoError(t, err)
	return conn, dir
}

func writeBSON(t *testing.T, conn adiomv1connect.ConnectorServiceHandler, ns string, docs ...any) {
	t.Helper()
	data := make([][]byte, 0, len(docs))
	for _, d := range docs {
		b, err := bson.Marshal(d)
		require.NoError(t, err)
		data = append(data, b)
	}
	_, err := conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: ns, Data: data, Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}))
	require.NoError(t, err)
}

func writeJSON(t *testing.T, conn adiomv1connect.ConnectorServiceHandler, ns string, docs ...string) {
	t.Helper()
	data := make([][]byte, 0, len(docs))
	for _, d := range docs {
		data = append(data, []byte(d))
	}
	_, err := conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: ns, Data: data, Type: adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	require.NoError(t, err)
}

func teardown(conn adiomv1connect.ConnectorServiceHandler) {
	conn.(interface{ Teardown() }).Teardown()
}

func readCSVFile(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll() // strict: every row must match the header width
	require.NoError(t, err)
	return rows
}

// readAll reads every document from a CSV file (or directory namespace) using pagination.
func readAll(t *testing.T, settings ConnectorSettings, dataType adiomv1.DataType) [][]byte {
	t.Helper()
	conn, err := NewConn(settings)
	require.NoError(t, err)
	plan, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)
	var out [][]byte
	for _, p := range plan.Msg.GetPartitions() {
		var cursor []byte
		for {
			resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
				Partition: p, Type: dataType, Cursor: cursor,
			}))
			require.NoError(t, err)
			out = append(out, resp.Msg.GetData()...)
			cursor = resp.Msg.GetNextCursor()
			if cursor == nil {
				break
			}
		}
	}
	return out
}

func readFileBSON(t *testing.T, path string) []bson.Raw {
	t.Helper()
	var out []bson.Raw
	for _, d := range readAll(t, ConnectorSettings{Uri: "file://" + path}, adiomv1.DataType_DATA_TYPE_MONGO_BSON) {
		out = append(out, bson.Raw(d))
	}
	return out
}

func writeCSVFixture(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fixture.csv")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	return p
}

// --- #89: header grows with new keys ---

func TestWriteCSV_NewKeysInLaterDocumentsAreKept(t *testing.T) {
	conn, dir := newSink(t)
	writeBSON(t, conn, "shop.orders",
		bson.D{{"_id", 1}, {"status", "paid"}, {"total", 42.5}},
		bson.D{{"_id", 2}, {"status", "new"}, {"total", 7}, {"coupon", "X1"}},
	)
	// A later batch adds another key, and one document lacks earlier keys.
	writeBSON(t, conn, "shop.orders",
		bson.D{{"_id", 3}, {"note", "gift"}},
	)
	teardown(conn)

	rows := readCSVFile(t, filepath.Join(dir, "shop", "orders.csv"))
	assert.Equal(t, [][]string{
		{"_id", "status", "total", "coupon", "note"},
		{"1", "paid", "42.5", "", ""},
		{"2", "new", "7", "X1", ""},
		{"3", "", "", "", "gift"},
	}, rows)

	docs := readFileBSON(t, filepath.Join(dir, "shop", "orders.csv"))
	require.Len(t, docs, 3)
	assert.Equal(t, "X1", docs[1].Lookup("coupon").StringValue())
	assert.Equal(t, "gift", docs[2].Lookup("note").StringValue())
	_, err := docs[2].LookupErr("status")
	assert.Error(t, err, "missing fields stay missing")
}

func TestWriteCSV_NoRewriteWhenHeaderIsStable(t *testing.T) {
	conn, dir := newSink(t)
	writeBSON(t, conn, "stable", bson.D{{"_id", 1}, {"a", "x"}}, bson.D{{"_id", 2}, {"a", "y"}})
	path := filepath.Join(dir, "stable.csv")
	before, err := os.Stat(path)
	require.NoError(t, err)
	teardown(conn)
	after, err := os.Stat(path)
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "file should not be replaced when no columns were added")
}

func TestWriteCSV_IdentifierFirstEvenWhenItAppearsLater(t *testing.T) {
	conn, dir := newSink(t)
	writeJSON(t, conn, "late_id", `{"b":"1"}`, `{"id":"x","a":"2"}`)
	teardown(conn)
	rows := readCSVFile(t, filepath.Join(dir, "late_id.csv"))
	assert.Equal(t, []string{"b", "id", "a"}, rows[0])
	assert.Equal(t, []string{"1", "", ""}, rows[1])
}

func TestWriteCSV_ManyNamespacesKeepSeparateHeaders(t *testing.T) {
	conn, dir := newSink(t)
	writeBSON(t, conn, "db.a", bson.D{{"_id", 1}, {"x", 1}})
	writeBSON(t, conn, "db.b", bson.D{{"_id", 1}, {"y", 1}})
	writeBSON(t, conn, "db.a", bson.D{{"_id", 2}, {"z", 1}})
	teardown(conn)
	assert.Equal(t, []string{"_id", "x", "z"}, readCSVFile(t, filepath.Join(dir, "db", "a.csv"))[0])
	assert.Equal(t, []string{"_id", "y"}, readCSVFile(t, filepath.Join(dir, "db", "b.csv"))[0])
}

func TestWriteCSV_ConcurrentWritersSameNamespace(t *testing.T) {
	conn, dir := newSink(t)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				b, _ := bson.Marshal(bson.D{{"_id", w*1000 + i}, {fmt.Sprintf("k%d", i%7), w}})
				_, err := conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
					Namespace: "conc", Data: [][]byte{b}, Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				}))
				assert.NoError(t, err)
			}
		}(w)
	}
	wg.Wait()
	teardown(conn)
	rows := readCSVFile(t, filepath.Join(dir, "conc.csv"))
	assert.Len(t, rows, 401)
	assert.Len(t, rows[0], 8) // _id + k0..k6
	docs := readFileBSON(t, filepath.Join(dir, "conc.csv"))
	assert.Len(t, docs, 400)
}

func TestWriteCSV_EmptyDocumentIsSkipped(t *testing.T) {
	conn, dir := newSink(t)
	writeJSON(t, conn, "empty", `{}`, `{"id":"1","a":"x"}`)
	teardown(conn)
	rows := readCSVFile(t, filepath.Join(dir, "empty.csv"))
	assert.Equal(t, [][]string{{"id", "a"}, {"1", "x"}}, rows)
}

// --- #90: value encoding ---

func TestEncodeBSONValues(t *testing.T) {
	oid := bson.NewObjectID()
	date := time.Date(2026, 11, 18, 10, 0, 0, 123_000_000, time.UTC)
	dec, err := bson.ParseDecimal128("1234.5600")
	require.NoError(t, err)

	cases := []struct {
		name string
		val  any
		want string
	}{
		{"string", "hello", "hello"},
		{"string with delimiter/quotes/newline", "a, \"b\"\nc", "a, \"b\"\nc"},
		{"unicode", "Grüße 🚀", "Grüße 🚀"},
		{"int32", int32(-7), "-7"},
		{"int64", int64(math.MaxInt64), "9223372036854775807"},
		{"double fractional", 42.5, "42.5"},
		{"double integral", 42.0, "42.0"},
		{"double negative zero", math.Copysign(0, -1), "-0.0"},
		{"double tiny", 1e-7, "1e-07"},
		{"double huge", 1e300, "1e+300"},
		{"NaN", math.NaN(), "NaN"},
		{"+Inf", math.Inf(1), "Infinity"},
		{"-Inf", math.Inf(-1), "-Infinity"},
		{"bool", true, "true"},
		{"null", nil, ""},
		{"objectid", oid, oid.Hex()},
		{"date", bson.NewDateTimeFromTime(date), "2026-11-18T10:00:00.123Z"},
		{"date pre-1970", bson.NewDateTimeFromTime(time.Date(1901, 1, 2, 3, 4, 5, 0, time.UTC)), "1901-01-02T03:04:05.000Z"},
		{"date year 10000", bson.DateTime(253402300800000 + 1000), `{"$date":{"$numberLong":"253402300801000"}}`},
		{"embedded doc keeps field order and types", bson.D{{"z", 1}, {"a", oid}, {"d", bson.NewDateTimeFromTime(date)}},
			`{"z":1,"a":{"$oid":"` + oid.Hex() + `"},"d":{"$date":"2026-11-18T10:00:00.123Z"}}`},
		{"array with spaces", bson.A{"a b", "c"}, `["a b","c"]`},
		{"array of docs", bson.A{bson.D{{"x", 1}}, bson.D{{"x", 2.0}}}, `[{"x":1},{"x":2.0}]`},
		{"empty array", bson.A{}, `[]`},
		{"empty doc", bson.D{}, `{}`},
		{"decimal128", dec, `{"$numberDecimal":"1234.5600"}`},
		{"binary", bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}, `{"$binary":{"base64":"AQID","subType":"00"}}`},
		{"timestamp", bson.Timestamp{T: 10, I: 2}, `{"$timestamp":{"t":10,"i":2}}`},
		{"regex", bson.Regex{Pattern: "^a", Options: "i"}, `{"$regularExpression":{"pattern":"^a","options":"i"}}`},
		{"minkey", bson.MinKey{}, `{"$minKey":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := bson.Marshal(bson.D{{"f", tc.val}})
			require.NoError(t, err)
			doc, err := encodeBSONDocument(raw)
			require.NoError(t, err)
			assert.Equal(t, tc.want, doc["f"])
		})
	}
}

func TestEncodeJSONValues(t *testing.T) {
	doc, err := encodeJSONDocument([]byte(`{"id":"1","s":"x","big":12345678901234567890,"f":42.0,"b":false,"n":null,
		"obj":{"z":1, "a":[1, "two", {"k":null}]},"arr":[ ]}`))
	require.NoError(t, err)
	assert.Equal(t, encodedDoc{
		"id":  "1",
		"s":   "x",
		"big": "12345678901234567890", // precision preserved
		"f":   "42.0",
		"b":   "false",
		"n":   "",
		"obj": `{"z":1,"a":[1,"two",{"k":null}]}`, // JSON, key order kept, not Go map[k:v]
		"arr": `[]`,
	}, doc)
}

// --- #91: reading back ---

func TestInferBSON(t *testing.T) {
	oid := bson.NewObjectID()
	d := &rowDecoder{inferTypes: true, dataType: adiomv1.DataType_DATA_TYPE_MONGO_BSON}
	cases := []struct {
		cell string
		want any
	}{
		{"42", int32(42)},
		{"-0", int32(0)},
		{"2147483648", int64(2147483648)},
		{"-9223372036854775808", int64(math.MinInt64)},
		{"9223372036854775808", "9223372036854775808"}, // out of int64 range stays a string
		{"42.0", 42.0},
		{"1e5", 1e5},
		{"-1.5E-3", -1.5e-3},
		{"1e400", "1e400"},
		{"NaN", math.NaN()},
		{"Infinity", math.Inf(1)},
		{"-Infinity", math.Inf(-1)},
		{"true", true},
		{"false", false},
		{"TRUE", "TRUE"},
		{"True", "True"},
		{"007", "007"},
		{"+5", "+5"},
		{" 42", " 42"},
		{"42 ", "42 "},
		{".5", ".5"},
		{"5.", "5."},
		{"1,000", "1,000"},
		{"null", "null"},
		{oid.Hex(), oid},
		{`ObjectID("` + oid.Hex() + `")`, oid}, // format written by older versions
		{`ObjectId("` + oid.Hex() + `")`, oid},
		{`ObjectID("xyz")`, `ObjectID("xyz")`},
		{strings.ToUpper(oid.Hex()), strings.ToUpper(oid.Hex())},
		{"2026-11-18T10:00:00.123Z", bson.NewDateTimeFromTime(time.Date(2026, 11, 18, 10, 0, 0, 123_000_000, time.UTC))},
		{"2026-11-18T12:00:00+02:00", bson.NewDateTimeFromTime(time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC))},
		{"2026-11-18", "2026-11-18"},
		{"2026-13-45T10:00:00Z", "2026-13-45T10:00:00Z"},
		{"{bad json", "{bad json"},
		{"[1,2", "[1,2"},
		{"hello", "hello"},
	}
	for _, tc := range cases {
		t.Run(tc.cell, func(t *testing.T) {
			got, err := d.inferBSON(tc.cell)
			require.NoError(t, err)
			if f, ok := tc.want.(float64); ok && math.IsNaN(f) {
				assert.True(t, math.IsNaN(got.(float64)))
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestInferBSON_ExtendedJSON(t *testing.T) {
	d := &rowDecoder{inferTypes: true, dataType: adiomv1.DataType_DATA_TYPE_MONGO_BSON}
	v, err := d.inferBSON(`{"$numberDecimal":"1.50"}`)
	require.NoError(t, err)
	rv := v.(bson.RawValue)
	assert.Equal(t, bson.TypeDecimal128, rv.Type)
	assert.Equal(t, "1.50", rv.Decimal128().String())

	v, err = d.inferBSON(`{"a":{"$oid":"6af802ef5455cb5fc3d56179"},"b":[1,2.0]}`)
	require.NoError(t, err)
	rv = v.(bson.RawValue)
	require.Equal(t, bson.TypeEmbeddedDocument, rv.Type)
	assert.Equal(t, bson.TypeObjectID, rv.Document().Lookup("a").Type)
	assert.Equal(t, bson.TypeDouble, rv.Document().Lookup("b", "1").Type)
}

func TestInferJSON(t *testing.T) {
	d := &rowDecoder{inferTypes: true, dataType: adiomv1.DataType_DATA_TYPE_JSON_ID}
	cases := map[string]string{
		"42":                       `42`,
		"12345678901234567890":     `12345678901234567890`,
		"42.0":                     `42.0`,
		"true":                     `true`,
		"NaN":                      `"NaN"`, // not valid JSON
		"6af802ef5455cb5fc3d56179": `"6af802ef5455cb5fc3d56179"`,
		"2026-11-18T10:00:00Z":     `"2026-11-18T10:00:00Z"`,
		`{"a": [1, 2]}`:            `{"a":[1,2]}`,
		"{bad":                     `"{bad"`,
		"007":                      `"007"`,
		`say "hi"`:                 `"say \"hi\""`,
	}
	for cell, want := range cases {
		assert.Equal(t, want, string(d.inferJSON(cell)), cell)
	}
}

func TestReadIDColumnMapping(t *testing.T) {
	oid := "6af802ef5455cb5fc3d56179"

	t.Run("id column becomes _id for BSON and is parsed as ObjectID", func(t *testing.T) {
		p := writeCSVFixture(t, "id,name\n"+oid+",a\n")
		docs := readFileBSON(t, p)
		require.Len(t, docs, 1)
		assert.Equal(t, bson.TypeObjectID, docs[0].Lookup("_id").Type)
		_, err := docs[0].LookupErr("id")
		assert.Error(t, err)
	})
	t.Run("_id column becomes id for JSON", func(t *testing.T) {
		p := writeCSVFixture(t, "_id,name\n7,a\n")
		docs := readAll(t, ConnectorSettings{Uri: "file://" + p}, adiomv1.DataType_DATA_TYPE_JSON_ID)
		require.Len(t, docs, 1)
		assert.JSONEq(t, `{"id":7,"name":"a"}`, string(docs[0]))
	})
	t.Run("both columns: each type keeps its own identifier and the other as a field", func(t *testing.T) {
		p := writeCSVFixture(t, "_id,id,name\n"+oid+",5,a\n")
		docs := readFileBSON(t, p)
		require.Len(t, docs, 1)
		assert.Equal(t, bson.TypeObjectID, docs[0].Lookup("_id").Type)
		assert.Equal(t, int32(5), docs[0].Lookup("id").Int32())

		jdocs := readAll(t, ConnectorSettings{Uri: "file://" + p}, adiomv1.DataType_DATA_TYPE_JSON_ID)
		assert.JSONEq(t, `{"id":5,"_id":"`+oid+`","name":"a"}`, string(jdocs[0]))
	})
	t.Run("composite _id stored as JSON", func(t *testing.T) {
		p := writeCSVFixture(t, "_id,v\n\"{\"\"a\"\":1,\"\"b\"\":\"\"x\"\"}\",1\n")
		docs := readFileBSON(t, p)
		require.Len(t, docs, 1)
		assert.Equal(t, bson.TypeEmbeddedDocument, docs[0].Lookup("_id").Type)
	})
	t.Run("empty id cell is omitted", func(t *testing.T) {
		p := writeCSVFixture(t, "id,name\n,a\n")
		docs := readFileBSON(t, p)
		_, err := docs[0].LookupErr("_id")
		assert.Error(t, err)
	})
}

func TestReadInferenceDisabled(t *testing.T) {
	p := writeCSVFixture(t, "id,n,b,j,e\n6af802ef5455cb5fc3d56179,42,true,{},\n")
	docs := readAll(t, ConnectorSettings{Uri: "file://" + p, DisableTypeInference: true}, adiomv1.DataType_DATA_TYPE_MONGO_BSON)
	doc := bson.Raw(docs[0])
	assert.Equal(t, "6af802ef5455cb5fc3d56179", doc.Lookup("_id").StringValue())
	assert.Equal(t, "42", doc.Lookup("n").StringValue())
	assert.Equal(t, "true", doc.Lookup("b").StringValue())
	assert.Equal(t, "{}", doc.Lookup("j").StringValue())
	_, err := doc.LookupErr("e")
	assert.Error(t, err, "empty cells are omitted even without inference")
}

func TestReadBOMHeader(t *testing.T) {
	p := writeCSVFixture(t, "\ufeffid,name\n1,a\n2,b\n")
	docs := readAll(t, ConnectorSettings{Uri: "file://" + p, BatchSize: 1}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, docs, 2)
	assert.JSONEq(t, `{"id":1,"name":"a"}`, string(docs[0]))
	assert.JSONEq(t, `{"id":2,"name":"b"}`, string(docs[1]))
}

func TestReadDuplicateColumnsFails(t *testing.T) {
	p := writeCSVFixture(t, "id,a,a\n1,x,y\n")
	conn, err := NewConn(ConnectorSettings{Uri: "file://" + p})
	require.NoError(t, err)
	plan, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
	require.NoError(t, err)
	_, err = conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
		Partition: plan.Msg.GetPartitions()[0], Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}))
	require.ErrorIs(t, err, ErrDuplicateColumn)
}

func TestReadInferenceAcrossPages(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,v\n")
	for i := 0; i < 25; i++ {
		fmt.Fprintf(&sb, "%d,%d.5\n", i, i)
	}
	p := writeCSVFixture(t, sb.String())
	docs := readAll(t, ConnectorSettings{Uri: "file://" + p, BatchSize: 4}, adiomv1.DataType_DATA_TYPE_MONGO_BSON)
	require.Len(t, docs, 25)
	for i, d := range docs {
		assert.Equal(t, int32(i), bson.Raw(d).Lookup("_id").Int32())
		assert.Equal(t, float64(i)+0.5, bson.Raw(d).Lookup("v").Double())
	}
}

// --- full round trips through the sink and the source ---

func TestRoundTripBSON(t *testing.T) {
	oid := bson.NewObjectID()
	dec, _ := bson.ParseDecimal128("99.990")
	date := bson.NewDateTimeFromTime(time.Date(2026, 11, 18, 10, 0, 0, 123_000_000, time.UTC))
	docs := []bson.D{
		{{"_id", oid}, {"status", "paid"}, {"total", 42.5}, {"qty", int32(3)}, {"big", int64(1) << 40},
			{"whole", 7.0}, {"ok", true}, {"createdAt", date},
			{"addr", bson.D{{"city", "Paris"}, {"zip", "75001"}}},
			{"items", bson.A{"a b", "c", bson.D{{"sku", "x"}}}},
			{"price", dec}, {"bin", bson.Binary{Subtype: 4, Data: make([]byte, 16)}},
			{"ref", bson.NewObjectID()}, {"text", "line1\nline2, \"q\""}},
		{{"_id", "string-id"}, {"status", "new"}, {"coupon", "X1"}, {"neg", math.Inf(-1)}},
		{{"_id", int32(17)}, {"id", "user-field"}},
		{{"_id", bson.D{{"a", int32(1)}, {"b", "x"}}}, {"old", bson.NewDateTimeFromTime(time.Date(1950, 6, 1, 0, 0, 0, 0, time.UTC))}},
	}

	conn, dir := newSink(t)
	anyDocs := make([]any, len(docs))
	for i := range docs {
		anyDocs[i] = docs[i]
	}
	writeBSON(t, conn, "shop.orders", anyDocs[:2]...)
	writeBSON(t, conn, "shop.orders", anyDocs[2:]...)
	teardown(conn)

	got := readFileBSON(t, filepath.Join(dir, "shop", "orders.csv"))
	require.Len(t, got, len(docs))
	for i, want := range docs {
		wantRaw, err := bson.Marshal(want)
		require.NoError(t, err)
		assertSameFields(t, bson.Raw(wantRaw), got[i])
	}
}

// assertSameFields compares two documents field by field, ignoring field order.
func assertSameFields(t *testing.T, want, got bson.Raw) {
	t.Helper()
	wantElems, err := want.Elements()
	require.NoError(t, err)
	gotElems, err := got.Elements()
	require.NoError(t, err)
	assert.Len(t, gotElems, len(wantElems), "fields of %v vs %v", want, got)
	for _, we := range wantElems {
		gv, err := got.LookupErr(we.Key())
		if !assert.NoError(t, err, "missing field %q in %v", we.Key(), got) {
			continue
		}
		assert.True(t, we.Value().Equal(gv), "field %q: want %v (%v), got %v (%v)", we.Key(), we.Value(), we.Value().Type, gv, gv.Type)
	}
}

func TestRoundTripJSON(t *testing.T) {
	conn, dir := newSink(t)
	in := []string{
		`{"id":"a1","n":12345678901234567890,"f":1.5,"b":true,"obj":{"z":[1,{"y":"w"}]},"s":"x,y"}`,
		`{"id":"a2","extra":"later"}`,
	}
	writeJSON(t, conn, "j", in...)
	teardown(conn)
	got := readAll(t, ConnectorSettings{Uri: "file://" + filepath.Join(dir, "j.csv")}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, got, 2)
	// JSON numbers have no range limit, so integers beyond int64 stay numbers
	assert.JSONEq(t, `{"id":"a1","n":12345678901234567890,"f":1.5,"b":true,"obj":{"z":[1,{"y":"w"}]},"s":"x,y"}`, string(got[0]))
	assert.JSONEq(t, `{"id":"a2","extra":"later"}`, string(got[1]))

	// JSONEq compares numbers as float64; check the exact token too.
	var check map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got[0], &check))
	assert.Equal(t, "12345678901234567890", string(check["n"]))
}

func TestRoundTrip_DocumentedLossyCases(t *testing.T) {
	conn, dir := newSink(t)
	writeBSON(t, conn, "lossy", bson.D{
		{"_id", 1}, {"nullField", nil}, {"emptyString", ""}, {"numericString", "42"},
		{"boolString", "true"}, {"hexString", "6af802ef5455cb5fc3d56179"},
		{"nestedInt64", bson.D{{"v", int64(5)}}}, {"smallInt64", int64(5)}, {"crlf", "a\r\nb"},
		{"extKeyword", bson.D{{"$numberLong", "5"}}},
	})
	teardown(conn)
	got := readFileBSON(t, filepath.Join(dir, "lossy.csv"))[0]

	_, err := got.LookupErr("nullField")
	assert.Error(t, err, "null becomes missing")
	_, err = got.LookupErr("emptyString")
	assert.Error(t, err, "empty string becomes missing")
	assert.Equal(t, bson.TypeInt32, got.Lookup("numericString").Type)
	assert.Equal(t, bson.TypeBoolean, got.Lookup("boolString").Type)
	assert.Equal(t, bson.TypeObjectID, got.Lookup("hexString").Type)
	assert.Equal(t, bson.TypeInt32, got.Lookup("nestedInt64", "v").Type, "relaxed ExtJSON loses int64 for small values")
	assert.Equal(t, bson.TypeInt32, got.Lookup("smallInt64").Type, "int64 that fits in int32 comes back as int32")
	assert.Equal(t, "a\nb", got.Lookup("crlf").StringValue(), "encoding/csv turns \\r\\n into \\n")
	assert.Equal(t, bson.TypeInt64, got.Lookup("extKeyword").Type, "a document spelling ExtJSON reads back as that value")
}

func TestWriteCSV_SpecialFieldNames(t *testing.T) {
	conn, dir := newSink(t)
	writeBSON(t, conn, "names", bson.D{{"_id", 1}, {"a,b", "1"}, {`q"uote`, "2"}, {"new\nline", "3"}, {"", "4"}})
	teardown(conn)
	got := readFileBSON(t, filepath.Join(dir, "names.csv"))[0]
	assert.Equal(t, int32(1), got.Lookup("a,b").Int32())
	assert.Equal(t, int32(2), got.Lookup(`q"uote`).Int32())
	assert.Equal(t, int32(3), got.Lookup("new\nline").Int32())
	assert.Equal(t, int32(4), got.Lookup("").Int32())
}

func TestWriteCSV_SemicolonDelimiterRewrite(t *testing.T) {
	dir := t.TempDir()
	conn, err := NewConn(ConnectorSettings{Uri: "file://" + dir, Delimiter: ';'})
	require.NoError(t, err)
	writeBSON(t, conn, "semi", bson.D{{"_id", 1}, {"a", "x;y"}})
	writeBSON(t, conn, "semi", bson.D{{"_id", 2}, {"b", 1.5}})
	teardown(conn)
	content, err := os.ReadFile(filepath.Join(dir, "semi.csv"))
	require.NoError(t, err)
	assert.Equal(t, "_id;a;b\n1;\"x;y\";\n2;;1.5\n", string(content))
}

func TestWriteCSV_SingleEmptyFieldRowsSurvive(t *testing.T) {
	conn, dir := newSink(t)
	writeJSON(t, conn, "single", `{"a":"x"}`, `{"a":null}`, `{"a":"y"}`)
	writeJSON(t, conn, "single_rewrite", `{"a":"x"}`, `{"a":""}`, `{"a":"y","b":"z"}`)
	writeJSON(t, conn, "empty_name", `{"":"x"}`, `{"":"y"}`)
	teardown(conn)

	got := readAll(t, ConnectorSettings{Uri: "file://" + filepath.Join(dir, "single.csv")}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, got, 3, "a row with one empty field must not become a blank line")
	assert.JSONEq(t, `{}`, string(got[1]))

	got = readAll(t, ConnectorSettings{Uri: "file://" + filepath.Join(dir, "single_rewrite.csv")}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, got, 3)
	assert.JSONEq(t, `{"a":"y","b":"z"}`, string(got[2]))

	got = readAll(t, ConnectorSettings{Uri: "file://" + filepath.Join(dir, "empty_name.csv")}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, got, 2, "a header made of one empty column name must not be a blank line")
	assert.JSONEq(t, `{"":"x"}`, string(got[0]))
}

func TestReadRaggedRows(t *testing.T) {
	// A file left behind without the Teardown rewrite: short rows are padded.
	p := writeCSVFixture(t, "id,a,b\n1,x,y\n2,x\n3,x,y\n")
	for _, batch := range []int{1, 2, 10} {
		docs := readAll(t, ConnectorSettings{Uri: "file://" + p, BatchSize: batch}, adiomv1.DataType_DATA_TYPE_JSON_ID)
		require.Len(t, docs, 3, "batch %d", batch)
		assert.JSONEq(t, `{"id":2,"a":"x"}`, string(docs[1]))
	}

	// A row longer than the header is an error wherever it falls in a page.
	p = writeCSVFixture(t, "id,a\n1,x\n2,x,z\n")
	for _, batch := range []int{1, 10} {
		conn, err := NewConn(ConnectorSettings{Uri: "file://" + p, BatchSize: batch})
		require.NoError(t, err)
		plan, err := conn.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{}))
		require.NoError(t, err)
		var cursor []byte
		var lastErr error
		for i := 0; i < 5; i++ {
			resp, err := conn.ListData(context.Background(), connect.NewRequest(&adiomv1.ListDataRequest{
				Partition: plan.Msg.GetPartitions()[0], Type: adiomv1.DataType_DATA_TYPE_JSON_ID, Cursor: cursor,
			}))
			if err != nil {
				lastErr = err
				break
			}
			if cursor = resp.Msg.GetNextCursor(); cursor == nil {
				break
			}
		}
		require.Error(t, lastErr, "batch %d", batch)
		assert.Contains(t, lastErr.Error(), "has 3 fields but the header has 2")
	}
}

func TestRoundTripBSON_AmbiguousObjectIDs(t *testing.T) {
	// ObjectIDs whose hex looks like a number: all digits, or digits around an "e".
	hexes := []string{"123456789012345678901234", "12345678901234567890e123", "000000000000000000000001"}
	conn, dir := newSink(t)
	for i, h := range hexes {
		oid, err := bson.ObjectIDFromHex(h)
		require.NoError(t, err)
		writeBSON(t, conn, "oids", bson.D{{"_id", i}, {"ref", oid}})
	}
	teardown(conn)
	got := readFileBSON(t, filepath.Join(dir, "oids.csv"))
	require.Len(t, got, len(hexes))
	for i, h := range hexes {
		v := got[i].Lookup("ref")
		require.Equal(t, bson.TypeObjectID, v.Type, h)
		assert.Equal(t, h, v.ObjectID().Hex())
	}
}

func TestRoundTripBSON_DBPointerNamespaceNeedsEscaping(t *testing.T) {
	oid := bson.NewObjectID()
	conn, dir := newSink(t)
	in := []bson.D{
		{{"_id", 1}, {"p", bson.DBPointer{DB: "db.coll", Pointer: oid}}},
		{{"_id", 2}, {"p", bson.DBPointer{DB: `we"ird\ns`, Pointer: oid}}},
		{{"_id", 3}, {"nested", bson.D{{"a", bson.A{bson.DBPointer{DB: `q"`, Pointer: oid}, "x"}}}}},
	}
	for _, d := range in {
		writeBSON(t, conn, "dbp", d)
	}
	teardown(conn)
	got := readFileBSON(t, filepath.Join(dir, "dbp.csv"))
	require.Len(t, got, len(in))
	for i, d := range in {
		want, err := bson.Marshal(d)
		require.NoError(t, err)
		assertSameFields(t, bson.Raw(want), got[i])
	}
}

func TestInferBSON_TrailingContentAfterJSONStaysString(t *testing.T) {
	d := &rowDecoder{inferTypes: true}
	for _, cell := range []string{`[1],"extra":2`, `{"a":1},"b":2`, `[1] [2]`} {
		v, err := d.inferBSON(cell)
		require.NoError(t, err)
		assert.Equal(t, cell, v, cell)
	}
}

func TestInferJSON_NumbersOutsideFloat64Range(t *testing.T) {
	d := &rowDecoder{inferTypes: true}
	assert.Equal(t, "1e400", string(d.inferJSON("1e400")))
	assert.Equal(t, "-1.5e-400", string(d.inferJSON("-1.5e-400")))
	// BSON has to produce a double, so out-of-range stays a string there.
	v, err := d.inferBSON("1e400")
	require.NoError(t, err)
	assert.Equal(t, "1e400", v)

	conn, dir := newSink(t)
	writeJSON(t, conn, "big", `{"id":"a","n":1e400}`)
	teardown(conn)
	got := readAll(t, ConnectorSettings{Uri: "file://" + filepath.Join(dir, "big.csv")}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, got, 1)
	var check map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got[0], &check))
	assert.Equal(t, "1e400", string(check["n"]))
}

func TestReadBOMBeforeQuotedHeader(t *testing.T) {
	p := writeCSVFixture(t, "\ufeff\"id\",\"name\"\n1,a\n2,b\n3,c\n")
	docs := readAll(t, ConnectorSettings{Uri: "file://" + p, BatchSize: 1}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, docs, 3)
	assert.JSONEq(t, `{"id":1,"name":"a"}`, string(docs[0]))
	assert.JSONEq(t, `{"id":2,"name":"b"}`, string(docs[1]))
	assert.JSONEq(t, `{"id":3,"name":"c"}`, string(docs[2]))
}

func TestWriteCSV_JSONIDMixedIdentifierSpellings(t *testing.T) {
	conn, dir := newSink(t)
	writeJSON(t, conn, "mixed", `{"_id":"a","x":1}`, `{"id":"b","x":2}`, `{"id":"c","_id":"extra"}`)
	teardown(conn)
	p := filepath.Join(dir, "mixed.csv")
	assert.Equal(t, []string{"id", "x", "_id"}, readCSVFile(t, p)[0])
	got := readAll(t, ConnectorSettings{Uri: "file://" + p}, adiomv1.DataType_DATA_TYPE_JSON_ID)
	require.Len(t, got, 3)
	assert.JSONEq(t, `{"id":"a","x":1}`, string(got[0]))
	assert.JSONEq(t, `{"id":"b","x":2}`, string(got[1]))
	assert.JSONEq(t, `{"id":"c","_id":"extra"}`, string(got[2]))
}

func TestNewConn_RejectsInvalidDelimiters(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []rune{'"', '\r', '\n', utf8.RuneError, -1} {
		_, err := NewConn(ConnectorSettings{Uri: "file://" + dir, Delimiter: d})
		assert.ErrorIs(t, err, ErrInvalidDelimiter, "%q", d)
	}
	for _, d := range []rune{',', ';', '\t', '|'} {
		_, err := NewConn(ConnectorSettings{Uri: "file://" + dir, Delimiter: d})
		assert.NoError(t, err, "%q", d)
	}
}

func TestWriteCSV_RewriteWithLongFileName(t *testing.T) {
	conn, dir := newSink(t)
	ns := strings.Repeat("n", 250) // 254-byte basename with ".csv"
	writeBSON(t, conn, ns, bson.D{{"_id", 1}, {"a", 1}}, bson.D{{"_id", 2}, {"b", 2}})
	teardown(conn)
	assert.Equal(t, []string{"_id", "a", "b"}, readCSVFile(t, filepath.Join(dir, ns+".csv"))[0])
}

func TestTeardown_RetriesFailedRewrite(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not apply to root")
	}
	conn, dir := newSink(t)
	writeBSON(t, conn, "retry", bson.D{{"_id", 1}, {"a", 1}}, bson.D{{"_id", 2}, {"b", 2}})

	// Make the rewrite fail: the temp file cannot be created in a read-only directory.
	require.NoError(t, os.Chmod(dir, 0o555))
	teardown(conn)
	require.NoError(t, os.Chmod(dir, 0o755))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no temp file is left behind")

	// Writes to a namespace that is waiting for a retry are refused, not lost.
	b, _ := bson.Marshal(bson.D{{"_id", 3}})
	_, err = conn.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "retry", Data: [][]byte{b}, Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}))
	assert.Error(t, err)

	teardown(conn) // retry succeeds
	p := filepath.Join(dir, "retry.csv")
	assert.Equal(t, [][]string{{"_id", "a", "b"}, {"1", "1", ""}, {"2", "", "2"}}, readCSVFile(t, p))
}
