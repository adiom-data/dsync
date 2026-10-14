package postgres

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	neturl "net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func livePostgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("POSTGRES_TEST_URL")
	if url == "" {
		t.Skip("POSTGRES_TEST_URL is not set")
	}
	return url
}

func TestPostgresConnectorLiveRoundTrip(t *testing.T) {
	ctx := context.Background()
	url := livePostgresURL(t)
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)

	for _, statement := range []string{
		`DROP SCHEMA IF EXISTS dsync_live CASCADE`,
		`DROP SCHEMA IF EXISTS dsync_live_other CASCADE`,
		`CREATE SCHEMA dsync_live`,
		`CREATE SCHEMA dsync_live_other`,
		`CREATE TABLE dsync_live.source_nonpk_id (pk integer PRIMARY KEY, _id text, _id2 text, value text)`,
		`CREATE TABLE dsync_live_other.source_nonpk_id (code text PRIMARY KEY, value text)`,
		`INSERT INTO dsync_live.source_nonpk_id VALUES (1, 'original-id', 'occupied', 'source')`,
		`CREATE TABLE dsync_live.source_id_pk (tenant text, _id text, value text, PRIMARY KEY (tenant, _id))`,
		`INSERT INTO dsync_live.source_id_pk VALUES ('tenant-a', 'id-a', 'source')`,
		`CREATE TABLE dsync_live.source_network_pk (network inet PRIMARY KEY, value text)`,
		`INSERT INTO dsync_live.source_network_pk VALUES ('192.0.2.0/24', 'source')`,
		`CREATE TABLE dsync_live.sink_with_id (_id text PRIMARY KEY, value text, payload bytea, uid uuid, ip inet)`,
		`CREATE TABLE dsync_live.sink_no_id (id integer PRIMARY KEY, value text)`,
		`CREATE TABLE dsync_live.sink_composite (tenant text, _id text, value text, PRIMARY KEY (tenant, _id))`,
		`CREATE TABLE dsync_live.sink_bytea (id bytea PRIMARY KEY, value text)`,
		`CREATE TABLE dsync_live.sink_replace (id integer PRIMARY KEY, required text NOT NULL, status text NOT NULL DEFAULT 'fresh', note text, computed text GENERATED ALWAYS AS (required || '-computed') STORED)`,
		`CREATE TABLE dsync_live.sink_identity (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text NOT NULL)`,
		`CREATE TABLE dsync_live.sink_order (id integer PRIMARY KEY, value text NOT NULL CHECK (value <> 'bad'))`,
		`CREATE TABLE dsync_live.sink_order_audit (sequence bigint GENERATED ALWAYS AS IDENTITY, action text NOT NULL)`,
		`CREATE FUNCTION dsync_live.audit_sink_order() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO dsync_live.sink_order_audit(action) VALUES (TG_OP); RETURN COALESCE(NEW, OLD); END $$`,
		`CREATE TRIGGER audit_sink_order AFTER INSERT OR UPDATE OR DELETE ON dsync_live.sink_order FOR EACH ROW EXECUTE FUNCTION dsync_live.audit_sink_order()`,
		`CREATE TABLE dsync_live.sink_restricted (id integer PRIMARY KEY, required text NOT NULL, status text NOT NULL DEFAULT 'fresh')`,
		`CREATE TABLE dsync_live.sink_replica (id integer PRIMARY KEY, touched boolean NOT NULL DEFAULT false)`,
		`CREATE FUNCTION dsync_live.mark_touched() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.touched = true; RETURN NEW; END $$`,
		`CREATE TRIGGER mark_touched BEFORE INSERT ON dsync_live.sink_replica FOR EACH ROW EXECUTE FUNCTION dsync_live.mark_touched()`,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	c, err := NewConn(ctx, PostgresSettings{URL: url, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Teardown()
	if !reflect.DeepEqual(c.pkeys["dsync_live.source_nonpk_id"], []string{"pk"}) ||
		!reflect.DeepEqual(c.pkeys["dsync_live_other.source_nonpk_id"], []string{"code"}) {
		t.Fatalf("primary keys leaked across schemas: %#v", c.pkeys)
	}

	list := func(namespace string) bson.M {
		t.Helper()
		res, err := c.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{
			Partition: &adiomv1.Partition{Namespace: namespace},
		}))
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Msg.GetData()) != 1 {
			t.Fatalf("%s returned %d documents", namespace, len(res.Msg.GetData()))
		}
		document, err := bsonToMap(c.bsonRegistry, res.Msg.GetData()[0], true)
		if err != nil {
			t.Fatal(err)
		}
		return document
	}

	nonPKID := list("dsync_live.source_nonpk_id")
	if nonPKID["_id"] != int32(1) || nonPKID["_id2"] != "occupied" || nonPKID["_id3"] != "original-id" {
		t.Fatalf("unexpected aliased source document: %#v", nonPKID)
	}
	idPK := list("dsync_live.source_id_pk")
	if idPK["_id"] != "id-a" || idPK["tenant"] != "tenant-a" {
		t.Fatalf("unexpected _id primary-key document: %#v", idPK)
	}
	networkPK := list("dsync_live.source_network_pk")
	if networkPK["_id"] != "192.0.2.0/24" || networkPK["network"] != "192.0.2.0/24" {
		t.Fatalf("unexpected network primary-key document: %#v", networkPK)
	}
	var network netip.Prefix
	if err := admin.QueryRow(ctx, `SELECT network FROM dsync_live.source_network_pk`).Scan(&network); err != nil {
		t.Fatal(err)
	}
	networkID, err := c.sourceUpdateID("dsync_live.source_network_pk", map[string]interface{}{"network": network})
	if err != nil {
		t.Fatal(err)
	}
	var networkIDValue string
	if err := bson.UnmarshalValue(bson.Type(networkID[0].GetType()), networkID[0].GetData(), &networkIDValue); err != nil {
		t.Fatal(err)
	}
	if networkIDValue != "192.0.2.0/24" {
		t.Fatalf("network update ID = %q", networkIDValue)
	}

	uuidBytes := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	writeDocument := func(namespace string, document bson.M) {
		t.Helper()
		data, err := marshalWithRegistry(c.bsonRegistry, document)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{Namespace: namespace, Data: [][]byte{data}})); err != nil {
			t.Fatal(err)
		}
	}
	writeDocument("dsync_live.sink_with_id", bson.M{
		"_id": "kept", "value": "written",
		"payload": bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte("bytes")},
		"uid":     bson.Binary{Subtype: bson.TypeBinaryUUID, Data: uuidBytes},
		"ip":      "192.0.2.1",
	})
	var gotID, gotValue, gotUUID, gotIP string
	var gotPayload []byte
	if err := admin.QueryRow(ctx, `SELECT _id, value, payload, uid::text, host(ip) FROM dsync_live.sink_with_id`).Scan(&gotID, &gotValue, &gotPayload, &gotUUID, &gotIP); err != nil {
		t.Fatal(err)
	}
	if gotID != "kept" || gotValue != "written" || !bytes.Equal(gotPayload, []byte("bytes")) || gotUUID != "00010203-0405-0607-0809-0a0b0c0d0e0f" || gotIP != "192.0.2.1" {
		t.Fatalf("unexpected typed sink row: id=%q value=%q payload=%q uuid=%q ip=%q", gotID, gotValue, gotPayload, gotUUID, gotIP)
	}

	writeDocument("dsync_live.sink_no_id", bson.M{"_id": "synthetic", "id": int32(7), "value": "written"})
	var noIDValue string
	if err := admin.QueryRow(ctx, `SELECT value FROM dsync_live.sink_no_id WHERE id = 7`).Scan(&noIDValue); err != nil || noIDValue != "written" {
		t.Fatalf("sink without _id: value=%q err=%v", noIDValue, err)
	}

	replacementDocuments := []bson.M{
		{"id": int32(1), "required": "first", "status": "custom", "note": "present"},
		{"id": int32(2), "required": "second"},
	}
	rejectedGenerated, err := marshalWithRegistry(c.bsonRegistry, bson.M{"id": int32(3), "required": "third", "computed": "source-value"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dsync_live.sink_replace",
		Data:      [][]byte{rejectedGenerated},
	})); err == nil {
		t.Fatal("expected supplied generated column to be rejected")
	}
	var replacementData [][]byte
	for _, document := range replacementDocuments {
		data, err := marshalWithRegistry(c.bsonRegistry, document)
		if err != nil {
			t.Fatal(err)
		}
		replacementData = append(replacementData, data)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dsync_live.sink_replace",
		Data:      replacementData,
	})); err != nil {
		t.Fatal(err)
	}
	var status string
	var note *string
	var computed string
	if err := admin.QueryRow(ctx, `SELECT status, note, computed FROM dsync_live.sink_replace WHERE id = 2`).Scan(&status, &note, &computed); err != nil {
		t.Fatal(err)
	}
	if status != "fresh" || note != nil || computed != "second-computed" {
		t.Fatalf("per-document defaults: status=%q note=%v computed=%q", status, note, computed)
	}

	replacementID, _, err := toMongoID(c.bsonRegistry, []interface{}{int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	replacementUpdate, err := marshalWithRegistry(c.bsonRegistry, bson.M{"required": "replaced"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dsync_live.sink_replace",
		Updates: []*adiomv1.Update{{
			Id: replacementID, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: replacementUpdate,
		}},
	})); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT status, note FROM dsync_live.sink_replace WHERE id = 1`).Scan(&status, &note); err != nil {
		t.Fatal(err)
	}
	if status != "fresh" || note != nil {
		t.Fatalf("full replacement did not reset omitted columns: status=%q note=%v", status, note)
	}

	writeDocument("dsync_live.sink_identity", bson.M{"id": int64(99), "value": "explicit"})
	var identityValue string
	if err := admin.QueryRow(ctx, `SELECT value FROM dsync_live.sink_identity WHERE id = 99`).Scan(&identityValue); err != nil || identityValue != "explicit" {
		t.Fatalf("identity override: value=%q err=%v", identityValue, err)
	}

	orderedID, _, err := toMongoID(c.bsonRegistry, []interface{}{int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	orderedFirst, err := marshalWithRegistry(c.bsonRegistry, bson.M{"value": "first"})
	if err != nil {
		t.Fatal(err)
	}
	orderedLast, err := marshalWithRegistry(c.bsonRegistry, bson.M{"value": "last"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dsync_live.sink_order",
		Updates: []*adiomv1.Update{
			{Id: orderedID, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: orderedFirst},
			{Id: orderedID, Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE},
			{Id: orderedID, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: orderedLast},
		},
	})); err != nil {
		t.Fatal(err)
	}
	var actions []string
	if err := admin.QueryRow(ctx, `SELECT array_agg(action ORDER BY sequence) FROM dsync_live.sink_order_audit`).Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actions, []string{"INSERT", "DELETE", "INSERT"}) {
		t.Fatalf("ordered update actions = %#v", actions)
	}

	atomicGoodID, _, err := toMongoID(c.bsonRegistry, []interface{}{int32(2)})
	if err != nil {
		t.Fatal(err)
	}
	atomicBadID, _, err := toMongoID(c.bsonRegistry, []interface{}{int32(3)})
	if err != nil {
		t.Fatal(err)
	}
	atomicGood, err := marshalWithRegistry(c.bsonRegistry, bson.M{"value": "good"})
	if err != nil {
		t.Fatal(err)
	}
	atomicBad, err := marshalWithRegistry(c.bsonRegistry, bson.M{"value": "bad"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dsync_live.sink_order",
		Updates: []*adiomv1.Update{
			{Id: atomicGoodID, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: atomicGood},
			{Id: atomicBadID, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: atomicBad},
		},
	})); err == nil {
		t.Fatal("expected batched constraint error")
	}
	var atomicCount int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM dsync_live.sink_order WHERE id IN (2, 3)`).Scan(&atomicCount); err != nil {
		t.Fatal(err)
	}
	if atomicCount != 0 {
		t.Fatalf("failed batch committed %d rows", atomicCount)
	}

	compositeID, err := toNamedID(c.bsonRegistry, []string{"tenant", "_id"}, map[string]interface{}{"tenant": "tenant-a", "_id": "id-a"})
	if err != nil {
		t.Fatal(err)
	}
	updateData, err := marshalWithRegistry(c.bsonRegistry, bson.M{"value": "updated"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dsync_live.sink_composite",
		Updates:   []*adiomv1.Update{{Id: compositeID, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: updateData}},
	})); err != nil {
		t.Fatal(err)
	}
	var compositeValue string
	if err := admin.QueryRow(ctx, `SELECT value FROM dsync_live.sink_composite WHERE tenant = 'tenant-a' AND _id = 'id-a'`).Scan(&compositeValue); err != nil || compositeValue != "updated" {
		t.Fatalf("multipart update: value=%q err=%v", compositeValue, err)
	}

	byteaKey := []byte{0xde, 0xad, 0xbe, 0xef}
	byteaID, _, err := toMongoID(c.bsonRegistry, []interface{}{byteaKey})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dsync_live.sink_bytea",
		Updates:   []*adiomv1.Update{{Id: byteaID, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: updateData}},
	})); err != nil {
		t.Fatal(err)
	}
	var gotByteaKey []byte
	if err := admin.QueryRow(ctx, `SELECT id FROM dsync_live.sink_bytea`).Scan(&gotByteaKey); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotByteaKey, byteaKey) {
		t.Fatalf("bytea key = %x, want %x", gotByteaKey, byteaKey)
	}

	sinkRole := fmt.Sprintf("dsync_sink_live_%d", time.Now().UnixNano())
	sanitizedSinkRole := pgx.Identifier{sinkRole}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE ROLE "+sanitizedSinkRole+" LOGIN PASSWORD 'dsync-sink-password'"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(ctx, "DROP OWNED BY "+sanitizedSinkRole)
		_, _ = admin.Exec(ctx, "DROP ROLE "+sanitizedSinkRole)
	}()
	for _, statement := range []string{
		"GRANT USAGE ON SCHEMA dsync_live TO " + sanitizedSinkRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON dsync_live.sink_restricted TO " + sanitizedSinkRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	var isSuperuser bool
	if err := admin.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = $1`, sinkRole).Scan(&isSuperuser); err != nil {
		t.Fatal(err)
	}
	if isSuperuser {
		t.Fatal("restricted sink role unexpectedly has superuser privileges")
	}
	var databaseName string
	if err := admin.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" TO "+sanitizedSinkRole); err != nil {
		t.Fatal(err)
	}
	sinkURL, err := neturl.Parse(url)
	if err != nil {
		t.Fatal(err)
	}
	sinkURL.User = neturl.UserPassword(sinkRole, "dsync-sink-password")
	restrictedConn, err := NewConn(ctx, PostgresSettings{URL: sinkURL.String(), Limit: 100})
	if err != nil {
		t.Fatalf("connect as restricted sink role: %v", err)
	}
	defer restrictedConn.Teardown()
	writeRestricted := func(document bson.M) {
		t.Helper()
		data, err := marshalWithRegistry(restrictedConn.bsonRegistry, document)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restrictedConn.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
			Namespace: "dsync_live.sink_restricted",
			Data:      [][]byte{data},
		})); err != nil {
			t.Fatalf("write as restricted sink role: %v", err)
		}
	}
	writeRestricted(bson.M{"id": int32(1), "required": "inserted", "status": "custom"})
	writeRestricted(bson.M{"id": int32(1), "required": "replaced"})
	restrictedID, _, err := toMongoID(restrictedConn.bsonRegistry, []interface{}{int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restrictedConn.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dsync_live.sink_restricted",
		Updates:   []*adiomv1.Update{{Id: restrictedID, Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE}},
	})); err != nil {
		t.Fatalf("delete as restricted sink role: %v", err)
	}
	var restrictedCount int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM dsync_live.sink_restricted`).Scan(&restrictedCount); err != nil {
		t.Fatal(err)
	}
	if restrictedCount != 0 {
		t.Fatalf("restricted sink left %d rows after delete", restrictedCount)
	}

	replicaConn, err := NewConn(ctx, PostgresSettings{URL: url, Limit: 100, EnableReplicaMode: true})
	if err != nil {
		t.Fatal(err)
	}
	defer replicaConn.Teardown()
	replicaData, err := marshalWithRegistry(replicaConn.bsonRegistry, bson.M{"_id": "synthetic", "id": int32(9)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replicaConn.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dsync_live.sink_replica",
		Data:      [][]byte{replicaData},
	})); err != nil {
		t.Fatal(err)
	}
	var touched bool
	if err := admin.QueryRow(ctx, `SELECT touched FROM dsync_live.sink_replica WHERE id = 9`).Scan(&touched); err != nil || touched {
		t.Fatalf("replica-mode trigger suppression: touched=%v err=%v", touched, err)
	}
}
