package pglib

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

func TestChangeStreamLiveToastAndTruncate(t *testing.T) {
	connectionURL := os.Getenv("POSTGRES_TEST_URL")
	if connectionURL == "" {
		t.Skip("POSTGRES_TEST_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, connectionURL)
	if err != nil {
		t.Fatal(err)
	}

	const publication = "dsync_live_publication"
	const slot = "dsync_live_slot"
	var cancelStream context.CancelFunc
	var streamDone <-chan struct{}
	t.Cleanup(func() {
		if cancelStream != nil {
			cancelStream()
			select {
			case <-streamDone:
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for replication stream to stop")
			}
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, statement := range []string{
			`SELECT pg_drop_replication_slot('dsync_live_slot') FROM pg_replication_slots WHERE slot_name = 'dsync_live_slot'`,
			`DROP PUBLICATION IF EXISTS dsync_live_publication`,
			`DROP SCHEMA IF EXISTS dsync_stream_live CASCADE`,
		} {
			if _, err := admin.Exec(cleanupCtx, statement); err != nil {
				t.Errorf("cleanup %s: %v", statement, err)
			}
		}
		if err := admin.Close(cleanupCtx); err != nil {
			t.Errorf("close admin connection: %v", err)
		}
	})

	for _, statement := range []string{
		`DROP PUBLICATION IF EXISTS dsync_live_publication`,
		`SELECT pg_drop_replication_slot('dsync_live_slot') FROM pg_replication_slots WHERE slot_name = 'dsync_live_slot'`,
		`DROP SCHEMA IF EXISTS dsync_stream_live CASCADE`,
		`CREATE SCHEMA dsync_stream_live`,
		`CREATE TABLE dsync_stream_live.events (id integer PRIMARY KEY, payload text, marker text)`,
		`ALTER TABLE dsync_stream_live.events REPLICA IDENTITY FULL`,
		`INSERT INTO dsync_stream_live.events SELECT 1, string_agg(md5(i::text), ''), 'before' FROM generate_series(1, 2000) AS i`,
		`CREATE PUBLICATION dsync_live_publication FOR TABLE dsync_stream_live.events`,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	var startText string
	if err := admin.QueryRow(ctx, `SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')`, slot).Scan(&startText); err != nil {
		t.Fatal(err)
	}
	startLSN, err := pglogrepl.ParseLSN(startText)
	if err != nil {
		t.Fatal(err)
	}
	replicationURL, err := url.Parse(connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	query := replicationURL.Query()
	query.Set("replication", "database")
	replicationURL.RawQuery = query.Encode()

	streamCtx, cancel := context.WithCancel(ctx)
	cancelStream = cancel
	done := make(chan struct{})
	streamDone = done
	updates := make(chan UpdateBatch, 4)
	streamErrors := make(chan error, 1)
	go func() {
		defer close(done)
		streamErrors <- (&ChangeStream{
			ReplicationURL:        replicationURL.String(),
			ReplicationSlotName:   slot,
			PublicationName:       publication,
			StartLSN:              uint64(startLSN),
			MinFlushDelay:         time.Millisecond,
			StandbyMessageTimeout: 100 * time.Millisecond,
		}).Run(streamCtx, updates)
	}()

	if _, err := admin.Exec(ctx, `UPDATE dsync_stream_live.events SET marker = 'after' WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `TRUNCATE dsync_stream_live.events`); err != nil {
		t.Fatal(err)
	}

	nextBatch := func() UpdateBatch {
		t.Helper()
		select {
		case batch := <-updates:
			return batch
		case err := <-streamErrors:
			t.Fatalf("change stream failed: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for change stream")
		}
		return UpdateBatch{}
	}

	updateBatch := nextBatch()
	if len(updateBatch.Updates) != 1 {
		t.Fatalf("update batch has %d updates", len(updateBatch.Updates))
	}
	update := updateBatch.Updates[0]
	payload, ok := update.New["payload"].(string)
	if !ok || len(payload) != 64000 || update.New["marker"] != "after" {
		t.Fatalf("TOAST update was not reconstructed: payload type=%T length=%d marker=%v", update.New["payload"], len(payload), update.New["marker"])
	}

	truncateBatch := nextBatch()
	if len(truncateBatch.Updates) != 0 || truncateBatch.LSN == 0 {
		t.Fatalf("TRUNCATE batch = %#v", truncateBatch)
	}
}
