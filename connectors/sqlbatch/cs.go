package sqlbatch

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"
)

func GetUpdateCursor(ctx context.Context, c *sql.DB, mapping *Mapping) (updateCursor, error) {
	eg, ctx := errgroup.WithContext(ctx)
	newUpdateCursor := updateCursor{
		Cursor: make([]any, len(mapping.Changes)),
	}
	var mut sync.Mutex
	for i, cfg := range mapping.Changes {
		eg.Go(func() error {
			rows, err := c.QueryContext(ctx, cfg.InitialCursorQuery)
			if err != nil {
				return fmt.Errorf("error getting initial cursor: %w", err)
			}
			defer rows.Close()
			cols, err := rows.Columns()
			if err != nil {
				return fmt.Errorf("error getting columns: %w", err)
			}
			if len(cols) != 1 {
				return fmt.Errorf("error expected exactly 1 column for cursor")
			}
			// Only want the first item
			if rows.Next() {
				res, err := ScanRow(rows, len(cols))
				if err != nil {
					return fmt.Errorf("error scanning row: %w", err)
				}
				mut.Lock()
				newUpdateCursor.Cursor[i] = res[0]
				mut.Unlock()
			} else {
				if rows.Err() != nil {
					return fmt.Errorf("err in get update cursor: %w", rows.Err())
				}
				return fmt.Errorf("error didn't get a cursor")
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return updateCursor{}, fmt.Errorf("error getting cursor: %w", err)
	}
	return newUpdateCursor, nil
}

type CursorUpdate struct {
	Index int
	Value any
}

type KeyFetchRequest struct {
	Key          []any
	UpdateType   adiomv1.UpdateType
	CursorUpdate *CursorUpdate
}

type BatchFetchRequest struct {
	ID   int64
	Keys [][]any
}

type BatchResult struct {
	ID      int64
	Updates []*adiomv1.Update
}

type PendingBatch struct {
	KnownUpdates []*adiomv1.Update
	Cursor       []byte
	Result       *BatchResult
}

func ToID(cols []string, keys []any) ([]*adiomv1.BsonValue, string, error) {
	if len(cols) != len(keys) {
		return nil, "", fmt.Errorf("keys do not match column size")
	}
	res := make([]*adiomv1.BsonValue, len(cols))
	_, d, err := bson.MarshalValue(keys)
	if err != nil {
		return nil, "", fmt.Errorf("err marshalling keys: %w", err)
	}
	raw, err := bson.Raw(d).Values()
	if err != nil {
		return nil, "", fmt.Errorf("err getting key values: %w", err)
	}
	for i, col := range cols {
		r := raw[i]

		res[i] = &adiomv1.BsonValue{
			Data: r.Value,
			Type: uint32(r.Type),
			Name: col,
		}
	}

	return res, base64.StdEncoding.EncodeToString(d), nil
}

func Poller(ctx context.Context, cursor updateCursor, mapping *Mapping, c *sql.DB, dialect SQLDialect, chOut chan<- *adiomv1.StreamUpdatesResponse) error {
	if len(mapping.Changes) != len(cursor.Cursor) {
		return fmt.Errorf("invalid cursor length got %v, expected %v", len(cursor.Cursor), len(mapping.Changes))
	}
	numFetchers := mapping.Fetchers
	if numFetchers < 1 {
		numFetchers = 1
	}

	keyFetchCh := make(chan KeyFetchRequest, mapping.Limit*2)
	defer close(keyFetchCh)

	batchFetchCh := make(chan BatchFetchRequest)
	defer close(batchFetchCh)

	batchResultCh := make(chan BatchResult)
	defer close(batchResultCh)

	eg, egCtx := errgroup.WithContext(ctx)

	// Poll for changes
	for i := range mapping.Changes {
		currentCursor := cursor.Cursor[i]
		cfg := mapping.Changes[i]
		interval := cfg.Interval
		if interval <= 0 {
			interval = time.Second * 5
		}

		eg.Go(func() error {
			defer slog.Debug("bye poller", "i", i, "ns", mapping.Namespace)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for {
						rows, err := c.QueryContext(egCtx, cfg.Query, currentCursor)
						if err != nil {
							return fmt.Errorf("error getting changes: %w", err)
						}
						cols, err := rows.Columns()
						if err != nil {
							_ = rows.Close()
							return fmt.Errorf("error getting columns: %w", err)
						}
						if len(cols) < 3 {
							_ = rows.Close()
							return fmt.Errorf("error expected at least 3 columns for changes")
						}

						var count int
						for rows.Next() {
							count += 1
							res, err := ScanRow(rows, len(cols))
							if err != nil {
								_ = rows.Close()
								return fmt.Errorf("error scanning row: %w", err)
							}
							updateType, nextCursor := res[len(res)-2], res[len(res)-1]
							updateTypeStr, ok := updateType.(string)
							if !ok {
								_ = rows.Close()
								return fmt.Errorf("unexpected update type %T", updateType)
							}
							// TODO: consider formalizing the protocol for determining type
							adiomType := adiomv1.UpdateType_UPDATE_TYPE_UPDATE
							if updateTypeStr == "D" {
								adiomType = adiomv1.UpdateType_UPDATE_TYPE_DELETE
							}

							slog.Debug("sqlbatch poll", "poller", i, "count", count, "currentCursor", currentCursor, "nextCursor", nextCursor)
							select {
							case keyFetchCh <- KeyFetchRequest{
								Key:        res[:len(res)-2],
								UpdateType: adiomType,
							}:
							case <-egCtx.Done():
								_ = rows.Close()
								return nil
							}

							currentCursor = nextCursor
						}
						if rows.Err() != nil {
							_ = rows.Close()
							return fmt.Errorf("err in poller: %w", rows.Err())
						}
						if count <= 0 {
							_ = rows.Close()
							break
						}
						select {
						case keyFetchCh <- KeyFetchRequest{
							CursorUpdate: &CursorUpdate{
								Index: i,
								Value: currentCursor,
							},
						}:
						case <-egCtx.Done():
							_ = rows.Close()
							return nil
						}
						if count < mapping.Limit {
							_ = rows.Close()
							break
						}
						_ = rows.Close()
					}
				case <-egCtx.Done():
					return nil
				}
			}
		})
	}

	pendingBatches := map[int64]*PendingBatch{}
	var pendingBatchesMut sync.Mutex
	// Handle batching and collecting the final results
	eg.Go(func() error {
		var minBatchIdx int64
		for {
			select {
			case result, ok := <-batchResultCh:
				if !ok {
					return nil
				}
				slog.Debug("sqlbatch fetch result", "id", result.ID, "min", minBatchIdx)
				pendingBatchesMut.Lock()
				pendingBatches[result.ID].Result = &result
				pendingBatchesMut.Unlock()
				for {
					pendingBatchesMut.Lock()
					if batch, ok := pendingBatches[minBatchIdx]; ok && batch.Result != nil {
						delete(pendingBatches, minBatchIdx)
						pendingBatchesMut.Unlock()
						minBatchIdx += 1
						updates := append(batch.KnownUpdates, batch.Result.Updates...)
						select {
						case chOut <- &adiomv1.StreamUpdatesResponse{
							Updates:    updates,
							Namespace:  mapping.Namespace,
							NextCursor: batch.Cursor,
						}:
						case <-egCtx.Done():
							return nil
						}
					} else {
						pendingBatchesMut.Unlock()
						break
					}
				}
			case <-egCtx.Done():
				return nil
			}
		}
	})
	eg.Go(func() error {
		currentCursor := updateCursor{
			Cursor: make([]any, len(cursor.Cursor)),
		}
		_ = copy(currentCursor.Cursor, cursor.Cursor)
		var nextBatchIdx int64

		idMap := map[string]int{}
		deletes := map[string]struct{}{}
		var batch [][]any
		var batchB64 []string
		var batchBson [][]*adiomv1.BsonValue
		for {
			select {
			case request, ok := <-keyFetchCh:
				if !ok {
					return nil
				}
				if request.Key != nil {
					id, b64, err := ToID(mapping.Cols, request.Key)
					if err != nil {
						return fmt.Errorf("error getting ID: %w", err)
					}
					if request.UpdateType == adiomv1.UpdateType_UPDATE_TYPE_DELETE {
						deletes[b64] = struct{}{}
					} else {
						delete(deletes, b64)
					}
					if _, ok := idMap[b64]; !ok {
						idMap[b64] = len(batch)
						batch = append(batch, request.Key)
						batchB64 = append(batchB64, b64)
						batchBson = append(batchBson, id)
					}
				}
				if request.CursorUpdate != nil {
					currentCursor.Cursor[request.CursorUpdate.Index] = request.CursorUpdate.Value
				}
				if len(batch) > 0 && (len(batch) >= mapping.Limit || len(keyFetchCh) == 0) {
					slog.Debug("sqlbatch batch request a fetch", "count", len(batch), "namespace", mapping.Namespace)
					// Request fetch for this batch
					cursor, err := currentCursor.Encode()
					if err != nil {
						return fmt.Errorf("error encoding cursor: %w", err)
					}
					knownUpdates := make([]*adiomv1.Update, len(deletes))
					j := 0
					k := 0
					actualBatch := make([][]any, len(batch)-len(deletes))
					for i := range batch {
						b64 := batchB64[i]
						if _, ok := deletes[b64]; ok {
							knownUpdates[j] = &adiomv1.Update{
								Id:   batchBson[i],
								Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
							}
							j++
						} else {
							actualBatch[k] = batch[i]
							k++
						}
					}
					pendingBatchesMut.Lock()
					pendingBatches[nextBatchIdx] = &PendingBatch{
						KnownUpdates: knownUpdates,
						Cursor:       cursor,
					}
					pendingBatchesMut.Unlock()
					select {
					case batchFetchCh <- BatchFetchRequest{
						ID:   nextBatchIdx,
						Keys: actualBatch,
					}:
					case <-egCtx.Done():
						return nil
					}
					nextBatchIdx += 1
					idMap = map[string]int{}
					deletes = map[string]struct{}{}
					batch = nil
					batchB64 = nil
					batchBson = nil
					knownUpdates = nil
				}
			case <-egCtx.Done():
				return nil
			}
		}
	})

	// Fetch data
	template := dialect.FetchTemplate(mapping.Cols, mapping.Query)
	for i := range numFetchers {
		eg.Go(func() error {
			defer slog.Debug("bye fetcher", "i", i, "ns", mapping.Namespace)
			for {
				select {
				case request, ok := <-batchFetchCh:
					if !ok {
						return nil
					}
					if len(request.Keys) == 0 {
						select {
						case batchResultCh <- BatchResult{
							ID:      request.ID,
							Updates: nil,
						}:
						case <-egCtx.Done():
							return nil
						}
						continue
					}

					numCols := len(mapping.Cols)
					keySub := dialect.KeySub(mapping.Cols, len(request.Keys))
					query := fmt.Sprintf(template, keySub)
					slog.Debug("sqlbatch fetch", "fetcher", i, "query", query, "len", len(request.Keys), "batch-id", request.ID)
					subs := make([]any, numCols*len(request.Keys))
					for i, k := range request.Keys {
						if len(k) != numCols {
							return fmt.Errorf("key has unexpected length %v, expected %v", len(k), numCols)
						}
						for j := range k {
							subs[i*numCols+j] = k[j]
						}
					}
					rows, err := c.QueryContext(egCtx, query, subs...)
					if err != nil {
						return fmt.Errorf("err querying: %w", err)
					}
					cols, err := rows.Columns()
					if err != nil {
						_ = rows.Close()
						return fmt.Errorf("err getting columns: %w", err)
					}
					results := make([]*adiomv1.Update, 0, len(request.Keys))
					for rows.Next() {
						m, err := ParseRow(cols, rows, mapping)
						if err != nil {
							_ = rows.Close()
							return fmt.Errorf("err in parse row: %w", err)
						}
						newData, err := json.Marshal(m)
						if err != nil {
							_ = rows.Close()
							return fmt.Errorf("err in json marshal: %w", err)
						}
						keys := make([]any, len(mapping.Cols))
						for i, k := range mapping.Cols {
							keys[i] = m[k]
						}
						bsonKey, _, err := ToID(mapping.Cols, keys)
						if err != nil {
							_ = rows.Close()
							return fmt.Errorf("err getting bson keys: %w", err)
						}
						results = append(results, &adiomv1.Update{
							Id:   bsonKey,
							Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
							Data: newData,
						})
					}
					if rows.Err() != nil {
						_ = rows.Close()
						return fmt.Errorf("err in fetcher: %w", rows.Err())
					}
					select {
					case batchResultCh <- BatchResult{
						ID:      request.ID,
						Updates: results,
					}:
					case <-egCtx.Done():
						_ = rows.Close()
						return nil
					}
					_ = rows.Close()
				case <-egCtx.Done():
					return nil
				}
			}
		})
	}

	return eg.Wait()
}
