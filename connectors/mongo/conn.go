package mongo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/metrics"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/cespare/xxhash"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ConnectorSettings struct {
	ConnectionString string

	ServerConnectTimeout       time.Duration
	PingTimeout                time.Duration
	WriterMaxBatchSize         int   // applies to batch inserts only; 0 means no limit
	TargetDocCountPerPartition int64 //target number of documents per partition (256k docs is 256MB with 1KB average doc size)
	SampleFactor               int   // a factor to determine how many extra samples per partition are used
	MaxPageSize                int
	PerNamespaceStreams        bool

	Query string // query filter, as a v2 Extended JSON string, e.g., '{\"x\":{\"$gt\":1}}'"

	UniqueIndexNamespaces map[string]struct{}
}

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

type bufferData struct {
	data [][]byte
	err  error
}

type buffer struct {
	ctr     int64
	ch      <-chan bufferData
	last    *adiomv1.ListDataResponse
	cleanup *time.Timer
	err     error
}

type conn struct {
	client *mongo.Client

	settings ConnectorSettings

	nextCursorID    atomic.Int64
	ctx             context.Context
	cancel          context.CancelFunc
	buffersMutex    sync.RWMutex
	buffers         map[int64]buffer
	cleanupInterval time.Duration

	query bson.D
}

// get all database names except system databases
func getAllDatabases(ctx context.Context, client *mongo.Client) ([]string, error) {
	dbNames, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	dbs := slices.DeleteFunc(dbNames, func(d string) bool {
		return slices.Contains(ExcludedDBListForIC, d)
	})

	return dbs, nil
}

// get all collections in a database except system collections
func getAllCollections(ctx context.Context, client *mongo.Client, dbName string) ([]string, error) {
	collectionsAll, err := client.Database(dbName).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	//remove all system collections that match the pattern
	r, _ := regexp.Compile(ExcludedSystemCollPattern)
	collections := slices.DeleteFunc(collectionsAll, func(n string) bool {
		return r.Match([]byte(n))
	})

	return collections, nil
}

func GetCol(client *mongo.Client, ns string) (*mongo.Collection, iface.Namespace, bool) {
	n, ok := ToNS(ns)
	return client.Database(n.Db).Collection(n.Col), n, ok
}

func ToNS(ns string) (iface.Namespace, bool) {
	db, col, ok := strings.Cut(ns, ".")
	return iface.Namespace{Db: db, Col: col}, ok
}

func NamespacePartitions(ctx context.Context, namespaces []string, client *mongo.Client) ([]*adiomv1.Partition, error) {
	var dbsToResolve []string //database names that we need to resolve
	var partitions []*adiomv1.Partition

	if len(namespaces) < 1 {
		var err error
		dbsToResolve, err = getAllDatabases(ctx, client)
		if err != nil {
			return nil, err
		}
	} else {
		// iterate over provided namespaces
		// if it has a dot, then it is a fully qualified namespace
		// otherwise, it is a database name to resolve
		for _, ns := range namespaces {
			db, _, isFQN := strings.Cut(ns, ".")
			if isFQN {
				partitions = append(partitions, &adiomv1.Partition{
					Namespace: ns,
				})
			} else {
				dbsToResolve = append(dbsToResolve, db)
			}
		}
	}

	slog.Debug(fmt.Sprintf("Databases to resolve: %v", dbsToResolve))

	//iterate over unresolved databases and get all collections
	for _, db := range dbsToResolve {
		cols, err := getAllCollections(ctx, client, db)
		if err != nil {
			return nil, err
		}
		for _, col := range cols {
			partitions = append(partitions, &adiomv1.Partition{
				Namespace: fmt.Sprintf("%v.%v", db, col),
			})
		}
	}

	return partitions, nil
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	partitions, err := NamespacePartitions(ctx, r.Msg.GetNamespaces(), c.client)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var updatesPartitions []*adiomv1.UpdatesPartition

	if r.Msg.GetUpdates() {
		if c.settings.PerNamespaceStreams {
			// TODO: maybe parallelize
			for _, partition := range partitions {
				ns, _ := ToNS(partition.Namespace)
				col := c.client.Database(ns.Db).Collection(ns.Col)
				pipeline := mongo.Pipeline{}
				if len(c.query) > 0 {
					pipeline = mongo.Pipeline{
						{{"$match", mapExtraFilterForChangeStream(c.query)}},
					}
				}
				resumeToken, err := getLatestResumeToken(ctx, col, pipeline)
				if err != nil {
					slog.Error(fmt.Sprintf("Failed to get latest resume token for ns %v: %v", partition.GetNamespace(), err))
					return nil, connect.NewError(connect.CodeInternal, err)
				}

				updatesPartitions = append(updatesPartitions, &adiomv1.UpdatesPartition{
					Namespaces: []string{partition.GetNamespace()},
					Cursor:     resumeToken,
				})
			}
		} else {
			var updatesNamespaces []string
			var namespaces []iface.Namespace
			if len(r.Msg.GetNamespaces()) > 0 {
				for _, partition := range partitions {
					updatesNamespaces = append(updatesNamespaces, partition.GetNamespace())
					ns, ok := ToNS(partition.GetNamespace())
					if !ok {
						return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
					}
					namespaces = append(namespaces, ns)
				}
			}

			nsFilter := createChangeStreamNamespaceFilterFromNamespaces(namespaces, c.query)
			resumeToken, err := getLatestResumeToken(ctx, c.client, mongo.Pipeline{
				{{"$match", nsFilter}},
			})
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get latest resume token: %v", err))
				return nil, connect.NewError(connect.CodeInternal, err)
			}

			updatesPartitions = []*adiomv1.UpdatesPartition{{Namespaces: updatesNamespaces, Cursor: resumeToken}}
		}
	}

	done := make(chan struct{})
	eg, ctx := errgroup.WithContext(ctx)
	var finalPartitions []*adiomv1.Partition
	ch := make(chan *adiomv1.Partition)

	go func() {
		defer close(done)
		for p := range ch {
			finalPartitions = append(finalPartitions, p)
		}
	}()

	for _, partition := range partitions {
		eg.Go(func() error {
			ns, _ := ToNS(partition.Namespace)
			col := c.client.Database(ns.Db).Collection(ns.Col)
			count, err := c.count(col, ctx)
			if err != nil {
				return err
			}
			if count < c.settings.TargetDocCountPerPartition*2 {
				ch <- &adiomv1.Partition{
					Namespace:      partition.GetNamespace(),
					EstimatedCount: uint64(count),
				}
				return nil
			}
			numSamples := (count / c.settings.TargetDocCountPerPartition) * int64(c.settings.SampleFactor)
			if numSamples*20 >= count {
				slog.Warn("Too many samples requested, adjusting...", "original", numSamples, "new", count/20)
				numSamples = count / 20
			}
			res, err := col.Aggregate(ctx, mongo.Pipeline{{{"$sample", bson.D{{"size", numSamples}}}}, {{"$sort", bson.D{{"_id", 1}}}}})
			if err != nil {
				return err
			}
			var factorCount = c.settings.SampleFactor / 2
			var low bson.RawValue
			for res.Next(ctx) {
				if factorCount > 0 {
					factorCount -= 1
					continue
				}
				factorCount = c.settings.SampleFactor - 1
				high := res.Current.Lookup("_id")
				ch <- &adiomv1.Partition{
					Namespace:      partition.GetNamespace(),
					EstimatedCount: uint64(c.settings.TargetDocCountPerPartition),
					Cursor:         EncodeCursor(low, high),
				}
				low = high
			}
			ch <- &adiomv1.Partition{
				Namespace:      partition.GetNamespace(),
				EstimatedCount: uint64(c.settings.TargetDocCountPerPartition),
				Cursor:         EncodeCursor(low, bson.RawValue{}),
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		close(ch)
		return nil, err
	}
	close(ch)
	<-done

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        finalPartitions,
		UpdatesPartitions: updatesPartitions,
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(ctx context.Context, r *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	// Get version of the MongoDB server
	var commandResult bson.M
	err := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&commandResult)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	version := commandResult["version"]

	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:      string(generateConnectorID(c.settings.ConnectionString + c.settings.Query)),
		DbType:  connectorDBType,
		Version: version.(string),
		Spec:    connectorSpec,
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
				LsnStream:          true,
				MultiNamespacePlan: true,
				DefaultPlan:        !c.settings.PerNamespaceStreams,
			},
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
			},
		},
	}), nil
}

func (c *conn) count(col *mongo.Collection, ctx context.Context) (int64, error) {
	if len(c.query) == 0 {
		return col.EstimatedDocumentCount(ctx)
	}
	col2, err := col.Clone(options.Collection().SetReadConcern(readconcern.Available()))
	if err != nil {
		return 0, err
	}
	return col2.CountDocuments(ctx, c.query)
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(ctx context.Context, r *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	collection, _, ok := GetCol(c.client, r.Msg.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}
	count, err := c.count(collection, ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error(fmt.Sprintf("Failed to count documents: %v", err))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(count),
	}), nil
}

func EncodeCursor(low bson.RawValue, high bson.RawValue) []byte {
	if low.IsZero() && high.IsZero() {
		return nil
	}
	var d bson.D
	if !low.IsZero() {
		d = append(d, primitive.E{"low", low})
	}
	if !high.IsZero() {
		d = append(d, primitive.E{"high", high})
	}
	res, _ := bson.Marshal(d)
	return res
}

func DecodeCursor(cursor []byte) (bson.RawValue, bson.RawValue) {
	if len(cursor) == 0 {
		return bson.RawValue{}, bson.RawValue{}
	}
	raw := bson.Raw(cursor)
	low := raw.Lookup("low")
	high := raw.Lookup("high")
	return low, high
}

func createFindFilterFromCursor(cursor []byte) bson.D {
	low, high := DecodeCursor(cursor)

	if low.IsZero() && high.IsZero() { //no boundaries
		return bson.D{}
	} else if low.IsZero() { //only upper boundary
		return bson.D{
			{"_id", bson.D{
				{"$lte", high},
			}},
		}
	} else if high.IsZero() { //only lower boundary
		return bson.D{
			{"_id", bson.D{
				{"$gt", low},
			}},
		}
	} else { //both boundaries
		return bson.D{
			{"_id", bson.D{
				{"$gt", low},
				{"$lte", high},
			}},
		}
	}
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	partition := r.Msg.GetPartition()
	collection, _, ok := GetCol(c.client, partition.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}

	var cursorID, ctr int64

	pageCursor := r.Msg.GetCursor()
	if pageCursor == nil {
		cursorID = c.nextCursorID.Add(1)
		ch := make(chan bufferData, 10)
		c.buffersMutex.Lock()
		c.buffers[cursorID] = buffer{
			ctr:  0,
			ch:   ch,
			last: nil,
			cleanup: time.AfterFunc(c.cleanupInterval, func() {
				c.buffersMutex.Lock()
				delete(c.buffers, cursorID)
				c.buffersMutex.Unlock()
			}),
		}
		c.buffersMutex.Unlock()

		filter := createFindFilterFromCursor(r.Msg.GetPartition().GetCursor())
		if len(c.query) > 0 { //if a query is provided, append it to the filter
			filter = append(filter, c.query...)
		}
		slog.Debug("query filter", "filter", filter)

		cursor, err := collection.Find(ctx, filter)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Debug("Find Error", "filter", filter)
				slog.Error(fmt.Sprintf("Failed to find documents: %v", err))
			}
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		go func(ctx context.Context) {
			defer cursor.Close(ctx)
			defer close(ch)
			var dataBatch [][]byte
			for cursor.Next(ctx) {
				dataBatch = append(dataBatch, cursor.Current)
				if cursor.RemainingBatchLength() == 0 || (c.settings.MaxPageSize != 0 && len(dataBatch) == int(c.settings.MaxPageSize)) {
					select {
					case ch <- bufferData{data: dataBatch}:
						dataBatch = nil
					case <-ctx.Done():
						ch <- bufferData{err: ctx.Err()}
						return
					}
				}
			}
			if cursor.Err() != nil {
				if !errors.Is(cursor.Err(), context.Canceled) {
					slog.Error(fmt.Sprintf("Failed to iterate through documents: %v", cursor.Err()))
				}
				ch <- bufferData{err: cursor.Err()}
			}
		}(c.ctx)
	} else {
		var err error
		br := bytes.NewReader(r.Msg.GetCursor())
		cursorID, err = binary.ReadVarint(br)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cursor: %w", err))
		}
		ctr, err = binary.ReadVarint(br)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cursor: %w", err))
		}
	}
	c.buffersMutex.RLock()
	buffer, ok := c.buffers[cursorID]
	c.buffersMutex.RUnlock()
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cursor"))
	}
	buffer.cleanup.Reset(c.cleanupInterval)
	if buffer.err != nil {
		return nil, connect.NewError(connect.CodeInternal, buffer.err)
	}
	if ctr+1 == buffer.ctr && buffer.last != nil {
		return connect.NewResponse(buffer.last), nil
	}
	if ctr != buffer.ctr { // Reject for old or invalid pages
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no longer available"))
	}

	for {
		select {
		case dataBatch, ok := <-buffer.ch:
			if dataBatch.err != nil {
				buffer.err = dataBatch.err
				c.buffersMutex.Lock()
				c.buffers[cursorID] = buffer
				c.buffersMutex.Unlock()
				return nil, connect.NewError(connect.CodeInternal, buffer.err)
			}
			var nextCursor []byte
			if !ok {
				buffer.last = &adiomv1.ListDataResponse{}
				return connect.NewResponse(buffer.last), nil
			}
			buffer.ctr++
			nextCursor = binary.AppendVarint(nil, cursorID)
			nextCursor = binary.AppendVarint(nextCursor, ctr+1)

			resp := &adiomv1.ListDataResponse{
				Data:       dataBatch.data,
				NextCursor: nextCursor,
			}
			buffer.last = resp
			buffer.cleanup.Reset(c.cleanupInterval)
			c.buffersMutex.Lock()
			c.buffers[cursorID] = buffer
			c.buffersMutex.Unlock()
			return connect.NewResponse(resp), nil
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	var watcher Watchable
	var pipeline mongo.Pipeline

	var namespaces []iface.Namespace
	for _, namespace := range r.Msg.GetNamespaces() {
		ns, ok := ToNS(namespace)
		if !ok {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
		}
		namespaces = append(namespaces, ns)
	}

	if c.settings.PerNamespaceStreams {
		if len(namespaces) != 1 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected exactly 1 namespace"))
		}
		watcher = c.client.Database(namespaces[0].Db).Collection(namespaces[0].Col)
		pipeline = mongo.Pipeline{}
		if len(c.query) > 0 {
			pipeline = mongo.Pipeline{
				{{"$match", mapExtraFilterForChangeStream(c.query)}},
			}
		}
	} else {
		watcher = c.client
		nsFilter := createChangeStreamNamespaceFilterFromNamespaces(namespaces, c.query)
		slog.Debug("LSN Filter", "filter", nsFilter)
		pipeline = mongo.Pipeline{
			{{"$match", nsFilter}},
		}
	}
	opts := moptions.ChangeStream().SetStartAfter(bson.Raw(r.Msg.GetCursor()))
	if len(c.query) > 0 {
		opts = opts.SetFullDocument("updateLookup")
		pipeline = append(pipeline, bson.D{{"$project", bson.M{"_id": 1}}})
	}

	changeStream, err := watcher.Watch(ctx, pipeline, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("LSN tracker: Failed to open change stream: %v", err))
		return connect.NewError(connect.CodeInternal, err)
	}
	defer changeStream.Close(ctx)

	lsn := 0
	for changeStream.Next(ctx) {
		lsn++
		if changeStream.RemainingBatchLength() == 0 {
			err := s.Send(&adiomv1.StreamLSNResponse{
				Lsn:        uint64(lsn),
				NextCursor: changeStream.ResumeToken(),
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					break
				}
			}
		}
	}
	if changeStream.Err() != nil {
		if errors.Is(changeStream.Err(), context.Canceled) {
			return nil
		}
		return connect.NewError(connect.CodeInternal, changeStream.Err())
	}
	return nil
}

func extraFilterForChangeStream(filters bson.D) bson.D {
	mapped := mapExtraFilterForChangeStream(filters).(bson.D)
	return bson.D{{"$or", []bson.D{mapped, {{"operationType", "delete"}}}}}
}

func mapExtraFilterForChangeStream(filters interface{}) interface{} {
	switch filter := filters.(type) {
	case bson.A:
		var newFilters bson.A
		for _, f := range filter {
			newFilters = append(newFilters, mapExtraFilterForChangeStream(f))
		}
		return newFilters
	case bson.D:
		var newFilters bson.D
		for _, f := range filter {
			newValue := mapExtraFilterForChangeStream(f.Value)
			if !strings.HasPrefix(f.Key, "$") {
				newKey := fmt.Sprintf("fullDocument.%v", f.Key)
				newFilters = append(newFilters, bson.E{newKey, newValue})
			} else {
				newFilters = append(newFilters, bson.E{f.Key, newValue})
			}
		}
		return newFilters
	case bson.M:
		newFilters := bson.M{}
		for k, v := range filter {
			newValue := mapExtraFilterForChangeStream(v)
			if !strings.HasPrefix(k, "$") {
				newKey := fmt.Sprintf("fullDocument.%v", k)
				newFilters[newKey] = newValue
			} else {
				newFilters[k] = newValue
			}
		}
		return newFilters
	default:
		return filter
	}
}

// create a change stream filter that covers all namespaces except system
func createChangeStreamNamespaceFilter(extraFilters bson.D) bson.D {
	filter := []bson.D{
		{{"ns.db", bson.D{{"$regex", primitive.Regex{Pattern: ExcludedDBPatternCS}}}}},
		{{"ns.coll", bson.D{{"$regex", primitive.Regex{Pattern: ExcludedSystemCollPatternCS}}}}},
	}
	if len(extraFilters) > 0 {
		filter = append(filter, extraFilterForChangeStream(extraFilters))
	}
	return bson.D{{"$and", filter}}
}

// creates a filter for the change stream to include only the specified namespaces
func createChangeStreamNamespaceFilterFromNamespaces(namespaces []iface.Namespace, extraFilters bson.D) bson.D {
	if len(namespaces) == 0 {
		return createChangeStreamNamespaceFilter(extraFilters)
	}

	var filters []bson.D
	for _, namespace := range namespaces {
		filters = append(filters, bson.D{{"ns.db", namespace.Db}, {"ns.coll", namespace.Col}})
	}

	if len(extraFilters) > 0 {
		return bson.D{{"$and", []bson.D{{{"$or", filters}}, extraFilterForChangeStream(extraFilters)}}}

	}

	return bson.D{{"$or", filters}}
}

func convertChangeStreamEventToUpdate(change bson.M) (*adiomv1.Update, error) {
	// slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

	optype := change["operationType"].(string)
	var update *adiomv1.Update

	switch optype {
	case "insert":
		// get the id of the document that was inserted
		id := change["documentKey"].(bson.M)["_id"]
		// convert id to raw bson
		idType, idVal, err := bson.MarshalValue(id)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal _id: %v", err)
		}
		fullDocument := change["fullDocument"].(bson.M)
		// convert fulldocument to BSON.Raw
		fullDocumentRaw, err := bson.Marshal(fullDocument)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal full document: %v", err)
		}
		update = &adiomv1.Update{
			Id: []*adiomv1.BsonValue{{
				Data: idVal,
				Type: uint32(idType),
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: fullDocumentRaw,
		}
	case "update", "replace":
		// get the id of the document that was changed
		id := change["documentKey"].(bson.M)["_id"]
		// convert id to raw bson
		idType, idVal, err := bson.MarshalValue(id)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal _id: %v", err)
		}
		// get the full state of the document after the change
		if change["fullDocument"] == nil {
			//TODO (AK, 6/2024): find a better way to report that we need to ignore this event
			return nil, nil // no full document, nothing to do (probably got deleted before we got to the event in the change stream)
		}
		fullDocument := change["fullDocument"].(bson.M)
		// convert fulldocument to BSON.Raw
		fullDocumentRaw, err := bson.Marshal(fullDocument)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal full document: %v", err)
		}
		update = &adiomv1.Update{
			Id: []*adiomv1.BsonValue{{
				Data: idVal,
				Type: uint32(idType),
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: fullDocumentRaw,
		}
	case "delete":
		// get the id of the document that was deleted
		id := change["documentKey"].(bson.M)["_id"]
		// convert id to raw bson
		idType, idVal, err := bson.MarshalValue(id)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal _id: %v", err)
		}
		update = &adiomv1.Update{
			Id: []*adiomv1.BsonValue{{
				Data: idVal,
				Type: uint32(idType),
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}
	default:
		return nil, fmt.Errorf("unsupported change event operation type: %v", optype)
	}

	return update, nil
}

func toTimestampPB(t primitive.Timestamp) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(time.Unix(int64(t.T), 0))
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	var watcher Watchable
	var pipeline mongo.Pipeline

	var namespaces []iface.Namespace
	for _, namespace := range r.Msg.GetNamespaces() {
		ns, ok := ToNS(namespace)
		if !ok {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
		}
		namespaces = append(namespaces, ns)
	}

	if c.settings.PerNamespaceStreams {
		if len(namespaces) != 1 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected exactly 1 namespace"))
		}
		watcher = c.client.Database(namespaces[0].Db).Collection(namespaces[0].Col)
		pipeline = mongo.Pipeline{}
		if len(c.query) > 0 {
			pipeline = mongo.Pipeline{
				{{"$match", mapExtraFilterForChangeStream(c.query)}},
			}
		}
	} else {
		watcher = c.client
		nsFilter := createChangeStreamNamespaceFilterFromNamespaces(namespaces, c.query)
		slog.Debug(fmt.Sprintf("Change stream namespace filter: %v", nsFilter))
		pipeline = mongo.Pipeline{
			{{"$match", nsFilter}},
		}
	}
	opts := moptions.ChangeStream().SetStartAfter(bson.Raw(r.Msg.GetCursor())).SetFullDocument("updateLookup")

	changeStream, err := watcher.Watch(ctx, pipeline, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to open change stream: %v", err))
		return connect.NewError(connect.CodeInternal, err)
	}
	defer changeStream.Close(ctx)

	var updates []*adiomv1.Update
	var currentNamespace string
	var lastResumeToken bson.Raw
	var lastTime primitive.Timestamp

	for changeStream.Next(ctx) {
		var change bson.M
		if err := changeStream.Decode(&change); err != nil {
			slog.Error(fmt.Sprintf("Failed to decode change stream event: %v", err))
			continue
		}

		update, err := convertChangeStreamEventToUpdate(change)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert change stream event to data message: %v", err))
			continue
		}
		if update == nil {
			continue
		}

		db := change["ns"].(bson.M)["db"].(string)
		col := change["ns"].(bson.M)["coll"].(string)
		newNamespace := fmt.Sprintf("%v.%v", db, col)

		if currentNamespace == "" || currentNamespace != newNamespace {
			if len(updates) > 0 {
				err := s.Send(&adiomv1.StreamUpdatesResponse{
					Updates:    updates,
					Namespace:  currentNamespace,
					NextCursor: lastResumeToken,
					Time:       toTimestampPB(lastTime),
				})
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					slog.Error(fmt.Sprintf("failed to send updates: %v", err))
				}
			}
			currentNamespace = newNamespace
			updates = nil
		}

		updates = append(updates, update)
		lastResumeToken = changeStream.ResumeToken()
		if lt, ok := change["clusterTime"].(primitive.Timestamp); ok {
			lastTime = lt
		}

		if changeStream.RemainingBatchLength() == 0 {
			err := s.Send(&adiomv1.StreamUpdatesResponse{
				Updates:    updates,
				Namespace:  currentNamespace,
				NextCursor: lastResumeToken,
				Time:       toTimestampPB(lastTime),
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				slog.Error(fmt.Sprintf("failed to send updates: %v", err))
			}
			updates = nil
		}
	}
	if changeStream.Err() != nil {
		if errors.Is(changeStream.Err(), context.Canceled) {
			return nil
		}
		return connect.NewError(connect.CodeInternal, changeStream.Err())
	}

	if len(updates) > 0 {
		err := s.Send(&adiomv1.StreamUpdatesResponse{
			Updates:    updates,
			Namespace:  currentNamespace,
			NextCursor: lastResumeToken,
			Time:       toTimestampPB(lastTime),
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error(fmt.Sprintf("failed to send updates: %v", err))
			}
		}
	}

	return nil
}

// inserts data and overwrites on conflict
func insertBatchOverwrite(ctx context.Context, collection *mongo.Collection, documents []interface{}, ignoreSecondDuplicateError bool) error {
	// eagerly attempt an unordered insert
	_, bwErr := collection.InsertMany(ctx, documents, options.InsertMany().SetOrdered(false))

	// check the errors and collect those that errored out due to duplicate key errors
	// we will skip all the other errors for now
	if bwErr != nil {
		var bulkOverwrite []mongo.WriteModel

		// check if it's a bulk write exception
		var bwErrWriteErrors mongo.BulkWriteException
		if errors.As(bwErr, &bwErrWriteErrors) {
			for _, we := range bwErrWriteErrors.WriteErrors {
				if mongo.IsDuplicateKeyError(we.WriteError) {
					doc := documents[we.Index]
					id := doc.(bson.Raw).Lookup("_id") //we know it's there because there was a conflict on _id //XXX: should we check that it's the right type?
					bulkOverwrite = append(bulkOverwrite, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": id}).SetReplacement(doc).SetUpsert(true))
				} else {
					slog.Error(fmt.Sprintf("Failure to insert document into collection: %v", we.WriteError))
					return bwErr
				}
			}
		} else {
			// check if the error is a BSONObjectTooLarge error
			// there could be edge cases such as all documents causing duplicate key errors and server's writeError response gets larger than 16MB
			// we will recursively split the batch in half and try again
			if isBSONObjectTooLargeError(bwErr) {
				slog.Debug(fmt.Sprintf("Bulk write failed due to BSON object too large: %v", bwErr))
				mid := len(documents) / 2
				err := insertBatchOverwrite(ctx, collection, documents[:mid], ignoreSecondDuplicateError)
				if err != nil {
					slog.Error(fmt.Sprintf("Bulk write failed (first half, up to %d): %v", mid, err))
					return err
				}
				err = insertBatchOverwrite(ctx, collection, documents[mid:], ignoreSecondDuplicateError)
				if err != nil {
					slog.Error(fmt.Sprintf("Bulk write failed (second half, from %d): %v", mid, err))
					return err
				}
				return nil
			}

			slog.Error(fmt.Sprintf("Bulk write failed: %v", bwErr))
			return bwErr
		}

		// redo them all as a bulk replace
		if len(bulkOverwrite) > 0 {
			_, err := collection.BulkWrite(ctx, bulkOverwrite, options.BulkWrite().SetOrdered(false))
			if err != nil {
				if ignoreSecondDuplicateError {
					// if all are duplicate errors again, ignore
					var bwErrWriteErrors mongo.BulkWriteException
					if errors.As(err, &bwErrWriteErrors) {
						var count int
						for _, we := range bwErrWriteErrors.WriteErrors {
							if mongo.IsDuplicateKeyError(we.WriteError) {
								count++
							}
						}
						if count == len(bwErrWriteErrors.WriteErrors) {
							slog.Debug("Ignoring second duplicate error", "count", count)
							return nil
						} else {
							slog.Debug("Not ignoring second duplicate error", "count", count, "errs", len(bwErrWriteErrors.WriteErrors))
						}
					}
				}
				slog.Error(fmt.Sprintf("Failed to overwrite documents in collection: %v", err))
				return err
			}
		}
	}
	return nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	col, _, ok := GetCol(c.client, r.Msg.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}
	_, ignoreSecondDuplicateError := c.settings.UniqueIndexNamespaces[r.Msg.GetNamespace()]

	var batch []interface{}
	for _, data := range r.Msg.GetData() {
		batch = append(batch, bson.Raw(data))
		if c.settings.WriterMaxBatchSize > 0 && len(batch) >= c.settings.WriterMaxBatchSize {
			err := insertBatchOverwrite(ctx, col, batch, ignoreSecondDuplicateError)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error(fmt.Sprintf("Failed to insert batch: %v", err))
				}
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			batch = nil
		}
	}
	if len(batch) > 0 {
		err := insertBatchOverwrite(ctx, col, batch, ignoreSecondDuplicateError)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error(fmt.Sprintf("Failed to insert batch: %v", err))
			}
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

type dataIdIndex struct {
	dataId []byte
	index  int
}

// returns the new item or existing item, and whether or not a new item was added
func addToIdIndexMap2(m map[int][]*dataIdIndex, update *adiomv1.Update) (*dataIdIndex, bool) {
	hasher := xxhash.New()
	_, _ = hasher.Write(update.GetId()[0].GetData())
	h := int(hasher.Sum64())
	items, found := m[h]
	if found {
		for _, item := range items {
			if slices.Equal(item.dataId, update.GetId()[0].GetData()) {
				return item, false
			}
		}
	}
	item := &dataIdIndex{update.GetId()[0].GetData(), -1}
	m[h] = append(items, item)
	return item, true
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	col, _, ok := GetCol(c.client, r.Msg.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}
	_, hasUniqueIndex := c.settings.UniqueIndexNamespaces[r.Msg.GetNamespace()]

	var models []mongo.WriteModel
	// keeps track of the index in models for a particular document because we want all ids to be unique in the batch
	hashToDataIdIndex := map[int][]*dataIdIndex{}

	for _, update := range r.Msg.GetUpdates() {
		idType := bsontype.Type(update.GetId()[0].GetType())

		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			dii, isNew := addToIdIndexMap2(hashToDataIdIndex, update)
			idFilter := bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: update.GetId()[0].GetData()}}}
			model := mongo.NewReplaceOneModel().SetFilter(idFilter).SetReplacement(bson.Raw(update.GetData())).SetUpsert(true)
			if isNew {
				dii.index = len(models)
				models = append(models, model)
			} else {
				models[dii.index] = model
			}
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			dii, isNew := addToIdIndexMap2(hashToDataIdIndex, update)
			idFilter := bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: update.GetId()[0].GetData()}}}
			if len(c.query) > 0 {
				idFilter = append(idFilter, c.query...)
			}
			model := mongo.NewDeleteOneModel().SetFilter(idFilter)
			if isNew {
				dii.index = len(models)
				models = append(models, model)
			} else {
				models[dii.index] = model
			}
		}
	}

	if hasUniqueIndex {
		var lastDupCount int
		tries := 1
		for {
			metrics.UpdateAttempts(r.Msg.GetNamespace(), tries, len(models))
			_, err := col.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			if err != nil {
				var bwErrWriteErrors mongo.BulkWriteException
				if errors.As(err, &bwErrWriteErrors) {
					var newModels []mongo.WriteModel
					var count int
					for _, we := range bwErrWriteErrors.WriteErrors {
						if mongo.IsDuplicateKeyError(we.WriteError) {
							count++
							newModels = append(newModels, models[we.Index])
						}
					}
					if count > 0 && count == len(bwErrWriteErrors.WriteErrors) {
						slog.Debug("duplicate error", "count", count, "last-count", lastDupCount, "attempt", tries)
						if count == lastDupCount {
							return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
						} else {
							lastDupCount = count
							models = newModels
							tries += 1
							continue
						}
					} else {
						slog.Debug("not all duplicate errors", "count", count, "errs", len(bwErrWriteErrors.WriteErrors))
					}
				}
				if !errors.Is(err, context.Canceled) {
					slog.Error(fmt.Sprintf("Failed to insert bulk updates: %v", err))
				}
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
		}
	}

	_, err := col.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error(fmt.Sprintf("Failed to insert bulk updates: %v", err))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func MongoClient(ctx context.Context, settings ConnectorSettings) (*mongo.Client, error) {
	setDefault(&settings.ServerConnectTimeout, 10*time.Second)
	setDefault(&settings.PingTimeout, 2*time.Second)

	// Connect to the MongoDB instance
	ctxConnect, cancelConnect := context.WithTimeout(ctx, settings.ServerConnectTimeout)
	defer cancelConnect()
	clientOptions := moptions.Client().SetAppName("dsync").ApplyURI(settings.ConnectionString).SetConnectTimeout(settings.ServerConnectTimeout)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return nil, err
	}

	// Check the connection
	ctxPing, cancelPing := context.WithTimeout(ctx, settings.PingTimeout)
	defer cancelPing()
	err = client.Ping(ctxPing, nil)
	if err != nil {
		return nil, err
	}

	return client, err
}

func (c *conn) Teardown() {
	c.cancel()
	_ = c.client.Disconnect(context.Background())
}

func NewConn(connSettings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	setDefault(&connSettings.TargetDocCountPerPartition, 512*1000)
	setDefault(&connSettings.SampleFactor, 1)
	client, err := MongoClient(context.Background(), connSettings)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to connect to mongo client: %v", err))
		return nil, err
	}
	return NewConnWithClient(client, connSettings), nil
}

func NewConnWithClient(client *mongo.Client, settings ConnectorSettings) adiomv1connect.ConnectorServiceHandler {
	ctx, cancel := context.WithCancel(context.Background())
	slog.Debug("Query", "query", settings.Query)
	query, err := stringToQuery(settings.Query)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to parse query (%v): %v", settings.Query, err))
	}
	return &conn{
		client:          client,
		settings:        settings,
		ctx:             ctx,
		cancel:          cancel,
		buffers:         map[int64]buffer{},
		cleanupInterval: 5 * time.Minute,
		query:           query,
	}
}
