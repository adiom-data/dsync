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
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/cespare/xxhash"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

type ConnectorSettings struct {
	ConnectionString string

	ServerConnectTimeout time.Duration
	PingTimeout          time.Duration
	WriterMaxBatchSize   int // applies to batch inserts only; 0 means no limit
}

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

type conn struct {
	client *mongo.Client

	settings ConnectorSettings

	nextCursorID atomic.Int64
	ctx          context.Context
	cancel       context.CancelFunc
	buffersMutex sync.RWMutex
	buffers      map[int64]<-chan [][]byte
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

func NamespacePartitions(ctx context.Context, namespaces []*adiomv1.Namespace, client *mongo.Client) ([]*adiomv1.Partition, error) {
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
			if ns.Col != "" {
				partitions = append(partitions, &adiomv1.Partition{
					Namespace: ns,
				})
			} else {
				dbsToResolve = append(dbsToResolve, ns.Db)
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
				Namespace: &adiomv1.Namespace{Db: db, Col: col},
			})
		}
	}

	return partitions, nil
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	resumeToken, err := getLatestResumeToken(ctx, c.client)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get latest resume token: %v", err))
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	partitions, err := NamespacePartitions(ctx, r.Msg.GetNamespaces(), c.client)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:  partitions,
		StartCursor: resumeToken,
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
		Id:                 string(generateConnectorID(c.settings.ConnectionString)),
		DbType:             connectorDBType,
		Version:            version.(string),
		Spec:               connectorSpec,
		SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
		Capabilities: &adiomv1.Capabilities{
			Source:    true,
			Sink:      true,
			Resumable: true,
			LsnStream: true,
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(ctx context.Context, r *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	ns := r.Msg.GetNamespace()
	collection := c.client.Database(ns.Db).Collection(ns.Col)
	count, err := collection.EstimatedDocumentCount(ctx)
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
	collection := c.client.Database(partition.GetNamespace().GetDb()).Collection(partition.GetNamespace().GetCol())

	var cursorID, ctr int64

	pageCursor := r.Msg.GetCursor()
	if pageCursor == nil {
		cursorID = c.nextCursorID.Add(1)
		ch := make(chan [][]byte, 10)
		c.buffersMutex.Lock()
		c.buffers[cursorID] = ch
		c.buffersMutex.Unlock()

		filter := createFindFilterFromCursor(r.Msg.GetPartition().GetCursor())
		cursor, err := collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
		if err != nil {
			if !errors.Is(err, context.Canceled) {
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
				if cursor.RemainingBatchLength() == 0 {
					select {
					case ch <- dataBatch:
						dataBatch = nil
					case <-ctx.Done():
						return
					}
				}
			}
			if cursor.Err() != nil {
				if !errors.Is(cursor.Err(), context.Canceled) {
					slog.Error(fmt.Sprintf("Failed to iterate through documents: %v", cursor.Err()))
				}
			}
		}(c.ctx) // This context may outlive the current request
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
	ch, ok := c.buffers[cursorID]
	c.buffersMutex.RUnlock()
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cursor"))
	}

	for {
		select {
		case dataBatch, ok := <-ch:
			if !ok {
				c.buffersMutex.Lock()
				delete(c.buffers, cursorID)
				c.buffersMutex.Unlock()

				return connect.NewResponse(&adiomv1.ListDataResponse{
					Data: dataBatch,
					Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				}), nil
			}
			nextCursor := binary.AppendVarint(nil, cursorID)
			nextCursor = binary.AppendVarint(nextCursor, ctr+1)
			return connect.NewResponse(&adiomv1.ListDataResponse{
				Data:       dataBatch,
				Type:       adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				NextCursor: nextCursor,
			}), nil
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	opts := moptions.ChangeStream().SetStartAfter(bson.Raw(r.Msg.GetCursor()))
	nsFilter := createChangeStreamNamespaceFilterFromNamespaces(r.Msg.GetNamespaces())

	changeStream, err := c.client.Watch(ctx, mongo.Pipeline{
		{{"$match", nsFilter}},
	}, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("LSN tracker: Failed to open change stream: %v", err))
		return connect.NewError(connect.CodeInternal, err)
	}
	defer changeStream.Close(ctx)

	lsn := 0
	for changeStream.Next(ctx) {
		var change bson.M
		if err := changeStream.Decode(&change); err != nil {
			slog.Error(fmt.Sprintf("LSN tracker: Failed to decode change stream event: %v", err))
			continue
		}

		if shouldIgnoreChangeStreamEvent(change) {
			continue
		}

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

// creates a filter for the change stream to include only the specified namespaces
func createChangeStreamNamespaceFilterFromNamespaces(namespaces []*adiomv1.Namespace) bson.D {
	if len(namespaces) == 0 {
		return createChangeStreamNamespaceFilter()
	}

	var filters []bson.D
	for _, namespace := range namespaces {
		filters = append(filters, bson.D{{"ns.db", namespace.Db}, {"ns.coll", namespace.Col}})
	}
	// add dummyDB and dummyCol to the filter so that we can track the changes in the dummy collection to get the cluster time (otherwise we can't use the resume token)
	filters = append(filters, bson.D{{"ns.db", dummyDB}, {"ns.coll", dummyCol}})
	return bson.D{{"$or", filters}}
}

func convertChangeStreamEventToUpdate(change bson.M) (*adiomv1.Update, error) {
	slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

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
			Id: &adiomv1.BsonValue{
				Data: idVal,
				Type: uint32(idType),
			},
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: fullDocumentRaw,
		}
	case "update":
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
			Id: &adiomv1.BsonValue{
				Data: idVal,
				Type: uint32(idType),
			},
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
			Id: &adiomv1.BsonValue{
				Data: idVal,
				Type: uint32(idType),
			},
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}
	default:
		return nil, fmt.Errorf("unsupported change event operation type: %v", optype)
	}

	return update, nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	opts := moptions.ChangeStream().SetStartAfter(bson.Raw(r.Msg.GetCursor())).SetFullDocument("updateLookup")
	nsFilter := createChangeStreamNamespaceFilterFromNamespaces(r.Msg.GetNamespaces())
	slog.Debug(fmt.Sprintf("Change stream namespace filter: %v", nsFilter))

	changeStream, err := c.client.Watch(ctx, mongo.Pipeline{
		{{"$match", nsFilter}},
	}, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to open change stream: %v", err))
		return connect.NewError(connect.CodeInternal, err)
	}
	defer changeStream.Close(ctx)

	var updates []*adiomv1.Update
	var currentNamespace *adiomv1.Namespace
	var lastResumeToken bson.Raw

	for changeStream.Next(ctx) {
		var change bson.M
		if err := changeStream.Decode(&change); err != nil {
			slog.Error(fmt.Sprintf("Failed to decode change stream event: %v", err))
			continue
		}

		if shouldIgnoreChangeStreamEvent(change) {
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

		if currentNamespace == nil || db != currentNamespace.Db || col != currentNamespace.Col {
			if len(updates) > 0 {
				err := s.Send(&adiomv1.StreamUpdatesResponse{
					Updates:    updates,
					Namespace:  currentNamespace,
					Type:       adiomv1.DataType_DATA_TYPE_MONGO_BSON,
					NextCursor: lastResumeToken,
				})
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					slog.Error(fmt.Sprintf("failed to send updates: %v", err))
				}
			}
			currentNamespace = &adiomv1.Namespace{Db: db, Col: col}
			updates = nil
		}

		updates = append(updates, update)
		lastResumeToken = changeStream.ResumeToken()

		if changeStream.RemainingBatchLength() == 0 {
			err := s.Send(&adiomv1.StreamUpdatesResponse{
				Updates:    updates,
				Namespace:  currentNamespace,
				Type:       adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				NextCursor: lastResumeToken,
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
			Type:       adiomv1.DataType_DATA_TYPE_MONGO_BSON,
			NextCursor: lastResumeToken,
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
func insertBatchOverwrite(ctx context.Context, collection *mongo.Collection, documents []interface{}) error {
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
					slog.Error(fmt.Sprintf("Skipping failure to insert document into collection: %v", we.WriteError))
				}
			}
		}

		// redo them all as a bulk replace
		if len(bulkOverwrite) > 0 {
			_, err := collection.BulkWrite(ctx, bulkOverwrite, options.BulkWrite().SetOrdered(false))
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to overwrite documents in collection: %v", err))
				return err
			}
		}
	}
	return nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	col := c.client.Database(r.Msg.GetNamespace().GetDb()).Collection(r.Msg.GetNamespace().GetCol())
	var batch []interface{}
	for _, data := range r.Msg.GetData() {
		batch = append(batch, bson.Raw(data))
		if c.settings.WriterMaxBatchSize > 0 && len(batch) >= c.settings.WriterMaxBatchSize {
			err := insertBatchOverwrite(ctx, col, batch)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error(fmt.Sprintf("Failed to insert batch: %v", err))
				}
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}
	}
	if len(batch) > 0 {
		err := insertBatchOverwrite(ctx, col, batch)
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
	hasher.Write(update.GetId().GetData())
	h := int(hasher.Sum64())
	items, found := m[h]
	if found {
		for _, item := range items {
			if slices.Equal(item.dataId, update.GetId().GetData()) {
				return item, false
			}
		}
	}
	item := &dataIdIndex{update.GetId().GetData(), -1}
	m[h] = append(items, item)
	return item, true
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	col := c.client.Database(r.Msg.GetNamespace().GetDb()).Collection(r.Msg.GetNamespace().GetCol())
	var models []mongo.WriteModel
	// keeps track of the index in models for a particular document because we want all ids to be unique in the batch
	hashToDataIdIndex := map[int][]*dataIdIndex{}

	for _, update := range r.Msg.GetUpdates() {
		idType := bsontype.Type(update.GetId().GetType())

		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			dii, isNew := addToIdIndexMap2(hashToDataIdIndex, update)
			idFilter := bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: update.GetId().GetData()}}}
			model := mongo.NewReplaceOneModel().SetFilter(idFilter).SetReplacement(bson.Raw(update.GetData())).SetUpsert(true)
			if isNew {
				dii.index = len(models)
				models = append(models, model)
			} else {
				models[dii.index] = model
			}
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			dii, isNew := addToIdIndexMap2(hashToDataIdIndex, update)
			idFilter := bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: update.GetId().GetData()}}}
			model := mongo.NewDeleteOneModel().SetFilter(idFilter)
			if isNew {
				dii.index = len(models)
				models = append(models, model)
			} else {
				models[dii.index] = model
			}
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
	clientOptions := moptions.Client().ApplyURI(settings.ConnectionString).SetConnectTimeout(settings.ServerConnectTimeout)
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

func NewConn(connSettings ConnectorSettings) adiomv1connect.ConnectorServiceHandler {
	client, err := MongoClient(context.Background(), connSettings)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to connect to mongo client: %v", err))
		panic(err)
	}
	return NewConnWithClient(client, connSettings)
}

func NewConnWithClient(client *mongo.Client, settings ConnectorSettings) adiomv1connect.ConnectorServiceHandler {
	ctx, cancel := context.WithCancel(context.Background())
	return &conn{
		client:   client,
		settings: settings,
		ctx:      ctx,
		cancel:   cancel,
		buffers:  map[int64]<-chan [][]byte{},
	}
}
