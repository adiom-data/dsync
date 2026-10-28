package cosmos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	moptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	connectorDBType string = "CosmosDB"               // We're a CosmosDB-compatible connector
	connectorSpec   string = "MongoDB Provisioned RU" // Only compatible with MongoDB API and provisioned deployments
)

type ConnectorSettings struct {
	mongoconn.ConnectorSettings
	MaxNumNamespaces            int    //we don't want to have too many parallel changestreams (after 10-15 we saw perf impact)
	NumParallelPartitionWorkers int    //number of workers used for partitioning
	partitionKey                string //partition key to use for collections
	WithDelete                  bool

	EmulateDeletes         bool // if true, we will generate delete events
	DeletesCheckInterval   time.Duration
	WitnessMongoConnString string
}

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

type conn struct {
	adiomv1connect.ConnectorServiceHandler

	connectorSettings  ConnectorSettings
	client             *mongo.Client
	witnessMongoClient *mongo.Client

	deletesCount atomic.Uint64

	forceDelete chan struct{} // Used for testing only
}

func encodeResumeToken(epoch int64, token []byte) ([]byte, error) {
	return bson.Marshal(struct {
		Epoch int64       `bson:"a"`
		Token bson.Binary `bson:"b"`
	}{
		Epoch: epoch,
		Token: bson.Binary{Data: token},
	})
}

func decodeResumeToken(input []byte) (int64, []byte, error) {
	raw := bson.Raw(input)
	if err := raw.Validate(); err != nil {
		return 0, nil, fmt.Errorf("invalid resume cursor BSON: %w", err)
	}

	epochValue, err := raw.LookupErr("a")
	if err != nil {
		return 0, nil, fmt.Errorf("resume cursor is missing epoch: %w", err)
	}
	epoch, ok := epochValue.Int64OK()
	if epoch32, isInt32 := epochValue.Int32OK(); isInt32 {
		epoch, ok = int64(epoch32), true
	}
	if !ok || epoch < 0 || epoch > math.MaxUint32 {
		return 0, nil, fmt.Errorf("resume cursor epoch must be an integer between 0 and %d", uint64(math.MaxUint32))
	}

	tokenValue, err := raw.LookupErr("b")
	if err != nil {
		return 0, nil, fmt.Errorf("resume cursor is missing token map: %w", err)
	}
	_, token, ok := tokenValue.BinaryOK()
	if !ok {
		// Older versions encoded an empty []byte as BSON null. Accept that one
		// representation so persisted empty-token cursors remain resumable.
		if tokenValue.Type == bson.TypeNull {
			return epoch, []byte{}, nil
		}
		return 0, nil, fmt.Errorf("resume cursor token map must be BSON binary")
	}
	return epoch, token, nil
}

// Missing namespace tokens intentionally fall back to the plan timestamp.
// The epoch is validated by decodeResumeToken before reaching this helper.
func resumeChangeStreamOptions(epoch int64, token bson.Raw) *moptions.ChangeStreamOptionsBuilder {
	opts := moptions.ChangeStream().SetFullDocument(moptions.UpdateLookup)
	if token != nil {
		return opts.SetResumeAfter(token)
	}
	ts := bson.Timestamp{T: uint32(epoch)}
	return opts.SetStartAtOperationTime(&ts)
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	initialPartitions, err := mongoconn.NamespacePartitions(ctx, r.Msg.GetNamespaces(), c.client)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if len(initialPartitions) > c.connectorSettings.MaxNumNamespaces {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("too many namespaces to copy: %d, max %d", len(initialPartitions), c.connectorSettings.MaxNumNamespaces))
	}

	var partitions []*adiomv1.Partition
	var nsTasks []iface.Namespace
	var updatesNamespaces []string
	for _, partition := range initialPartitions {
		ns, ok := mongoconn.ToNS(partition.GetNamespace())
		if !ok {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("namespace should be fully qualified"))
		}
		nsTasks = append(nsTasks, ns)
		if len(r.Msg.GetNamespaces()) > 0 {
			updatesNamespaces = append(updatesNamespaces, partition.GetNamespace())
		}
	}

	if r.Msg.GetInitialSync() {
		p := planner{
			Ctx:      ctx,
			Client:   c.client,
			settings: c.connectorSettings,
		}
		partitionedTasks, err := p.partitionTasksIfNecessary(nsTasks)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if len(nsTasks) > 1 {
			shuffleTasks(partitionedTasks)
		}

		// TODO: clean up read plan task usage
		for _, task := range partitionedTasks {
			var low bson.RawValue
			var high bson.RawValue
			if task.Def.Low != nil {
				low = task.Def.Low.(bson.RawValue)
			}
			if task.Def.High != nil {
				high = task.Def.High.(bson.RawValue)
			}
			partitions = append(partitions, &adiomv1.Partition{
				Namespace:      fmt.Sprintf("%v.%v", task.Def.Db, task.Def.Col),
				Cursor:         mongoconn.EncodeCursor(low, high),
				EstimatedCount: uint64(task.EstimatedDocCount),
			})
		}
	} else {
		partitions = initialPartitions
	}

	tokenMap := NewTokenMap()

	//create resume token for each task
	wg := sync.WaitGroup{}
	var cleanupErr error
	var cleanupErrOnce sync.Once
	for _, nsTask := range nsTasks {
		wg.Add(1)
		go func(ns iface.Namespace) {
			defer wg.Done()
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			resumeToken, err := getLatestResumeToken(ctx, c.client, loc, c.connectorSettings.WithDelete)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get latest resume token for namespace %v: %v", ns, err))
				if errors.Is(err, errDummyRecordCleanup) {
					cleanupErrOnce.Do(func() { cleanupErr = err })
				}
				return
			}
			tokenMap.AddToken(loc, resumeToken)
		}(nsTask)
	}
	wg.Wait()
	if cleanupErr != nil {
		return nil, connect.NewError(connect.CodeInternal, cleanupErr)
	}
	slog.Debug(fmt.Sprintf("Read Plan Resume token map: %v", tokenMap.Map))
	//serialize the resume token map
	resumeToken, err := tokenMap.encodeMap()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to serialize the resume token map: %v", err))
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	epoch := time.Now().Unix()
	encodedResumeToken, err := encodeResumeToken(epoch, resumeToken)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to encode resume cursor: %w", err))
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        partitions,
		UpdatesPartitions: []*adiomv1.UpdatesPartition{{Namespaces: updatesNamespaces, Cursor: encodedResumeToken}},
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(ctx context.Context, r *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	res, err := c.ConnectorServiceHandler.GetInfo(ctx, r)
	res.Msg.DbType = connectorDBType
	res.Msg.Spec = connectorSpec
	return res, err
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	tokenMap := NewTokenMap()
	readPlanStartAt, resumeToken, err := decodeResumeToken(r.Msg.GetCursor())
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	err = tokenMap.decodeMap(resumeToken)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to deserialize the resume token map: %v", err))
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	slog.Debug(fmt.Sprintf("Initial Deserialized resume token map: %v", tokenMap.Map))

	partitions, err := mongoconn.NamespacePartitions(ctx, r.Msg.GetNamespaces(), c.client)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	var namespaces []iface.Namespace
	for _, partition := range partitions {
		ns, ok := mongoconn.ToNS(partition.GetNamespace())
		if !ok {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("namespace should be fully qualified"))
		}
		namespaces = append(namespaces, ns)
	}

	var sendMutex sync.Mutex
	var wg sync.WaitGroup
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(namespaces))
	reportError := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		select {
		case errCh <- err:
			cancel()
		default:
		}
	}
	lsnTracker := NewMultiNsLSNTracker()
	for _, namespace := range namespaces {
		ns := iface.Namespace{Db: namespace.Db, Col: namespace.Col}
		wg.Add(1)
		go func() {
			defer wg.Done()
			//get task location and retrieve resume token
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			slog.Info(fmt.Sprintf("Connector %s is starting to track LSN for namespace %s", loc.Database, loc.Collection))

			token, _ := tokenMap.GetToken(loc) // Missing tokens intentionally use the plan timestamp below.
			opts := resumeChangeStreamOptions(readPlanStartAt, token)
			changeStream, err := createChangeStream(streamCtx, c.client, loc, opts, c.connectorSettings.WithDelete)
			if err != nil {
				reportError(fmt.Errorf("failed to create LSN change stream for %s.%s: %w", loc.Database, loc.Collection, err))
				return
			}
			defer changeStream.Close(streamCtx)

			for changeStream.Next(streamCtx) {
				lsnTracker.IncrementLSN(ns)

				if changeStream.RemainingBatchLength() == 0 {
					tokenMap.AddToken(loc, changeStream.ResumeToken())
					encodedToken, err := tokenMap.encodeMap()
					if err != nil {
						reportError(fmt.Errorf("failed to encode LSN token map: %w", err))
						return
					}
					nextCursor, err := encodeResumeToken(readPlanStartAt, encodedToken)
					if err != nil {
						reportError(fmt.Errorf("failed to encode LSN cursor: %w", err))
						return
					}
					sendMutex.Lock()
					err = s.Send(&adiomv1.StreamLSNResponse{
						Lsn:        uint64(lsnTracker.GetGlobalLSN()) + c.deletesCount.Load(),
						NextCursor: nextCursor,
					})
					sendMutex.Unlock()
					if err != nil {
						reportError(fmt.Errorf("failed to send LSN update: %w", err))
						return
					}
				}
			}
			if err := changeStream.Err(); err != nil {
				reportError(fmt.Errorf("LSN change stream for %s.%s failed: %w", loc.Database, loc.Collection, err))
			}
		}()
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return connect.NewError(connect.CodeInternal, err)
	default:
	}
	return nil
}

func convertChangeStreamEventToUpdate(change mongoconn.MongoUpdate) (*adiomv1.Update, error) {
	//slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

	// get the id of the document that was changed
	var idType bson.Type
	var idVal []byte
	for _, k := range change.DocumentKey {
		if k.Key == "_id" {
			var err error
			idType, idVal, err = bson.MarshalValue(k.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal _id: %v", err)
			}
			break
		}
	}
	if idVal == nil {
		return nil, fmt.Errorf("_id not found in documentKey")
	}

	if change.OperationType == "delete" {
		return &adiomv1.Update{
			Id: []*adiomv1.BsonValue{{
				Data: idVal,
				Type: uint32(idType),
				Name: "_id",
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}, nil
	}

	// get the full state of the document after the change
	if len(change.FullDocument) == 0 {
		//TODO (AK, 6/2024): find a better way to report that we need to ignore this event
		return nil, nil // no full document, nothing to do (probably got deleted before we got to the event in the change stream)
	}
	update := &adiomv1.Update{
		Id: []*adiomv1.BsonValue{{
			Data: idVal,
			Type: uint32(idType),
			Name: "_id",
		}},
		Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
		Data: change.FullDocument,
	}
	return update, nil
}

func checkForDeletes(ctx context.Context, client *mongo.Client, witnessClient *mongo.Client, namespaces []iface.Namespace) map[iface.Location][]*adiomv1.Update {
	// Preparations
	mismatchedNamespaces := make(chan iface.Namespace)
	idsToCheck := make(chan idsWithLocation)  //channel to post ids to check
	idsToDelete := make(chan idsWithLocation) //channel to post ids to delete

	// 1. Compare the doc count on both sides (asynchoronously so that we can proceed with the next steps here)
	go compareDocCountWithWitness(ctx, client, witnessClient, namespaces, mismatchedNamespaces)

	// 2. For mismatches, use Witness index to find out what has been deleted (async so that we can proceed with the next steps here)
	go parallelScanWitnessNamespaces(ctx, witnessClient, mismatchedNamespaces, idsToCheck)
	go checkSourceIdsAndGenerateDeletes(ctx, client, idsToCheck, idsToDelete)

	// 3. Generate delete events
	var count uint64
	updatesWithLoc := map[iface.Location][]*adiomv1.Update{}
	for idWithLoc := range idsToDelete {
		for i := 0; i < len(idWithLoc.ids); i++ {
			// convert id to raw bson
			idType, idVal, err := bson.MarshalValue(idWithLoc.ids[i])
			if err != nil {
				slog.Error(fmt.Sprintf("failed to marshal _id: %v", err))
			}
			updatesWithLoc[idWithLoc.loc] = append(updatesWithLoc[idWithLoc.loc], &adiomv1.Update{
				Id: []*adiomv1.BsonValue{{
					Data: idVal,
					Type: uint32(idType),
					Name: "_id",
				}},
				Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
			})

			count += 1
		}
	}
	slog.Debug(fmt.Sprintf("Generated %v delete messages", count))
	return updatesWithLoc
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	tokenMap := NewTokenMap()
	readPlanStartAt, resumeToken, err := decodeResumeToken(r.Msg.GetCursor())
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	err = tokenMap.decodeMap(resumeToken)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to deserialize the resume token map: %v", err))
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	slog.Debug(fmt.Sprintf("Initial Deserialized resume token map: %v", tokenMap.Map))

	partitions, err := mongoconn.NamespacePartitions(ctx, r.Msg.GetNamespaces(), c.client)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	var namespaces []iface.Namespace
	for _, partition := range partitions {
		ns, ok := mongoconn.ToNS(partition.GetNamespace())
		if !ok {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("namespace should be fully qualified"))
		}
		namespaces = append(namespaces, ns)
	}

	var sendMutex sync.Mutex
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(namespaces)+1)
	reportError := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		select {
		case errCh <- err:
			cancel()
		default:
		}
	}

	if c.connectorSettings.EmulateDeletes {
		var nss []iface.Namespace
		for _, namespace := range namespaces {
			nss = append(nss, iface.Namespace{Db: namespace.Db, Col: namespace.Col})
		}
		go func() {
			ticker := time.NewTicker(c.connectorSettings.DeletesCheckInterval)
			defer ticker.Stop()

			runDelete := func() {
				// check for deletes
				slog.Debug("Checking for deletes")
				updatesMap := checkForDeletes(streamCtx, c.client, c.witnessMongoClient, nss)
				for loc, updates := range updatesMap {
					c.deletesCount.Add(uint64(len(updates)))
					encodedToken, err := tokenMap.encodeMap()
					if err != nil {
						reportError(fmt.Errorf("failed to encode emulated-delete token map: %w", err))
						return
					}
					nextCursor, err := encodeResumeToken(readPlanStartAt, encodedToken)
					if err != nil {
						reportError(fmt.Errorf("failed to encode emulated-delete cursor: %w", err))
						return
					}
					sendMutex.Lock()
					err = s.Send(&adiomv1.StreamUpdatesResponse{
						Updates:    updates,
						Namespace:  fmt.Sprintf("%v.%v", loc.Database, loc.Collection),
						NextCursor: nextCursor,
					})
					sendMutex.Unlock()
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						reportError(fmt.Errorf("failed to send emulated deletes: %w", err))
						return
					}
				}
				ticker.Reset(c.connectorSettings.DeletesCheckInterval)
			}

			for {
				select {
				case <-streamCtx.Done():
					return
				case <-c.forceDelete:
					runDelete()
				case <-ticker.C:
					runDelete()
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for _, ns := range namespaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//get task location and retrieve resume token
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			token, _ := tokenMap.GetToken(loc) // Missing tokens intentionally use the plan timestamp below.
			opts := resumeChangeStreamOptions(readPlanStartAt, token)
			changeStream, err := createChangeStream(streamCtx, c.client, loc, opts, c.connectorSettings.WithDelete)
			if err != nil {
				reportError(fmt.Errorf("failed to create update change stream for %s.%s: %w", loc.Database, loc.Collection, err))
				return
			}
			defer changeStream.Close(streamCtx)

			var updates []*adiomv1.Update

			for changeStream.Next(streamCtx) {
				var change mongoconn.MongoUpdate
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

				updates = append(updates, update)

				if changeStream.RemainingBatchLength() == 0 {
					tokenMap.AddToken(loc, changeStream.ResumeToken())
					encodedToken, err := tokenMap.encodeMap()
					if err != nil {
						reportError(fmt.Errorf("failed to encode update token map: %w", err))
						return
					}
					nextCursor, err := encodeResumeToken(readPlanStartAt, encodedToken)
					if err != nil {
						reportError(fmt.Errorf("failed to encode update cursor: %w", err))
						return
					}
					sendMutex.Lock()
					err = s.Send(&adiomv1.StreamUpdatesResponse{
						Updates:    updates,
						Namespace:  fmt.Sprintf("%v.%v", ns.Db, ns.Col),
						NextCursor: nextCursor,
					})
					sendMutex.Unlock()
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						reportError(fmt.Errorf("failed to send updates for %s.%s: %w", loc.Database, loc.Collection, err))
						return
					}
					updates = nil
				}
			}
			if err := changeStream.Err(); err != nil {
				reportError(fmt.Errorf("update change stream for %s.%s failed: %w", loc.Database, loc.Collection, err))
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return connect.NewError(connect.CodeInternal, err)
	default:
	}
	return nil
}

func (c *conn) ForceDelete() {
	c.forceDelete <- struct{}{}
}

func (c *conn) Teardown() {
	if teardownable, ok := c.ConnectorServiceHandler.(common.Teardownable); ok {
		teardownable.Teardown()
	}
	_ = c.client.Disconnect(context.Background())
	if c.witnessMongoClient != nil {
		_ = c.witnessMongoClient.Disconnect(context.Background())
	}
}

func NewConn(settings ConnectorSettings) adiomv1connect.ConnectorServiceHandler {
	setDefault(&settings.ServerConnectTimeout, 15*time.Second)
	setDefault(&settings.PingTimeout, 10*time.Second)
	setDefault(&settings.WriterMaxBatchSize, 0)
	setDefault(&settings.MaxNumNamespaces, 8)
	setDefault(&settings.TargetDocCountPerPartition, 512*1000)
	setDefault(&settings.NumParallelPartitionWorkers, 4)
	setDefault(&settings.DeletesCheckInterval, 60*time.Second)
	settings.partitionKey = "_id"

	var witnessMongoClient *mongo.Client
	// Connect to the witness MongoDB instance
	if settings.EmulateDeletes {
		clientOptions := moptions.Client().ApplyURI(settings.WitnessMongoConnString).SetConnectTimeout(settings.ServerConnectTimeout)
		client, err := mongo.Connect(clientOptions)
		if err != nil {
			panic(err)
		}
		witnessMongoClient = client
	}

	client, err := mongoconn.MongoClient(context.Background(), settings.ConnectorSettings)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to connect to mongo client: %v", err))
		panic(err)
	}

	mongoConn := mongoconn.NewConnWithClient(client, settings.ConnectorSettings)
	return &conn{
		ConnectorServiceHandler: mongoConn,
		connectorSettings:       settings,
		client:                  client,
		witnessMongoClient:      witnessMongoClient,
		forceDelete:             make(chan struct{}),
	}
}
