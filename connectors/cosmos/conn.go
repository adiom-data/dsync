package cosmos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
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

func encodeResumeToken(epoch int64, token []byte) []byte {
	res, _ := bson.Marshal(bson.M{"a": epoch, "b": token})
	return res
}

func decodeResumeToken(input []byte) (int64, []byte) {
	var s bson.M
	_ = bson.Unmarshal(input, &s)
	return s["a"].(int64), s["b"].(primitive.Binary).Data
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
	for _, nsTask := range nsTasks {
		wg.Add(1)
		go func(ns iface.Namespace) {
			defer wg.Done()
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			resumeToken, err := getLatestResumeToken(ctx, c.client, loc, c.connectorSettings.WithDelete)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get latest resume token for namespace %v: %v", ns, err))
				return
			}
			tokenMap.AddToken(loc, resumeToken)
		}(nsTask)
	}
	wg.Wait()
	slog.Debug(fmt.Sprintf("Read Plan Resume token map: %v", tokenMap.Map))
	//serialize the resume token map
	resumeToken, err := tokenMap.encodeMap()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to serialize the resume token map: %v", err))
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	epoch := time.Now().Unix()
	encodedResumeToken := encodeResumeToken(epoch, resumeToken)

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
	readPlanStartAt, resumeToken := decodeResumeToken(r.Msg.GetCursor())
	err := tokenMap.decodeMap(resumeToken)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to deserialize the resume token map: %v", err))
		return connect.NewError(connect.CodeInternal, err)
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
	lsnTracker := NewMultiNsLSNTracker()
	for _, namespace := range namespaces {
		ns := iface.Namespace{Db: namespace.Db, Col: namespace.Col}
		wg.Add(1)
		go func() {
			defer wg.Done()
			//get task location and retrieve resume token
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			slog.Info(fmt.Sprintf("Connector %s is starting to track LSN for namespace %s", loc.Database, loc.Collection))

			token, err := tokenMap.GetToken(loc)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get resume token for location %v: %v", loc, err))
			}
			var opts *moptions.ChangeStreamOptions
			if token != nil {
				//set the change stream options to start from the resume token
				opts = moptions.ChangeStream().SetResumeAfter(token).SetFullDocument(moptions.UpdateLookup)
			} else { //we need to start from the read plan creation time to be safe
				// create timestamp from read plan start time
				ts := primitive.Timestamp{T: uint32(readPlanStartAt)}
				slog.Debug(fmt.Sprintf("Starting change stream for %v at timestamp %v", ns, ts))
				opts = moptions.ChangeStream().SetStartAtOperationTime(&ts).SetFullDocument(moptions.UpdateLookup)
			}
			changeStream, err := createChangeStream(ctx, c.client, loc, opts, c.connectorSettings.WithDelete)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					slog.Debug(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v, but the context was cancelled", loc.Database, loc.Collection, err))
				} else {
					slog.Error(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v", loc.Database, loc.Collection, err))
				}
				return
			}
			defer changeStream.Close(ctx)

			for changeStream.Next(ctx) {
				var change bson.M
				if err := changeStream.Decode(&change); err != nil {
					slog.Error(fmt.Sprintf("Failed to decode change stream event: %v", err))
					continue
				}

				lsnTracker.IncrementLSN(ns)

				if changeStream.RemainingBatchLength() == 0 {
					tokenMap.AddToken(loc, changeStream.ResumeToken())
					encodedToken, _ := tokenMap.encodeMap()
					sendMutex.Lock()
					_ = s.Send(&adiomv1.StreamLSNResponse{
						Lsn:        uint64(lsnTracker.GetGlobalLSN()) + c.deletesCount.Load(),
						NextCursor: encodeResumeToken(readPlanStartAt, encodedToken), // TODO: does the ts never change?,
					})
					sendMutex.Unlock()
				}
			}
			if changeStream.Err() != nil {
				if !errors.Is(changeStream.Err(), context.Canceled) {
					slog.Error(fmt.Sprintf("Change stream error: %v", changeStream.Err()))
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

func convertChangeStreamEventToUpdate(change bson.M) (*adiomv1.Update, error) {
	//slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

	// treat all change stream events as updates
	// get the id of the document that was changed
	id := change["documentKey"].(bson.M)["_id"]
	// convert id to raw bson
	idType, idVal, err := bson.MarshalValue(id)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal _id: %v", err)
	}

	if change["operationType"] == "delete" {
		return &adiomv1.Update{
			Id: []*adiomv1.BsonValue{{
				Data: idVal,
				Type: uint32(idType),
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}, nil
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
	update := &adiomv1.Update{
		Id: []*adiomv1.BsonValue{{
			Data: idVal,
			Type: uint32(idType),
		}},
		Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
		Data: fullDocumentRaw,
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
	readPlanStartAt, resumeToken := decodeResumeToken(r.Msg.GetCursor())
	err := tokenMap.decodeMap(resumeToken)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to deserialize the resume token map: %v", err))
		return connect.NewError(connect.CodeInternal, err)
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
				updatesMap := checkForDeletes(ctx, c.client, c.witnessMongoClient, nss)
				for loc, updates := range updatesMap {
					c.deletesCount.Add(uint64(len(updates)))
					encodedToken, _ := tokenMap.encodeMap()
					sendMutex.Lock()
					err := s.Send(&adiomv1.StreamUpdatesResponse{
						Updates:    updates,
						Namespace:  fmt.Sprintf("%v.%v", loc.Database, loc.Collection),
						NextCursor: encodeResumeToken(readPlanStartAt, encodedToken),
					})
					sendMutex.Unlock()
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						slog.Error(fmt.Sprintf("failed to send updates: %v", err))
					}
				}
				ticker.Reset(c.connectorSettings.DeletesCheckInterval)
			}

			for {
				select {
				case <-ctx.Done():
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
			token, err := tokenMap.GetToken(loc)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get resume token for location %v: %v", loc, err))
			}
			var opts *moptions.ChangeStreamOptions
			if token != nil {
				//set the change stream options to start from the resume token
				opts = moptions.ChangeStream().SetResumeAfter(token).SetFullDocument(moptions.UpdateLookup)
			} else { //we need to start from the read plan creation time to be safe
				// create timestamp from read plan start time
				ts := primitive.Timestamp{T: uint32(readPlanStartAt)}
				slog.Debug(fmt.Sprintf("Starting change stream for %v at timestamp %v", ns, ts))
				opts = moptions.ChangeStream().SetStartAtOperationTime(&ts).SetFullDocument(moptions.UpdateLookup)
			}
			changeStream, err := createChangeStream(ctx, c.client, loc, opts, c.connectorSettings.WithDelete)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Debug(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v, but the context was cancelled", loc.Database, loc.Collection, err))
				} else {
					slog.Error(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v", loc.Database, loc.Collection, err))
				}
				return
			}
			defer changeStream.Close(ctx)

			var updates []*adiomv1.Update

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

				updates = append(updates, update)

				if changeStream.RemainingBatchLength() == 0 {
					tokenMap.AddToken(loc, changeStream.ResumeToken())
					encodedToken, _ := tokenMap.encodeMap()
					sendMutex.Lock()
					err := s.Send(&adiomv1.StreamUpdatesResponse{
						Updates:    updates,
						Namespace:  fmt.Sprintf("%v.%v", ns.Db, ns.Col),
						NextCursor: encodeResumeToken(readPlanStartAt, encodedToken), // TODO: does the ts never change?
					})
					sendMutex.Unlock()
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						slog.Error(fmt.Sprintf("failed to send updates: %v", err))
					}
					updates = nil
				}
			}
			if changeStream.Err() != nil {
				if !errors.Is(changeStream.Err(), context.Canceled) {
					slog.Error(fmt.Sprintf("Change stream error: %v", changeStream.Err()))
				}
			}
		}()
	}
	wg.Wait()
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
		ctxConnect, cancel := context.WithTimeout(context.Background(), settings.ServerConnectTimeout)
		defer cancel()
		clientOptions := moptions.Client().ApplyURI(settings.WitnessMongoConnString).SetConnectTimeout(settings.ServerConnectTimeout)
		client, err := mongo.Connect(ctxConnect, clientOptions)
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
