package dynamodb

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/dynamodb/stream"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"golang.org/x/sync/errgroup"
)

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	client        *client
	streamsClient *dynamodbstreams.Client
	spec          string

	options Options
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	var tableNames []string
	namespaces := r.Msg.GetNamespaces()
	if len(namespaces) < 1 {
		var err error
		tableNames, err = c.client.GetAllTableNames(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	} else {
		for _, namespace := range namespaces {
			tableNames = append(tableNames, namespace)
		}
	}

	gatheringPartitions := make(chan struct{})
	partitionsCh := make(chan *adiomv1.Partition)
	gatheringStates := make(chan struct{})
	statesCh := make(chan stream.StreamState)
	var partitions []*adiomv1.Partition
	stateMap := map[string]stream.StreamState{}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(c.options.PlanParallelism)

	go func() {
		defer close(gatheringPartitions)
		for partition := range partitionsCh {
			partitions = append(partitions, partition)
		}
	}()

	go func() {
		defer close(gatheringStates)
		for state := range statesCh {
			stateMap[state.StreamARN] = state
		}
	}()

	for _, name := range tableNames {
		eg.Go(func() error {
			tableDetails, err := c.client.TableDetails(egCtx, name)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					slog.Warn("Table not found. Ignoring.", "name", name)
					return nil
				}
				return err
			}

			if r.Msg.GetUpdates() {
				if tableDetails.StreamARN == "" {
					if c.spec == "localstack" {
						slog.Debug("No stream found, starting stream", "table", name)
						streamARN, err := c.client.StartStream(egCtx, name, false)
						if err != nil {
							return err
						}
						tableDetails.StreamARN = streamARN
					} else {
						return fmt.Errorf("no stream found")
					}
				} else if tableDetails.IncompatibleStream {
					if c.spec == "localstack" {
						slog.Debug("Incompatible stream found, restarting stream", "table", name)
						streamARN, err := c.client.StartStream(egCtx, name, true)
						if err != nil {
							return err
						}
						tableDetails.StreamARN = streamARN
					} else {
						return fmt.Errorf("incompatible stream found")
					}
				}

				state, err := c.client.GetStreamState(egCtx, tableDetails.StreamARN)
				if err != nil {
					return err
				}
				statesCh <- state
			}

			if !r.Msg.GetInitialSync() {
				return nil
			}

			// TODO: reconsider how to map namespaces properly
			ns := name
			totalSegments := 1

			if r.Msg.GetInitialSync() && c.options.DocsPerSegment > 0 {
				totalSegments = int(tableDetails.Count / uint64(c.options.DocsPerSegment))
				totalSegments = max(1, min(1000000, totalSegments))
			}

			for i := 0; i < totalSegments; i++ {
				cursor, err := c.client.CreateScanCursor(i, totalSegments, tableDetails.KeySchema)
				if err != nil {
					return err
				}
				partitionsCh <- &adiomv1.Partition{
					Namespace:      ns,
					Cursor:         cursor,
					EstimatedCount: tableDetails.Count / uint64(max(1, totalSegments)),
				}
			}
			return nil
		})
	}

	err := eg.Wait()
	close(partitionsCh)
	close(statesCh)
	<-gatheringPartitions
	<-gatheringStates

	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var updates []*adiomv1.UpdatesPartition
	if r.Msg.GetUpdates() {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(stateMap)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		updates = append(updates, &adiomv1.UpdatesPartition{Namespaces: namespaces, Cursor: buf.Bytes()})
	}

	if len(partitions) > 0 {
		slog.Debug("shuffling partitions")
		rand.Shuffle(len(partitions), func(i, j int) {
			partitions[i], partitions[j] = partitions[j], partitions[i]
		})
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        partitions,
		UpdatesPartitions: updates,
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:      c.options.ID,
		DbType:  "dynamodb",
		Version: "",
		Spec:    c.spec,
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON, adiomv1.DataType_DATA_TYPE_JSON_ID},
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(ctx context.Context, r *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	res, err := c.client.TableDetails(ctx, r.Msg.GetNamespace())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: res.Count,
	}), nil
}

func isThrottled(err error) bool {
	var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
	var throttlingException *types.ThrottlingException
	var requestLimitExceeded *types.RequestLimitExceeded

	if errors.As(err, &provisionedThroughputExceededException) {
		return true
	}
	if errors.As(err, &throttlingException) {
		return true
	}
	if errors.As(err, &requestLimitExceeded) {
		return true
	}
	return false
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	cursor := r.Msg.GetCursor()
	if len(r.Msg.GetCursor()) == 0 {
		cursor = r.Msg.GetPartition().GetCursor()
	}

	res, err := c.client.Scan(ctx, r.Msg.GetType(), r.Msg.GetPartition().GetNamespace(), true, cursor)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		if isThrottled(err) {
			return nil, connect.NewError(connect.CodeResourceExhausted, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       res.Items,
		NextCursor: res.NextCursor,
	}), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	cursor := r.Msg.GetCursor()
	var state map[string]stream.StreamState
	dec := gob.NewDecoder(bytes.NewReader(cursor))
	if err := dec.Decode(&state); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	var tableNames []string
	namespaces := r.Msg.GetNamespaces()
	if len(namespaces) < 1 {
		var err error
		tableNames, err = c.client.GetAllTableNames(ctx)
		if err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
	} else {
		tableNames = namespaces
	}

	arnToTableDetails := map[string]TableDetailsResult{}
	var lock sync.Mutex // TODO: lazy to use a channel

	eg, egCtx := errgroup.WithContext(ctx)
	for _, name := range tableNames {
		eg.Go(func() error {
			tableDetails, err := c.client.TableDetails(egCtx, name)
			if err != nil {
				return err
			}
			if tableDetails.StreamARN == "" {
				return fmt.Errorf("stream not found for %v", name)
			}
			if tableDetails.IncompatibleStream {
				return fmt.Errorf("incompatible stream found %v", name)
			}

			lock.Lock()
			arnToTableDetails[tableDetails.StreamARN] = tableDetails
			lock.Unlock()

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		if errors.Is(err, ErrNotFound) {
			return connect.NewError(connect.CodeNotFound, err)
		} else if errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeCanceled, err)
		}
		return connect.NewError(connect.CodeInternal, err)
	}

	ch := make(chan stream.StreamRecords)
	defer close(ch)

	var eg2 errgroup.Group
	eg2.Go(func() error {
	Loop:
		for records := range ch {
			var updates []*adiomv1.Update
			for _, record := range records.Records {
				update, err := streamRecordToUpdate(record, r.Msg.GetType(), arnToTableDetails[records.StreamARN].KeySchema)
				if err != nil {
					slog.Error("skipping, error creating update,", "err", err)
					continue Loop
				}
				updates = append(updates, update)
			}
			state[records.StreamARN].UpdateFromStreamRecords(records)
			if len(records.Records) > 0 {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.Encode(state); err != nil {
					slog.Error("skipping, error encoding state", "err", err)
					continue
				}
				cursor := buf.Bytes()
				if err := s.Send(&adiomv1.StreamUpdatesResponse{
					Updates:    updates,
					Namespace:  arnToTableDetails[records.StreamARN].Name,
					NextCursor: cursor,
				}); err != nil {
					slog.Error("skipping, error sending update", "err", err)
					continue
				}
			}
		}
		return nil
	})

	streamMult := NewStreamMult(c.streamsClient, state, ch)
	if err := streamMult.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	if err := streamMult.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	close(ch)
	if err := eg2.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeCanceled, err)
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	return nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	data := r.Msg.GetData()
	var batched [][]byte
	for i, d := range data {
		batched = append(batched, d)

		if len(batched) == 25 || i == len(data)-1 {
			err := c.client.BulkInsert(ctx, r.Msg.GetNamespace(), batched)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			batched = nil
		}
	}

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
	// return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func AWSClientHelper(connStr string) (*dynamodb.Client, *dynamodbstreams.Client) {
	var endpoint string
	if connStr == "localstack" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL")
		if endpoint == "" {
			endpoint = "http://localhost:4566"
		}
	}
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	dynamoClient := dynamodb.NewFromConfig(awsConfig, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	streamsClient := dynamodbstreams.NewFromConfig(awsConfig, func(o *dynamodbstreams.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	return dynamoClient, streamsClient
}

type Options struct {
	ID              string
	DocsPerSegment  int
	PlanParallelism int
}

func WithID(s string) func(*Options) {
	return func(o *Options) {
		o.ID = s
	}
}

func WithPlanParallelism(n int) func(*Options) {
	return func(o *Options) {
		o.PlanParallelism = n
	}
}

func WithDocsPerSegment(n int) func(*Options) {
	return func(o *Options) {
		o.DocsPerSegment = n
	}
}

func NewConn(connStr string, optFns ...func(*Options)) adiomv1connect.ConnectorServiceHandler {
	opts := Options{
		DocsPerSegment:  50000,
		PlanParallelism: 4,
	}
	for _, fn := range optFns {
		fn(&opts)
	}

	dynamoClient, streamsClient := AWSClientHelper(connStr)
	spec := "aws"
	if connStr == "localstack" {
		spec = connStr
	}

	client := NewClient(dynamoClient, streamsClient)
	return &conn{
		client:        client,
		streamsClient: streamsClient,
		options:       opts,
		spec:          spec,
	}
}
