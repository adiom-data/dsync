package s3vector

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/util"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/document"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/pgvector/pgvector-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"golang.org/x/sync/errgroup"
)

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	client         *s3vectors.Client
	bucketName     *string
	vectorKey      string
	maxParallelism int
	batchSize      int
	limiter        util.NamespaceLimiter
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	panic("unimplemented")
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     *c.bucketName,
		DbType: "s3vector",
		Capabilities: &adiomv1.Capabilities{
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	panic("unimplemented")
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	panic("unimplemented")
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	if len(r.Msg.GetData()) == 0 {
		return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
	}

	indexName := aws.String(r.Msg.GetNamespace())
	vectors := make([]types.PutInputVector, len(r.Msg.GetData()))
	for i, data := range r.Msg.GetData() {
		vector, err := dataToPIV(c.vectorKey, data)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err getting input vector: %w", err))
		}
		vectors[i] = vector
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(c.maxParallelism)
	limiter := c.limiter.Get(r.Msg.GetNamespace())
	for batch := range slices.Chunk(vectors, c.batchSize) {
		eg.Go(func() error {
			if err := limiter.WaitN(ctx, len(batch)); err != nil {
				return fmt.Errorf("err in limiter: %w", err)
			}
			if _, err := c.client.PutVectors(ctx, &s3vectors.PutVectorsInput{
				Vectors:          batch,
				IndexName:        indexName,
				VectorBucketName: c.bucketName,
			}); err != nil {
				return fmt.Errorf("err putting vectors: %w", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

func idToKey(id []*adiomv1.BsonValue) (string, error) {
	if len(id) == 1 {
		first := id[0]
		var res string
		var idAny any
		if err := bson.UnmarshalValue(bsontype.Type(first.GetType()), first.GetData(), &idAny); err != nil {
			return "", fmt.Errorf("err unmarshalling id: %w", err)
		}
		switch t := idAny.(type) {
		case string:
			res = t
		default:
			res = fmt.Sprintf("%v", t)
		}
		return res, nil
	} else {
		// for now we only support single id
		return "", fmt.Errorf("unsupported id type")
	}
}

func dataToPIV(vectorKey string, data []byte) (types.PutInputVector, error) {
	var m map[string]any
	if err := bson.Unmarshal(data, &m); err != nil {
		return types.PutInputVector{}, fmt.Errorf("err unmarshalling data: %w", err)
	}
	var id string
	var idAny any
	rawID := bson.Raw(data).Lookup("_id")
	if err := bson.UnmarshalValue(rawID.Type, rawID.Value, &idAny); err != nil {
		return types.PutInputVector{}, fmt.Errorf("err unmarshalling data _id: %w", err)
	}
	switch t := idAny.(type) {
	case string:
		id = t
	default:
		id = fmt.Sprintf("%v", t)
	}

	v, ok := m[vectorKey]
	if !ok {
		return types.PutInputVector{}, fmt.Errorf("vector key not found in data")
	}
	delete(m, vectorKey)
	m["_id"] = id

	vectorData := &types.VectorDataMemberFloat32{}
	switch t := v.(type) {
	case string:
		// pgvector encoded string
		var vector pgvector.Vector
		if err := vector.Parse(t); err != nil {
			return types.PutInputVector{}, fmt.Errorf("err parsing string vector: %w", err)
		}
		vectorData.Value = vector.Slice()
	case bson.A:
		for _, f := range t {
			switch t2 := f.(type) {
			case string:
				fv, err := strconv.ParseFloat(t2, 32)
				if err != nil {
					return types.PutInputVector{}, fmt.Errorf("err converting vector value str to float32: %w", err)
				}
				vectorData.Value = append(vectorData.Value, float32(fv))
			case float32:
				vectorData.Value = append(vectorData.Value, t2)
			case float64:
				vectorData.Value = append(vectorData.Value, float32(t2))
			case int:
				vectorData.Value = append(vectorData.Value, float32(t2))
			case int32:
				vectorData.Value = append(vectorData.Value, float32(t2))
			case int64:
				vectorData.Value = append(vectorData.Value, float32(t2))
			default:
				return types.PutInputVector{}, fmt.Errorf("unsupported type for vector value: %T", t2)
			}
		}
	default:
		return types.PutInputVector{}, fmt.Errorf("unsupported type for vector values: %T", t)
	}

	return types.PutInputVector{
		Data:     vectorData,
		Key:      aws.String(id),
		Metadata: document.NewLazyDocument(m),
	}, nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	indexName := aws.String(r.Msg.GetNamespace())
	updates := util.KeepLastUpdate(r.Msg.GetUpdates())
	var toDelete []string
	var vectors []types.PutInputVector
	for _, update := range updates {
		if update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_DELETE {
			key, err := idToKey(update.GetId())
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err getting id: %w", err))
			}
			toDelete = append(toDelete, key)
		} else if update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_INSERT || update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_UPDATE {
			vector, err := dataToPIV(c.vectorKey, update.GetData())
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err getting input vector: %w", err))
			}
			vectors = append(vectors, vector)
		}
	}
	if len(updates) > 0 {
		limiter := c.limiter.Get(r.Msg.GetNamespace())
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(c.maxParallelism)
		for batch := range slices.Chunk(toDelete, c.batchSize) {
			eg.Go(func() error {
				if err := limiter.WaitN(ctx, len(batch)); err != nil {
					return fmt.Errorf("err in limiter: %w", err)
				}
				if _, err := c.client.DeleteVectors(ctx, &s3vectors.DeleteVectorsInput{
					Keys:             batch,
					IndexName:        indexName,
					VectorBucketName: c.bucketName,
				}); err != nil {
					return fmt.Errorf("err deleting vectors: %w", err)
				}
				return nil
			})
		}
		for batch := range slices.Chunk(vectors, c.batchSize) {
			eg.Go(func() error {
				if err := limiter.WaitN(ctx, len(batch)); err != nil {
					return fmt.Errorf("err in limiter: %w", err)
				}
				if _, err := c.client.PutVectors(ctx, &s3vectors.PutVectorsInput{
					Vectors:          batch,
					IndexName:        indexName,
					VectorBucketName: c.bucketName,
				}); err != nil {
					return fmt.Errorf("err putting vectors: %w", err)
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func NewConn(bucketName string, vectorKey string, maxParallelism int, batchSize int, rateLimit int) (*conn, error) {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	client := s3vectors.NewFromConfig(awsConfig)
	return &conn{
		client:         client,
		bucketName:     aws.String(bucketName),
		vectorKey:      vectorKey,
		maxParallelism: maxParallelism,
		batchSize:      batchSize,
		limiter:        util.NewNamespaceLimiter(nil, rateLimit),
	}, nil
}

var _ adiomv1connect.ConnectorServiceHandler = &conn{}
