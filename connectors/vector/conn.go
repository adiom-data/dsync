package vector

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler

	chunker  adiomv1connect.ChunkingServiceClient
	embedder adiomv1connect.EmbeddingServiceClient

	vectorDBConnector VectorDBConnector

	id        string
	name      string
	batchSize int
}

type VectorDBConnector interface {
	UpsertDocuments(context.Context, string, []*VectorDocument) error
}

// GetInfo implements adiomv1connect.ConnectorServiceClient.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     c.id,
		DbType: c.name,
		Capabilities: &adiomv1.Capabilities{
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
			},
		},
	}), nil
}

type Chunk struct {
	Vector []float64
	Data   interface{}
}

type VectorDocument struct {
	Chunks []Chunk
	ID     string
}

func mongoBsonToInterface(d []byte) (interface{}, error) {
	var v interface{}
	if err := bson.Unmarshal(d, &v); err != nil {
		return nil, err
	}
	return fromBson(v)
}

func fromBson(bs interface{}) (interface{}, error) {
	switch b := bs.(type) {
	case bson.A:
		var arr []interface{}
		for _, v := range b {
			vv, err := fromBson(v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, vv)
		}
		return arr, nil
	case bson.D:
		m := map[string]interface{}{}
		for _, v := range b {
			vv, err := fromBson(v.Value)
			if err != nil {
				return nil, err
			}
			m[v.Key] = vv
		}
		return m, nil
	case bson.M:
		m := map[string]interface{}{}
		for k, v := range b {
			vv, err := fromBson(v)
			if err != nil {
				return nil, err
			}
			m[k] = vv
		}
		return m, nil
	case bool:
		return b, nil
	case int32:
		return b, nil
	case int64:
		return b, nil
	case float64:
		return b, nil
	case string:
		return b, nil
	case primitive.DateTime:
		return b.Time().Format(time.RFC3339), nil
	case primitive.ObjectID:
		return b.Hex(), nil
	case primitive.Binary:
		return b.Data, nil
	case primitive.Decimal128:
		return b.String(), nil
	default:
		return "UNSUPPORTED", nil
	}
}

func Batcher[T any](items []T, maxBatchSize int, f func([]T) error) error {
	if maxBatchSize <= 0 || len(items) < maxBatchSize {
		return f(items)
	}
	var batch []T
	for i, item := range items {
		batch = append(batch, item)
		if len(batch) >= maxBatchSize || i == len(items)-1 {
			err := f(batch)
			if err != nil {
				return err
			}
			batch = nil
		}
	}
	return nil
}

func (c *conn) upsertVectorDocuments(ctx context.Context, data [][]byte, dataType adiomv1.DataType) ([]*VectorDocument, error) {
	res, err := c.chunker.GetChunked(ctx, connect.NewRequest(&adiomv1.GetChunkedRequest{
		Data: data,
		Type: dataType,
	}))
	if err != nil {
		return nil, err
	}

	var docs []*VectorDocument
	for i, d := range res.Msg.GetData() {
		originalData := data[i]
		embeddingRes, err := c.embedder.GetEmbedding(ctx, connect.NewRequest(&adiomv1.GetEmbeddingRequest{
			Data: d.GetData(),
			Type: dataType,
		}))
		if err != nil {
			return nil, err
		}
		embeddings := embeddingRes.Msg.GetData()
		if len(embeddings) != len(d.GetData()) {
			return nil, fmt.Errorf("unexpected embeddings size")
		}
		var chunks []Chunk
		for j, data := range d.GetData() {
			embedding := embeddings[j]
			m, err := mongoBsonToInterface(data)
			if err != nil {
				return nil, err
			}
			chunk := Chunk{
				Vector: embedding.GetData(),
				Data:   m,
			}
			chunks = append(chunks, chunk)
		}
		var id string
		v := bson.Raw(originalData).Lookup("_id")
		if err := bson.UnmarshalValue(v.Type, v.Value, &id); err != nil {
			return nil, err
		}
		docs = append(docs, &VectorDocument{
			Chunks: chunks,
			ID:     id,
		})
	}
	return docs, nil
}

// WriteData implements adiomv1connect.ConnectorServiceClient.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	if len(r.Msg.GetData()) == 0 {
		return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
	}
	err := Batcher(r.Msg.GetData(), c.batchSize, func(d [][]byte) error {
		docs, err := c.upsertVectorDocuments(ctx, d, r.Msg.GetType())
		if err != nil {
			return err
		}
		if err := c.vectorDBConnector.UpsertDocuments(ctx, r.Msg.GetNamespace(), docs); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceClient.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	if len(r.Msg.GetUpdates()) == 0 {
		return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
	}

	allUpdates := r.Msg.GetUpdates()
	dedupedUpdates := []*adiomv1.Update{}
	seen := map[string]struct{}{}
	for i := range allUpdates {
		update := allUpdates[len(allUpdates)-1-i]
		idPart := update.GetId()[0]
		var id interface{}
		if err := bson.UnmarshalValue(bsontype.Type(idPart.GetType()), idPart.GetData(), &id); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		idStr, err := fromBson(id)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if _, ok := seen[idStr.(string)]; ok {
			continue
		}
		seen[idStr.(string)] = struct{}{}
		dedupedUpdates = append(dedupedUpdates, update)
	}
	slices.Reverse(dedupedUpdates)

	err := Batcher(dedupedUpdates, c.batchSize, func(updates []*adiomv1.Update) error {

		var docs []*VectorDocument
		var upserts [][]byte
		for _, update := range updates {
			idPart := update.GetId()[0]
			var id interface{}
			if err := bson.UnmarshalValue(bsontype.Type(idPart.GetType()), idPart.GetData(), &id); err != nil {
				return err
			}
			idStr, err := fromBson(id)
			if err != nil {
				return err
			}

			switch update.GetType() {
			case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
				docs = append(docs, &VectorDocument{
					ID: idStr.(string),
				})
			case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
				upserts = append(upserts, update.GetData())
			default:
				return fmt.Errorf("unsupported update type")
			}
		}
		upsertDocs, err := c.upsertVectorDocuments(ctx, upserts, r.Msg.GetType())
		if err != nil {
			return err
		}
		docs = append(docs, upsertDocs...)
		if err := c.vectorDBConnector.UpsertDocuments(ctx, r.Msg.GetNamespace(), docs); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func NewWeaviateConn(chunker adiomv1connect.ChunkingServiceClient, embedder adiomv1connect.EmbeddingServiceClient, url string, groupID string, apiKey string, useIdentityMapper bool) (adiomv1connect.ConnectorServiceHandler, error) {
	splitted := strings.SplitN(url, "://", 2)
	if len(splitted) != 2 {
		return nil, fmt.Errorf("could not split url into scheme and host")
	}
	scheme := splitted[0]
	host := splitted[1]
	mapper := TextOnlyDataMapper
	if useIdentityMapper {
		mapper = IdentityDataMapper
	}
	return NewConn(url, "Weaviate", 200, chunker, embedder, NewWeaviateConnector(host, scheme, groupID, mapper, apiKey)), nil
}

func NewConn(id string, name string, batchSize int, chunker adiomv1connect.ChunkingServiceClient, embedder adiomv1connect.EmbeddingServiceClient, vectorDBConnector VectorDBConnector) adiomv1connect.ConnectorServiceHandler {
	return &conn{
		chunker:           chunker,
		embedder:          embedder,
		vectorDBConnector: vectorDBConnector,
		id:                id,
		name:              name,
		batchSize:         batchSize,
	}
}
