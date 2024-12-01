package vector

import (
	"context"
	"fmt"
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
}

type VectorDBConnector interface {
	UpsertDocuments(context.Context, string, []*VectorDocument) error
}

// GetInfo implements adiomv1connect.ConnectorServiceClient.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     "someid",
		DbType: "somevectordb",
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

func (c *conn) upsertVectorDocuments(ctx context.Context, data [][]byte, dataType adiomv1.DataType) ([]*VectorDocument, error) {
	res, err := c.chunker.GetChunk(ctx, connect.NewRequest(&adiomv1.GetChunkRequest{
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
		bson.UnmarshalValue(v.Type, v.Value, &id)
		docs = append(docs, &VectorDocument{
			Chunks: chunks,
			ID:     id,
		})
	}
	return docs, nil
}

// WriteData implements adiomv1connect.ConnectorServiceClient.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	docs, err := c.upsertVectorDocuments(ctx, r.Msg.GetData(), r.Msg.GetType())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := c.vectorDBConnector.UpsertDocuments(ctx, r.Msg.GetNamespace(), docs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceClient.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	var docs []*VectorDocument
	var upserts [][]byte
	for _, update := range r.Msg.GetUpdates() {
		idPart := update.GetId()[0]
		var id interface{}
		bson.UnmarshalValue(bsontype.Type(idPart.GetType()), idPart.GetData(), &id)
		idStr, err := fromBson(id)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			docs = append(docs, &VectorDocument{
				ID: idStr.(string),
			})
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			upserts = append(upserts, update.GetData())
		default:
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unsupported update type"))
		}
	}
	upsertDocs, err := c.upsertVectorDocuments(ctx, upserts, r.Msg.GetType())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	docs = append(docs, upsertDocs...)
	if err := c.vectorDBConnector.UpsertDocuments(ctx, r.Msg.GetNamespace(), docs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func NewDemoConn() adiomv1connect.ConnectorServiceHandler {
	s := &simple{}
	return NewConn(s, s, NewWeaviateConnector("localhost:8080", "http", "g_id", TextOnlyDataMapper))
}

func NewConn(chunker adiomv1connect.ChunkingServiceClient, embedder adiomv1connect.EmbeddingServiceClient, vectorDBConnector VectorDBConnector) adiomv1connect.ConnectorServiceHandler {
	return &conn{
		chunker:           chunker,
		embedder:          embedder,
		vectorDBConnector: vectorDBConnector,
	}
}
