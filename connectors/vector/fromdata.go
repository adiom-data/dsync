package vector

import (
	"context"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/bson"
)

type fromData struct {
	field string
}

// GetEmbedding implements adiomv1connect.EmbeddingServiceHandler.
func (s *fromData) GetEmbedding(ctx context.Context, r *connect.Request[adiomv1.GetEmbeddingRequest]) (*connect.Response[adiomv1.GetEmbeddingResponse], error) {
	var embeddings []*adiomv1.Embedding
	for _, data := range r.Msg.GetData() {
		var d []float64
		v := bson.Raw(data).Lookup(s.field)
		if v.IsZero() {
			embeddings = append(embeddings, &adiomv1.Embedding{})
			continue
		}
		if err := bson.UnmarshalValue(v.Type, v.Value, &d); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		embeddings = append(embeddings, &adiomv1.Embedding{Data: d})
	}
	return connect.NewResponse(&adiomv1.GetEmbeddingResponse{
		Data: embeddings,
	}), nil
}

func (s *fromData) GetSupportedDataTypes(context.Context, *connect.Request[adiomv1.GetSupportedDataTypesRequest]) (*connect.Response[adiomv1.GetSupportedDataTypesResponse], error) {
	return connect.NewResponse(&adiomv1.GetSupportedDataTypesResponse{
		Types: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
	}), nil
}

func NewFromData(field string) *fromData {
	return &fromData{field: field}
}
