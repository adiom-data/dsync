package vector

import (
	"context"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
)

type simple struct{}

// GetEmbedding implements adiomv1connect.EmbeddingServiceHandler.
func (s *simple) GetEmbedding(ctx context.Context, r *connect.Request[adiomv1.GetEmbeddingRequest]) (*connect.Response[adiomv1.GetEmbeddingResponse], error) {
	var embeddings []*adiomv1.Embedding
	for range r.Msg.GetData() {
		embeddings = append(embeddings, &adiomv1.Embedding{})
	}
	return connect.NewResponse(&adiomv1.GetEmbeddingResponse{
		Data: embeddings,
	}), nil
}

// GetChunked implements adiomv1connect.ChunkingServiceHandler.
func (s *simple) GetChunked(ctx context.Context, r *connect.Request[adiomv1.GetChunkedRequest]) (*connect.Response[adiomv1.GetChunkedResponse], error) {
	var chunked []*adiomv1.Chunked
	for _, d := range r.Msg.GetData() {
		chunked = append(chunked, &adiomv1.Chunked{
			Data: [][]byte{d},
		})
	}
	return connect.NewResponse(&adiomv1.GetChunkedResponse{
		Data: chunked,
	}), nil
}

func (s *simple) GetSupportedDataTypes(context.Context, *connect.Request[adiomv1.GetSupportedDataTypesRequest]) (*connect.Response[adiomv1.GetSupportedDataTypesResponse], error) {
	return connect.NewResponse(&adiomv1.GetSupportedDataTypesResponse{
		Types: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
	}), nil
}

func NewSimple() *simple {
	return &simple{}
}
