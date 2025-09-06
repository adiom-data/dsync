package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/bson"
)

type ollamaDemo struct {
	baseURL string
	model   string
}

type EmbeddingResponse struct {
	Embedding []float64
}

func (s *ollamaDemo) getEmbedding(payload interface{}) ([]float64, error) {
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(map[string]string{
		"model":  s.model,
		"prompt": string(payloadData),
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, s.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var embeddingResponse EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embeddingResponse); err != nil {
		return nil, err
	}

	return embeddingResponse.Embedding, nil
}

// GetEmbedding implements adiomv1connect.EmbeddingServiceHandler.
func (s *ollamaDemo) GetEmbedding(ctx context.Context, r *connect.Request[adiomv1.GetEmbeddingRequest]) (*connect.Response[adiomv1.GetEmbeddingResponse], error) {
	var embeddings []*adiomv1.Embedding
	for _, d := range r.Msg.GetData() {
		var m map[string]interface{}
		if err := bson.Unmarshal(d, &m); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		e, err := s.getEmbedding(m)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		embeddings = append(embeddings, &adiomv1.Embedding{Data: e})
	}
	return connect.NewResponse(&adiomv1.GetEmbeddingResponse{
		Data: embeddings,
	}), nil
}

func (s *ollamaDemo) GetSupportedDataTypes(context.Context, *connect.Request[adiomv1.GetSupportedDataTypesRequest]) (*connect.Response[adiomv1.GetSupportedDataTypesResponse], error) {
	return connect.NewResponse(&adiomv1.GetSupportedDataTypesResponse{
		Types: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
	}), nil
}

func NewOllama(baseURL string, model string) *ollamaDemo {
	return &ollamaDemo{baseURL: baseURL, model: model}
}
