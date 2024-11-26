package vector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/auth"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate/entities/models"
)

type weaviateConnector struct {
	client     *weaviate.Client
	groupID    string // identifier that all chunks of the same document share (used to store _id)
	dataMapper func(interface{}) interface{}
}

func NewWeaviateConnector(host string, scheme string, groupID string, dataMapper func(interface{}) interface{}, apiKey string) *weaviateConnector {
	var authConfig auth.Config
	if apiKey != "" {
		authConfig = auth.ApiKey{Value: apiKey}
	}
	cfg := weaviate.Config{
		Host:       host,
		Scheme:     scheme,
		AuthConfig: authConfig,
	}

	client, err := weaviate.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	return &weaviateConnector{client: client, groupID: groupID, dataMapper: dataMapper}
}

func (c *weaviateConnector) UpsertDocuments(ctx context.Context, namespace string, docs []*VectorDocument) error {
	var toDelete []string
	for _, doc := range docs {
		toDelete = append(toDelete, doc.ID)
	}
	slog.Debug("deleting ids", "count", len(toDelete))
	delResp, err := c.client.Batch().ObjectsBatchDeleter().WithClassName(namespace).WithWhere(filters.Where().WithPath([]string{c.groupID}).WithOperator(filters.ContainsAny).WithValueString(toDelete...)).Do(ctx)
	if err != nil {
		return err
	}
	slog.Debug("success", "count", delResp.Results.Successful)
	if delResp.Results.Failed > 0 {
		slog.Error("deleting failed", "count", delResp.Results.Failed)
		return fmt.Errorf("was not able to delete some existing items")
	}

	var objects []*models.Object
	for _, doc := range docs {
		for _, chunk := range doc.Chunks {
			var vector []float32
			for _, v := range chunk.Vector {
				vector = append(vector, float32(v))
			}
			object := &models.Object{
				Class:      namespace,
				Properties: c.mapData(chunk.Data),
				Vector:     vector,
			}
			objects = append(objects, object)
		}
	}
	if len(objects) == 0 {
		return nil
	}
	slog.Debug("inserting", "count", len(objects))
	res, err := c.client.Batch().ObjectsBatcher().WithObjects(objects...).Do(ctx)
	if err != nil {
		return err
	}
	for _, res := range res {
		if res.Result.Errors != nil {
			slog.Error(res.Result.Errors.Error[0].Message)
			return fmt.Errorf("error inserting")
		}
	}
	return nil
}

func (c *weaviateConnector) mapData(data interface{}) interface{} {
	m := map[string]interface{}{}
	for k, v := range data.(map[string]interface{}) {
		if k == "_id" {
			m[c.groupID] = v
		} else {
			m[strings.ToLower(k)] = v
		}
	}
	return c.dataMapper(m)
}

func TextOnlyDataMapper(d interface{}) interface{} {
	m := d.(map[string]interface{})
	for k, v := range m {
		if _, ok := v.(string); !ok {
			delete(m, k)
		}
	}
	return m
}

func IdentityDataMapper(d interface{}) interface{} {
	return d
}
