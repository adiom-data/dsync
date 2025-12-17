package vector

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

type qdrantConnector struct {
	client *qdrant.Client
}

func NewQdrantConnector(host string, port int) (*qdrantConnector, error) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	if err != nil {
		return nil, err
	}

	return &qdrantConnector{client: client}, nil
}

var True bool = true

func (c *qdrantConnector) UpsertDocuments(ctx context.Context, namespace string, docs []*VectorDocument) error {
	if len(docs) == 0 {
		return nil
	}
	var points []*qdrant.PointStruct

	var keywords []string
	for _, doc := range docs {
		keywords = append(keywords, doc.ID)
	}
	deleteRes, err := c.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: namespace,
		Wait:           &True,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Filter{
				Filter: &qdrant.Filter{
					Must: []*qdrant.Condition{
						qdrant.NewMatchKeywords("_id", keywords...),
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	if deleteRes.GetStatus() != qdrant.UpdateStatus_Completed {
		return fmt.Errorf("qdrant delete not completed successfully")
	}

	for _, doc := range docs {
		for _, chunk := range doc.Chunks {
			if len(chunk.Vector) == 0 {
				continue
			}
			var vectors []float32
			for _, v := range chunk.Vector {
				vectors = append(vectors, float32(v))
			}
			d := chunk.Data.(map[string]interface{})
			d["_id"] = doc.ID
			points = append(points, &qdrant.PointStruct{
				Id:      qdrant.NewID(uuid.New().String()),
				Payload: qdrant.NewValueMap(d),
				Vectors: qdrant.NewVectors(vectors...),
			})
		}
	}

	operationInfo, err := c.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: namespace,
		Points:         points,
		Wait:           &True,
	})
	if err != nil {
		return err
	}
	if operationInfo.GetStatus() == qdrant.UpdateStatus_Completed {
		return nil
	}
	return fmt.Errorf("qdrant upsert not completed successfully")
}
