package dynamodb

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/adiom-data/dsync/connectors/dynamodb/stream"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

type client struct {
	dynamoClient  *dynamodb.Client
	streamsClient *dynamodbstreams.Client
}

type scanCursor struct {
	segment           int32
	totalSegments     int32
	keySchema         []string
	exclusiveStartKey map[string]types.AttributeValue
}

func (s scanCursor) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s.segment); err != nil {
		return nil, err
	}
	if err := enc.Encode(s.totalSegments); err != nil {
		return nil, err
	}
	if err := enc.Encode(s.keySchema); err != nil {
		return nil, err
	}
	if err := enc.Encode(s.exclusiveStartKey != nil); err != nil {
		return nil, err
	}
	if s.exclusiveStartKey == nil {
		return buf.Bytes(), nil
	}
	var m map[string]interface{}
	if err := attributevalue.UnmarshalMap(s.exclusiveStartKey, &m); err != nil {
		return nil, err
	}
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil

}

func (s *scanCursor) UnmarshalBinary(in []byte) error {
	br := bytes.NewReader(in)
	dec := gob.NewDecoder(br)
	if err := dec.Decode(&s.segment); err != nil {
		return err
	}
	if err := dec.Decode(&s.totalSegments); err != nil {
		return err
	}
	if err := dec.Decode(&s.keySchema); err != nil {
		return err
	}
	var b bool
	if err := dec.Decode(&b); err != nil {
		return err
	}
	if b {
		var m map[string]interface{}
		if err := dec.Decode(&m); err != nil {
			return err
		}
		exclusiveStartKey, err := attributevalue.MarshalMap(m)
		if err != nil {
			return err
		}
		s.exclusiveStartKey = exclusiveStartKey
	}
	return nil
}

type ScanResult struct {
	Items      [][]byte
	NextCursor []byte
}

var ErrNotFound error = errors.New("not found")

func (c *client) CreateScanCursor(segment int, totalSegments int, keySchema []string) ([]byte, error) {
	return scanCursor{
		segment:       int32(segment),
		totalSegments: int32(totalSegments),
		keySchema:     keySchema,
	}.MarshalBinary()
}

func (c *client) Scan(ctx context.Context, dataType adiomv1.DataType, tableName string, consistent bool, cursor []byte) (ScanResult, error) {
	var sc scanCursor
	if err := sc.UnmarshalBinary(cursor); err != nil {
		return ScanResult{}, err
	}
	res, err := c.dynamoClient.Scan(ctx, &dynamodb.ScanInput{
		TableName:         aws.String(tableName),
		ConsistentRead:    aws.Bool(consistent),
		ExclusiveStartKey: sc.exclusiveStartKey,
		Segment:           aws.Int32(sc.segment),
		TotalSegments:     aws.Int32(sc.totalSegments),
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return ScanResult{}, ErrNotFound
		}
		return ScanResult{}, err
	}

	var nextCursor []byte
	if res.LastEvaluatedKey != nil {
		nextCursor, err = scanCursor{
			segment:           sc.segment,
			totalSegments:     sc.totalSegments,
			keySchema:         sc.keySchema,
			exclusiveStartKey: res.LastEvaluatedKey,
		}.MarshalBinary()
		if err != nil {
			return ScanResult{}, err
		}
	}

	var items [][]byte

	switch dataType {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		// TODO: factor in primary key so we can match with updates
		items, err = itemsToBson(res.Items, sc.keySchema)
		if err != nil {
			return ScanResult{}, err
		}
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		items, err = itemsToJson(res.Items, sc.keySchema)
		if err != nil {
			return ScanResult{}, err
		}
	}

	return ScanResult{items, nextCursor}, nil
}

type TableDetailsResult struct {
	Name               string
	Count              uint64
	StreamARN          string
	IncompatibleStream bool
	KeySchema          []string
}

func (c *client) TableDetails(ctx context.Context, name string) (TableDetailsResult, error) {
	res, err := c.dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return TableDetailsResult{}, ErrNotFound
		}
		return TableDetailsResult{}, err
	}
	count := uint64(*res.Table.ItemCount)
	streamSpec := res.Table.StreamSpecification
	var streamARN string
	var incompatibleStream bool
	if res.Table.LatestStreamArn != nil && streamSpec != nil &&
		streamSpec.StreamEnabled != nil && *streamSpec.StreamEnabled {
		if streamSpec.StreamViewType == types.StreamViewTypeNewAndOldImages || streamSpec.StreamViewType == types.StreamViewTypeNewImage {
			streamARN = *res.Table.LatestStreamArn
		} else {
			incompatibleStream = true
		}
	}

	if len(res.Table.KeySchema) == 0 || len(res.Table.KeySchema) > 2 {
		return TableDetailsResult{}, fmt.Errorf("unexpected key schema")
	}
	var keySchema []string
	if len(res.Table.KeySchema) == 1 {
		keySchema = []string{*res.Table.KeySchema[0].AttributeName}
	} else if res.Table.KeySchema[0].KeyType == types.KeyTypeHash {
		keySchema = []string{*res.Table.KeySchema[0].AttributeName, *res.Table.KeySchema[1].AttributeName}
	} else {
		keySchema = []string{*res.Table.KeySchema[1].AttributeName, *res.Table.KeySchema[0].AttributeName}
	}

	return TableDetailsResult{name, count, streamARN, incompatibleStream, keySchema}, nil
}

func (c *client) GetAllTableNames(ctx context.Context) ([]string, error) {
	var names []string
	var exclusiveStartTableName *string
	for {
		res, err := c.dynamoClient.ListTables(ctx, &dynamodb.ListTablesInput{
			ExclusiveStartTableName: exclusiveStartTableName,
		})
		if err != nil {
			return nil, err
		}
		names = append(names, res.TableNames...)
		if res.LastEvaluatedTableName == nil {
			return names, nil
		}
		exclusiveStartTableName = res.LastEvaluatedTableName
	}
}

func (c *client) BulkInsert(ctx context.Context, tableName string, data [][]byte) error {
	var writeRequests []types.WriteRequest
	for _, data := range data {
		item, err := itemFromBson(data)
		if err != nil {
			return err
		}
		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}
	_, err := c.dynamoClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{tableName: writeRequests},
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *client) StartStream(ctx context.Context, tableName string, deleteOld bool) (string, error) {
	if deleteOld {
		_, err := c.dynamoClient.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName: aws.String(tableName),
			StreamSpecification: &types.StreamSpecification{
				StreamEnabled: aws.Bool(false),
			},
		})
		if err != nil {
			var notFound *types.ResourceNotFoundException
			if errors.As(err, &notFound) {
				return "", ErrNotFound
			}
			return "", err
		}
	}
	res, err := c.dynamoClient.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	if res.TableDescription.LatestStreamArn != nil {
		return *res.TableDescription.LatestStreamArn, nil
	}
	return "", fmt.Errorf("unable to start stream %v", tableName)
}

func (c *client) GetStreamState(ctx context.Context, arn string) (stream.StreamState, error) {
	helper := stream.NewDynamodbStreamHelper(c.streamsClient, arn)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := helper.Start(ctx)
	if err != nil {
		return stream.StreamState{}, err
	}
	state, err := helper.ApproximateLatestState(ctx)
	if err != nil {
		return stream.StreamState{}, err
	}
	cancel()
	err = helper.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return stream.StreamState{}, err
	}
	return state, nil
}

func NewClient(dynamoClient *dynamodb.Client, streamsClient *dynamodbstreams.Client) *client {
	return &client{dynamoClient: dynamoClient, streamsClient: streamsClient}
}
