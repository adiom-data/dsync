//go:build external
// +build external

package dynamodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/pkg/test"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/stretchr/testify/suite"
)

func TestDynamoDBConnectorSuite(t *testing.T) {
	tableName := "test"
	client, streamClient := AWSClientHelper("localstack")

	tSuite := test.NewConnectorTestSuite(tableName, func() adiomv1connect.ConnectorServiceClient {
		return test.ClientFromHandler(NewConn("localstack"))
	}, func(ctx context.Context) error {
		_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			var notFound *types.ResourceNotFoundException
			if !errors.As(err, &notFound) {
				return err
			}
		}
		// localstack seems to have race condition if we create table too early that the stream does not get created
		time.Sleep(time.Second * 5)
		for {
			_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
				AttributeDefinitions: []types.AttributeDefinition{
					{
						AttributeName: aws.String("_id"),
						AttributeType: types.ScalarAttributeTypeS,
					},
				},
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("_id"),
						KeyType:       types.KeyTypeHash,
					},
				},
				TableName: aws.String(tableName),
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(20),
					WriteCapacityUnits: aws.Int64(20),
				},
				StreamSpecification: &types.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: types.StreamViewTypeNewImage,
				},
			})
			if err != nil {
				var alreadyExists *types.ResourceInUseException
				if !errors.As(err, &alreadyExists) {
					return err
				}
			}
			break
		}

		var streamARN string
		for {
			res, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(tableName),
			})
			if err != nil {
				var notFound *types.ResourceNotFoundException
				if !errors.As(err, &notFound) {
					return err
				}
			} else {
				streamARN = *res.Table.LatestStreamArn
				if streamARN != "" && res.Table.TableStatus == types.TableStatusActive {
					break
				}
			}
		}

		for {
			_, err := streamClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
				StreamArn: aws.String(streamARN),
			})
			if err != nil {
				var notFound *types.ResourceNotFoundException
				if !errors.As(err, &notFound) {
					continue
				}
				return err
			}
			break
		}

		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"_id":  &types.AttributeValueMemberS{Value: "id1"},
				"Blah": &types.AttributeValueMemberS{Value: "asdf"},
			},
			TableName: aws.String(tableName),
		})
		if err != nil {
			return err
		}

		return nil
	}, func(ctx context.Context) error {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"_id":  &types.AttributeValueMemberS{Value: "id2"},
				"Blah": &types.AttributeValueMemberS{Value: "asdf2"},
			},
			TableName: aws.String(tableName),
		})
		if err != nil {
			return err
		}

		return nil
	}, 1, 3)
	suite.Run(t, tSuite)
}
