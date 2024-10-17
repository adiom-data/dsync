package dynamodb

import (
	"context"

	"github.com/adiom-data/dsync/connectors/dynamodb/stream"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"golang.org/x/sync/errgroup"
)

type StartStoppable interface {
	Start(context.Context) error
	Wait() error
}

type streamMult struct {
	streams []StartStoppable
	helpers []StartStoppable
}

func NewStreamMult(client *dynamodbstreams.Client, arnsToState map[string]stream.StreamState, ch chan<- stream.StreamRecords) *streamMult {
	var streams []StartStoppable
	var helpers []StartStoppable
	for arn, state := range arnsToState {
		helper := stream.NewDynamodbStreamHelper(client, arn)
		helpers = append(helpers, helper)

		inputs := stream.ShardIteratorInputFromState(state, true)
		streams = append(streams, stream.NewStream(client, helper, arn, inputs, ch))
	}
	return &streamMult{
		streams: streams,
		helpers: helpers,
	}
}

func (s *streamMult) Start(ctx context.Context) error {
	var anErr error

	for _, helper := range s.helpers {
		err := helper.Start(ctx)
		if err != nil {
			anErr = err
		}
	}
	for _, stream := range s.streams {
		err := stream.Start(ctx)
		if err != nil {
			anErr = err
		}
	}
	return anErr
}

func (s *streamMult) Wait() error {
	var eg errgroup.Group

	for _, helper := range s.helpers {
		eg.Go(func() error {
			return helper.Wait()
		})
	}

	for _, stream := range s.streams {
		eg.Go(func() error {
			return stream.Wait()
		})
	}

	return eg.Wait()
}
