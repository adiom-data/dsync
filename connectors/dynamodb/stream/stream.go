package stream

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type StreamOptions struct {
	EmptyGetRecordsDelay time.Duration
	GetRecordsLimiter    Limiter
	Logger               Logger
}

type stream struct {
	streamClient *dynamodbstreams.Client
	dynamoHelper *dynamodbStreamHelper
	streamARN    string

	ch            chan<- StreamRecords
	initialInputs []*dynamodbstreams.GetShardIteratorInput

	once sync.Once
	done chan struct{}
	err  error

	options StreamOptions
}

type StreamRecords struct {
	StreamARN     string
	ShardID       string
	Records       []types.Record
	ChildShardIDs []string
}

func NewStream(streamClient *dynamodbstreams.Client, dynamoHelper *dynamodbStreamHelper, streamARN string, shardIteratorInputs []*dynamodbstreams.GetShardIteratorInput, ch chan<- StreamRecords, optsFn ...func(*StreamOptions)) *stream {
	opts := StreamOptions{
		EmptyGetRecordsDelay: time.Second * 2,
		GetRecordsLimiter:    rate.NewLimiter(rate.Limit(5), 5),
		Logger:               slog.Default(),
	}
	for _, fn := range optsFn {
		fn(&opts)
	}

	return &stream{
		streamClient:  streamClient,
		dynamoHelper:  dynamoHelper,
		streamARN:     streamARN,
		ch:            ch,
		initialInputs: shardIteratorInputs,
		done:          make(chan struct{}),
		options:       opts,
	}
}

func (s *stream) Start(ctx context.Context) error {
	err := ErrAlreadyRunning
	s.once.Do(func() {
		go func() {
			s.err = s.startProcessShards(ctx, s.initialInputs, s.ch)
			close(s.done)
		}()
		err = nil
	})
	return err
}

func (s *stream) Wait() error {
	<-s.done
	return s.err
}

func (s *stream) findChildren(ctx context.Context, shardIteratorInput *dynamodbstreams.GetShardIteratorInput) ([]*dynamodbstreams.GetShardIteratorInput, error) {
	shards, err := s.dynamoHelper.GetShards(ctx)
	if err != nil {
		return nil, err
	}
	var childShards []*dynamodbstreams.GetShardIteratorInput
	for _, shard := range shards {
		if *shard.ParentShardId == *shardIteratorInput.ShardId {
			childShardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
				ShardId:           shard.ShardId,
				ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
				StreamArn:         shardIteratorInput.StreamArn,
			}
			childShards = append(childShards, childShardIteratorInput)
		}
	}
	return childShards, nil
}

func (s *stream) startProcessShards(ctx context.Context, shards []*dynamodbstreams.GetShardIteratorInput, ch chan<- StreamRecords) error {
	shardsToRun := make(chan *dynamodbstreams.GetShardIteratorInput)
	defer close(shardsToRun)

	done := make(chan struct{})
	eg, egCtx := errgroup.WithContext(ctx)

	go func() {
		for _, shard := range shards {
			shardsToRun <- shard
		}
		close(done)
	}()

	for {
		select {
		case shard := <-shardsToRun:
			s.options.Logger.Debug("processing shard", "stream_arn", *shard.StreamArn, "shard_id", *shard.ShardId)
			eg.Go(func() error {
				defer s.options.Logger.Debug("done shard", "stream_arn", *shard.StreamArn, "shard_id", *shard.ShardId)
				err := s.processShard(egCtx, shard, ch)
				if err != nil {
					return err
				}

				childShards, err := s.findChildren(egCtx, shard)
				if err != nil {
					return err
				}

				if len(childShards) == 0 {
					s.options.Logger.Debug("no child shards", "stream_arn", *shard.StreamArn, "shard_id", *shard.ShardId)
					return nil
				}

				var childShardIDs []string
				for _, childShard := range childShards {
					s.options.Logger.Debug("child shard found", "stream_arn", *shard.StreamArn, "shard_id", *shard.ShardId, "child_shard_id", *childShard.ShardId)
					childShardIDs = append(childShardIDs, *childShard.ShardId)
				}

				ch <- StreamRecords{
					StreamARN:     s.streamARN,
					ShardID:       *shard.ShardId,
					ChildShardIDs: childShardIDs,
				}

				for _, childShard := range childShards {
					shardsToRun <- childShard
				}

				return nil
			})
		case <-egCtx.Done():
			<-done
			err := eg.Wait()
			if err != nil {
				return err
			}
			return nil
		}
	}
}

func (s *stream) processShard(ctx context.Context, shardInput *dynamodbstreams.GetShardIteratorInput, ch chan<- StreamRecords) error {
	shardIteratorRes, err := s.streamClient.GetShardIterator(ctx, shardInput)
	if err != nil {
		return err
	}
	shardIterator := shardIteratorRes.ShardIterator

	for {
		if s.options.GetRecordsLimiter != nil {
			if err := s.options.GetRecordsLimiter.Wait(ctx); err != nil {
				return err
			}
		}
		recordsRes, err := s.streamClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: shardIterator,
			Limit:         aws.Int32(1000),
		})
		if err != nil {
			return err
		}

		if len(recordsRes.Records) > 0 {
			ch <- StreamRecords{
				StreamARN: *shardInput.StreamArn,
				ShardID:   *shardInput.ShardId,
				Records:   recordsRes.Records,
			}
		}

		shardIterator = recordsRes.NextShardIterator
		if shardIterator == nil {
			break
		}
		// throttle self if we are at waiting for new data
		// not clear from the documentation if this can happen "mid-stream",
		// but it doesn't look like we can easily identify which scenario
		// if both are possible
		// anyway, we assume this means we are waiting for new data
		if s.options.EmptyGetRecordsDelay > 0 && len(recordsRes.Records) == 0 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(s.options.EmptyGetRecordsDelay):
			}
		}
	}

	return nil
}
