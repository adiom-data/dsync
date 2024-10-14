package stream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var ErrAlreadyRunning = fmt.Errorf("already running")
var ErrNotRunning = fmt.Errorf("not running")

var DefaultDescribeLimiter = rate.NewLimiter(rate.Limit(5), 5)

type Limiter interface {
	Wait(context.Context) error
}

type Logger interface {
	Debug(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}

type shardRequestResult struct {
	shards []types.Shard
	err    error
}

type dynamodbStreamHelper struct {
	streamClient          *dynamodbstreams.Client
	streamARN             string
	shardRequestCh        chan chan shardRequestResult
	describeStreamLimiter Limiter
	logger                Logger
	skipEmpty             int

	running atomic.Bool
	once    sync.Once
	done    chan struct{}
	err     error
}

type HelperOptions struct {
	DescribeStreamLimiter Limiter
	Logger                Logger
	SkipEmpty             int // For use when searching for approximate latest
}

func NewDynamodbStreamHelper(streamClient *dynamodbstreams.Client, streamARN string, optsFn ...func(*HelperOptions)) *dynamodbStreamHelper {
	opts := HelperOptions{
		DescribeStreamLimiter: DefaultDescribeLimiter,
		Logger:                slog.Default(),
		SkipEmpty:             5,
	}
	for _, fn := range optsFn {
		fn(&opts)
	}
	return &dynamodbStreamHelper{
		streamClient:          streamClient,
		streamARN:             streamARN,
		shardRequestCh:        make(chan chan shardRequestResult), // We don't close this, but that is ok
		describeStreamLimiter: opts.DescribeStreamLimiter,
		logger:                opts.Logger,
		skipEmpty:             opts.SkipEmpty,
		done:                  make(chan struct{}),
	}
}

func (s *dynamodbStreamHelper) Start(ctx context.Context) error {
	err := ErrAlreadyRunning
	s.once.Do(func() {
		s.running.Store(true)
		go func() {
			s.err = s.startGetShards(ctx)
			s.running.Store((false))
			close(s.done)
		}()
		err = nil
	})
	return err
}

func (s *dynamodbStreamHelper) Wait() error {
	<-s.done
	return s.err
}

func (s *dynamodbStreamHelper) GetShards(ctx context.Context) ([]types.Shard, error) {
	if !s.running.Load() {
		return nil, ErrNotRunning
	}
	recv := make(chan shardRequestResult)
	select {
	case s.shardRequestCh <- recv:
		select {
		case result := <-recv:
			return result.shards, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *dynamodbStreamHelper) ApproximateLatestState(ctx context.Context) (StreamState, error) {
	if !s.running.Load() {
		return StreamState{}, ErrNotRunning
	}
	return s.approximateLatestState(ctx, time.Now())
}

func (s *dynamodbStreamHelper) ShardIteratorInputForEarliest(ctx context.Context) ([]*dynamodbstreams.GetShardIteratorInput, error) {
	if !s.running.Load() {
		return nil, ErrNotRunning
	}
	return s.shardIteratorInputForEarliest(ctx, s.streamARN)
}

func (s *dynamodbStreamHelper) ShardIteratorInputForLatest(ctx context.Context) ([]*dynamodbstreams.GetShardIteratorInput, error) {
	if !s.running.Load() {
		return nil, ErrNotRunning
	}
	return s.shardIteratorInputForLatest(ctx, s.streamARN)
}

func (s *dynamodbStreamHelper) getShards(ctx context.Context) ([]types.Shard, error) {
	var shards []types.Shard
	var exclusiveStartShardId *string
	streamARN := aws.String(s.streamARN)
	for {
		if s.describeStreamLimiter != nil {
			if err := s.describeStreamLimiter.Wait(ctx); err != nil {
				return nil, err
			}
		}
		res, err := s.streamClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn:             streamARN,
			ExclusiveStartShardId: exclusiveStartShardId,
			Limit:                 aws.Int32(100),
		})
		if err != nil {
			return nil, err
		}
		shards = append(shards, res.StreamDescription.Shards...)
		exclusiveStartShardId = res.StreamDescription.LastEvaluatedShardId
		if exclusiveStartShardId == nil {
			break
		}
	}
	return shards, nil
}

func (s *dynamodbStreamHelper) startGetShards(ctx context.Context) error {
	done := make(chan struct{})
	requestCh := make(chan []chan shardRequestResult)
	readyCh := make(chan struct{})

	go func() {
		defer close(done)
		defer close(readyCh)
		readyCh <- struct{}{}
		for requesters := range requestCh {
			slog.Debug("serving get shard request", "requesters", len(requesters))
			res, err := s.getShards(ctx)
			srr := shardRequestResult{res, err}
			for _, requester := range requesters {
				requester <- srr
				close(requester)
			}
			select {
			case readyCh <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()

	var currentRequests []chan shardRequestResult
	for {
		select {
		case <-readyCh:
			if len(currentRequests) == 0 {
				select {
				case <-ctx.Done():
					close(requestCh)
					<-done
					return ctx.Err()
				case requester := <-s.shardRequestCh:
					currentRequests = append(currentRequests, requester)
				}
			}
			requestCh <- currentRequests
			currentRequests = nil
		case requester := <-s.shardRequestCh:
			currentRequests = append(currentRequests, requester)
		case <-ctx.Done():
			close(requestCh)
			<-done
			return ctx.Err()
		}
	}
}

func (s *dynamodbStreamHelper) shardIteratorInputForEarliest(ctx context.Context, arn string) ([]*dynamodbstreams.GetShardIteratorInput, error) {
	var res []*dynamodbstreams.GetShardIteratorInput
	shards, err := s.GetShards(ctx)
	if err != nil {
		return nil, err
	}

	shardSet := map[string]struct{}{}
	var candidates []types.Shard

	for _, shard := range shards {
		shardSet[*shard.ShardId] = struct{}{}
	}
	for _, shard := range shards {
		if shard.ParentShardId == nil {
			candidates = append(candidates, shard)
		} else if _, ok := shardSet[*shard.ParentShardId]; !ok {
			candidates = append(candidates, shard)
		}
	}
	for _, shard := range candidates {
		res = append(res, &dynamodbstreams.GetShardIteratorInput{
			ShardId:           shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamArn:         aws.String(arn),
		})
	}
	return res, nil
}

func (s *dynamodbStreamHelper) shardIteratorInputForLatest(ctx context.Context, arn string) ([]*dynamodbstreams.GetShardIteratorInput, error) {
	var res []*dynamodbstreams.GetShardIteratorInput
	shards, err := s.GetShards(ctx)
	if err != nil {
		return nil, err
	}

	openShards := s.openShards(shards)
	for _, shard := range openShards {
		res = append(res, &dynamodbstreams.GetShardIteratorInput{
			ShardId:           shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeLatest,
			StreamArn:         aws.String(arn),
		})
	}
	return res, nil
}

func (s *dynamodbStreamHelper) openShards(shards []types.Shard) []types.Shard {
	parentSet := map[string]struct{}{}
	for _, shard := range shards {
		if shard.ParentShardId != nil {
			parentSet[*shard.ParentShardId] = struct{}{}
		}
	}

	var openShards []types.Shard
	for _, shard := range shards {
		if shard.SequenceNumberRange.EndingSequenceNumber != nil {
			continue
		}
		if _, ok := parentSet[*shard.ShardId]; ok {
			continue
		}
		openShards = append(openShards, shard)
	}
	return openShards
}

func (s *dynamodbStreamHelper) approximateLatestState(ctx context.Context, approximateNow time.Time) (StreamState, error) {
	// Strategy here is to read only from the open shards, and find the sequence number just before the approximate time.
	// We cannot use the LATEST shard iterator since that relies on new entries
	shards, err := s.GetShards(ctx)
	if err != nil {
		return StreamState{}, err
	}
	openShards := s.openShards(shards)

	streamARN := aws.String(s.streamARN)
	state := NewStreamState(s.streamARN)

	eg, egCtx := errgroup.WithContext(ctx)

	for _, shard := range openShards {
		eg.Go(func() error {
			shardIteratorRes, err := s.streamClient.GetShardIterator(egCtx, &dynamodbstreams.GetShardIteratorInput{
				ShardId:           shard.ShardId,
				ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
				StreamArn:         streamARN,
			})
			if err != nil {
				return err
			}
			shardIterator := shardIteratorRes.ShardIterator
			var lastSeqNum *string = shard.SequenceNumberRange.StartingSequenceNumber
			defer s.logger.Debug("using sequence number", "shard_id", *shard.ShardId, "sequence_number", *lastSeqNum)
			skipped := 0
		Loop:
			for {
				recordsRes, err := s.streamClient.GetRecords(egCtx, &dynamodbstreams.GetRecordsInput{
					ShardIterator: shardIterator,
					Limit:         aws.Int32(1000),
				})
				if err != nil {
					return err
				}
				s.logger.Debug("get records", "shard_id", *shard.ShardId, "num_records", len(recordsRes.Records))

				if len(recordsRes.Records) > 0 {
					skipped = 0
					last := recordsRes.Records[len(recordsRes.Records)-1]
					if last.Dynamodb.ApproximateCreationDateTime.After(approximateNow) {
						for _, record := range recordsRes.Records {
							lastSeqNum = record.Dynamodb.SequenceNumber
							if record.Dynamodb.ApproximateCreationDateTime.After(approximateNow) {
								break Loop
							}
						}
					} else {
						lastSeqNum = last.Dynamodb.SequenceNumber
					}
				} else {
					skipped++
					if skipped > s.skipEmpty {
						break
					}
				}

				shardIterator = recordsRes.NextShardIterator
				if shardIterator == nil {
					break
				}
			}

			state.UpdateSequenceNumber(*shard.ShardId, *lastSeqNum)
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		return StreamState{}, err
	}

	return state, nil
}
