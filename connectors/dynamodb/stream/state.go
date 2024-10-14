package stream

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type StreamState struct {
	StreamARN               string
	ShardIDToSequenceNumber map[string]string
}

func NewStreamState(streamARN string) StreamState {
	return StreamState{
		StreamARN:               streamARN,
		ShardIDToSequenceNumber: map[string]string{},
	}
}

func ShardIteratorInputFromState(state StreamState, after bool) []*dynamodbstreams.GetShardIteratorInput {
	var res []*dynamodbstreams.GetShardIteratorInput
	typ := types.ShardIteratorTypeAtSequenceNumber
	if after {
		typ = types.ShardIteratorTypeAfterSequenceNumber
	}
	for k, v := range state.ShardIDToSequenceNumber {
		var seqNum *string
		if v != "" {
			seqNum = aws.String(v)
		} else {
			typ = types.ShardIteratorTypeTrimHorizon
		}
		res = append(res, &dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String(k),
			ShardIteratorType: typ,
			StreamArn:         aws.String(state.StreamARN),
			SequenceNumber:    seqNum,
		})
	}
	return res
}

func (s StreamState) UpdateSequenceNumber(shardID string, seqNum string) {
	s.ShardIDToSequenceNumber[shardID] = seqNum
}

func (s StreamState) UpdateFromStreamRecords(records StreamRecords) {
	if len(records.ChildShardIDs) > 0 {
		delete(s.ShardIDToSequenceNumber, records.ShardID)
		for _, shardID := range records.ChildShardIDs {
			s.ShardIDToSequenceNumber[shardID] = ""
		}
	} else if len(records.Records) > 0 {
		s.ShardIDToSequenceNumber[records.ShardID] = *records.Records[len(records.Records)-1].Dynamodb.SequenceNumber
	}
}
