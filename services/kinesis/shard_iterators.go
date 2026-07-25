package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"
)

// iteratorToken encodes a shard iterator as a base64 JSON token.
func encodeIterator(it *ShardIterator) (string, error) {
	data, err := json.Marshal(it)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

// decodeIterator decodes a base64 JSON shard iterator token.
func decodeIterator(token string) (*ShardIterator, error) {
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrShardIteratorExpired
	}

	var it ShardIterator
	if err = json.Unmarshal(data, &it); err != nil {
		return nil, ErrShardIteratorExpired
	}

	if !it.CreatedAt.IsZero() && time.Since(it.CreatedAt) > iteratorTTL {
		return nil, ErrShardIteratorExpired
	}

	return &it, nil
}

// GetShardIterator returns an iterator for reading records from a shard.
func (b *InMemoryBackend) GetShardIterator(
	ctx context.Context,
	input *GetShardIteratorInput,
) (*GetShardIteratorOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetShardIterator")

	stream, exists := b.streams.Get(streamKey(region, input.StreamName))
	if !exists {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("GetShardIterator.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	// Find the shard
	shard := findShard(stream.Shards, input.ShardID)

	if shard == nil {
		return nil, ErrInvalidArgument
	}

	var position int

	switch input.ShardIteratorType {
	case iteratorTypeTrimHorizon:
		position = 0
	case iteratorTypeLatest:
		position = shard.Records.len()
	case iteratorTypeAtSequenceNumber, iteratorTypeAfterSequenceNumber:
		if input.StartingSequenceNumber == "" {
			return nil, ErrInvalidArgument
		}
		position = findSequencePosition(
			&shard.Records,
			input.StartingSequenceNumber,
			input.ShardIteratorType == iteratorTypeAfterSequenceNumber,
		)
	case iteratorTypeAtTimestamp:
		// Timestamp is required for AT_TIMESTAMP; a genuinely omitted value
		// (nil, distinguished at the JSON layer from an explicit epoch-zero
		// timestamp) is rejected rather than silently treated as position 0.
		if input.Timestamp == nil {
			return nil, ErrInvalidArgument
		}
		position = findTimestampPosition(&shard.Records, *input.Timestamp)
	default:
		return nil, ErrInvalidArgument
	}

	it := &ShardIterator{
		StreamName:     input.StreamName,
		ShardID:        input.ShardID,
		Position:       position,
		SequenceNumber: input.StartingSequenceNumber,
		Region:         region,
		CreatedAt:      time.Now(),
	}

	token, err := encodeIterator(it)
	if err != nil {
		return nil, err
	}

	return &GetShardIteratorOutput{ShardIterator: token}, nil
}
