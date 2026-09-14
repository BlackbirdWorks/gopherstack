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

	now := b.nowFunc()

	var position int

	switch input.ShardIteratorType {
	case iteratorTypeTrimHorizon:
		// "TRIM_HORIZON - Start reading at the last untrimmed record in the
		// shard in the system, which is the oldest data record in the
		// shard" (api_op_GetShardIterator.go). Position 0 in the ring
		// buffer is only the oldest *currently retained* record once the
		// background janitor sweep (janitor.go) has actually evicted
		// expired ones, which can lag a retention decrease by up to its
		// polling interval -- so this resolves the retention cutoff
		// synchronously here instead of trusting position 0, honoring
		// per-record ApproximateArrivalTimestamp against the stream's
		// retention window even before the janitor catches up.
		position = findTimestampPosition(&shard.Records, retentionCutoff(stream, now))
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
		// "If the time stamp is older than the current trim horizon, the
		// iterator returned is for the oldest untrimmed data record
		// (TRIM_HORIZON)." (api_op_GetShardIterator.go, Timestamp doc
		// comment) -- clamp up to the retention cutoff before searching,
		// same rationale as the TRIM_HORIZON case above.
		ts := *input.Timestamp
		if th := retentionCutoff(stream, now); ts.Before(th) {
			ts = th
		}
		position = findTimestampPosition(&shard.Records, ts)
	default:
		return nil, ErrInvalidArgument
	}

	it := &ShardIterator{
		StreamName:     input.StreamName,
		ShardID:        input.ShardID,
		Position:       position,
		SequenceNumber: input.StartingSequenceNumber,
		Region:         region,
		CreatedAt:      now,
	}

	token, err := encodeIterator(it)
	if err != nil {
		return nil, err
	}

	return &GetShardIteratorOutput{ShardIterator: token}, nil
}
