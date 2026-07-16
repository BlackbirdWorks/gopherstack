package kinesis

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 used as a non-cryptographic hash key for Kinesis shard routing, matching the AWS API contract
	"fmt"
	"math/big"
	"time"
)

func initializeStreamRuntime(stream *Stream, streamName string) {
	if stream.Name == "" {
		stream.Name = streamName
	}
	if stream.Consumers == nil {
		stream.Consumers = map[string]*Consumer{}
	}
	if stream.mu == nil {
		stream.mu = newStreamLock(stream.Name)
	}
	if stream.Shards == nil {
		stream.Shards = []*Shard{}
	}
	if stream.StreamMode == "" {
		stream.StreamMode = streamModeProvisioned
	}
	if stream.EnhancedMonitoring == nil {
		stream.EnhancedMonitoring = []string{}
	}
	if stream.MaxRecordSizeBytes <= 0 {
		stream.MaxRecordSizeBytes = defaultMaxRecordSizeBytes
	}
}

// hashKey computes the Kinesis hash key for a partition key using MD5, matching
// the AWS API contract. The result is in the range [0, 2^128-1].
func hashKey(partitionKey string) *big.Int {
	//nolint:gosec // MD5 is intentional: AWS Kinesis uses MD5 for partition-key → shard routing
	sum := md5.Sum([]byte(partitionKey))

	return new(big.Int).SetBytes(sum[:])
}

// shardForHashKey selects the open shard whose hash key range contains h.
func shardForHashKey(shards []*Shard, h *big.Int) *Shard {
	for _, s := range shards {
		if s.Closed {
			continue
		}
		start := new(big.Int)
		start.SetString(s.HashKeyRangeStart, hashKeyDecimalBase)
		end := new(big.Int)
		end.SetString(s.HashKeyRangeEnd, hashKeyDecimalBase)
		if h.Cmp(start) >= 0 && h.Cmp(end) <= 0 {
			return s
		}
	}
	// fallback: first open shard
	for _, s := range shards {
		if !s.Closed {
			return s
		}
	}
	if len(shards) > 0 {
		return shards[0]
	}

	return nil
}

// shardForPartitionKey selects the open shard for the given partition key by hash.
func shardForPartitionKey(shards []*Shard, partitionKey string) *Shard {
	return shardForHashKey(shards, hashKey(partitionKey))
}

// nextSequenceNumber generates a new sequence number for a shard.
func (s *Shard) nextSequenceNumber() string {
	s.NextSeq++

	var idx int64
	if _, err := fmt.Sscanf(s.ID, "shardId-%d", &idx); err != nil {
		idx = 0
	}
	const shardIDModulus = 10000

	// AWS sequence numbers encode time and shard info. We use a 49-prefix, timestamp, shard index, and seq.
	return fmt.Sprintf(
		"49%014d%04d%020d",
		time.Now().UnixNano()/int64(time.Millisecond),
		idx%shardIDModulus,
		s.NextSeq,
	)
}

func checkOnDemandLimit(streams []*Stream, limit int) error {
	onDemandCount := 0
	for _, s := range streams {
		if s.StreamMode == streamModeOnDemand {
			onDemandCount++
		}
	}
	if onDemandCount >= limit {
		return ErrLimitExceeded
	}

	return nil
}

// buildInitialShards partitions the full Kinesis hash key space
// ([0, 2^128-1]) into shardCount contiguous, non-overlapping open shards with
// sequential shard IDs starting at shardId-000000000000.
func buildInitialShards(shardCount int) []*Shard {
	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), maxHashKeyBits),
		big.NewInt(1),
	)
	shardRange := new(big.Int).Div(
		new(big.Int).Add(maxHashKey, big.NewInt(1)),
		big.NewInt(int64(shardCount)),
	)

	// No capacity hint — user-derived values in the make capacity position
	// trigger CodeQL go/slice-memory-allocation-excessive-size even after
	// clamping. shardCount is only used for the loop count below (safe).
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	shards := make([]*Shard, 0)
	for i := range shardCount {
		start := new(big.Int).Mul(shardRange, big.NewInt(int64(i)))
		var end *big.Int
		if i == shardCount-1 {
			end = maxHashKey
		} else {
			end = new(big.Int).Sub(
				new(big.Int).Mul(shardRange, big.NewInt(int64(i+1))),
				big.NewInt(1),
			)
		}

		shards = append(shards, &Shard{
			ID:                fmt.Sprintf("shardId-%012d", i),
			HashKeyRangeStart: start.String(),
			HashKeyRangeEnd:   end.String(),
		})
	}

	return shards
}

// findShard returns the shard with the given ID from a slice, or nil if not found.
func findShard(shards []*Shard, shardID string) *Shard {
	for _, s := range shards {
		if s.ID == shardID {
			return s
		}
	}

	return nil
}

// shardDescription builds a ShardDescription from a Shard.
func shardDescription(s *Shard) ShardDescription {
	seqStart := "0"
	if s.Records.len() > 0 {
		seqStart = s.Records.at(0).SequenceNumber
	}

	// EndingSequenceNumber is reported only for CLOSED shards. AWS leaves it
	// absent on open shards regardless of whether they currently hold records —
	// KCL-style consumers treat its presence as the signal that a shard is
	// closed and they should advance to the child shards. Reporting it on an
	// open shard with records would make a consumer prematurely abandon the shard.
	var seqEnd string
	if s.Closed {
		seqEnd = "0"
		if s.Records.len() > 0 {
			seqEnd = s.Records.last().SequenceNumber
		}
	}

	return ShardDescription{
		ShardID:                  s.ID,
		HashKeyRangeStart:        s.HashKeyRangeStart,
		HashKeyRangeEnd:          s.HashKeyRangeEnd,
		SequenceNumberRangeStart: seqStart,
		SequenceNumberRangeEnd:   seqEnd,
		ParentShardID:            s.ParentShardID,
		AdjacentParentShardID:    s.AdjacentParentShardID,
		Closed:                   s.Closed,
	}
}

// listShardsAtShardID returns shards matching the AT_SHARD_ID filter.
func listShardsAtShardID(shards []*Shard, targetID string) []ShardDescription {
	result := make([]ShardDescription, 0, len(shards))
	for _, s := range shards {
		if s.ID != targetID && s.ParentShardID != targetID && s.AdjacentParentShardID != targetID {
			continue
		}
		result = append(result, shardDescription(s))
	}

	return result
}

// shardFilterIncludesAll reports whether the given ShardFilter value requests all shards
// (open and closed). Used by ListShards.
func shardFilterIncludesAll(filter string) bool {
	return filter == "FROM_TRIM_HORIZON" || filter == "AT_TIMESTAMP" || filter == "FROM_TIMESTAMP"
}

// ListShards returns the shards for a stream.
func (b *InMemoryBackend) ListShards(ctx context.Context, input *ListShardsInput) (*ListShardsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListShards")

	stream, exists := b.streams.Get(streamKey(region, input.StreamName))
	if !exists {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("ListShards.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	if input.ShardFilterType == "AT_SHARD_ID" && input.ShardFilterShardID != "" {
		return &ListShardsOutput{Shards: listShardsAtShardID(stream.Shards, input.ShardFilterShardID)}, nil
	}

	includeAll := shardFilterIncludesAll(input.ShardFilter)

	// NextToken acts as an exclusive start shard ID for pagination.
	startShardID := input.ExclusiveStartShardID
	if startShardID == "" {
		startShardID = input.NextToken
	}

	skip := startShardID != ""
	result := make([]ShardDescription, 0, len(stream.Shards))

	for _, s := range stream.Shards {
		if skip {
			if s.ID == startShardID {
				skip = false
			}

			continue
		}

		// By default only open shards are returned. Include closed shards when
		// the caller requests FROM_TRIM_HORIZON or other inclusive filter types.
		if s.Closed && !includeAll {
			continue
		}

		result = append(result, shardDescription(s))
	}

	// Apply MaxResults pagination.
	if input.MaxResults > 0 && input.MaxResults < len(result) {
		nextToken := result[input.MaxResults-1].ShardID

		return &ListShardsOutput{Shards: result[:input.MaxResults], NextToken: nextToken}, nil
	}

	return &ListShardsOutput{Shards: result}, nil
}

// CountOpenShards returns the total number of open (non-closed) shards across
// every stream in the region carried on ctx. DescribeLimits is region-scoped in
// AWS, so this counts within a single region.
func (b *InMemoryBackend) CountOpenShards(ctx context.Context) int {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CountOpenShards")
	defer b.mu.RUnlock()

	count := 0
	for _, s := range b.streamsByRegion.Get(region) {
		s.mu.RLock("CountOpenShards.stream")
		for _, sh := range s.Shards {
			if !sh.Closed {
				count++
			}
		}
		s.mu.RUnlock()
	}

	return count
}
