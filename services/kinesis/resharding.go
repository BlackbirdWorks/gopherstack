package kinesis

import (
	"context"
	"fmt"
	"math/big"
	"time"
)

func findOverlappingParents(start, end *big.Int, oldOpenShards []*Shard) (string, string) {
	var parents []string
	for _, os := range oldOpenShards {
		osStart, _ := new(big.Int).SetString(os.HashKeyRangeStart, hashKeyDecimalBase)
		osEnd, _ := new(big.Int).SetString(os.HashKeyRangeEnd, hashKeyDecimalBase)

		maxStart := start
		if osStart.Cmp(maxStart) > 0 {
			maxStart = osStart
		}
		minEnd := end
		if osEnd.Cmp(minEnd) < 0 {
			minEnd = osEnd
		}
		if maxStart.Cmp(minEnd) <= 0 {
			parents = append(parents, os.ID)
		}
	}
	var pid, apid string
	if len(parents) > 0 {
		pid = parents[0]
	}
	if len(parents) > 1 {
		apid = parents[1]
	}

	return pid, apid
}

// UpdateShardCount resizes a stream to the given number of shards.
// Existing records in the stream are not migrated; new shards start empty.
func (b *InMemoryBackend) UpdateShardCount(
	ctx context.Context,
	input *UpdateShardCountInput,
) (*UpdateShardCountOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateShardCount")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(region, input.StreamName))
	if !ok {
		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("UpdateShardCount.stream")
	defer stream.mu.Unlock()

	if stream.StreamMode == streamModeOnDemand {
		return nil, ErrInvalidArgument
	}

	if input.TargetShardCount <= 0 {
		return nil, ErrInvalidArgument
	}

	if input.ScalingType != "" && input.ScalingType != scalingTypeUniformScaling {
		return nil, ErrInvalidArgument
	}

	// Count only open shards as the current count (AWS semantics).
	currentCount := countOpenShards(stream.Shards)
	targetCount := input.TargetShardCount

	if targetCount > maxShardsPerStream {
		return nil, ErrInvalidArgument
	}

	// AWS caps a single UpdateShardCount call to between 50% and 200% of the
	// current open shard count: the target may not exceed double the current
	// count, nor drop below half of it. Requests outside this window are
	// rejected with ValidationException (never partially applied).
	if targetCount > 2*currentCount || 2*targetCount < currentCount {
		return nil, ErrShardCountScaling
	}

	reshardTo(stream, targetCount)

	return &UpdateShardCountOutput{
		StreamName:        input.StreamName,
		CurrentShardCount: currentCount,
		TargetShardCount:  targetCount,
	}, nil
}

// countOpenShards returns the number of non-closed shards in shards.
func countOpenShards(shards []*Shard) int {
	count := 0
	for _, s := range shards {
		if !s.Closed {
			count++
		}
	}

	return count
}

// reshardTo replaces stream's open shard set with targetCount new shards
// spanning the full hash key space, closing every currently open shard and
// assigning parent/adjacent-parent lineage from hash-range overlap (same
// math UpdateShardCount uses). Shared by UpdateShardCount and
// UpdateStreamMode's PROVISIONED->ON_DEMAND auto-scale reshard. Caller must
// already hold stream.mu.
func reshardTo(stream *Stream, targetCount int) {
	var oldOpenShards []*Shard
	for _, s := range stream.Shards {
		if !s.Closed {
			oldOpenShards = append(oldOpenShards, s)
		}
	}

	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), maxHashKeyBits),
		big.NewInt(1),
	)
	shardRange := new(big.Int).Div(
		new(big.Int).Add(maxHashKey, big.NewInt(1)),
		big.NewInt(int64(targetCount)),
	)

	// Assign new shard IDs from beyond the existing maximum to avoid collisions.
	startIdx := nextShardIDIndex(stream.Shards)
	now := time.Now()

	newShards := make([]*Shard, targetCount)
	for i := range targetCount {
		start := new(big.Int).Mul(shardRange, big.NewInt(int64(i)))

		var end *big.Int
		if i == targetCount-1 {
			end = maxHashKey
		} else {
			end = new(big.Int).Sub(
				new(big.Int).Mul(shardRange, big.NewInt(int64(i+1))),
				big.NewInt(1),
			)
		}

		pid, apid := findOverlappingParents(start, end, oldOpenShards)

		newShards[i] = &Shard{
			ID:                    fmt.Sprintf("shardId-%012d", startIdx+i),
			HashKeyRangeStart:     start.String(),
			HashKeyRangeEnd:       end.String(),
			ParentShardID:         pid,
			AdjacentParentShardID: apid,
			StartedAt:             now,
		}
	}

	// Mark all currently open shards as CLOSED (AWS semantics: old shards
	// remain visible in DescribeStream/ListShards with CLOSED status).
	for _, s := range oldOpenShards {
		closeShard(s)
	}

	allShards := make([]*Shard, 0, len(stream.Shards)+targetCount)
	allShards = append(allShards, stream.Shards...)
	allShards = append(allShards, newShards...)
	stream.Shards = allShards
}

// nextShardIDIndex returns the numeric index for the next unique shard ID.
func nextShardIDIndex(shards []*Shard) int {
	maxIdx := len(shards) // safe lower bound
	for _, s := range shards {
		var n int
		if _, err := fmt.Sscanf(s.ID, "shardId-%012d", &n); err == nil && n+1 > maxIdx {
			maxIdx = n + 1
		}
	}

	return maxIdx
}

// nextShardID computes the next unique shard ID for a stream by finding the
// maximum existing shard index and incrementing it. This ensures IDs remain
// unique across merge/split operations even after shards are removed.
func nextShardID(shards []*Shard) string {
	return fmt.Sprintf("shardId-%012d", nextShardIDIndex(shards))
}

// MergeShards merges two adjacent shards into one.
// The merged shard spans the combined hash key range of both parent shards.
func (b *InMemoryBackend) MergeShards(ctx context.Context, input *MergeShardsInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("MergeShards")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("MergeShards.stream")
	defer stream.mu.Unlock()

	if stream.StreamMode == streamModeOnDemand {
		return ErrInvalidArgument
	}

	shard1 := findShard(stream.Shards, input.ShardToMerge)
	shard2 := findShard(stream.Shards, input.AdjacentShardToMerge)

	if shard1 == nil || shard2 == nil {
		return ErrInvalidArgument
	}
	if shard1.Closed || shard2.Closed {
		return ErrInvalidArgument
	}

	// Determine the merged range: min start, max end.
	s1Start := new(big.Int)
	s1Start.SetString(shard1.HashKeyRangeStart, hashKeyDecimalBase)
	s2Start := new(big.Int)
	s2Start.SetString(shard2.HashKeyRangeStart, hashKeyDecimalBase)
	s1End := new(big.Int)
	s1End.SetString(shard1.HashKeyRangeEnd, hashKeyDecimalBase)
	s2End := new(big.Int)
	s2End.SetString(shard2.HashKeyRangeEnd, hashKeyDecimalBase)

	s1EndPlusOne := new(big.Int).Add(s1End, big.NewInt(1))
	s2EndPlusOne := new(big.Int).Add(s2End, big.NewInt(1))
	if s1EndPlusOne.Cmp(s2Start) != 0 && s2EndPlusOne.Cmp(s1Start) != 0 {
		return ErrInvalidArgument
	}

	startKey := s1Start
	if s2Start.Cmp(s1Start) < 0 {
		startKey = s2Start
	}

	endKey := s1End
	if s2End.Cmp(s1End) > 0 {
		endKey = s2End
	}

	mergedID := nextShardID(stream.Shards)
	merged := &Shard{
		ID:                mergedID,
		HashKeyRangeStart: startKey.String(),
		HashKeyRangeEnd:   endKey.String(),
		StartedAt:         time.Now(),
	}

	// Mark parents as closed (keep them in the list)
	closeShard(shard1)
	closeShard(shard2)

	merged.ParentShardID = input.ShardToMerge
	merged.AdjacentParentShardID = input.AdjacentShardToMerge

	newShards := make([]*Shard, 0, len(stream.Shards)+1)
	newShards = append(newShards, stream.Shards...)
	newShards = append(newShards, merged)
	stream.Shards = newShards

	return nil
}

// SplitShard splits a shard into two at the given new starting hash key.
func (b *InMemoryBackend) SplitShard(ctx context.Context, input *SplitShardInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("SplitShard")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("SplitShard.stream")
	defer stream.mu.Unlock()

	if stream.StreamMode == streamModeOnDemand {
		return ErrInvalidArgument
	}

	shard := findShard(stream.Shards, input.ShardToSplit)
	if shard == nil {
		return ErrInvalidArgument
	}

	newStart := new(big.Int)
	if _, valid := newStart.SetString(input.NewStartingHashKey, hashKeyDecimalBase); !valid {
		return ErrInvalidArgument
	}

	shardStart := new(big.Int)
	shardStart.SetString(shard.HashKeyRangeStart, hashKeyDecimalBase)
	shardEnd := new(big.Int)
	shardEnd.SetString(shard.HashKeyRangeEnd, hashKeyDecimalBase)

	// NewStartingHashKey must be strictly inside the shard's range.
	if newStart.Cmp(shardStart) <= 0 || newStart.Cmp(shardEnd) >= 0 {
		return ErrInvalidArgument
	}

	shard1End := new(big.Int).Sub(newStart, big.NewInt(1))

	shard1ID := nextShardID(stream.Shards)

	var shard1Idx int
	if _, err := fmt.Sscanf(shard1ID, "shardId-%012d", &shard1Idx); err != nil {
		// nextShardID guarantees the format; this path is unreachable in practice.
		shard1Idx = len(stream.Shards)
	}

	shard2ID := fmt.Sprintf("shardId-%012d", shard1Idx+1)
	now := time.Now()
	shard1 := &Shard{
		ID:                shard1ID,
		HashKeyRangeStart: shard.HashKeyRangeStart,
		HashKeyRangeEnd:   shard1End.String(),
		StartedAt:         now,
	}
	shard2 := &Shard{
		ID:                shard2ID,
		HashKeyRangeStart: input.NewStartingHashKey,
		HashKeyRangeEnd:   shard.HashKeyRangeEnd,
		StartedAt:         now,
	}

	closeShard(shard)

	shard1.ParentShardID = input.ShardToSplit
	shard2.ParentShardID = input.ShardToSplit

	// splitShardResultCount is the number of new shards produced by a split.
	const splitShardResultCount = 2

	newShards := make([]*Shard, 0, len(stream.Shards)+splitShardResultCount)
	newShards = append(newShards, stream.Shards...)
	newShards = append(newShards, shard1, shard2)
	stream.Shards = newShards

	return nil
}
