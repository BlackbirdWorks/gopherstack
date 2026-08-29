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
// sequential shard IDs starting at shardId-000000000000. startedAt is stamped
// on every shard as its StartedAt (used by ListShards' timestamp-bounded
// ShardFilter types); callers pass the stream's creation time.
func buildInitialShards(shardCount int, startedAt time.Time) []*Shard {
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
			StartedAt:         startedAt,
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

// ShardFilterType values recognized by ListShards' ShardFilter.Type (matches
// types.ShardFilterType in aws-sdk-go-v2/service/kinesis exactly -- gopherstack
// previously invented a nonexistent "AT_SHARD_ID" value here; it has been
// removed in favor of the real AFTER_SHARD_ID semantics below).
const (
	shardFilterAfterShardID    = "AFTER_SHARD_ID"
	shardFilterAtTrimHorizon   = "AT_TRIM_HORIZON"
	shardFilterFromTrimHorizon = "FROM_TRIM_HORIZON"
	shardFilterAtLatest        = "AT_LATEST"
	shardFilterAtTimestamp     = "AT_TIMESTAMP"
	shardFilterFromTimestamp   = "FROM_TIMESTAMP"
)

// shardOpenAt reports whether shard s was open (existed and not yet closed)
// at time ts -- the AT_TIMESTAMP/AT_TRIM_HORIZON ShardFilter predicate ("start
// timestamp is <= ts and end timestamp is >= ts or still open"). A zero
// StartedAt/ClosedAt (shards persisted by a snapshot from before these fields
// existed) is treated permissively so old snapshots degrade to "matches"
// rather than silently vanishing from filtered results.
func shardOpenAt(s *Shard, ts time.Time) bool {
	if !s.StartedAt.IsZero() && s.StartedAt.After(ts) {
		return false
	}
	if !s.Closed {
		return true
	}

	return s.ClosedAt.IsZero() || !s.ClosedAt.Before(ts)
}

// shardClosedAtOrAfter reports whether shard s is open, or closed with
// ClosedAt >= ts -- the FROM_TIMESTAMP ShardFilter predicate ("all closed
// shards whose end timestamp is >= ts, and also all open shards").
func shardClosedAtOrAfter(s *Shard, ts time.Time) bool {
	if !s.Closed {
		return true
	}

	return s.ClosedAt.IsZero() || !s.ClosedAt.Before(ts)
}

// earliestShardStart returns the minimum non-zero StartedAt across shards, or
// the zero time if none is set (e.g. an all-legacy-snapshot shard list).
func earliestShardStart(shards []*Shard) time.Time {
	var earliest time.Time
	for _, s := range shards {
		if s.StartedAt.IsZero() {
			continue
		}
		if earliest.IsZero() || s.StartedAt.Before(earliest) {
			earliest = s.StartedAt
		}
	}

	return earliest
}

// trimHorizon returns the oldest point still within stream's retention
// window, clamped so it never predates the stream's own oldest shard --
// otherwise a freshly created stream (retention window older than the
// stream itself) would report an empty AT_TRIM_HORIZON result, which
// contradicts "TRIM_HORIZON always resolves to the oldest data still
// available," not to a fixed wall-clock offset.
func trimHorizon(stream *Stream) time.Time {
	hours := stream.RetentionPeriod
	if hours <= 0 {
		hours = defaultRetentionHours
	}

	th := time.Now().Add(-time.Duration(hours) * time.Hour)
	if earliest := earliestShardStart(stream.Shards); !earliest.IsZero() && earliest.After(th) {
		th = earliest
	}

	return th
}

// resolveShardFilter interprets a ListShardsInput's ShardFilter into whether
// closed shards should be considered at all (includeAll) and an optional
// per-shard time predicate. Returns ErrInvalidArgument if a timestamp-bound
// filter type is used without a timestamp, and ErrValidation for an
// unrecognized filter type.
func resolveShardFilter(input *ListShardsInput, stream *Stream) (bool, func(*Shard) bool, error) {
	// ShardFilterType is what the HTTP handler populates from the wire
	// ShardFilter.Type; ShardFilter is the plain-string form some direct
	// Go-level backend callers/tests still use. Both are honored, with
	// ShardFilterType taking precedence when both happen to be set.
	filterType := input.ShardFilterType
	if filterType == "" {
		filterType = input.ShardFilter
	}

	switch filterType {
	case "", shardFilterAtLatest:
		return false, nil, nil
	case shardFilterFromTrimHorizon, shardFilterAfterShardID:
		return true, nil, nil
	case shardFilterAtTrimHorizon:
		ts := trimHorizon(stream)

		return true, func(s *Shard) bool { return shardOpenAt(s, ts) }, nil
	case shardFilterAtTimestamp:
		if input.ShardFilterTimestamp == nil {
			return false, nil, ErrInvalidArgument
		}
		ts := *input.ShardFilterTimestamp

		return true, func(s *Shard) bool { return shardOpenAt(s, ts) }, nil
	case shardFilterFromTimestamp:
		if input.ShardFilterTimestamp == nil {
			return false, nil, ErrInvalidArgument
		}
		ts := *input.ShardFilterTimestamp
		// AWS corrects a FROM_TIMESTAMP value below TRIM_HORIZON up to TRIM_HORIZON.
		if th := trimHorizon(stream); ts.Before(th) {
			ts = th
		}

		return true, func(s *Shard) bool { return shardClosedAtOrAfter(s, ts) }, nil
	default:
		return false, nil, ErrValidation
	}
}

// resolveListShardsStartCursor determines the exclusive-start shard ID for
// ListShards pagination: ExclusiveStartShardID takes priority, then NextToken
// (a resumed page), then -- only on a first page with no cursor yet --
// AFTER_SHARD_ID's own ShardId plays the same exclusive-start role.
func resolveListShardsStartCursor(input *ListShardsInput) string {
	if input.ExclusiveStartShardID != "" {
		return input.ExclusiveStartShardID
	}
	if input.NextToken != "" {
		return input.NextToken
	}
	if input.ShardFilterShardID != "" &&
		(input.ShardFilterType == shardFilterAfterShardID || input.ShardFilter == shardFilterAfterShardID) {
		return input.ShardFilterShardID
	}

	return ""
}

// filterShards walks a stream's shards in order, skipping up to and including
// startShardID (an exclusive-start cursor), then keeping every shard that
// passes the open/closed and predicate checks resolveShardFilter produced.
func filterShards(
	shards []*Shard,
	startShardID string,
	includeAll bool,
	predicate func(*Shard) bool,
) []ShardDescription {
	skip := startShardID != ""
	result := make([]ShardDescription, 0, len(shards))

	for _, s := range shards {
		if skip {
			if s.ID == startShardID {
				skip = false
			}

			continue
		}

		// By default only open shards are returned. Include closed shards when
		// the caller requests FROM_TRIM_HORIZON or another inclusive filter type.
		if s.Closed && !includeAll {
			continue
		}

		if predicate != nil && !predicate(s) {
			continue
		}

		result = append(result, shardDescription(s))
	}

	return result
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

	includeAll, predicate, err := resolveShardFilter(input, stream)
	if err != nil {
		return nil, err
	}

	startShardID := resolveListShardsStartCursor(input)
	result := filterShards(stream.Shards, startShardID, includeAll, predicate)

	// Apply MaxResults pagination. AWS documents a default AND max of 1000
	// (api_op_ListShards.go): an omitted or out-of-range value still caps the
	// page, it doesn't return every shard the stream has ever had.
	const maxListShardsResults = 1000

	maxResults := input.MaxResults
	if maxResults <= 0 || maxResults > maxListShardsResults {
		maxResults = maxListShardsResults
	}

	if maxResults < len(result) {
		nextToken := result[maxResults-1].ShardID

		return &ListShardsOutput{Shards: result[:maxResults], NextToken: nextToken}, nil
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
