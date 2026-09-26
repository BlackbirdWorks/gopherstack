package kinesis

import (
	"context"
	"regexp"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// streamNameRe validates Kinesis stream names: 1–128 alphanumeric, hyphen, underscore, or dot chars.
var streamNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

// isValidStreamName reports whether s is a valid Kinesis stream name.
func isValidStreamName(s string) bool {
	return streamNameRe.MatchString(s)
}

// resolveCreateStreamMaxRecordSize validates CreateStreamInput's own
// MaxRecordSizeInKiB member (kinesis@v1.46.4 api_op_CreateStream.go:101-103)
// and converts it to bytes, or returns defaultMaxRecordSizeBytes when unset
// (kiB <= 0).
func resolveCreateStreamMaxRecordSize(kiB int) (int, error) {
	if kiB <= 0 {
		return defaultMaxRecordSizeBytes, nil
	}

	sizeBytes := kiB * bytesPerKiB
	if sizeBytes < defaultMaxRecordSizeBytes || sizeBytes > absoluteMaxRecordSizeBytes {
		return 0, ErrInvalidArgument
	}

	return sizeBytes, nil
}

// resolveCreateStreamModeAndShardCount validates CreateStreamInput's
// StreamMode and ShardCount together and returns the effective values.
// ON_DEMAND streams ignore any caller-supplied ShardCount — AWS auto-manages
// capacity and a freshly created on-demand stream starts with
// defaultOnDemandShardCount shards.
func resolveCreateStreamModeAndShardCount(mode string, shardCount int) (string, int, error) {
	if mode == "" {
		mode = streamModeProvisioned
	} else if mode != streamModeProvisioned && mode != streamModeOnDemand {
		return "", 0, ErrInvalidArgument
	}

	if mode == streamModeOnDemand {
		shardCount = defaultOnDemandShardCount
	} else if shardCount <= 0 {
		shardCount = defaultShardCount
	}
	if shardCount > maxShardsPerStream {
		return "", 0, ErrInvalidArgument
	}
	if shardCount > maxShardCount {
		shardCount = maxShardCount
	}

	return mode, shardCount, nil
}

// CreateStream creates a new Kinesis stream.
func (b *InMemoryBackend) CreateStream(ctx context.Context, input *CreateStreamInput) error {
	region := getRegion(ctx, b.region)
	if input.Region != "" {
		region = input.Region
	}

	b.mu.Lock("CreateStream")
	defer b.mu.Unlock()

	if !isValidStreamName(input.StreamName) {
		return ErrValidation
	}

	if _, err := b.resolveStreamTransitionLocked(region, input.StreamName); err == nil {
		return ErrStreamAlreadyExists
	}

	streamMode, shardCount, err := resolveCreateStreamModeAndShardCount(input.StreamMode, input.ShardCount)
	if err != nil {
		return err
	}

	now := b.nowFunc()
	shards := buildInitialShards(shardCount, now)

	accountID := b.accountID
	if input.AccountID != "" {
		accountID = input.AccountID
	}

	if streamMode == streamModeOnDemand {
		if odErr := checkOnDemandLimit(b.resolveRegionStreamsLocked(region), b.onDemandStreamCountLimit); odErr != nil {
			return odErr
		}
	}

	maxRecordSizeBytes, err := resolveCreateStreamMaxRecordSize(input.MaxRecordSizeInKiB)
	if err != nil {
		return err
	}

	if input.WarmThroughputMiBps < 0 || input.WarmThroughputMiBps > maxWarmThroughputMiBps {
		return ErrInvalidArgument
	}

	streamARN := arn.Build("kinesis", region, accountID, "stream/"+input.StreamName)

	b.streams.Put(&Stream{
		Name:                input.StreamName,
		ARN:                 streamARN,
		Region:              region,
		Status:              streamStatusCreating,
		ReadyAt:             now.Add(streamTransitionDelay),
		Shards:              shards,
		mu:                  newStreamLock(input.StreamName),
		Tags:                tags.New("kinesis.stream." + input.StreamName + ".tags"),
		CreatedAt:           now,
		RetentionPeriod:     defaultRetentionHours,
		Consumers:           make(map[string]*Consumer),
		StreamMode:          streamMode,
		MaxRecordSizeBytes:  maxRecordSizeBytes,
		WarmThroughputMiBps: input.WarmThroughputMiBps,
	})

	return nil
}

// DeleteStream removes a stream, then flushes (best-effort) the buffer of
// any channel sourced from it, so records already accepted by PutRecord are
// not silently dropped by the removal (see channel_delivery.go).
func (b *InMemoryBackend) DeleteStream(ctx context.Context, input *DeleteStreamInput) error {
	region := getRegion(ctx, b.region)
	channelARNs := b.channelARNsForStream(region, input.StreamName)

	if err := b.deleteStreamLocked(ctx, input); err != nil {
		return err
	}

	for _, channelARN := range channelARNs {
		b.FlushChannel(ctx, channelARN)
	}

	return nil
}

// deleteStreamLocked marks a resolved, ACTIVE stream DELETING. Real AWS
// deletes asynchronously (StreamStatus DELETING until removal completes,
// still visible via DescribeStreamSummary) -- physical removal happens
// later, lazily, in finishStreamDeletionLocked via
// resolveStreamTransitionLocked, matching CreateStream's CREATING->ACTIVE
// pattern.
func (b *InMemoryBackend) deleteStreamLocked(ctx context.Context, input *DeleteStreamInput) error {
	region := getRegion(ctx, b.region)

	var found bool
	var opErr error

	func() {
		b.mu.Lock("DeleteStream")
		defer b.mu.Unlock()

		stream, err := b.resolveStreamTransitionLocked(region, input.StreamName)
		if err != nil {
			return
		}
		found = true

		stream.mu.Lock("DeleteStream.stream")
		defer stream.mu.Unlock()

		if stream.Status != streamStatusActive {
			opErr = ErrStreamNotActive

			return
		}

		if len(stream.Consumers) > 0 && !input.EnforceConsumerDeletion {
			opErr = ErrStreamHasConsumers

			return
		}

		stream.Status = streamStatusDeleting
		stream.ReadyAt = b.nowFunc().Add(streamTransitionDelay)
	}()

	if !found {
		return ErrStreamNotFound
	}

	return opErr
}

// DescribeStream returns full stream details including shards.
func (b *InMemoryBackend) DescribeStream(
	ctx context.Context,
	input *DescribeStreamInput,
) (*DescribeStreamOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DescribeStream")

	stream, err := b.resolveStreamTransitionLocked(region, input.StreamName)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}
	stream.mu.RLock("DescribeStream.stream")
	b.mu.Unlock()
	defer stream.mu.RUnlock()

	// AWS paginates the Shards list: default page size 100, max 10000, resumed
	// via ExclusiveStartShardId. A stream that has been resharded many times
	// accumulates CLOSED shards forever (they remain visible for lineage), so
	// long-lived, heavily-resharded streams can exceed a single page.
	const (
		defaultDescribeStreamShardLimit = 100
		maxDescribeStreamShardLimit     = 10000
	)

	limit := input.Limit
	if limit <= 0 {
		limit = defaultDescribeStreamShardLimit
	}
	if limit > maxDescribeStreamShardLimit {
		limit = maxDescribeStreamShardLimit
	}

	startIdx := 0
	if input.ExclusiveStartShardID != "" {
		for i, s := range stream.Shards {
			if s.ID == input.ExclusiveStartShardID {
				startIdx = i + 1

				break
			}
		}
	}

	all := stream.Shards[startIdx:]
	hasMore := len(all) > limit
	if hasMore {
		all = all[:limit]
	}

	shards := make([]ShardDescription, len(all))
	for i, s := range all {
		shards[i] = shardDescription(s)
	}

	encType := stream.EncryptionType
	if encType == "" {
		encType = encryptionTypeNone
	}

	return &DescribeStreamOutput{
		StreamName:              stream.Name,
		StreamARN:               stream.ARN,
		StreamStatus:            stream.Status,
		Shards:                  shards,
		HasMoreShards:           hasMore,
		RetentionPeriodHours:    stream.RetentionPeriod,
		EncryptionType:          encType,
		KeyID:                   stream.KeyID,
		EnhancedMonitoring:      append([]string{}, stream.EnhancedMonitoring...),
		StreamCreationTimestamp: stream.CreatedAt,
		StreamMode:              stream.StreamMode,
		MaxRecordSizeBytes:      stream.MaxRecordSizeBytes,
		WarmThroughputMiBps:     stream.WarmThroughputMiBps,
	}, nil
}

// ListStreams returns stream names with optional pagination.
//
// AWS contract: results are returned in alphabetical order. When `Limit` is
// set the response contains at most that many names. Pagination is keyed on
// either `ExclusiveStartStreamName` or the opaque `NextToken` (which we treat
// as the previously returned last stream name) so that callers can iterate
// over arbitrarily large account inventories.
func (b *InMemoryBackend) ListStreams(ctx context.Context, input *ListStreamsInput) (*ListStreamsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("ListStreams")
	defer b.mu.Unlock()

	regionStreams := b.resolveRegionStreamsLocked(region)

	// AWS returns streams in alphabetical order by name.
	sort.Slice(regionStreams, func(i, j int) bool { return regionStreams[i].Name < regionStreams[j].Name })

	// Apply pagination start point: prefer ExclusiveStartStreamName, then NextToken.
	start := input.ExclusiveStartStreamName
	if start == "" {
		start = input.NextToken
	}

	if start != "" {
		idx := sort.Search(len(regionStreams), func(i int) bool { return regionStreams[i].Name >= start })
		// Skip the matched name itself; Search returns the insertion point so
		// equal entries land at idx — advance past it.
		if idx < len(regionStreams) && regionStreams[idx].Name == start {
			idx++
		}

		regionStreams = regionStreams[idx:]
	}

	const (
		defaultListStreamsLimit = 100
		maxListStreamsLimit     = 100
	)

	limit := input.Limit
	if limit <= 0 || limit > maxListStreamsLimit {
		limit = defaultListStreamsLimit
	}

	if limit > len(regionStreams) {
		limit = len(regionStreams)
	}

	page := regionStreams[:limit]
	hasMore := len(regionStreams) > limit

	names := make([]string, len(page))
	summaries := make([]StreamSummary, len(page))
	for i, s := range page {
		names[i] = s.Name
		summaries[i] = StreamSummary{
			StreamName:              s.Name,
			StreamARN:               s.ARN,
			StreamStatus:            s.Status,
			StreamMode:              s.StreamMode,
			StreamCreationTimestamp: s.CreatedAt,
		}
	}

	var nextToken string
	if hasMore && len(page) > 0 {
		nextToken = page[len(page)-1].Name
	}

	return &ListStreamsOutput{
		StreamNames:     names,
		StreamSummaries: summaries,
		HasMoreStreams:  hasMore,
		NextToken:       nextToken,
	}, nil
}
