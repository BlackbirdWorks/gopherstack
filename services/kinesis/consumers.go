package kinesis

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// consumerNameRe validates Kinesis consumer names: same charset as stream names.
var consumerNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

// isValidConsumerName reports whether s is a valid Kinesis consumer name.
func isValidConsumerName(s string) bool {
	return consumerNameRe.MatchString(s)
}

// consumerInfoFromARN extracts the stream name and consumer name from a consumer ARN.
// Consumer ARN format: arn:aws:kinesis:{region}:{account}:stream/{stream}/consumer/{name}:{timestamp}.
func consumerInfoFromARN(consumerARN string) (string, string) {
	parts := strings.Split(consumerARN, ":")
	const arnConsumerResourceIdx = 5
	if len(parts) <= arnConsumerResourceIdx {
		return "", ""
	}

	resourcePath := parts[arnConsumerResourceIdx]
	segments := strings.Split(resourcePath, "/")
	// segments: ["stream", "{streamName}", "consumer", "{consumerName}"]
	const expectedSegments = 4
	if len(segments) < expectedSegments {
		return "", ""
	}

	return segments[1], segments[3]
}

// buildConsumerARN builds a Kinesis consumer ARN from stream ARN, consumer name, and creation timestamp.
func buildConsumerARN(streamARN, consumerName string, createdAt time.Time) string {
	return fmt.Sprintf("%s/consumer/%s:%d", streamARN, consumerName, createdAt.Unix())
}

// RegisterStreamConsumer registers a new enhanced fan-out consumer on a stream.
func (b *InMemoryBackend) RegisterStreamConsumer(
	ctx context.Context,
	input *RegisterStreamConsumerInput,
) (*RegisterStreamConsumerOutput, error) {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("RegisterStreamConsumer")

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streams.Get(streamKey(region, streamName))

	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("RegisterStreamConsumer.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if stream.Consumers == nil {
		stream.Consumers = make(map[string]*Consumer)
	}

	if !isValidConsumerName(input.ConsumerName) {
		return nil, ErrInvalidArgument
	}

	if _, exists := stream.Consumers[input.ConsumerName]; exists {
		return nil, ErrConsumerAlreadyExists
	}

	// AWS caps enhanced fan-out registrations at 20 consumers per stream.
	if len(stream.Consumers) >= maxConsumersPerStream {
		return nil, ErrLimitExceeded
	}

	now := time.Now()
	consumerARN := buildConsumerARN(input.StreamARN, input.ConsumerName, now)

	consumer := &Consumer{
		ConsumerName:              input.ConsumerName,
		ConsumerARN:               consumerARN,
		ConsumerStatus:            consumerStatusActive,
		ConsumerCreationTimestamp: now,
		StreamARN:                 input.StreamARN,
	}
	stream.Consumers[input.ConsumerName] = consumer

	return &RegisterStreamConsumerOutput{Consumer: *consumer}, nil
}

// DescribeStreamConsumer returns details about a registered consumer.
// Lookup is by ConsumerARN, or by StreamARN + ConsumerName.
func (b *InMemoryBackend) DescribeStreamConsumer(
	ctx context.Context,
	input *DescribeStreamConsumerInput,
) (*DescribeStreamConsumerOutput, error) {
	var sName string
	var cName string
	var arnForRegion string
	if input.ConsumerARN != "" {
		sName, cName = consumerInfoFromARN(input.ConsumerARN)
		arnForRegion = input.ConsumerARN
	} else {
		sName = streamNameFromARN(input.StreamARN)
		cName = input.ConsumerName
		arnForRegion = input.StreamARN
	}

	region := regionFromARNOrCtx(ctx, arnForRegion, b.region)

	b.mu.RLock("DescribeStreamConsumer")
	stream, ok := b.streams.Get(streamKey(region, sName))
	if !ok {
		b.mu.RUnlock()
		if input.ConsumerARN != "" {
			return nil, ErrConsumerNotFound
		}

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("DescribeStreamConsumer.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	consumer, ok := stream.Consumers[cName]
	if !ok {
		return nil, ErrConsumerNotFound
	}

	return &DescribeStreamConsumerOutput{ConsumerDescription: *consumer}, nil
}

// ListStreamConsumers lists all registered consumers for a stream.
func (b *InMemoryBackend) ListStreamConsumers(
	ctx context.Context,
	input *ListStreamConsumersInput,
) (*ListStreamConsumersOutput, error) {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("ListStreamConsumers")

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streams.Get(streamKey(region, streamName))

	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("ListStreamConsumers.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	consumers := make([]Consumer, 0, len(stream.Consumers))
	for _, c := range stream.Consumers {
		consumers = append(consumers, *c)
	}

	// Sort for deterministic ordering.
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerName < consumers[j].ConsumerName
	})

	// Apply NextToken as exclusive start consumer name.
	if input.NextToken != "" {
		start := 0
		for start < len(consumers) && consumers[start].ConsumerName <= input.NextToken {
			start++
		}
		consumers = consumers[start:]
	}

	// Apply MaxResults cap.
	var nextToken string
	if input.MaxResults > 0 && input.MaxResults < len(consumers) {
		nextToken = consumers[input.MaxResults-1].ConsumerName
		consumers = consumers[:input.MaxResults]
	}

	return &ListStreamConsumersOutput{Consumers: consumers, NextToken: nextToken}, nil
}

// DeregisterStreamConsumer removes a registered consumer from a stream.
func (b *InMemoryBackend) DeregisterStreamConsumer(ctx context.Context, input *DeregisterStreamConsumerInput) error {
	sName, cName := func() (string, string) {
		if input.ConsumerARN != "" {
			return consumerInfoFromARN(input.ConsumerARN)
		}

		return streamNameFromARN(input.StreamARN), input.ConsumerName
	}()

	arnForRegion := input.StreamARN
	if input.ConsumerARN != "" {
		arnForRegion = input.ConsumerARN
	}
	region := regionFromARNOrCtx(ctx, arnForRegion, b.region)

	b.mu.RLock("DeregisterStreamConsumer")
	stream, ok := b.streams.Get(streamKey(region, sName))
	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("DeregisterStreamConsumer.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if _, ok = stream.Consumers[cName]; !ok {
		return ErrConsumerNotFound
	}

	delete(stream.Consumers, cName)

	return nil
}

// SubscribeToShard delivers records from a shard to an enhanced fan-out consumer.
// For mock purposes this is a single-shot delivery of all available records.
func (b *InMemoryBackend) SubscribeToShard(
	ctx context.Context,
	input *SubscribeToShardInput,
) (*SubscribeToShardOutput, error) {
	sName, cName := consumerInfoFromARN(input.ConsumerARN)
	region := regionFromARNOrCtx(ctx, input.ConsumerARN, b.region)

	b.mu.RLock("SubscribeToShard")

	stream, ok := b.streams.Get(streamKey(region, sName))
	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("SubscribeToShard.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	if _, exists := stream.Consumers[cName]; !exists {
		return nil, ErrConsumerNotFound
	}

	shard := findShard(stream.Shards, input.ShardID)
	if shard == nil {
		return nil, ErrInvalidArgument
	}

	var startPos int

	switch input.StartingPosition.Type {
	case iteratorTypeTrimHorizon:
		startPos = 0
	case iteratorTypeLatest:
		startPos = shard.Records.len()
	case iteratorTypeAtSequenceNumber:
		startPos = findSequencePosition(&shard.Records, input.StartingPosition.SequenceNumber, false)
	case iteratorTypeAfterSequenceNumber:
		startPos = findSequencePosition(&shard.Records, input.StartingPosition.SequenceNumber, true)
	case iteratorTypeAtTimestamp:
		// Timestamp is required for AT_TIMESTAMP; a genuinely omitted value
		// (nil) is rejected rather than silently treated as position 0,
		// mirroring GetShardIterator (see shard_iterators.go).
		if input.StartingPosition.Timestamp == nil {
			return nil, ErrInvalidArgument
		}

		startPos = findTimestampPosition(&shard.Records, *input.StartingPosition.Timestamp)
	default:
		return nil, ErrInvalidArgument
	}

	n := shard.Records.len()
	records := make([]GetRecordResult, 0, n-startPos)

	for i := startPos; i < n; i++ {
		r := shard.Records.at(i)
		records = append(records, GetRecordResult{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
		})
	}

	var continuationSeq string
	if len(records) > 0 {
		continuationSeq = records[len(records)-1].SequenceNumber
	}

	millisBehind := int64(0)
	if n > 0 && startPos < n {
		millisBehind = time.Since(shard.Records.last().ApproximateArrivalTimestamp).Milliseconds()
	}

	return &SubscribeToShardOutput{
		Event: SubscribeToShardEvent{
			Records:                    records,
			ContinuationSequenceNumber: continuationSeq,
			MillisBehindLatest:         millisBehind,
		},
	}, nil
}
