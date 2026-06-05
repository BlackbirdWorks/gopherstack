package firehose

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const (
	kinesisPollerInterval   = time.Second
	kinesisPollerBatchLimit = 100
)

// launchKinesisPoller starts one goroutine per shard for the given Kinesis source stream.
// It stores the cancel func in b.pollerCancel[streamName] so DeleteDeliveryStream can stop it.
func (b *InMemoryBackend) launchKinesisPoller(firehoseStream, kinesisStreamARN string) {
	ctx, cancel := context.WithCancel(b.svcCtx)

	b.mu.Lock("launchKinesisPoller")
	b.pollerCancel[firehoseStream] = cancel
	b.mu.Unlock()

	go b.pollKinesisStream(ctx, firehoseStream, kinesisStreamARN)
}

// pollKinesisStream lists shards and starts a per-shard polling loop.
func (b *InMemoryBackend) pollKinesisStream(
	ctx context.Context,
	firehoseStream, kinesisStreamARN string,
) {
	streamName := kinesisStreamNameFromARN(kinesisStreamARN)
	if streamName == "" {
		streamName = kinesisStreamARN
	}

	shards, err := b.kinesisBackend.ListShards(streamName)
	if err != nil {
		slog.WarnContext(ctx, "firehose kinesis poller: ListShards failed",
			"stream", firehoseStream, "kinesis", streamName, "error", err)

		return
	}

	for _, shardID := range shards {
		sid := shardID
		go b.pollKinesisShard(ctx, firehoseStream, streamName, sid)
	}
}

// pollKinesisShard reads records from a single Kinesis shard and injects them into the Firehose stream.
func (b *InMemoryBackend) pollKinesisShard(
	ctx context.Context,
	firehoseStream, kinesisStream, shardID string,
) {
	iter, err := b.kinesisBackend.GetShardIterator(kinesisStream, shardID)
	if err != nil {
		slog.WarnContext(ctx, "firehose kinesis poller: GetShardIterator failed",
			"stream", firehoseStream, "shard", shardID, "error", err)

		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		records, nextIter, err := b.kinesisBackend.GetRecords(iter, kinesisPollerBatchLimit)
		if err != nil {
			slog.WarnContext(ctx, "firehose kinesis poller: GetRecords failed",
				"stream", firehoseStream, "shard", shardID, "error", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(kinesisPollerInterval):
			}

			continue
		}

		for _, rec := range records {
			if err := b.injectKinesisRecord(firehoseStream, rec); err != nil {
				slog.WarnContext(ctx, "firehose kinesis poller: inject failed",
					"stream", firehoseStream, "error", err)
			}
		}

		if nextIter == "" {
			// Shard is closed and exhausted.
			return
		}

		iter = nextIter

		if len(records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(kinesisPollerInterval):
			}
		}
	}
}

// injectKinesisRecord appends a record to a KinesisStreamAsSource stream, bypassing the
// DirectPut type guard that PutRecord enforces.
func (b *InMemoryBackend) injectKinesisRecord(streamName string, data []byte) error {
	b.mu.Lock("injectKinesisRecord")
	defer b.mu.Unlock()

	s, ok := b.streams[streamName]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	s.Records = append(s.Records, data)
	s.Metrics.TotalRecords++
	s.Metrics.TotalBytes += int64(len(data))

	return nil
}

// kinesisStreamNameFromARN extracts the stream name from a Kinesis ARN.
// ARN format: arn:aws:kinesis:<region>:<account>:stream/<name>
func kinesisStreamNameFromARN(kinesisARN string) string {
	if !strings.Contains(kinesisARN, ":stream/") {
		return ""
	}

	parts := strings.SplitN(kinesisARN, ":stream/", 2)
	if len(parts) == 2 {
		return parts[1]
	}

	return ""
}
