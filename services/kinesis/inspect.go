package kinesis

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrShardNotFound is returned by the local inspection API for an unknown shard ID.
var ErrShardNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

const (
	defaultInspectRecordsLimit = 100
	maxInspectRecordsLimit     = 1000
)

// InspectedShard is the read-only inspection view of one shard.
type InspectedShard struct {
	ShardID                  string
	ParentShardID            string
	AdjacentParentShardID    string
	HashKeyRangeStart        string
	HashKeyRangeEnd          string
	SequenceNumberRangeStart string
	SequenceNumberRangeEnd   string
	ChildShardIDs            []string
	RecordCount              int
	Closed                   bool
}

// InspectedStream is the read-only inspection view of a stream's shard layout.
type InspectedStream struct {
	StreamName string
	StreamARN  string
	Status     string
	Shards     []InspectedShard
}

// InspectedRecords is a page of records peeked from a shard.
type InspectedRecords struct {
	ShardID string
	Records []Record
	Closed  bool
}

// InspectStreamShards returns every shard (open and closed) of a stream.
func (b *InMemoryBackend) InspectStreamShards(ctx context.Context, name string) (*InspectedStream, error) {
	stream, unlock, err := b.lockStreamForInspect(ctx, name)
	if err != nil {
		return nil, err
	}
	defer unlock()

	shards := make([]InspectedShard, 0, len(stream.Shards))
	for _, s := range stream.Shards {
		d := shardDescription(s)
		shards = append(shards, InspectedShard{
			ShardID:                  s.ID,
			ParentShardID:            s.ParentShardID,
			AdjacentParentShardID:    s.AdjacentParentShardID,
			HashKeyRangeStart:        s.HashKeyRangeStart,
			HashKeyRangeEnd:          s.HashKeyRangeEnd,
			SequenceNumberRangeStart: d.SequenceNumberRangeStart,
			SequenceNumberRangeEnd:   d.SequenceNumberRangeEnd,
			ChildShardIDs:            childShardIDs(stream.Shards, s.ID),
			RecordCount:              s.Records.len(),
			Closed:                   s.Closed,
		})
	}

	return &InspectedStream{
		StreamName: stream.Name,
		StreamARN:  stream.ARN,
		Status:     effectiveStreamStatus(stream, b.nowFunc()),
		Shards:     shards,
	}, nil
}

func childShardIDs(shards []*Shard, id string) []string {
	children := []string{}

	for _, s := range shards {
		if s.ParentShardID == id || s.AdjacentParentShardID == id {
			children = append(children, s.ID)
		}
	}

	return children
}

// InspectShardRecords peeks at a shard's retained records starting at the
// first sequence number >= fromSeq (or the oldest retained record when
// fromSeq is empty) without a shard iterator.
func (b *InMemoryBackend) InspectShardRecords(
	ctx context.Context,
	name, shardID, fromSeq string,
	limit int,
) (*InspectedRecords, error) {
	stream, unlock, err := b.lockStreamForInspect(ctx, name)
	if err != nil {
		return nil, err
	}
	defer unlock()

	shard := findShard(stream.Shards, shardID)
	if shard == nil {
		return nil, ErrShardNotFound
	}

	if limit <= 0 {
		limit = defaultInspectRecordsLimit
	}

	limit = min(limit, maxInspectRecordsLimit)

	pos := findTimestampPosition(&shard.Records, retentionCutoff(stream, b.nowFunc()))
	if fromSeq != "" {
		pos = max(pos, findSequencePosition(&shard.Records, fromSeq, false))
	}

	end := min(shard.Records.len(), pos+limit)
	records := make([]Record, 0, max(0, end-pos))

	for i := pos; i < end; i++ {
		rec := *shard.Records.at(i)
		rec.Data = append([]byte(nil), rec.Data...)
		records = append(records, rec)
	}

	return &InspectedRecords{ShardID: shard.ID, Records: records, Closed: shard.Closed}, nil
}

// InspectConsumer returns the registered consumer for a consumer ARN.
func (b *InMemoryBackend) InspectConsumer(ctx context.Context, consumerARN string) (*Consumer, error) {
	sName, cName := consumerInfoFromARN(consumerARN)
	if sName == "" || cName == "" {
		return nil, ErrConsumerNotFound
	}

	region := regionFromARNOrCtx(ctx, consumerARN, b.region)

	b.mu.RLock("InspectConsumer")
	defer b.mu.RUnlock()

	stream, ok := b.streams.Get(streamKey(region, sName))
	if !ok {
		return nil, ErrConsumerNotFound
	}

	stream.mu.RLock("InspectConsumer.stream")
	defer stream.mu.RUnlock()

	c, ok := stream.Consumers[cName]
	if !ok || c.ConsumerARN != consumerARN {
		return nil, ErrConsumerNotFound
	}

	cp := *c

	return &cp, nil
}

func (b *InMemoryBackend) lockStreamForInspect(ctx context.Context, name string) (*Stream, func(), error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("Inspect")

	stream, ok := b.streams.Get(streamKey(region, name))
	if !ok {
		b.mu.RUnlock()

		return nil, nil, ErrStreamNotFound
	}

	stream.mu.RLock("Inspect.stream")
	b.mu.RUnlock()

	if streamEffectivelyGone(stream, b.nowFunc()) {
		stream.mu.RUnlock()

		return nil, nil, ErrStreamNotFound
	}

	return stream, stream.mu.RUnlock, nil
}
