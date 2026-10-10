package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// retiredStreamRetention is how long a disabled stream stays readable.
const retiredStreamRetention = 24 * time.Hour

// RetiredStream is a disabled stream kept readable (DISABLING then DISABLED)
// until the janitor expires it. Its slices are immutable once created.
type RetiredStream struct {
	CreatedAt  time.Time             `json:"CreatedAt"`
	DisabledAt time.Time             `json:"DisabledAt"`
	ARN        string                `json:"ARN"`
	ViewType   string                `json:"ViewType,omitempty"`
	Shards     []StreamShard         `json:"Shards,omitempty"`
	Records    []models.StreamRecord `json:"Records,omitempty"`
}

// SetStreamDisablingDelay sets how long a just-disabled stream reports
// DISABLING before DISABLED. Default 0.
func (db *InMemoryDB) SetStreamDisablingDelay(d time.Duration) {
	db.mu.Lock("SetStreamDisablingDelay")
	defer db.mu.Unlock()

	db.streamDisablingDelay = d
}

func (db *InMemoryDB) streamDisablingDelayRLocked() time.Duration {
	db.mu.RLock("streamDisablingDelay")
	defer db.mu.RUnlock()

	return db.streamDisablingDelay
}

// retireStreamLocked moves the live stream's shards and records into
// RetiredStreams with every open shard closed. Caller holds table.mu.
func (t *Table) retireStreamLocked(now time.Time) {
	if t.StreamARN == "" {
		return
	}

	tail, head := t.streamRecordsInOrder()
	records := make([]models.StreamRecord, 0, len(tail)+len(head))
	records = append(append(records, tail...), head...)

	shards := make([]StreamShard, len(t.StreamShards))
	copy(shards, t.StreamShards)

	for i := range shards {
		if shards[i].EndingSequenceNum == 0 {
			shards[i].EndingSequenceNum = max(t.streamSeq, shards[i].StartingSequenceNum, 1)
		}
	}

	t.RetiredStreams = append(t.RetiredStreams, RetiredStream{
		ARN:        t.StreamARN,
		ViewType:   t.StreamViewType,
		CreatedAt:  t.StreamCreatedAt,
		DisabledAt: now,
		Shards:     shards,
		Records:    records,
	})
}

func (rs *RetiredStream) status(now time.Time, delay time.Duration) streamstypes.StreamStatus {
	if now.Before(rs.DisabledAt.Add(delay)) {
		return streamstypes.StreamStatusDisabling
	}

	return streamstypes.StreamStatusDisabled
}

// findRetiredStream looks a retired stream up by ARN across all tables.
func (db *InMemoryDB) findRetiredStream(streamARN string) (*Table, RetiredStream, bool) {
	db.mu.RLock("findRetiredStream")
	tables := db.tables.All()
	db.mu.RUnlock()

	for _, t := range tables {
		if rs, ok := retiredStreamOf(t, streamARN); ok {
			return t, rs, true
		}
	}

	return nil, RetiredStream{}, false
}

func retiredStreamOf(t *Table, streamARN string) (RetiredStream, bool) {
	t.mu.RLock("retiredStreamOf")
	defer t.mu.RUnlock()

	for i := range t.RetiredStreams {
		if t.RetiredStreams[i].ARN == streamARN {
			return t.RetiredStreams[i], true
		}
	}

	return RetiredStream{}, false
}

func (db *InMemoryDB) describeRetiredStream(
	table *Table,
	rs RetiredStream,
	input *dynamodbstreams.DescribeStreamInput,
) (*dynamodbstreams.DescribeStreamOutput, error) {
	limit := maxDescribeShards
	if input.Limit != nil && *input.Limit > 0 && int(*input.Limit) < limit {
		limit = int(*input.Limit)
	}

	childParent, err := parseShardFilter(input.ShardFilter)
	if err != nil {
		return nil, err
	}

	shards, lastEvaluated := pageShards(rs.Shards, aws.ToString(input.ExclusiveStartShardId), limit, childParent)

	table.mu.RLock("DescribeStream.retired")
	name, keySchema := table.Name, table.KeySchema
	table.mu.RUnlock()

	sdkKeySchema := make([]streamstypes.KeySchemaElement, 0, len(keySchema))
	for _, ks := range keySchema {
		sdkKeySchema = append(sdkKeySchema, streamstypes.KeySchemaElement{
			AttributeName: aws.String(ks.AttributeName),
			KeyType:       streamstypes.KeyType(ks.KeyType),
		})
	}

	desc := &streamstypes.StreamDescription{
		StreamArn:            aws.String(rs.ARN),
		StreamLabel:          aws.String(streamLabelFromARN(rs.ARN)),
		StreamStatus:         rs.status(time.Now(), db.streamDisablingDelayRLocked()),
		StreamViewType:       streamstypes.StreamViewType(rs.ViewType),
		TableName:            aws.String(name),
		KeySchema:            sdkKeySchema,
		LastEvaluatedShardId: lastEvaluated,
		Shards:               buildSDKShardsList(shards, false),
	}
	if !rs.CreatedAt.IsZero() {
		desc.CreationRequestDateTime = aws.Time(rs.CreatedAt)
	}

	return &dynamodbstreams.DescribeStreamOutput{StreamDescription: desc}, nil
}

func (db *InMemoryDB) getShardIteratorRetired(
	table *Table,
	rs RetiredStream,
	input *dynamodbstreams.GetShardIteratorInput,
) (*dynamodbstreams.GetShardIteratorOutput, error) {
	var shard *StreamShard

	var lastSeq int64

	for i := range rs.Shards {
		lastSeq = max(lastSeq, rs.Shards[i].EndingSequenceNum)

		if rs.Shards[i].ShardID == aws.ToString(input.ShardId) {
			shard = &rs.Shards[i]
		}
	}

	if shard == nil {
		return nil, NewResourceNotFoundException(
			"Shard " + aws.ToString(input.ShardId) + " does not exist in stream " + rs.ARN,
		)
	}

	startSeq, err := resolveStartSeq(input, lastSeq, 0, shard.StartingSequenceNum, shard.EndingSequenceNum)
	if err != nil {
		return nil, err
	}

	token, err := db.iteratorStore.PutForStream(
		table.Name, streamARNRegion(rs.ARN), rs.ARN, startSeq, shard.EndingSequenceNum,
	)
	if err != nil {
		return nil, err
	}

	return &dynamodbstreams.GetShardIteratorOutput{ShardIterator: aws.String(token)}, nil
}

func (db *InMemoryDB) getRecordsFromRetired(
	ctx context.Context,
	table *Table,
	entry *ShardIteratorEntry,
	limit int64,
) (*dynamodbstreams.GetRecordsOutput, error) {
	rs, ok := retiredStreamOf(table, entry.StreamARN)
	if !ok {
		return nil, NewResourceNotFoundException("stream not found: " + entry.StreamARN)
	}

	out, err := db.getRecordsRetired(ctx, rs, entry, limit)
	if err == nil {
		telemetry.RecordStreamEvents("dynamodb", len(out.Records))
	}

	return out, err
}

func (db *InMemoryDB) getRecordsRetired(
	ctx context.Context,
	rs RetiredStream,
	entry *ShardIteratorEntry,
	limit int64,
) (*dynamodbstreams.GetRecordsOutput, error) {
	region := getRegionFromContext(ctx, db)
	records, nextSeq := collectStreamRecords(rs.Records, nil, entry.StartSeq, limit, entry.StartSeq, region)

	if int64(len(records)) < limit {
		return &dynamodbstreams.GetRecordsOutput{Records: records}, nil
	}

	token, err := db.iteratorStore.PutForStream(entry.TableName, entry.Region, rs.ARN, nextSeq, entry.EndSeq)
	if err != nil {
		return nil, err
	}

	return &dynamodbstreams.GetRecordsOutput{Records: records, NextShardIterator: aws.String(token)}, nil
}

// sweepRetiredStreams drops retired streams past the retention window.
func sweepRetiredStreams(t *Table, now time.Time) int {
	t.mu.Lock("sweepRetiredStreams")
	defer t.mu.Unlock()

	if len(t.RetiredStreams) == 0 {
		return 0
	}

	var kept []RetiredStream

	for _, rs := range t.RetiredStreams {
		if now.Sub(rs.DisabledAt) < retiredStreamRetention {
			kept = append(kept, rs)
		}
	}

	removed := len(t.RetiredStreams) - len(kept)
	t.RetiredStreams = kept

	return removed
}

// collectRetiredStreams lists retired streams visible in requestRegion.
func (db *InMemoryDB) collectRetiredStreams(requestRegion, filterTable string) []streamListEntry {
	db.mu.RLock("ListStreams.retired")
	tables := db.tables.All()
	db.mu.RUnlock()

	var out []streamListEntry

	for _, t := range tables {
		t.mu.RLock("ListStreams.retired")
		name := t.Name

		if filterTable == "" || filterTable == name {
			for i := range t.RetiredStreams {
				arnStr := t.RetiredStreams[i].ARN
				if r := streamARNRegion(arnStr); r == "" || r == requestRegion {
					out = append(out, streamListEntry{tableName: name, arn: arnStr})
				}
			}
		}

		t.mu.RUnlock()
	}

	return out
}

// nextStreamCreatedAt keeps a re-enabled stream's ms-precision label distinct
// from every retired generation. Caller holds table.mu.
func (t *Table) nextStreamCreatedAt(now time.Time) time.Time {
	now = now.UTC()

	for _, rs := range t.RetiredStreams {
		if last := rs.CreatedAt.Truncate(time.Millisecond); !now.Truncate(time.Millisecond).After(last) {
			now = last.Add(time.Millisecond)
		}
	}

	return now
}
