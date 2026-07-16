package dynamodb

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// Sentinel errors for streams operations.
var (
	ErrInvalidAttributeValue = errors.New("expected map[string]any for attribute value")
	ErrInvalidTypeKeyCount   = errors.New("expected exactly 1 type key")
	ErrTypeMismatchS         = errors.New("expected string for S")
	ErrTypeMismatchN         = errors.New("expected string for N")
	ErrTypeMismatchBOOL      = errors.New("expected bool for BOOL")
	ErrTypeMismatchM         = errors.New("expected map for M")
	ErrTypeMismatchL         = errors.New("expected slice for L")
	ErrTypeMismatchB         = errors.New("expected []byte or base64 string for B")
	ErrUnknownAttributeType  = errors.New("unknown attribute type")
	ErrEmptySequenceNumber   = errors.New("empty sequence number")
)

const (
	streamShardID       = "shardId-00000000000000000001-00000001"
	maxRecords          = 1000
	iteratorPartCount   = 3 // tableName:sequenceNumber:timestamp (legacy; opaque tokens now used)
	maxListStreamsLimit = 100
	maxDescribeShards   = 100
	seqNumWidth         = 20 // zero-padded width for sequence numbers
)

// StreamShard models one DynamoDB Streams shard with its sequence range and genealogy.
type StreamShard struct {
	// ShardID is the unique identifier for this shard.
	ShardID string
	// ParentShardID is the ID of the parent shard (empty for the first shard of a stream).
	ParentShardID string
	// StartingSequenceNum is the first sequence number in this shard (inclusive).
	StartingSequenceNum int64
	// EndingSequenceNum is the last sequence number in this shard (0 = still open).
	EndingSequenceNum int64
}

// StreamsBackend defines the interface for DynamoDB Streams operations.
type StreamsBackend interface {
	EnableStream(ctx context.Context, tableName, viewType string) error
	DisableStream(ctx context.Context, tableName string) error
	DescribeStream(
		ctx context.Context,
		input *dynamodbstreams.DescribeStreamInput,
	) (*dynamodbstreams.DescribeStreamOutput, error)
	GetShardIterator(
		ctx context.Context,
		input *dynamodbstreams.GetShardIteratorInput,
	) (*dynamodbstreams.GetShardIteratorOutput, error)
	GetRecords(
		ctx context.Context,
		input *dynamodbstreams.GetRecordsInput,
	) (*dynamodbstreams.GetRecordsOutput, error)
	ListStreams(
		ctx context.Context,
		input *dynamodbstreams.ListStreamsInput,
	) (*dynamodbstreams.ListStreamsOutput, error)
	GetRecentEvents(tableName string) []models.StreamRecord
}

// EnableStream enables DynamoDB Streams on a table with the given view type.
func (db *InMemoryDB) EnableStream(ctx context.Context, tableName, viewType string) error {
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return err
	}

	if viewType == "" {
		viewType = streamViewTypeNewAndOldImages
	}

	region := getRegionFromContext(ctx, db)

	now := time.Now().UTC()

	db.enableStreamFieldsLocked(table, tableName, region, viewType, now)

	// Update the reverse index under db.mu (after releasing table lock to preserve lock ordering).
	db.mu.Lock("EnableStream.streamARNIndex")
	defer db.mu.Unlock()
	db.streamARNIndex.Put(table)

	return nil
}

// enableStreamFieldsLocked applies the EnableStream field mutations under a
// defer-protected table.mu.Lock, so a panic partway through (e.g. from
// buildStreamARNInRegion) can never leave table.mu permanently locked.
func (db *InMemoryDB) enableStreamFieldsLocked(
	table *Table,
	tableName, region, viewType string,
	now time.Time,
) {
	table.mu.Lock("EnableStream")
	defer table.mu.Unlock()

	table.StreamsEnabled = true
	table.StreamViewType = viewType
	table.StreamCreatedAt = now
	table.StreamARN = db.buildStreamARNInRegion(tableName, region, now)
	// Initialize the first shard when enabling streams (clearing any prior shard history).
	table.streamShards = []StreamShard{
		{
			ShardID:             streamShardID,
			StartingSequenceNum: table.streamSeq + 1,
		},
	}
}

// DisableStream disables DynamoDB Streams on a table.
func (db *InMemoryDB) DisableStream(ctx context.Context, tableName string) error {
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return err
	}

	oldARN := disableStreamFieldsLocked(table)

	// Remove from reverse index under db.mu (after releasing table lock to preserve lock ordering).
	if oldARN != "" {
		db.mu.Lock("DisableStream.streamARNIndex")
		defer db.mu.Unlock()
		db.streamARNIndex.Delete(oldARN)
	}

	return nil
}

// disableStreamFieldsLocked applies the DisableStream field resets under a
// defer-protected table.mu.Lock and returns the stream ARN that was in effect
// before the reset (empty if streams were not enabled).
func disableStreamFieldsLocked(table *Table) string {
	table.mu.Lock("DisableStream")
	defer table.mu.Unlock()

	oldARN := table.StreamARN
	table.StreamsEnabled = false
	table.StreamARN = ""
	table.StreamViewType = ""
	table.StreamRecords = nil
	table.streamSeq = 0
	table.StreamHead = 0
	table.streamTrimSeq = 0
	table.streamShards = nil

	return oldARN
}

// DescribeStream returns details about a stream (identified by its ARN).
// Supports ExclusiveStartShardId pagination.
func (db *InMemoryDB) DescribeStream(
	_ context.Context,
	input *dynamodbstreams.DescribeStreamInput,
) (*dynamodbstreams.DescribeStreamOutput, error) {
	if aws.ToString(input.StreamArn) == "" {
		return nil, NewValidationException("StreamArn is required")
	}

	streamARN := aws.ToString(input.StreamArn)

	found := db.findTableByStreamARNRLocked(streamARN)
	if found == nil {
		return nil, NewResourceNotFoundException(
			"stream not found: " + streamARN,
		)
	}

	exclusiveStart := aws.ToString(input.ExclusiveStartShardId)

	limit := maxDescribeShards
	if input.Limit != nil && *input.Limit > 0 && int(*input.Limit) < limit {
		limit = int(*input.Limit)
	}

	tableName, viewType, keySchema, streamCreatedAt, shards, lastEvaluatedShardID :=
		describeStreamSnapshot(found, exclusiveStart, limit)

	sdkKeySchema := make([]streamstypes.KeySchemaElement, 0, len(keySchema))
	for _, ks := range keySchema {
		sdkKeySchema = append(sdkKeySchema, streamstypes.KeySchemaElement{
			AttributeName: aws.String(ks.AttributeName),
			KeyType:       streamstypes.KeyType(ks.KeyType),
		})
	}

	// Build the shard list. If no shards exist yet (stream just enabled but no records),
	// return a single open shard with empty sequence numbers.
	sdkShards := buildSDKShards(shards)

	var creationRequestDateTime *time.Time
	if !streamCreatedAt.IsZero() {
		t := streamCreatedAt
		creationRequestDateTime = &t
	}

	return &dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &streamstypes.StreamDescription{
			StreamArn:               aws.String(streamARN),
			StreamLabel:             aws.String(streamLabelFromARN(streamARN)),
			StreamStatus:            streamstypes.StreamStatusEnabled,
			StreamViewType:          streamstypes.StreamViewType(viewType),
			TableName:               aws.String(tableName),
			KeySchema:               sdkKeySchema,
			CreationRequestDateTime: creationRequestDateTime,
			LastEvaluatedShardId:    lastEvaluatedShardID,
			Shards:                  sdkShards,
		},
	}, nil
}

// streamLabelFromARN extracts the stream label from a DynamoDB stream ARN.
// The label is the last path segment after /stream/: e.g. "2024-01-01T00:00:00.000".
func streamLabelFromARN(streamARN string) string {
	const sep = "/stream/"
	if idx := strings.LastIndex(streamARN, sep); idx >= 0 {
		return streamARN[idx+len(sep):]
	}

	return streamARN
}

// buildSDKShards converts internal StreamShard slice to SDK Shard slice.
// When the slice is empty, returns a single placeholder shard so callers
// can always obtain an iterator even before any records are written.
func buildSDKShards(shards []StreamShard) []streamstypes.Shard {
	if len(shards) == 0 {
		return []streamstypes.Shard{
			{
				ShardId: aws.String(streamShardID),
				SequenceNumberRange: &streamstypes.SequenceNumberRange{
					StartingSequenceNumber: aws.String(""),
				},
			},
		}
	}

	out := make([]streamstypes.Shard, 0, len(shards))
	for _, s := range shards {
		shard := streamstypes.Shard{
			ShardId: aws.String(s.ShardID),
			SequenceNumberRange: &streamstypes.SequenceNumberRange{
				StartingSequenceNumber: aws.String(
					seqNumString(s.StartingSequenceNum),
				),
			},
		}
		if s.ParentShardID != "" {
			shard.ParentShardId = aws.String(s.ParentShardID)
		}
		if s.EndingSequenceNum > 0 {
			shard.SequenceNumberRange.EndingSequenceNumber = aws.String(
				seqNumString(s.EndingSequenceNum),
			)
		}
		out = append(out, shard)
	}

	return out
}

// seqNumString formats a sequence number as a zero-padded string.
func seqNumString(seq int64) string {
	if seq <= 0 {
		return ""
	}

	return fmt.Sprintf("%0*d", seqNumWidth, seq)
}

// parseSeqNum parses a zero-padded sequence number string to int64.
func parseSeqNum(s string) (int64, error) {
	if s == "" {
		return 0, ErrEmptySequenceNumber
	}

	return strconv.ParseInt(strings.TrimLeft(s, "0"), 10, 64)
}

// GetShardIterator returns an opaque shard iterator for reading stream records.
// The iterator is a random token stored in the server-side ShardIteratorStore,
// preventing clients from decoding or forging iterator state.
func (db *InMemoryDB) GetShardIterator(
	_ context.Context,
	input *dynamodbstreams.GetShardIteratorInput,
) (*dynamodbstreams.GetShardIteratorOutput, error) {
	if aws.ToString(input.StreamArn) == "" {
		return nil, NewValidationException("StreamArn is required")
	}
	if aws.ToString(input.ShardId) == "" {
		return nil, NewValidationException("ShardId is required")
	}

	streamARN := aws.ToString(input.StreamArn)
	requestedShardID := aws.ToString(input.ShardId)

	found := db.findTableByStreamARNRLocked(streamARN)
	if found == nil {
		return nil, NewResourceNotFoundException(
			"stream not found: " + streamARN,
		)
	}

	currentSeq, trimSeq, shardStartSeq, shardEndSeq, foundShard := shardSeqRangeRLocked(found, requestedShardID)
	if !foundShard {
		return nil, NewResourceNotFoundException(
			"Shard " + requestedShardID + " does not exist in stream " + streamARN,
		)
	}

	// Determine start sequence from iterator type.
	startSeq, seqErr := resolveStartSeq(input, currentSeq, trimSeq, shardStartSeq, shardEndSeq)
	if seqErr != nil {
		return nil, seqErr
	}

	// Carry the shard's ending sequence (0 for an open shard) so GetRecords can
	// return a nil NextShardIterator once a closed shard is fully drained.
	token, err := db.iteratorStore.PutWithEnd(found.Name, startSeq, shardEndSeq)
	if err != nil {
		return nil, fmt.Errorf("create shard iterator: %w", err)
	}

	return &dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String(token),
	}, nil
}

// shardSeqRangeRLocked returns the stream's current/trim sequence numbers and
// the named shard's start/end sequence range under a defer-protected
// found.mu.RLock, so a panic while scanning found.streamShards can never leave
// table.mu read-locked forever.
func shardSeqRangeRLocked(found *Table, requestedShardID string) (int64, int64, int64, int64, bool) {
	found.mu.RLock("GetShardIterator")
	defer found.mu.RUnlock()

	currentSeq := found.streamSeq
	trimSeq := found.streamTrimSeq

	var shardStartSeq, shardEndSeq int64

	var foundShard bool

	for _, s := range found.streamShards {
		if s.ShardID == requestedShardID {
			shardStartSeq = s.StartingSequenceNum
			shardEndSeq = s.EndingSequenceNum
			foundShard = true

			break
		}
	}

	return currentSeq, trimSeq, shardStartSeq, shardEndSeq, foundShard
}

// resolveStartSeq resolves the starting sequence number for a new shard iterator
// based on the requested iterator type and stream state.
func resolveStartSeq(
	input *dynamodbstreams.GetShardIteratorInput,
	currentSeq, trimSeq, shardStartSeq, shardEndSeq int64,
) (int64, error) {
	var startSeq int64

	switch input.ShardIteratorType {
	case streamstypes.ShardIteratorTypeLatest:
		startSeq = currentSeq + 1
	case streamstypes.ShardIteratorTypeAtSequenceNumber,
		streamstypes.ShardIteratorTypeAfterSequenceNumber:
		var err error
		startSeq, err = resolveExplicitStartSeq(input, trimSeq, shardStartSeq, shardEndSeq)
		if err != nil {
			return 0, err
		}
	case streamstypes.ShardIteratorTypeTrimHorizon:
		startSeq = shardStartSeq
		if startSeq == 0 {
			startSeq = 1
		}

		if trimSeq > startSeq {
			startSeq = trimSeq
		}
	default:
		return 0, NewValidationException("Invalid ShardIteratorType: " + string(input.ShardIteratorType))
	}

	// For closed shards, clamp startSeq beyond the shard's end so GetRecords returns nothing.
	if shardEndSeq > 0 && startSeq > shardEndSeq {
		startSeq = shardEndSeq + 1
	}

	return startSeq, nil
}

func resolveExplicitStartSeq(
	input *dynamodbstreams.GetShardIteratorInput,
	trimSeq, shardStartSeq, shardEndSeq int64,
) (int64, error) {
	seqStr := aws.ToString(input.SequenceNumber)
	if seqStr == "" {
		return 0, NewValidationException(
			"SequenceNumber is required for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER iterator types",
		)
	}

	seq, err := parseSeqNum(seqStr)
	if err != nil {
		return 0, NewValidationException("Invalid SequenceNumber: " + seqStr)
	}

	if trimSeq > 0 && seq < trimSeq {
		return 0, NewTrimmedDataAccessException(
			fmt.Sprintf("Sequence number %s has been trimmed; earliest available is %s",
				seqStr, seqNumString(trimSeq)),
		)
	}
	if seq < shardStartSeq || (shardEndSeq > 0 && seq > shardEndSeq) {
		return 0, NewValidationException("SequenceNumber is outside the bounds of the shard")
	}

	if input.ShardIteratorType == streamstypes.ShardIteratorTypeAfterSequenceNumber {
		return seq + 1, nil
	}

	return seq, nil
}

// GetRecords reads stream records starting from the given opaque shard iterator.
func (db *InMemoryDB) GetRecords(
	ctx context.Context,
	input *dynamodbstreams.GetRecordsInput,
) (*dynamodbstreams.GetRecordsOutput, error) {
	token := aws.ToString(input.ShardIterator)
	if token == "" {
		return nil, NewValidationException("ShardIterator is required")
	}

	// Resolve the opaque token. Falls back to legacy "tableName:seq:ts" format
	// for backward compatibility with tests that construct iterators directly.
	tableName, startSeq, endSeq, err := db.resolveIterator(token)
	if err != nil {
		return nil, err
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	limit := int64(maxRecords)
	if input.Limit != nil && *input.Limit > 0 && int64(*input.Limit) < limit {
		limit = int64(*input.Limit)
	}

	trimSeq, currentSeq, tail, head := streamRecordsSnapshotRLocked(table)

	// If the requested start is before the trim horizon, the data has been evicted.
	if trimSeq > 0 && startSeq < trimSeq {
		return nil, NewTrimmedDataAccessException(
			fmt.Sprintf("Sequence number %s has been trimmed; earliest available is %s",
				seqNumString(startSeq), seqNumString(trimSeq)),
		)
	}

	region := getRegionFromContext(ctx, db)
	records, nextSeq := collectStreamRecords(tail, head, startSeq, limit, currentSeq, region)

	telemetry.RecordStreamEvents("dynamodb", len(records))

	// A closed (split) shard that has been fully drained returns a nil
	// NextShardIterator so consumers know to advance to the child shard. AWS
	// signals end-of-shard this way; KCL-style consumers depend on it.
	if endSeq > 0 && nextSeq > endSeq {
		return &dynamodbstreams.GetRecordsOutput{
			Records:           records,
			NextShardIterator: nil,
		}, nil
	}

	// Generate the next opaque iterator for continued reading, preserving the
	// owning shard's end sequence so the terminal state above is reachable.
	nextToken, tokenErr := db.iteratorStore.PutWithEnd(tableName, nextSeq, endSeq)
	if tokenErr != nil {
		return nil, fmt.Errorf("create next shard iterator: %w", tokenErr)
	}

	return &dynamodbstreams.GetRecordsOutput{
		Records:           records,
		NextShardIterator: aws.String(nextToken),
	}, nil
}

// streamRecordsSnapshotRLocked returns the trim/current sequence numbers and
// the ordered stream-record halves under a defer-protected table.mu.RLock, so
// a panic while ordering the ring buffer can never leave table.mu read-locked
// forever.
func streamRecordsSnapshotRLocked(table *Table) (int64, int64, []models.StreamRecord, []models.StreamRecord) {
	table.mu.RLock("GetRecords")
	defer table.mu.RUnlock()

	tail, head := table.streamRecordsInOrder()

	return table.streamTrimSeq, table.streamSeq, tail, head
}

// resolveIterator resolves a shard iterator token to (tableName, startSeq, endSeq).
func (db *InMemoryDB) resolveIterator(token string) (string, int64, int64, error) {
	entry := db.iteratorStore.Get(token)
	if entry != nil {
		if time.Now().After(entry.ExpiresAt) {
			db.iteratorStore.Delete(token)

			return "", 0, 0, NewExpiredIteratorException("Shard iterator has expired")
		}

		return entry.TableName, entry.StartSeq, entry.EndSeq, nil
	}

	return "", 0, 0, NewValidationException("Invalid shard iterator")
}

// ListStreams returns a list of all enabled streams, optionally filtered by table name.
// Supports ExclusiveStartStreamArn and Limit for pagination.
// Only streams whose ARN region matches the request region (from ctx) are returned.
func (db *InMemoryDB) ListStreams(
	ctx context.Context,
	input *dynamodbstreams.ListStreamsInput,
) (*dynamodbstreams.ListStreamsOutput, error) {
	filterTable := aws.ToString(input.TableName)
	exclusiveStart := aws.ToString(input.ExclusiveStartStreamArn)
	requestRegion := getRegionFromContext(ctx, db)

	limit := maxListStreamsLimit
	if input.Limit != nil && *input.Limit > 0 && int(*input.Limit) < limit {
		limit = int(*input.Limit)
	}

	collected := db.collectEnabledStreams(requestRegion, filterTable)

	// Sort by ARN for stable pagination.
	sortStreamListEntries(collected)

	// Apply ExclusiveStartStreamArn pagination.
	if exclusiveStart != "" {
		for i, s := range collected {
			if s.arn == exclusiveStart {
				collected = collected[i+1:]

				break
			}
		}
	}

	// Apply limit.
	var lastEvaluatedARN *string
	if len(collected) > limit {
		lastEvaluatedARN = aws.String(collected[limit-1].arn)
		collected = collected[:limit]
	}

	streams := make([]streamstypes.Stream, 0, len(collected))
	for _, se := range collected {
		streams = append(streams, streamstypes.Stream{
			TableName:   aws.String(se.tableName),
			StreamArn:   aws.String(se.arn),
			StreamLabel: aws.String(streamLabelFromARN(se.arn)),
		})
	}

	return &dynamodbstreams.ListStreamsOutput{
		Streams:                streams,
		LastEvaluatedStreamArn: lastEvaluatedARN,
	}, nil
}

// streamListEntry is a (tableName, ARN) pair used during ListStreams pagination.
type streamListEntry struct {
	tableName string
	arn       string
}

// sortStreamListEntries sorts entries by ARN for deterministic pagination.
func sortStreamListEntries(entries []streamListEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].arn < entries[j].arn
	})
}

func (db *InMemoryDB) GetRecentEvents(tableName string) []models.StreamRecord {
	table, ok := db.GetTable(tableName)
	if !ok {
		return nil
	}

	table.mu.RLock("GetRecentEvents")
	defer table.mu.RUnlock()

	tail, head := table.streamRecordsInOrder()
	result := make([]models.StreamRecord, 0, len(tail)+len(head))
	result = append(result, tail...)
	result = append(result, head...)

	return result
}

// collectEnabledStreams snapshots the streamARNIndex and returns entries whose
// region matches requestRegion and (if non-empty) whose table name matches filterTable.
func (db *InMemoryDB) collectEnabledStreams(requestRegion, filterTable string) []streamListEntry {
	type arnEntry struct {
		table *Table
		arn   string
	}

	// Snapshot under db.mu (read lock). This avoids holding db.mu while also
	// acquiring table.mu, which would invert the lock order.
	entries := func() []arnEntry {
		db.mu.RLock("ListStreams")
		defer db.mu.RUnlock()

		all := db.streamARNIndex.All()
		out := make([]arnEntry, 0, len(all))
		for _, t := range all {
			out = append(out, arnEntry{table: t, arn: t.StreamARN})
		}

		return out
	}()

	var collected []streamListEntry
	for _, e := range entries {
		if arnRegion := streamARNRegion(e.arn); arnRegion != "" && arnRegion != requestRegion {
			continue
		}

		name, enabled := tableStreamInfoRLocked(e.table)
		if !enabled || (filterTable != "" && name != filterTable) {
			continue
		}

		collected = append(collected, streamListEntry{tableName: name, arn: e.arn})
	}

	return collected
}

// tableStreamInfoRLocked returns table.Name and table.StreamsEnabled under a
// defer-protected table.mu.RLock.
func tableStreamInfoRLocked(table *Table) (string, bool) {
	table.mu.RLock("ListStreams.table")
	defer table.mu.RUnlock()

	return table.Name, table.StreamsEnabled
}

// buildStreamARNInRegion generates a stream ARN for the given table in a specific region.
// The stream label embedded in the ARN is the ISO 8601 timestamp (ms precision) at which
// the stream was enabled, matching real AWS DynamoDB Streams behavior.
func (db *InMemoryDB) buildStreamARNInRegion(tableName, region string, createdAt time.Time) string {
	label := createdAt.UTC().Format("2006-01-02T15:04:05.000")

	return arn.Build(
		"dynamodb",
		region,
		db.accountID,
		"table/"+tableName+"/stream/"+label,
	)
}

// streamARNRegion extracts the region from a DynamoDB stream ARN
// (arn:aws:dynamodb:region:account:table/T/stream/label). Returns "" if unparseable.
func streamARNRegion(streamARN string) string {
	const regionIdx = 3
	parts := strings.Split(streamARN, ":")
	if len(parts) > regionIdx {
		return parts[regionIdx]
	}

	return ""
}

// buildSDKRecord converts an internal StreamRecord to the AWS SDK type.
// region is the backend's default region, included in the EventSource metadata.
func buildSDKRecord(r models.StreamRecord, region string) streamstypes.Record {
	createdAt := time.Unix(r.ApproximateCreationDateTime, 0)
	rec := streamstypes.Record{
		EventID:      aws.String(r.EventID),
		EventName:    streamstypes.OperationType(r.EventName),
		EventVersion: aws.String("1.0"),
		EventSource:  aws.String("aws:dynamodb"),
		AwsRegion:    aws.String(region),
		Dynamodb: &streamstypes.StreamRecord{
			SequenceNumber:              aws.String(r.SequenceNumber),
			ApproximateCreationDateTime: &createdAt,
			StreamViewType:              streamstypes.StreamViewType(r.StreamViewType),
			SizeBytes:                   aws.Int64(r.SizeBytes),
		},
	}
	if r.UserIdentityPrincipalID != "" {
		rec.UserIdentity = &streamstypes.Identity{
			PrincipalId: aws.String(r.UserIdentityPrincipalID),
			Type:        aws.String(r.UserIdentityType),
		}
	}

	if r.Keys != nil {
		keys, err := buildSDKStreamItem(r.Keys)
		if err == nil {
			rec.Dynamodb.Keys = keys
		}
	}

	if r.NewImage != nil {
		newImg, err := buildSDKStreamItem(r.NewImage)
		if err == nil {
			rec.Dynamodb.NewImage = newImg
		}
	}

	if r.OldImage != nil {
		oldImg, err := buildSDKStreamItem(r.OldImage)
		if err == nil {
			rec.Dynamodb.OldImage = oldImg
		}
	}

	return rec
}

// buildSDKStreamItem converts an internal wire-format item to a dynamodbstreams attribute map.
// The dynamodbstreams AttributeValue is a different Go interface from dynamodb/types.AttributeValue,
// so we need a parallel converter here.
func buildSDKStreamItem(item map[string]any) (map[string]streamstypes.AttributeValue, error) {
	out := make(map[string]streamstypes.AttributeValue, len(item))

	for k, v := range item {
		av, err := toStreamAttributeValue(v)
		if err != nil {
			return nil, err
		}

		out[k] = av
	}

	return out, nil
}

// toStreamAttributeValue converts a wire-format attribute value (single-key type map)
// to a dynamodbstreams AttributeValue.
func toStreamAttributeValue(
	v any,
) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
	m, ok := v.(map[string]any)
	if !ok {
		return nil, ErrInvalidAttributeValue
	}

	if len(m) != 1 {
		return nil, ErrInvalidTypeKeyCount
	}

	for typKey, val := range m {
		return dispatchStreamType(typKey, val)
	}

	return nil, ErrUnknownAttributeType
}

func dispatchStreamType(
	typKey string,
	val any,
) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
	switch typKey {
	case "S":
		s, ok := val.(string)
		if !ok {
			return nil, ErrTypeMismatchS
		}

		return &streamstypes.AttributeValueMemberS{Value: s}, nil
	case "N":
		s, ok := val.(string)
		if !ok {
			return nil, ErrTypeMismatchN
		}

		return &streamstypes.AttributeValueMemberN{Value: s}, nil
	case typeBOOL:
		b, ok := val.(bool)
		if !ok {
			return nil, ErrTypeMismatchBOOL
		}

		return &streamstypes.AttributeValueMemberBOOL{Value: b}, nil
	case typeNULL:
		return &streamstypes.AttributeValueMemberNULL{Value: true}, nil
	case "M":
		return handleMapAttribute(val)
	case "L":
		return handleListAttribute(val)
	case "SS":
		return &streamstypes.AttributeValueMemberSS{Value: toStringSliceFrom(val)}, nil
	case "NS":
		return &streamstypes.AttributeValueMemberNS{Value: toStringSliceFrom(val)}, nil
	case "B":
		return dispatchStreamTypeBinary(val)
	case "BS":
		return dispatchStreamTypeBinarySet(val)
	default:
		return nil, ErrUnknownAttributeType
	}
}

// dispatchStreamTypeBinary converts a wire "B" value ([]byte or base64 string) to a streams AttributeValue.
func dispatchStreamTypeBinary(
	val any,
) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
	switch b := val.(type) {
	case []byte:
		return &streamstypes.AttributeValueMemberB{Value: b}, nil
	case string:
		decoded, err := base64.StdEncoding.DecodeString(b)
		if err != nil {
			return nil, ErrTypeMismatchB
		}

		return &streamstypes.AttributeValueMemberB{Value: decoded}, nil
	default:
		return nil, ErrTypeMismatchB
	}
}

// dispatchStreamTypeBinarySet converts a wire "BS" value to a streams AttributeValue.
// Accepts [][]byte, []string (base64), or []any containing the above.
func dispatchStreamTypeBinarySet(
	val any,
) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
	bs, err := toByteSliceSliceFrom(val)
	if err != nil {
		return nil, err
	}

	return &streamstypes.AttributeValueMemberBS{Value: bs}, nil
}

func handleMapAttribute(
	val any,
) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
	mVal, ok := val.(map[string]any)
	if !ok {
		return nil, ErrTypeMismatchM
	}

	inner, err := buildSDKStreamItem(mVal)
	if err != nil {
		return nil, err
	}

	return &streamstypes.AttributeValueMemberM{Value: inner}, nil
}

func handleListAttribute(
	val any,
) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
	lVal, ok := val.([]any)
	if !ok {
		return nil, ErrTypeMismatchL
	}

	items := make([]streamstypes.AttributeValue, 0, len(lVal))
	for _, elem := range lVal {
		av, err := toStreamAttributeValue(elem)
		if err != nil {
			return nil, err
		}

		items = append(items, av)
	}

	return &streamstypes.AttributeValueMemberL{Value: items}, nil
}

// toStringSliceFrom coerces an any to []string (accepts both []string and []any of strings).
func toStringSliceFrom(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, 0, len(s))
		for _, elem := range s {
			if str, ok := elem.(string); ok {
				out = append(out, str)
			}
		}

		return out
	default:
		return nil
	}
}

// toByteSliceSliceFrom coerces an any to [][]byte.
// Accepts [][]byte directly, or []any / []string containing base64-encoded strings.
func toByteSliceSliceFrom(v any) ([][]byte, error) {
	switch s := v.(type) {
	case [][]byte:
		return s, nil
	case []string:
		out := make([][]byte, 0, len(s))
		for _, elem := range s {
			decoded, err := base64.StdEncoding.DecodeString(elem)
			if err != nil {
				return nil, ErrTypeMismatchB
			}

			out = append(out, decoded)
		}

		return out, nil
	case []any:
		out := make([][]byte, 0, len(s))
		for _, elem := range s {
			switch b := elem.(type) {
			case []byte:
				out = append(out, b)
			case string:
				decoded, err := base64.StdEncoding.DecodeString(b)
				if err != nil {
					return nil, ErrTypeMismatchB
				}

				out = append(out, decoded)
			default:
				return nil, ErrTypeMismatchB
			}
		}

		return out, nil
	default:
		return nil, ErrTypeMismatchB
	}
}

// findTableByStreamARN looks up a table by stream ARN using the reverse index.
// Must be called with db.mu held.
func (db *InMemoryDB) findTableByStreamARN(streamARN string) *Table {
	if t, ok := db.streamARNIndex.Get(streamARN); ok {
		return t
	}

	return nil
}

// findTableByStreamARNRLocked wraps findTableByStreamARN in a defer-protected
// db.mu RLock, so a panic during the lookup can never leave db.mu read-locked
// forever.
func (db *InMemoryDB) findTableByStreamARNRLocked(streamARN string) *Table {
	db.mu.RLock("DescribeStream")
	defer db.mu.RUnlock()

	return db.findTableByStreamARN(streamARN)
}

// describeStreamSnapshot copies the shard/table metadata needed by
// DescribeStream under a single found.mu.RLock/defer, so a panic while
// trimming/copying the shard slice can never leave table.mu read-locked
// forever. limit and exclusiveStart are computed by the caller from the
// request input (no lock needed for that).
func describeStreamSnapshot(
	found *Table,
	exclusiveStart string,
	limit int,
) (
	string,
	string,
	[]models.KeySchemaElement,
	time.Time,
	[]StreamShard,
	*string,
) {
	found.mu.RLock("DescribeStream")
	defer found.mu.RUnlock()

	shardSlice := found.streamShards

	if exclusiveStart != "" {
		foundStart := false
		for i, s := range shardSlice {
			if s.ShardID == exclusiveStart {
				shardSlice = shardSlice[i+1:]
				foundStart = true

				break
			}
		}
		if !foundStart {
			shardSlice = nil
		}
	}

	var lastEvaluatedShardID *string
	if len(shardSlice) > limit {
		lastEvaluatedShardID = aws.String(shardSlice[limit-1].ShardID)
		shardSlice = shardSlice[:limit]
	}

	shards := make([]StreamShard, len(shardSlice))
	copy(shards, shardSlice)

	return found.Name, found.StreamViewType, found.KeySchema, found.StreamCreatedAt, shards, lastEvaluatedShardID
}

// collectStreamRecords collects up to limit records starting at startSeq
// from two slices representing the ordered halves of the ring buffer.
// tail is iterated first (oldest records), then head (newest records that
// wrapped around). When the buffer is not yet full, head is nil.
func collectStreamRecords(
	tail, head []models.StreamRecord,
	startSeq, limit, initialNextSeq int64,
	region string,
) ([]streamstypes.Record, int64) {
	records := make([]streamstypes.Record, 0)
	nextSeq := initialNextSeq

	records, nextSeq = appendMatchingRecords(records, tail, startSeq, limit, nextSeq, region)
	records, nextSeq = appendMatchingRecords(records, head, startSeq, limit, nextSeq, region)

	return records, nextSeq
}

// appendMatchingRecords appends records from src that are at or after startSeq,
// stopping when limit is reached. Returns the updated slice and next sequence.
func appendMatchingRecords(
	records []streamstypes.Record,
	src []models.StreamRecord,
	startSeq, limit, nextSeq int64,
	region string,
) ([]streamstypes.Record, int64) {
	if len(src) == 0 || int64(len(records)) >= limit {
		return records, nextSeq
	}

	startSeqStr := seqNumString(startSeq)
	idx := sort.Search(len(src), func(i int) bool {
		return src[i].SequenceNumber >= startSeqStr
	})

	for i := idx; i < len(src); i++ {
		if int64(len(records)) >= limit {
			return records, nextSeq
		}
		r := src[i]
		records = append(records, buildSDKRecord(r, region))
		if seq, err := parseSeqNum(r.SequenceNumber); err == nil {
			nextSeq = seq + 1
		}
	}

	return records, nextSeq
}
