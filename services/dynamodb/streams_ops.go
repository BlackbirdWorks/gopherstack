package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
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
	ErrUnknownAttributeType  = errors.New("unknown attribute type")
)

const (
	streamShardID = "shardId-00000000000000000001-00000001"
	maxRecords    = 1000
)

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

	table.mu.Lock("EnableStream")
	table.StreamsEnabled = true
	table.StreamViewType = viewType
	table.StreamARN = db.buildStreamARN(tableName)
	newARN := table.StreamARN
	table.mu.Unlock()

	// Update the reverse index under db.mu (after releasing table lock to preserve lock ordering).
	db.mu.Lock("EnableStream.streamARNIndex")
	db.streamARNIndex[newARN] = table
	db.mu.Unlock()

	return nil
}

// DisableStream disables DynamoDB Streams on a table.
func (db *InMemoryDB) DisableStream(ctx context.Context, tableName string) error {
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return err
	}

	table.mu.Lock("DisableStream")
	oldARN := table.StreamARN
	table.StreamsEnabled = false
	table.StreamARN = ""
	table.StreamViewType = ""
	table.StreamRecords = nil
	table.streamSeq = 0
	table.mu.Unlock()

	// Remove from reverse index under db.mu (after releasing table lock to preserve lock ordering).
	if oldARN != "" {
		db.mu.Lock("DisableStream.streamARNIndex")
		delete(db.streamARNIndex, oldARN)
		db.mu.Unlock()
	}

	return nil
}

// DescribeStream returns details about a stream (identified by its ARN).
func (db *InMemoryDB) DescribeStream(
	_ context.Context,
	input *dynamodbstreams.DescribeStreamInput,
) (*dynamodbstreams.DescribeStreamOutput, error) {
	streamARN := aws.ToString(input.StreamArn)

	db.mu.RLock("DescribeStream")
	found := db.findTableByStreamARN(streamARN)
	db.mu.RUnlock()

	if found == nil {
		return nil, NewResourceNotFoundException(
			fmt.Sprintf("stream not found: %s", streamARN),
		)
	}

	found.mu.RLock("DescribeStream")
	tableName := found.Name
	viewType := found.StreamViewType
	seqFirst := ""
	seqLast := ""

	if len(found.StreamRecords) > 0 {
		seqFirst = found.StreamRecords[0].SequenceNumber
		seqLast = found.StreamRecords[len(found.StreamRecords)-1].SequenceNumber
	}
	found.mu.RUnlock()

	return &dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &streamstypes.StreamDescription{
			StreamArn:               aws.String(streamARN),
			StreamLabel:             aws.String("latest"),
			StreamStatus:            streamstypes.StreamStatusEnabled,
			StreamViewType:          streamstypes.StreamViewType(viewType),
			TableName:               aws.String(tableName),
			KeySchema:               nil, // optional detail, omitting for simplicity
			CreationRequestDateTime: nil,
			LastEvaluatedShardId:    nil,
			Shards: []streamstypes.Shard{
				{
					ShardId: aws.String(streamShardID),
					SequenceNumberRange: &streamstypes.SequenceNumberRange{
						StartingSequenceNumber: aws.String(seqFirst),
						EndingSequenceNumber:   aws.String(seqLast),
					},
				},
			},
		},
	}, nil
}

// GetShardIterator returns a shard iterator for reading stream records.
//
// The iterator encodes the starting sequence position as a simple string:
// "<tableName>:<startSeq>" so GetRecords can decode it without server-side state.
func (db *InMemoryDB) GetShardIterator(
	_ context.Context,
	input *dynamodbstreams.GetShardIteratorInput,
) (*dynamodbstreams.GetShardIteratorOutput, error) {
	streamARN := aws.ToString(input.StreamArn)

	db.mu.RLock("GetShardIterator")
	found := db.findTableByStreamARN(streamARN)
	db.mu.RUnlock()

	if found == nil {
		return nil, NewResourceNotFoundException(
			fmt.Sprintf("stream not found: %s", streamARN),
		)
	}

	// Determine start sequence from iterator type
	var startSeq int64

	switch input.ShardIteratorType {
	case streamstypes.ShardIteratorTypeLatest:
		found.mu.RLock("GetShardIterator")
		startSeq = found.streamSeq
		found.mu.RUnlock()
	case streamstypes.ShardIteratorTypeAtSequenceNumber,
		streamstypes.ShardIteratorTypeAfterSequenceNumber:
		if seq, err := strconv.ParseInt(strings.TrimLeft(
			aws.ToString(input.SequenceNumber), "0"), 10, 64); err == nil {
			if input.ShardIteratorType == streamstypes.ShardIteratorTypeAfterSequenceNumber {
				startSeq = seq + 1
			} else {
				startSeq = seq
			}
		}
	default: // TrimHorizon — start from beginning
		startSeq = 0
	}

	iterator := fmt.Sprintf("%s:%d", found.Name, startSeq)

	return &dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String(iterator),
	}, nil
}

// GetRecords reads stream records starting from the given shard iterator.
func (db *InMemoryDB) GetRecords(
	ctx context.Context,
	input *dynamodbstreams.GetRecordsInput,
) (*dynamodbstreams.GetRecordsOutput, error) {
	iterator := aws.ToString(input.ShardIterator)
	parts := strings.SplitN(iterator, ":", 2) //nolint:mnd // 2-part split for table:seq

	const iterParts = 2
	if len(parts) != iterParts {
		return nil, NewValidationException("invalid shard iterator")
	}

	tableName := parts[0]

	startSeq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, NewValidationException("invalid shard iterator sequence")
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	limit := int64(maxRecords)
	if input.Limit != nil && *input.Limit > 0 && int64(*input.Limit) < limit {
		limit = int64(*input.Limit)
	}

	table.mu.RLock("GetRecords")
	defer table.mu.RUnlock()

	records, nextSeq := collectStreamRecords(table.StreamRecords, startSeq, limit, table.streamSeq)

	telemetry.RecordStreamEvents("dynamodb", len(records))

	nextIterator := fmt.Sprintf("%s:%d", tableName, nextSeq)

	return &dynamodbstreams.GetRecordsOutput{
		Records:           records,
		NextShardIterator: aws.String(nextIterator),
	}, nil
}

// ListStreams returns a list of all enabled streams, optionally filtered by table name.
func (db *InMemoryDB) ListStreams(
	_ context.Context,
	input *dynamodbstreams.ListStreamsInput,
) (*dynamodbstreams.ListStreamsOutput, error) {
	filterTable := aws.ToString(input.TableName)

	db.mu.RLock("ListStreams")
	defer db.mu.RUnlock()

	var streams []streamstypes.Stream

	for _, regionTables := range db.Tables {
		for _, t := range regionTables {
			if !t.StreamsEnabled {
				continue
			}

			if filterTable != "" && t.Name != filterTable {
				continue
			}

			streams = append(streams, streamstypes.Stream{
				TableName:   aws.String(t.Name),
				StreamArn:   aws.String(t.StreamARN),
				StreamLabel: aws.String("latest"),
			})
		}
	}

	return &dynamodbstreams.ListStreamsOutput{
		Streams: streams,
	}, nil
}

// buildStreamARN generates a stream ARN for the given table using the backend's account and region.
func (db *InMemoryDB) buildStreamARN(tableName string) string {
	return arn.Build("dynamodb", db.defaultRegion, db.accountID, "table/"+tableName+"/stream/2024-01-01T00:00:00.000")
}

// buildSDKRecord converts an internal StreamRecord to the AWS SDK type.
func buildSDKRecord(r StreamRecord) streamstypes.Record {
	rec := streamstypes.Record{
		EventID:   aws.String(r.EventID),
		EventName: streamstypes.OperationType(r.EventName),
		Dynamodb: &streamstypes.StreamRecord{
			SequenceNumber:              aws.String(r.SequenceNumber),
			ApproximateCreationDateTime: nil, // optional; omit to keep things simple
		},
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
func toStreamAttributeValue(v any) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
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

func dispatchStreamType(typKey string, val any) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
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
	case "BOOL":
		b, ok := val.(bool)
		if !ok {
			return nil, ErrTypeMismatchBOOL
		}

		return &streamstypes.AttributeValueMemberBOOL{Value: b}, nil
	case "NULL":
		return &streamstypes.AttributeValueMemberNULL{Value: true}, nil
	case "M":
		return handleMapAttribute(val)
	case "L":
		return handleListAttribute(val)
	case "SS":
		return &streamstypes.AttributeValueMemberSS{Value: toStringSliceFrom(val)}, nil
	case "NS":
		return &streamstypes.AttributeValueMemberNS{Value: toStringSliceFrom(val)}, nil
	default:
		return nil, ErrUnknownAttributeType
	}
}

func handleMapAttribute(val any) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
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

func handleListAttribute(val any) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
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

// findTableByStreamARN looks up a table by stream ARN using the reverse index.
// Must be called with db.mu held.
func (db *InMemoryDB) findTableByStreamARN(streamARN string) *Table {
	if t, ok := db.streamARNIndex[streamARN]; ok {
		return t
	}

	return nil
}

// collectStreamRecords collects up to limit records starting at startSeq.
func collectStreamRecords(
	streamRecords []StreamRecord,
	startSeq, limit, initialNextSeq int64,
) ([]streamstypes.Record, int64) {
	var records []streamstypes.Record

	nextSeq := initialNextSeq

	for _, r := range streamRecords {
		seq, parseErr := strconv.ParseInt(strings.TrimLeft(r.SequenceNumber, "0"), 10, 64)
		if parseErr != nil {
			seq = 0
		}

		if seq < startSeq {
			continue
		}

		if int64(len(records)) >= limit {
			return records, nextSeq
		}

		records = append(records, buildSDKRecord(r))
		nextSeq = seq + 1
	}

	return records, nextSeq
}
