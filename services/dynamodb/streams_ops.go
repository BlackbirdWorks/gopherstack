package dynamodb

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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
)

const (
	streamShardID     = "shardId-00000000000000000001-00000001"
	maxRecords        = 1000
	iteratorPartCount = 3 // tableName:sequenceNumber:timestamp
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
	table.StreamHead = 0
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
			"stream not found: " + streamARN,
		)
	}

	found.mu.RLock("DescribeStream")
	tableName := found.Name
	viewType := found.StreamViewType
	keySchema := found.KeySchema
	seqFirst := ""
	seqLast := ""

	if len(found.StreamRecords) > 0 {
		seqFirst, seqLast = found.streamSeqRange()
	}
	found.mu.RUnlock()

	sdkKeySchema := make([]streamstypes.KeySchemaElement, 0, len(keySchema))
	for _, ks := range keySchema {
		sdkKeySchema = append(sdkKeySchema, streamstypes.KeySchemaElement{
			AttributeName: aws.String(ks.AttributeName),
			KeyType:       streamstypes.KeyType(ks.KeyType),
		})
	}

	return &dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &streamstypes.StreamDescription{
			StreamArn:               aws.String(streamARN),
			StreamLabel:             aws.String("latest"),
			StreamStatus:            streamstypes.StreamStatusEnabled,
			StreamViewType:          streamstypes.StreamViewType(viewType),
			TableName:               aws.String(tableName),
			KeySchema:               sdkKeySchema,
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
			"stream not found: " + streamARN,
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
		seqStr := aws.ToString(input.SequenceNumber)
		if seqStr == "" {
			return nil, NewValidationException(
				"SequenceNumber is required for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER iterator types",
			)
		}
		if seq, err := strconv.ParseInt(strings.TrimLeft(seqStr, "0"), 10, 64); err == nil {
			if input.ShardIteratorType == streamstypes.ShardIteratorTypeAfterSequenceNumber {
				startSeq = seq + 1
			} else {
				startSeq = seq
			}
		}
	default: // TrimHorizon — start from beginning
		startSeq = 0
	}

	// Shard iterator format: tableName:startSequenceNumber:creationTimestamp
	// creationTimestamp is used to enforce the 15-minute expiration rule.
	iterator := fmt.Sprintf("%s:%d:%d", found.Name, startSeq, time.Now().Unix())

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
	parts := strings.Split(iterator, ":")
	if len(parts) != iteratorPartCount {
		return nil, NewValidationException("Invalid shard iterator")
	}

	tableName := parts[0]
	startSeq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, NewValidationException("Invalid shard iterator: invalid sequence number")
	}

	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, NewValidationException("Invalid shard iterator: invalid timestamp")
	}

	// AWS: Shard iterators expire after 15 minutes.
	// Reject future timestamps to prevent forged iterators from bypassing expiration.
	iterTime := time.Unix(ts, 0)
	now := time.Now()
	if iterTime.After(now) || now.Sub(iterTime) > 15*time.Minute {
		return nil, NewExpiredIteratorException("Shard iterator has expired")
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

	tail, head := table.streamRecordsInOrder()
	records, nextSeq := collectStreamRecords(tail, head, startSeq, limit, table.streamSeq, db.defaultRegion)

	telemetry.RecordStreamEvents("dynamodb", len(records))

	nextIterator := fmt.Sprintf("%s:%d:%d", tableName, nextSeq, time.Now().Unix())

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

	// Snapshot the streamARNIndex under db.mu (read lock). This avoids holding
	// db.mu while also acquiring table.mu, which would invert the lock order
	// (EnableStream/DisableStream take table.mu first, then db.mu).
	type arnEntry struct {
		table *Table
		arn   string
	}

	db.mu.RLock("ListStreams")
	entries := make([]arnEntry, 0, len(db.streamARNIndex))
	for arn, t := range db.streamARNIndex {
		entries = append(entries, arnEntry{table: t, arn: arn})
	}
	db.mu.RUnlock()

	var streams []streamstypes.Stream

	for _, e := range entries {
		e.table.mu.RLock("ListStreams.table")
		name := e.table.Name
		enabled := e.table.StreamsEnabled
		e.table.mu.RUnlock()

		if !enabled {
			continue
		}

		if filterTable != "" && name != filterTable {
			continue
		}

		streams = append(streams, streamstypes.Stream{
			TableName:   aws.String(name),
			StreamArn:   aws.String(e.arn),
			StreamLabel: aws.String("latest"),
		})
	}

	return &dynamodbstreams.ListStreamsOutput{
		Streams: streams,
	}, nil
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

// buildStreamARN generates a stream ARN for the given table using the backend's account and region.
func (db *InMemoryDB) buildStreamARN(tableName string) string {
	return arn.Build("dynamodb", db.defaultRegion, db.accountID, "table/"+tableName+"/stream/2024-01-01T00:00:00.000")
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
		},
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
func dispatchStreamTypeBinary(val any) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
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
func dispatchStreamTypeBinarySet(val any) (streamstypes.AttributeValue, error) { //nolint:ireturn // SDK interface
	bs, err := toByteSliceSliceFrom(val)
	if err != nil {
		return nil, err
	}

	return &streamstypes.AttributeValueMemberBS{Value: bs}, nil
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
	if t, ok := db.streamARNIndex[streamARN]; ok {
		return t
	}

	return nil
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
	for _, r := range src {
		if int64(len(records)) >= limit {
			return records, nextSeq
		}

		seq, parseErr := strconv.ParseInt(strings.TrimLeft(r.SequenceNumber, "0"), 10, 64)
		if parseErr != nil {
			seq = 0
		}

		if seq < startSeq {
			continue
		}

		records = append(records, buildSDKRecord(r, region))
		nextSeq = seq + 1
	}

	return records, nextSeq
}
