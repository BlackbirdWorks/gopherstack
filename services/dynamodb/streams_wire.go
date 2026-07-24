package dynamodb

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// Sentinel errors for the wire<->SDK AttributeValue conversions below. These
// cover both directions: FromStreamAttributeValue (SDK -> wire, used when
// serializing GetRecords/DescribeStream responses) and toStreamAttributeValue
// (wire -> SDK, used when building streamstypes.Record from the internal
// wire-format item stored on each models.StreamRecord).
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

// WireStreamDescription mirrors StreamDescription but with timestamps as float64 epoch seconds.
type WireStreamDescription struct {
	CreationRequestDateTime *float64                        `json:"CreationRequestDateTime,omitempty"`
	LastEvaluatedShardID    *string                         `json:"LastEvaluatedShardId,omitempty"`
	StreamArn               *string                         `json:"StreamArn,omitempty"`
	StreamLabel             *string                         `json:"StreamLabel,omitempty"`
	TableName               *string                         `json:"TableName,omitempty"`
	StreamStatus            streamstypes.StreamStatus       `json:"StreamStatus,omitempty"`
	StreamViewType          streamstypes.StreamViewType     `json:"StreamViewType,omitempty"`
	KeySchema               []streamstypes.KeySchemaElement `json:"KeySchema,omitempty"`
	Shards                  []streamstypes.Shard            `json:"Shards,omitempty"`
}

type WireDescribeStreamOutput struct {
	StreamDescription *WireStreamDescription `json:"StreamDescription,omitempty"`
}

func ToWireDescribeStreamOutput(out *dynamodbstreams.DescribeStreamOutput) *WireDescribeStreamOutput {
	if out == nil || out.StreamDescription == nil {
		return &WireDescribeStreamOutput{}
	}
	sd := out.StreamDescription
	wd := &WireStreamDescription{
		KeySchema:            sd.KeySchema,
		LastEvaluatedShardID: sd.LastEvaluatedShardId,
		Shards:               sd.Shards,
		StreamArn:            sd.StreamArn,
		StreamLabel:          sd.StreamLabel,
		StreamStatus:         sd.StreamStatus,
		StreamViewType:       sd.StreamViewType,
		TableName:            sd.TableName,
	}
	if sd.CreationRequestDateTime != nil {
		epochSecs := float64(sd.CreationRequestDateTime.Unix())
		wd.CreationRequestDateTime = &epochSecs
	}

	return &WireDescribeStreamOutput{StreamDescription: wd}
}

type WireStreamRecord struct {
	Dynamodb     *WireStreamRecordData      `json:"dynamodb,omitempty"`
	UserIdentity *streamstypes.Identity     `json:"userIdentity,omitempty"`
	EventID      string                     `json:"eventID,omitempty"`
	EventName    streamstypes.OperationType `json:"eventName,omitempty"`
	EventVersion string                     `json:"eventVersion,omitempty"`
	EventSource  string                     `json:"eventSource,omitempty"`
	AwsRegion    string                     `json:"awsRegion,omitempty"`
}

type WireStreamRecordData struct {
	// ApproximateCreationDateTime is Unix epoch seconds (float64) per DynamoDB Streams JSON 1.0 protocol.
	ApproximateCreationDateTime *float64                    `json:"ApproximateCreationDateTime,omitempty"`
	Keys                        map[string]any              `json:"Keys,omitempty"`
	NewImage                    map[string]any              `json:"NewImage,omitempty"`
	OldImage                    map[string]any              `json:"OldImage,omitempty"`
	SequenceNumber              *string                     `json:"SequenceNumber,omitempty"`
	SizeBytes                   *int64                      `json:"SizeBytes,omitempty"`
	StreamViewType              streamstypes.StreamViewType `json:"StreamViewType,omitempty"`
}

type WireGetRecordsOutput struct {
	NextShardIterator *string            `json:"NextShardIterator,omitempty"`
	Records           []WireStreamRecord `json:"Records"`
}

func ToWireGetRecordsOutput(out *dynamodbstreams.GetRecordsOutput) (*WireGetRecordsOutput, error) {
	if out == nil {
		return &WireGetRecordsOutput{}, nil
	}

	records := make([]WireStreamRecord, 0, len(out.Records))
	for _, rec := range out.Records {
		var wireData *WireStreamRecordData
		if rec.Dynamodb != nil {
			var err error
			wireData, err = ToWireStreamRecordData(rec.Dynamodb)
			if err != nil {
				return nil, err
			}
		}

		records = append(records, WireStreamRecord{
			EventID:      aws.ToString(rec.EventID),
			EventName:    rec.EventName,
			EventVersion: aws.ToString(rec.EventVersion),
			EventSource:  aws.ToString(rec.EventSource),
			AwsRegion:    aws.ToString(rec.AwsRegion),
			Dynamodb:     wireData,
			UserIdentity: rec.UserIdentity,
		})
	}

	return &WireGetRecordsOutput{
		Records:           records,
		NextShardIterator: out.NextShardIterator,
	}, nil
}

func ToWireStreamRecordData(record *streamstypes.StreamRecord) (*WireStreamRecordData, error) {
	wireData := &WireStreamRecordData{
		SequenceNumber: record.SequenceNumber,
		SizeBytes:      record.SizeBytes,
		StreamViewType: record.StreamViewType,
	}

	if record.Keys != nil {
		keys, err := FromStreamItem(record.Keys)
		if err != nil {
			return nil, err
		}
		wireData.Keys = keys
	}

	if record.NewImage != nil {
		newImage, err := FromStreamItem(record.NewImage)
		if err != nil {
			return nil, err
		}
		wireData.NewImage = newImage
	}

	if record.OldImage != nil {
		oldImage, err := FromStreamItem(record.OldImage)
		if err != nil {
			return nil, err
		}
		wireData.OldImage = oldImage
	}

	if record.ApproximateCreationDateTime != nil {
		epochSecs := float64(record.ApproximateCreationDateTime.Unix())
		wireData.ApproximateCreationDateTime = &epochSecs
	}

	return wireData, nil
}

func FromStreamItem(item map[string]streamstypes.AttributeValue) (map[string]any, error) {
	out := make(map[string]any, len(item))
	for key, value := range item {
		wireValue, err := FromStreamAttributeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = wireValue
	}

	return out, nil
}

func FromStreamAttributeValue(av streamstypes.AttributeValue) (map[string]any, error) {
	switch v := av.(type) {
	case *streamstypes.AttributeValueMemberS:
		return map[string]any{"S": v.Value}, nil
	case *streamstypes.AttributeValueMemberN:
		return map[string]any{"N": v.Value}, nil
	case *streamstypes.AttributeValueMemberBOOL:
		return map[string]any{typeBOOL: v.Value}, nil
	case *streamstypes.AttributeValueMemberNULL:
		return map[string]any{typeNULL: v.Value}, nil
	case *streamstypes.AttributeValueMemberB:
		return map[string]any{"B": v.Value}, nil
	case *streamstypes.AttributeValueMemberSS:
		return map[string]any{"SS": v.Value}, nil
	case *streamstypes.AttributeValueMemberNS:
		return map[string]any{"NS": v.Value}, nil
	case *streamstypes.AttributeValueMemberBS:
		return map[string]any{"BS": v.Value}, nil
	case *streamstypes.AttributeValueMemberM:
		m, err := FromStreamItem(v.Value)
		if err != nil {
			return nil, err
		}

		return map[string]any{"M": m}, nil
	case *streamstypes.AttributeValueMemberL:
		items := make([]any, 0, len(v.Value))
		for _, item := range v.Value {
			wireItem, err := FromStreamAttributeValue(item)
			if err != nil {
				return nil, err
			}
			items = append(items, wireItem)
		}

		return map[string]any{"L": items}, nil
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnknownAttributeType, av)
	}
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
