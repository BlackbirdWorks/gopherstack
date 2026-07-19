package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// wireStreamDescription mirrors StreamDescription but with timestamps as float64 epoch seconds.
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
