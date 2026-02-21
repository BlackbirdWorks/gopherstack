package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type wireStreamRecord struct {
	Dynamodb     *wireStreamRecordData      `json:"dynamodb,omitempty"`
	UserIdentity *streamstypes.Identity     `json:"userIdentity,omitempty"`
	EventID      string                     `json:"eventID,omitempty"`
	EventName    streamstypes.OperationType `json:"eventName,omitempty"`
	EventVersion string                     `json:"eventVersion,omitempty"`
	EventSource  string                     `json:"eventSource,omitempty"`
	AwsRegion    string                     `json:"awsRegion,omitempty"`
}

type wireStreamRecordData struct {
	ApproximateCreationDateTime *string                     `json:"ApproximateCreationDateTime,omitempty"`
	Keys                        map[string]any              `json:"Keys,omitempty"`
	NewImage                    map[string]any              `json:"NewImage,omitempty"`
	OldImage                    map[string]any              `json:"OldImage,omitempty"`
	SequenceNumber              *string                     `json:"SequenceNumber,omitempty"`
	SizeBytes                   *int64                      `json:"SizeBytes,omitempty"`
	StreamViewType              streamstypes.StreamViewType `json:"StreamViewType,omitempty"`
}

type wireGetRecordsOutput struct {
	NextShardIterator *string            `json:"NextShardIterator,omitempty"`
	Records           []wireStreamRecord `json:"Records"`
}

func toWireGetRecordsOutput(out *dynamodbstreams.GetRecordsOutput) (*wireGetRecordsOutput, error) {
	if out == nil {
		return &wireGetRecordsOutput{}, nil
	}

	records := make([]wireStreamRecord, 0, len(out.Records))
	for _, rec := range out.Records {
		var wireData *wireStreamRecordData
		if rec.Dynamodb != nil {
			var err error
			wireData, err = toWireStreamRecordData(rec.Dynamodb)
			if err != nil {
				return nil, err
			}
		}

		records = append(records, wireStreamRecord{
			EventID:      aws.ToString(rec.EventID),
			EventName:    rec.EventName,
			EventVersion: aws.ToString(rec.EventVersion),
			EventSource:  aws.ToString(rec.EventSource),
			AwsRegion:    aws.ToString(rec.AwsRegion),
			Dynamodb:     wireData,
			UserIdentity: rec.UserIdentity,
		})
	}

	return &wireGetRecordsOutput{
		Records:           records,
		NextShardIterator: out.NextShardIterator,
	}, nil
}

func toWireStreamRecordData(record *streamstypes.StreamRecord) (*wireStreamRecordData, error) {
	wireData := &wireStreamRecordData{
		SequenceNumber: record.SequenceNumber,
		SizeBytes:      record.SizeBytes,
		StreamViewType: record.StreamViewType,
	}

	if record.Keys != nil {
		keys, err := fromStreamItem(record.Keys)
		if err != nil {
			return nil, err
		}
		wireData.Keys = keys
	}

	if record.NewImage != nil {
		newImage, err := fromStreamItem(record.NewImage)
		if err != nil {
			return nil, err
		}
		wireData.NewImage = newImage
	}

	if record.OldImage != nil {
		oldImage, err := fromStreamItem(record.OldImage)
		if err != nil {
			return nil, err
		}
		wireData.OldImage = oldImage
	}

	if record.ApproximateCreationDateTime != nil {
		wireData.ApproximateCreationDateTime = aws.String(
			record.ApproximateCreationDateTime.Format("2006-01-02T15:04:05Z"),
		)
	}

	return wireData, nil
}

func fromStreamItem(item map[string]streamstypes.AttributeValue) (map[string]any, error) {
	out := make(map[string]any, len(item))
	for key, value := range item {
		wireValue, err := fromStreamAttributeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = wireValue
	}

	return out, nil
}

func fromStreamAttributeValue(av streamstypes.AttributeValue) (map[string]any, error) {
	switch v := av.(type) {
	case *streamstypes.AttributeValueMemberS:
		return map[string]any{"S": v.Value}, nil
	case *streamstypes.AttributeValueMemberN:
		return map[string]any{"N": v.Value}, nil
	case *streamstypes.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": v.Value}, nil
	case *streamstypes.AttributeValueMemberNULL:
		return map[string]any{"NULL": v.Value}, nil
	case *streamstypes.AttributeValueMemberB:
		return map[string]any{"B": v.Value}, nil
	case *streamstypes.AttributeValueMemberSS:
		return map[string]any{"SS": v.Value}, nil
	case *streamstypes.AttributeValueMemberNS:
		return map[string]any{"NS": v.Value}, nil
	case *streamstypes.AttributeValueMemberBS:
		return map[string]any{"BS": v.Value}, nil
	case *streamstypes.AttributeValueMemberM:
		m, err := fromStreamItem(v.Value)
		if err != nil {
			return nil, err
		}

		return map[string]any{"M": m}, nil
	case *streamstypes.AttributeValueMemberL:
		items := make([]any, 0, len(v.Value))
		for _, item := range v.Value {
			wireItem, err := fromStreamAttributeValue(item)
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
