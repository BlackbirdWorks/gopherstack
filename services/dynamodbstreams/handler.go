package dynamodbstreams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

const (
	targetPrefix = "DynamoDBStreams_20120810."
)

var errUnknownOperation = errors.New("UnknownOperationException")

// Handler handles HTTP requests for DynamoDB Streams operations.
type Handler struct {
	Streams ddbbackend.StreamsBackend
}

// NewHandler creates a new DynamoDB Streams handler with the given backend.
func NewHandler(backend ddbbackend.StreamsBackend) *Handler {
	return &Handler{Streams: backend}
}

// Name returns the service identifier.
func (h *Handler) Name() string { return "DynamoDBStreams" }

// GetSupportedOperations returns the list of supported DynamoDB Streams operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"DescribeStream",
		"GetRecords",
		"GetShardIterator",
		"ListStreams",
	}
}

// RouteMatcher returns a matcher for DynamoDB Streams requests (by X-Amz-Target header).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the DynamoDB Streams operation from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, targetPrefix)
}

// ExtractResource extracts the stream ARN from the DynamoDB Streams request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if v, ok := data["StreamArn"]; ok {
		if s, strOk := v.(string); strOk {
			return s
		}
	}

	return ""
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "dynamodbstreams" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions (DynamoDB Streams shares the DynamoDB backend region).
func (h *Handler) ChaosRegions() []string { return []string{} }

// Handler returns the Echo handler function for DynamoDB Streams requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		operation := h.ExtractOperation(c)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		log.DebugContext(ctx, "DynamoDB Streams request", "operation", operation)

		response, reqErr := h.dispatch(ctx, operation, body)
		if reqErr != nil {
			return h.handleError(ctx, c, operation, reqErr)
		}

		payload, err := json.Marshal(response)
		if err != nil {
			log.ErrorContext(ctx, "failed to marshal JSON response", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		checksum := crc32.ChecksumIEEE(payload)
		c.Response().Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))
		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

		return c.JSONBlob(http.StatusOK, payload)
	}
}

func (h *Handler) dispatch(ctx context.Context, operation string, body []byte) (any, error) {
	if h.Streams == nil {
		return nil, fmt.Errorf("%w:%s", errUnknownOperation, operation)
	}

	switch operation {
	case "DescribeStream":
		return dispatchStreamsOp(ctx, body, h.Streams.DescribeStream)
	case "GetShardIterator":
		return dispatchStreamsOp(ctx, body, h.Streams.GetShardIterator)
	case "GetRecords":
		return dispatchGetRecords(ctx, body, h.Streams.GetRecords)
	case "ListStreams":
		return dispatchStreamsOp(ctx, body, h.Streams.ListStreams)
	default:
		return nil, fmt.Errorf("%w:%s", errUnknownOperation, operation)
	}
}

func dispatchStreamsOp[In any, Out any](
	ctx context.Context,
	body []byte,
	op func(context.Context, *In) (*Out, error),
) (any, error) {
	var input In
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	return op(ctx, &input)
}

func dispatchGetRecords(
	ctx context.Context,
	body []byte,
	op func(context.Context, *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error),
) (any, error) {
	var input dynamodbstreams.GetRecordsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	out, err := op(ctx, &input)
	if err != nil {
		return nil, err
	}

	return toWireGetRecordsOutput(out)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, operation string, reqErr error) error {
	if strings.HasPrefix(reqErr.Error(), "UnknownOperationException:") {
		body := []byte(
			`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`,
		)
		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

		return c.JSONBlob(http.StatusBadRequest, body)
	}

	msg := reqErr.Error()
	body, _ := json.Marshal(map[string]string{
		"__type":  "com.amazonaws.dynamodbstreams#" + operation + "Exception",
		"message": msg,
	})
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

	return c.JSONBlob(http.StatusBadRequest, body)
}

// Wire format types and functions for GetRecords response serialization.

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

var errUnknownAttributeType = errors.New("unknown attribute type")

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
		return nil, fmt.Errorf("%w: %T", errUnknownAttributeType, av)
	}
}
