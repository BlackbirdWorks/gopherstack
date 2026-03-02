package firehose

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const firehoseTargetPrefix = "Firehose_20150804."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Kinesis Firehose operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new Firehose handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Firehose" }

// GetSupportedOperations returns the list of supported Firehose operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDeliveryStream",
		"DeleteDeliveryStream",
		"DescribeDeliveryStream",
		"ListDeliveryStreams",
		"PutRecord",
		"PutRecordBatch",
		"ListTagsForDeliveryStream",
		"TagDeliveryStream",
		"UntagDeliveryStream",
	}
}

// RouteMatcher returns a function that matches Firehose requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), firehoseTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Firehose action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, firehoseTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type deliveryStreamNameInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
}

// ExtractResource extracts the delivery stream name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req deliveryStreamNameInput
	_ = json.Unmarshal(body, &req)

	return req.DeliveryStreamName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Firehose", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateDeliveryStream":      service.WrapOp(h.handleCreateDeliveryStream),
		"DeleteDeliveryStream":      service.WrapOp(h.handleDeleteDeliveryStream),
		"DescribeDeliveryStream":    service.WrapOp(h.handleDescribeDeliveryStream),
		"ListDeliveryStreams":       service.WrapOp(h.handleListDeliveryStreams),
		"PutRecord":                 service.WrapOp(h.handlePutRecord),
		"PutRecordBatch":            service.WrapOp(h.handlePutRecordBatch),
		"ListTagsForDeliveryStream": service.WrapOp(h.handleListTagsForDeliveryStream),
		"TagDeliveryStream":         service.WrapOp(h.handleTagDeliveryStream),
		"UntagDeliveryStream":       service.WrapOp(h.handleUntagDeliveryStream),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound,
			map[string]any{"__type": "ResourceNotFoundException", "message": err.Error()})
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type createDeliveryStreamOutput struct {
	DeliveryStreamARN string `json:"DeliveryStreamARN"`
}

func (h *Handler) handleCreateDeliveryStream(
	_ context.Context,
	in *deliveryStreamNameInput,
) (*createDeliveryStreamOutput, error) {
	s, err := h.Backend.CreateDeliveryStream(in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	return &createDeliveryStreamOutput{DeliveryStreamARN: s.ARN}, nil
}

type deleteDeliveryStreamOutput struct{}

func (h *Handler) handleDeleteDeliveryStream(
	_ context.Context,
	in *deliveryStreamNameInput,
) (*deleteDeliveryStreamOutput, error) {
	if err := h.Backend.DeleteDeliveryStream(in.DeliveryStreamName); err != nil {
		return nil, err
	}

	return &deleteDeliveryStreamOutput{}, nil
}

type deliveryStreamDescriptionFields struct {
	DeliveryStreamName   string `json:"DeliveryStreamName"`
	DeliveryStreamARN    string `json:"DeliveryStreamARN"`
	DeliveryStreamStatus string `json:"DeliveryStreamStatus"`
}

type describeDeliveryStreamOutput struct {
	DeliveryStreamDescription deliveryStreamDescriptionFields `json:"DeliveryStreamDescription"`
}

func (h *Handler) handleDescribeDeliveryStream(
	_ context.Context,
	in *deliveryStreamNameInput,
) (*describeDeliveryStreamOutput, error) {
	s, err := h.Backend.DescribeDeliveryStream(in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	return &describeDeliveryStreamOutput{
		DeliveryStreamDescription: deliveryStreamDescriptionFields{
			DeliveryStreamName:   s.Name,
			DeliveryStreamARN:    s.ARN,
			DeliveryStreamStatus: s.Status,
		},
	}, nil
}

type listDeliveryStreamsInput struct{}

type listDeliveryStreamsOutput struct {
	DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
	HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
}

func (h *Handler) handleListDeliveryStreams(
	_ context.Context,
	_ *listDeliveryStreamsInput,
) (*listDeliveryStreamsOutput, error) {
	names := h.Backend.ListDeliveryStreams()

	return &listDeliveryStreamsOutput{
		DeliveryStreamNames:    names,
		HasMoreDeliveryStreams: false,
	}, nil
}

type handlePutRecordInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
	Record             struct {
		Data string `json:"Data"`
	} `json:"Record"`
}

type putRecordOutput struct {
	RecordID string `json:"RecordId"`
}

func (h *Handler) handlePutRecord(_ context.Context, in *handlePutRecordInput) (*putRecordOutput, error) {
	data, err := base64.StdEncoding.DecodeString(in.Record.Data)
	if err != nil {
		data = []byte(in.Record.Data)
	}

	if putErr := h.Backend.PutRecord(in.DeliveryStreamName, data); putErr != nil {
		return nil, putErr
	}

	return &putRecordOutput{RecordID: "stub-record-id"}, nil
}

type handlePutRecordBatchInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
	Records            []struct {
		Data string `json:"Data"`
	} `json:"Records"`
}

type putRecordBatchOutput struct {
	RequestResponses []struct{} `json:"RequestResponses"`
	FailedPutCount   int        `json:"FailedPutCount"`
}

func (h *Handler) handlePutRecordBatch(
	_ context.Context,
	in *handlePutRecordBatchInput,
) (*putRecordBatchOutput, error) {
	records := make([][]byte, 0, len(in.Records))
	for _, r := range in.Records {
		data, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			data = []byte(r.Data)
		}

		records = append(records, data)
	}

	failedCount, err := h.Backend.PutRecordBatch(in.DeliveryStreamName, records)
	if err != nil {
		return nil, err
	}

	return &putRecordBatchOutput{
		FailedPutCount:   failedCount,
		RequestResponses: []struct{}{},
	}, nil
}

type firehoseTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type listTagsInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
}

type listTagsForDeliveryStreamOutput struct {
	Tags        []firehoseTag `json:"Tags"`
	HasMoreTags bool          `json:"HasMoreTags"`
}

func (h *Handler) handleListTagsForDeliveryStream(
	_ context.Context,
	in *listTagsInput,
) (*listTagsForDeliveryStreamOutput, error) {
	tags, err := h.Backend.ListTagsForDeliveryStream(in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	tagList := make([]firehoseTag, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, firehoseTag{Key: k, Value: v})
	}

	return &listTagsForDeliveryStreamOutput{
		Tags:        tagList,
		HasMoreTags: false,
	}, nil
}

type tagDeliveryStreamInput struct {
	DeliveryStreamName string        `json:"DeliveryStreamName"`
	Tags               []firehoseTag `json:"Tags"`
}

type tagDeliveryStreamOutput struct{}

func (h *Handler) handleTagDeliveryStream(
	_ context.Context,
	in *tagDeliveryStreamInput,
) (*tagDeliveryStreamOutput, error) {
	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagDeliveryStream(in.DeliveryStreamName, tagMap); err != nil {
		return nil, err
	}

	return &tagDeliveryStreamOutput{}, nil
}

type untagDeliveryStreamInput struct {
	DeliveryStreamName string   `json:"DeliveryStreamName"`
	TagKeys            []string `json:"TagKeys"`
}

type untagDeliveryStreamOutput struct{}

func (h *Handler) handleUntagDeliveryStream(
	_ context.Context,
	in *untagDeliveryStreamInput,
) (*untagDeliveryStreamOutput, error) {
	if err := h.Backend.UntagDeliveryStream(in.DeliveryStreamName, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagDeliveryStreamOutput{}, nil
}
