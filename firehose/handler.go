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

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var result any
	var err error

	switch action {
	case "CreateDeliveryStream":
		result, err = h.handleCreateDeliveryStream(body)
	case "DeleteDeliveryStream":
		result, err = h.handleDeleteDeliveryStream(body)
	case "DescribeDeliveryStream":
		result, err = h.handleDescribeDeliveryStream(body)
	case "ListDeliveryStreams":
		result, err = h.handleListDeliveryStreams()
	case "PutRecord":
		result, err = h.handlePutRecord(body)
	case "PutRecordBatch":
		result, err = h.handlePutRecordBatch(body)
	case "ListTagsForDeliveryStream", "TagDeliveryStream", "UntagDeliveryStream":
		result, err = h.handleTagOperation(action)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

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

func (h *Handler) handleCreateDeliveryStream(body []byte) (any, error) {
	var req deliveryStreamNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	s, err := h.Backend.CreateDeliveryStream(req.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"DeliveryStreamARN": s.ARN,
	}, nil
}

func (h *Handler) handleDeleteDeliveryStream(body []byte) (any, error) {
	var req deliveryStreamNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.DeleteDeliveryStream(req.DeliveryStreamName); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

func (h *Handler) handleDescribeDeliveryStream(body []byte) (any, error) {
	var req deliveryStreamNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	s, err := h.Backend.DescribeDeliveryStream(req.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"DeliveryStreamDescription": map[string]any{
			"DeliveryStreamName":   s.Name,
			"DeliveryStreamARN":    s.ARN,
			"DeliveryStreamStatus": s.Status,
		},
	}, nil
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListDeliveryStreams() (any, error) {
	names := h.Backend.ListDeliveryStreams()

	return map[string]any{
		"DeliveryStreamNames":    names,
		"HasMoreDeliveryStreams": false,
	}, nil
}

type handlePutRecordInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
	Record             struct {
		Data string `json:"Data"`
	} `json:"Record"`
}

func (h *Handler) handlePutRecord(body []byte) (any, error) {
	var req handlePutRecordInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	data, err := base64.StdEncoding.DecodeString(req.Record.Data)
	if err != nil {
		data = []byte(req.Record.Data)
	}

	if putErr := h.Backend.PutRecord(req.DeliveryStreamName, data); putErr != nil {
		return nil, putErr
	}

	return map[string]string{
		"RecordId": "stub-record-id",
	}, nil
}

type handlePutRecordBatchInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
	Records            []struct {
		Data string `json:"Data"`
	} `json:"Records"`
}

func (h *Handler) handlePutRecordBatch(body []byte) (any, error) {
	var req handlePutRecordBatchInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	records := make([][]byte, 0, len(req.Records))
	for _, r := range req.Records {
		data, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			data = []byte(r.Data)
		}

		records = append(records, data)
	}

	failedCount, err := h.Backend.PutRecordBatch(req.DeliveryStreamName, records)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"FailedPutCount":   failedCount,
		"RequestResponses": []map[string]string{},
	}, nil
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleTagOperation(action string) (any, error) {
	if action == "ListTagsForDeliveryStream" {
		return map[string]any{
			"Tags":        []any{},
			"HasMoreTags": false,
		}, nil
	}
	// TagDeliveryStream and UntagDeliveryStream: no-op stubs.
	return map[string]any{}, nil
}
