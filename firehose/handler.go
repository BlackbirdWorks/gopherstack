package firehose

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const firehoseTargetPrefix = "Firehose_20150804."

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
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), firehoseTargetPrefix)
		switch action {
		case "CreateDeliveryStream":
			return h.handleCreateDeliveryStream(c, body)
		case "DeleteDeliveryStream":
			return h.handleDeleteDeliveryStream(c, body)
		case "DescribeDeliveryStream":
			return h.handleDescribeDeliveryStream(c, body)
		case "ListDeliveryStreams":
			return h.handleListDeliveryStreams(c)
		case "PutRecord":
			return h.handlePutRecord(c, body)
		case "PutRecordBatch":
			return h.handlePutRecordBatch(c, body)
		case "ListTagsForDeliveryStream", "TagDeliveryStream", "UntagDeliveryStream":
			return h.handleTagOperation(c, action, body)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

func (h *Handler) handleCreateDeliveryStream(c *echo.Context, body []byte) error {
	var req deliveryStreamNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	s, err := h.Backend.CreateDeliveryStream(req.DeliveryStreamName)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"DeliveryStreamARN": s.ARN,
	})
}

func (h *Handler) handleDeleteDeliveryStream(c *echo.Context, body []byte) error {
	var req deliveryStreamNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.DeleteDeliveryStream(req.DeliveryStreamName); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]any{"__type": "ResourceNotFoundException", "message": err.Error()},
			)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleDescribeDeliveryStream(c *echo.Context, body []byte) error {
	var req deliveryStreamNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	s, err := h.Backend.DescribeDeliveryStream(req.DeliveryStreamName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]any{"__type": "ResourceNotFoundException", "message": err.Error()},
			)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"DeliveryStreamDescription": map[string]any{
			"DeliveryStreamName":   s.Name,
			"DeliveryStreamARN":    s.ARN,
			"DeliveryStreamStatus": s.Status,
		},
	})
}

func (h *Handler) handleListDeliveryStreams(c *echo.Context) error {
	names := h.Backend.ListDeliveryStreams()

	return c.JSON(http.StatusOK, map[string]any{
		"DeliveryStreamNames":    names,
		"HasMoreDeliveryStreams": false,
	})
}

type handlePutRecordInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
	Record             struct {
		Data string `json:"Data"`
	} `json:"Record"`
}

func (h *Handler) handlePutRecord(c *echo.Context, body []byte) error {
	var req handlePutRecordInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	data, err := base64.StdEncoding.DecodeString(req.Record.Data)
	if err != nil {
		data = []byte(req.Record.Data)
	}

	if putErr := h.Backend.PutRecord(req.DeliveryStreamName, data); putErr != nil {
		if errors.Is(putErr, ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]any{"__type": "ResourceNotFoundException", "message": putErr.Error()},
			)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": putErr.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"RecordId": "stub-record-id",
	})
}

type handlePutRecordBatchInput struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
	Records            []struct {
		Data string `json:"Data"`
	} `json:"Records"`
}

func (h *Handler) handlePutRecordBatch(c *echo.Context, body []byte) error {
	var req handlePutRecordBatchInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
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
		if errors.Is(err, ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]any{"__type": "ResourceNotFoundException", "message": err.Error()},
			)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FailedPutCount":   failedCount,
		"RequestResponses": []map[string]string{},
	})
}

func (h *Handler) handleTagOperation(c *echo.Context, action string, _ []byte) error {
	if action == "ListTagsForDeliveryStream" {
		return c.JSON(http.StatusOK, map[string]any{
			"Tags":        []any{},
			"HasMoreTags": false,
		})
	}
	// TagDeliveryStream and UntagDeliveryStream: no-op stubs.
	return c.JSON(http.StatusOK, map[string]any{})
}
