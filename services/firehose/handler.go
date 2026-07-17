package firehose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	firehoseTargetPrefix = "Firehose_20150804."
	errFieldMessage      = "message"
	errFieldType         = "__type"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Kinesis Firehose operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Firehose handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all state in the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Firehose" }

// StartWorker starts the background interval flusher.
// It implements service.BackgroundWorker.
func (h *Handler) StartWorker(ctx context.Context) error {
	h.Backend.RunFlusher(ctx)

	return nil
}

// Shutdown implements service.Shutdowner.
// It flushes any buffered records to their destinations before the process
// exits so that records received since the last interval flush are not lost.
// If ctx expires before FlushAll returns, Shutdown returns immediately.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.Backend == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		h.Backend.FlushAll(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Ensure Handler implements service.BackgroundWorker and service.Shutdowner at compile time.
var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

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
		"UpdateDestination",
		"StartDeliveryStreamEncryption",
		"StopDeliveryStreamEncryption",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "firehose" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Firehose instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

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
	body, err := httputils.ReadBody(c.Request())
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
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		return service.HandleTarget(
			c, logger.Load(ctx),
			"Firehose", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateDeliveryStream":          service.WrapOp(h.handleCreateDeliveryStream),
		"DeleteDeliveryStream":          service.WrapOp(h.handleDeleteDeliveryStream),
		"DescribeDeliveryStream":        service.WrapOp(h.handleDescribeDeliveryStream),
		"ListDeliveryStreams":           service.WrapOp(h.handleListDeliveryStreams),
		"PutRecord":                     service.WrapOp(h.handlePutRecord),
		"PutRecordBatch":                service.WrapOp(h.handlePutRecordBatch),
		"ListTagsForDeliveryStream":     service.WrapOp(h.handleListTagsForDeliveryStream),
		"TagDeliveryStream":             service.WrapOp(h.handleTagDeliveryStream),
		"UntagDeliveryStream":           service.WrapOp(h.handleUntagDeliveryStream),
		"UpdateDestination":             service.WrapOp(h.handleUpdateDestination),
		"StartDeliveryStreamEncryption": service.WrapOp(h.handleStartDeliveryStreamEncryption),
		"StopDeliveryStreamEncryption":  service.WrapOp(h.handleStopDeliveryStreamEncryption),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound,
			map[string]any{errFieldType: "ResourceNotFoundException", errFieldMessage: err.Error()})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest,
			map[string]any{errFieldType: "ResourceInUseException", errFieldMessage: err.Error()})
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest,
			map[string]any{errFieldType: "UnknownOperationException", errFieldMessage: err.Error()})
	case errors.Is(err, errInvalidRequest), errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, ErrValidation), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest,
			map[string]any{errFieldType: "InvalidArgumentException", errFieldMessage: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{errFieldMessage: err.Error()})
	}
}
