package timestreamwrite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	targetPrefix    = "Timestream_20181101."
	keyTypeField    = "__type"
	keyMessageField = "message"
)

// defaultTimestreamMaxResults is the default page size when MaxResults is not specified.
const defaultTimestreamMaxResults = 100

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// emptyOutput is the response shape for operations with no meaningful return payload.
type emptyOutput struct{}

// Handler is the Echo HTTP handler for Amazon Timestream Write operations.
type Handler struct {
	Backend      *InMemoryBackend
	ops          map[string]service.JSONOpFunc
	supportedOps map[string]bool
}

// NewHandler creates a new Timestream Write handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()
	supported := h.GetSupportedOperations()
	h.supportedOps = make(map[string]bool, len(supported))

	for _, op := range supported {
		h.supportedOps[op] = true
	}

	return h
}

// Reset clears the backend state and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// buildOps constructs the static dispatch table for JSON operations.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateBatchLoadTask":   service.WrapOp(h.handleCreateBatchLoadTask),
		"CreateDatabase":        service.WrapOp(h.handleCreateDatabase),
		"CreateTable":           service.WrapOp(h.handleCreateTable),
		"DeleteDatabase":        service.WrapOp(h.handleDeleteDatabase),
		"DeleteTable":           service.WrapOp(h.handleDeleteTable),
		"DescribeBatchLoadTask": service.WrapOp(h.handleDescribeBatchLoadTask),
		"DescribeDatabase":      service.WrapOp(h.handleDescribeDatabase),
		"DescribeEndpoints":     service.WrapOp(h.handleDescribeEndpoints),
		"DescribeTable":         service.WrapOp(h.handleDescribeTable),
		"ListBatchLoadTasks":    service.WrapOp(h.handleListBatchLoadTasks),
		"ListDatabases":         service.WrapOp(h.handleListDatabases),
		"ListTables":            service.WrapOp(h.handleListTables),
		"ListTagsForResource":   service.WrapOp(h.handleListTagsForResource),
		"ResumeBatchLoadTask":   service.WrapOp(h.handleResumeBatchLoadTask),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
		"UpdateDatabase":        service.WrapOp(h.handleUpdateDatabase),
		"UpdateTable":           service.WrapOp(h.handleUpdateTable),
		"WriteRecords":          service.WrapOp(h.handleWriteRecords),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "TimestreamWrite" }

// StartWorker starts the background janitor for Timestream record retention.
func (h *Handler) StartWorker(ctx context.Context) error {
	janitor := NewJanitor(h.Backend)
	go janitor.Run(ctx)

	return nil
}

// GetSupportedOperations returns the list of supported Timestream Write operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateBatchLoadTask",
		"CreateDatabase",
		"CreateTable",
		"DeleteDatabase",
		"DeleteTable",
		"DescribeBatchLoadTask",
		"DescribeDatabase",
		"DescribeEndpoints",
		"DescribeTable",
		"ListBatchLoadTasks",
		"ListDatabases",
		"ListTables",
		"ListTagsForResource",
		"ResumeBatchLoadTask",
		"TagResource",
		"UntagResource",
		"UpdateDatabase",
		"UpdateTable",
		"WriteRecords",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "timestreamwrite" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler covers.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Timestream Write requests.
// It only matches operations explicitly supported by this handler to avoid
// intercepting operations belonging to other Timestream services (e.g. TimestreamQuery)
// that share the same X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if !strings.HasPrefix(target, targetPrefix) {
			return false
		}

		operation := strings.TrimPrefix(target, targetPrefix)

		return h.supportedOps[operation]
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Timestream Write action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, targetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource returns an empty string (no meaningful resource in request body for routing).
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		if service.IsCBORRequest(c.Request()) {
			return h.handleCBOR(c)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"TimestreamWrite", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) handleCBOR(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	if c.Request().Method != http.MethodPost {
		return c.String(http.StatusMethodNotAllowed, "Method not allowed")
	}

	const targetParts = 2

	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.SplitN(target, ".", targetParts)
	if len(parts) != targetParts || parts[1] == "" {
		return c.String(http.StatusBadRequest, "Missing or invalid X-Amz-Target")
	}
	action := parts[1]

	raw, err := readBodyBytes(c)
	if err != nil {
		log.ErrorContext(ctx, "failed to read CBOR body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	jsonBody, err := service.CBORToJSON(raw)
	if err != nil {
		log.ErrorContext(ctx, "failed to decode CBOR body", "error", err)

		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "SerializationException",
			keyMessageField: "invalid CBOR body: " + err.Error(),
		})
	}

	log.DebugContext(ctx, "TimestreamWrite CBOR request", "action", action)

	jsonResp, reqErr := h.dispatch(ctx, action, jsonBody)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	cborPayload, err := service.JSONToCBOR(jsonResp, nil)
	if err != nil {
		log.ErrorContext(ctx, "failed to encode CBOR response", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(http.StatusOK, service.ContentTypeCBOR, cborPayload)
}

func readBodyBytes(c *echo.Context) ([]byte, error) {
	r := c.Request()
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()

	const maxBody = 10 << 20 // 10 MiB

	return io.ReadAll(io.LimitReader(r.Body, maxBody))
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
	var rejectedErr *RejectedRecordsError

	switch {
	case errors.As(err, &rejectedErr):
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyTypeField:      "RejectedRecordsException",
			keyMessageField:   err.Error(),
			"RejectedRecords": rejectedErr.RejectedRecords,
		})
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		// The awsJson1.0 protocol has no per-exception HTTP status: every client-fault
		// error (including ConflictException) is reported over HTTP 400, and the SDK
		// determines the concrete exception type from the body's __type field, not the
		// status code (see aws-sdk-go-v2/service/timestreamwrite deserializers.go).
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ConflictException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter) ||
		errors.Is(err, errInvalidRequest) ||
		errors.Is(err, errUnknownAction) ||
		errors.As(err, &syntaxErr) ||
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ValidationException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServerException",
			keyMessageField: err.Error(),
		})
	}
}
