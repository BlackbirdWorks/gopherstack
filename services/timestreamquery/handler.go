package timestreamquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyArn = "Arn"
)

const (
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
)

const (
	timestreamQueryService      = "timestream"
	timestreamQueryTargetPrefix = "Timestream_20181101."
	contentType                 = "application/x-amz-json-1.0"
	endpointCachePeriod         = int64(1440)
)

// writeServiceTagOps returns the set of tag operations shared between the
// Timestream Write and Query services.  The Write service provides a unified
// tag store for all Timestream resource types, so the Query RouteMatcher must
// not claim these operations.
func writeServiceTagOps() map[string]bool {
	return map[string]bool{
		opTagResource:         true,
		opUntagResource:       true,
		opListTagsForResource: true,
	}
}

// ErrUnknownOperation is returned when an unrecognized operation is requested.
var ErrUnknownOperation = errors.New("unknown operation")

// Handler is the Echo HTTP handler for the Timestream Query service.
type Handler struct {
	Backend      StorageBackend
	supportedOps map[string]bool
}

// NewHandler creates a new Timestream Query handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	ops := h.GetSupportedOperations()
	h.supportedOps = make(map[string]bool, len(ops))

	for _, op := range ops {
		h.supportedOps[op] = true
	}

	return h
}

// Reset clears handler state, delegating to the backend if it supports Reset.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Name returns the handler name.
func (h *Handler) Name() string { return "TimestreamQuery" }

// GetSupportedOperations returns all supported Timestream Query operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CancelQuery",
		"CreateScheduledQuery",
		"DeleteScheduledQuery",
		"DescribeAccountSettings",
		"DescribeEndpoints",
		"DescribeScheduledQuery",
		"ExecuteScheduledQuery",
		"ListScheduledQueries",
		opListTagsForResource,
		"PrepareQuery",
		"Query",
		opTagResource,
		opUntagResource,
		"UpdateAccountSettings",
		"UpdateScheduledQuery",
	}
}

// ChaosServiceName returns the service name for chaos injection.
func (h *Handler) ChaosServiceName() string { return timestreamQueryService }

// ChaosOperations returns the operations subject to chaos injection.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the default region for chaos injection.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a matcher that identifies Timestream Query requests.
// It only matches operations explicitly supported by this handler to avoid
// intercepting operations belonging to other Timestream services (e.g. TimestreamWrite)
// that share the same X-Amz-Target prefix.  Tag operations (TagResource,
// UntagResource, ListTagsForResource) are intentionally excluded: they are
// routed to the TimestreamWrite handler which provides a single unified tag
// store for all Timestream resource types.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if !strings.HasPrefix(target, timestreamQueryTargetPrefix) {
			return false
		}

		operation := strings.TrimPrefix(target, timestreamQueryTargetPrefix)

		// Defer shared tag operations to the TimestreamWrite handler so that
		// database/table ARNs and scheduled-query ARNs all share the same tag
		// store under a single endpoint.
		if writeServiceTagOps()[operation] {
			return false
		}

		return h.supportedOps[operation]
	}
}

// MatchPriority returns the matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), timestreamQueryTargetPrefix)
}

// ExtractResource returns the ARN or name from the request body.
// It checks ScheduledQueryArn, ResourceARN, Arn, and Name fields in order.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ScheduledQueryArn string `json:"ScheduledQueryArn"`
		ResourceARN       string `json:"ResourceARN"`
		Arn               string `json:"Arn"`
		Name              string `json:"Name"`
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	if req.ScheduledQueryArn != "" {
		return req.ScheduledQueryArn
	}

	if req.ResourceARN != "" {
		return req.ResourceARN
	}

	if req.Arn != "" {
		return req.Arn
	}

	return req.Name
}

// Handler returns the Echo handler function for Timestream Query requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "timestreamquery: failed to read request body", "error", err)

			return h.handleError(c, err)
		}

		op := h.ExtractOperation(c)
		result, dispErr := h.dispatch(ctx, op, body, c.Request().Host)

		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		c.Response().Header().Set("Content-Type", contentType)

		if result == nil {
			return c.JSONBlob(http.StatusOK, []byte(`{}`))
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte, host string) ([]byte, error) {
	switch op {
	case "DescribeEndpoints":
		return h.handleDescribeEndpoints(host)
	case "Query":
		return h.handleQuery(ctx, body)
	case "CancelQuery":
		return h.handleCancelQuery(ctx, body)
	default:
		return h.dispatchScheduledQueryAndTagOps(ctx, op, body)
	}
}

func (h *Handler) dispatchScheduledQueryAndTagOps(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "CreateScheduledQuery":
		return h.handleCreateScheduledQuery(ctx, body)
	case "DeleteScheduledQuery":
		return h.handleDeleteScheduledQuery(ctx, body)
	case "DescribeScheduledQuery":
		return h.handleDescribeScheduledQuery(ctx, body)
	case "ExecuteScheduledQuery":
		return h.handleExecuteScheduledQuery(ctx, body)
	case "ListScheduledQueries":
		return h.handleListScheduledQueries(ctx, body)
	case "UpdateScheduledQuery":
		return h.handleUpdateScheduledQuery(ctx, body)
	case opTagResource:
		return h.handleTagResource(ctx, body)
	case opUntagResource:
		return h.handleUntagResource(ctx, body)
	case opListTagsForResource:
		return h.handleListTagsForResource(ctx, body)
	default:
		return h.dispatchAccountOps(ctx, op, body)
	}
}

func (h *Handler) dispatchAccountOps(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "DescribeAccountSettings":
		return h.handleDescribeAccountSettings(ctx)
	case "PrepareQuery":
		return h.handlePrepareQuery(ctx, body)
	case "UpdateAccountSettings":
		return h.handleUpdateAccountSettings(ctx, body)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, op)
	}
}

func (h *Handler) handleDescribeEndpoints(host string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"Endpoints": []map[string]any{
			{
				"Address":              host,
				"CachePeriodInMinutes": endpointCachePeriod,
			},
		},
	})
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	c.Response().Header().Set("Content-Type", contentType)

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(http.StatusConflict, errorPayload("ConflictException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ValidationException", err.Error()))
	case errors.Is(err, ErrUnknownOperation):
		return c.JSONBlob(http.StatusBadRequest, errorPayload("ValidationException", err.Error()))
	default:
		return c.JSONBlob(http.StatusInternalServerError, errorPayload("InternalServerException", err.Error()))
	}
}

func errorPayload(errType, msg string) []byte {
	b, _ := json.Marshal(map[string]string{
		"__type":  errType,
		"message": msg,
	})

	return b
}
