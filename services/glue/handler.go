package glue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const glueTargetPrefix = "AWSGlue."

const unknownAction = "Unknown"

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for AWS Glue operations.
type Handler struct {
	Backend StorageBackend
	// ops is the pre-built dispatch table mapping operation names to handler
	// functions, initialized in NewHandler.
	ops map[string]service.JSONOpFunc
}

// NewHandler creates a new Glue handler backed by backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state. Used for test isolation.
func (h *Handler) Reset() { h.Backend.Reset() }

// StartWorker implements service.BackgroundWorker. It starts the managed lifecycle
// reconciler using the framework-provided background context, so no
// context.Background() is introduced.
func (h *Handler) StartWorker(ctx context.Context) error {
	h.Backend.StartReconciler(ctx)

	return nil
}

// Shutdown implements service.Shutdowner. It stops the reconciler and waits for its
// goroutine to exit, guaranteeing a clean, leak-free shutdown.
func (h *Handler) Shutdown(_ context.Context) {
	h.Backend.StopReconciler()
}

// Ensure Handler satisfies the optional background-lifecycle interfaces.

var _ service.BackgroundWorker = (*Handler)(nil)

var _ service.Shutdowner = (*Handler)(nil)

// Name returns the service name.
func (h *Handler) Name() string { return glueServiceName }

// GetSupportedOperations returns the list of supported Glue operations.
func (h *Handler) GetSupportedOperations() []string {
	names := make([]string, len(glueOpBindings))
	for i, b := range glueOpBindings {
		names[i] = b.name
	}

	return names
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "glue" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Glue requests via X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, glueTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, glueTargetPrefix)

	if action == "" || action == target {
		return unknownAction
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		Name           string `json:"Name"`
		DatabaseName   string `json:"DatabaseName"`
		ResourceArn    string `json:"ResourceArn"`
		CrawlerName    string `json:"CrawlerName"`
		JobName        string `json:"JobName"`
		ConnectionName string `json:"ConnectionName"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ResourceArn != "":
		return req.ResourceArn
	case req.Name != "":
		return req.Name
	case req.CrawlerName != "":
		return req.CrawlerName
	case req.JobName != "":
		return req.JobName
	case req.ConnectionName != "":
		return req.ConnectionName
	case req.DatabaseName != "":
		return req.DatabaseName
	}

	return ""
}

// Handler returns the Echo handler function for Glue requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			glueServiceName, "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := make(map[string]service.JSONOpFunc, len(glueOpBindings))
	for _, b := range glueOpBindings {
		ops[b.name] = b.bind(h)
	}

	return ops
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
	switch {
	case errors.Is(err, ErrCrawlerRunning):
		return c.JSON(http.StatusBadRequest, errorResponse("CrawlerRunningException", err.Error()))
	case errors.Is(err, ErrCrawlerNotRunning):
		return c.JSON(http.StatusBadRequest, errorResponse("CrawlerNotRunningException", err.Error()))
	case errors.Is(err, ErrConnectionTypeBuiltIn):
		return c.JSON(http.StatusBadRequest, errorResponse("AccessDeniedException", err.Error()))
	case errors.Is(err, ErrResourcePolicyConditionFailed):
		return c.JSON(http.StatusBadRequest, errorResponse("ConditionCheckFailureException", err.Error()))
	case errors.Is(err, ErrConcurrentRunsExceeded):
		return c.JSON(http.StatusBadRequest, errorResponse("ConcurrentRunsExceededException", err.Error()))
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("EntityNotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("AlreadyExistsException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidInputException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, errorResponse("UnknownOperationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// paginateSlice applies NextToken-based pagination to a sorted slice.
// It returns the page and the next token (empty string when no more pages).
func paginateSlice[T any](items []T, nextToken string, limit int) ([]T, string) {
	start := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx > 0 && idx < len(items) {
			start = idx
		}
	}

	end := start + limit
	if end >= len(items) {
		return items[start:], ""
	}

	return items[start:end], strconv.Itoa(end)
}

type emptyOutput struct{}
