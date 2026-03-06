package resourcegroupstaggingapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// taggingTargetPrefix is the X-Amz-Target prefix for the Resource Groups Tagging API.
	taggingTargetPrefix = "ResourceGroupsTaggingAPI_20170126."
)

// ErrUnknownOperation is returned when the requested Tagging API operation is not supported.
var ErrUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for Resource Groups Tagging API operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Resource Groups Tagging API handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ResourceGroupsTaggingAPI" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"GetResources",
		"GetTagKeys",
		"GetTagValues",
		"TagResources",
		"UntagResources",
	}
}

// RouteMatcher returns a function that matches Resource Groups Tagging API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), taggingTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	op := strings.TrimPrefix(target, taggingTargetPrefix)
	if op == "" || op == target {
		return "Unknown"
	}

	return op
}

// ExtractResource returns an empty string (the tagging API has no single resource concept).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ResourceGroupsTaggingAPI", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetResources":   service.WrapOp(h.handleGetResources),
		"GetTagKeys":     service.WrapOp(h.handleGetTagKeys),
		"GetTagValues":   service.WrapOp(h.handleGetTagValues),
		"TagResources":   service.WrapOp(h.handleTagResources),
		"UntagResources": service.WrapOp(h.handleUntagResources),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, ErrUnknownOperation
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

	code := http.StatusInternalServerError
	errType := "InternalFailure"

	switch {
	case errors.Is(err, ErrUnknownOperation):
		code = http.StatusBadRequest
		errType = "UnknownOperationException"
	case errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		code = http.StatusBadRequest
		errType = "ValidationException"
	}

	payload, _ := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	return c.JSONBlob(code, payload)
}

type getTagKeysInput struct{}

func (h *Handler) handleGetResources(_ context.Context, in *GetResourcesInput) (*GetResourcesOutput, error) {
	return h.Backend.GetResources(in), nil
}

func (h *Handler) handleGetTagKeys(_ context.Context, _ *getTagKeysInput) (*GetTagKeysOutput, error) {
	return h.Backend.GetTagKeys(), nil
}

func (h *Handler) handleGetTagValues(_ context.Context, in *GetTagValuesInput) (*GetTagValuesOutput, error) {
	return h.Backend.GetTagValues(in), nil
}

func (h *Handler) handleTagResources(_ context.Context, in *TagResourcesInput) (*TagResourcesOutput, error) {
	return h.Backend.TagResources(in), nil
}

func (h *Handler) handleUntagResources(_ context.Context, in *UntagResourcesInput) (*UntagResourcesOutput, error) {
	return h.Backend.UntagResources(in), nil
}
