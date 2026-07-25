package resourcegroupstaggingapi

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
	// taggingTargetPrefix is the X-Amz-Target prefix for the Resource Groups Tagging API.
	taggingTargetPrefix = "ResourceGroupsTaggingAPI_20170126."
)

// ErrUnknownOperation is returned when the requested Tagging API operation is not supported.
var ErrUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for Resource Groups Tagging API operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Resource Groups Tagging API handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears handler state by delegating to the backend if it supports it.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(Resettable); ok {
		r.Reset()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ResourceGroupsTaggingAPI" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"DescribeReportCreation",
		"GetComplianceSummary",
		"GetResources",
		"GetTagKeys",
		"GetTagValues",
		"ListRequiredTags",
		"StartReportCreation",
		"TagResources",
		"UntagResources",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "tagging" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Resource Groups Tagging API instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

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
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ResourceGroupsTaggingAPI", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// buildOps constructs the dispatch map once at handler-creation time.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DescribeReportCreation": service.WrapOp(h.handleDescribeReportCreation),
		"GetComplianceSummary":   service.WrapOp(h.handleGetComplianceSummary),
		"GetResources":           service.WrapOp(h.handleGetResources),
		"GetTagKeys":             service.WrapOp(h.handleGetTagKeys),
		"GetTagValues":           service.WrapOp(h.handleGetTagValues),
		"ListRequiredTags":       service.WrapOp(h.handleListRequiredTags),
		"StartReportCreation":    service.WrapOp(h.handleStartReportCreation),
		"TagResources":           service.WrapOp(h.handleTagResources),
		"UntagResources":         service.WrapOp(h.handleUntagResources),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	case errors.Is(err, ErrMissingS3Bucket), errors.Is(err, ErrValidation):
		// Real resourcegroupstaggingapi has no "ValidationException" shape; parameter
		// validation failures are modeled as InvalidParameterException (see
		// aws-sdk-go-v2/service/resourcegroupstaggingapi/types/errors.go).
		code = http.StatusBadRequest
		errType = errCodeInvalidParameter
	case errors.Is(err, ErrConcurrentModification):
		code = http.StatusConflict
		errType = "ConcurrentModificationException"
	case errors.Is(err, ErrPaginationTokenExpired):
		code = http.StatusBadRequest
		errType = "PaginationTokenExpiredException"
	case errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		// Malformed request bodies are a protocol-level failure, not a modeled
		// operation error; the AWS JSON 1.1 protocol reports these as
		// SerializationException (matches services/organizations' convention for
		// the same protocol).
		code = http.StatusBadRequest
		errType = "SerializationException"
	}

	payload, _ := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	return c.JSONBlob(code, payload)
}

func (h *Handler) handleGetResources(ctx context.Context, in *GetResourcesInput) (*GetResourcesOutput, error) {
	return h.Backend.GetResources(ctx, in)
}

func (h *Handler) handleGetTagKeys(ctx context.Context, in *GetTagKeysInput) (*GetTagKeysOutput, error) {
	return h.Backend.GetTagKeys(ctx, in)
}

func (h *Handler) handleGetTagValues(ctx context.Context, in *GetTagValuesInput) (*GetTagValuesOutput, error) {
	if in.Key == nil || *in.Key == "" {
		return nil, fmt.Errorf("%w: Key is required for GetTagValues", ErrValidation)
	}

	return h.Backend.GetTagValues(ctx, in)
}

func (h *Handler) handleTagResources(ctx context.Context, in *TagResourcesInput) (*TagResourcesOutput, error) {
	return h.Backend.TagResources(ctx, in)
}

func (h *Handler) handleUntagResources(ctx context.Context, in *UntagResourcesInput) (*UntagResourcesOutput, error) {
	return h.Backend.UntagResources(ctx, in)
}

func (h *Handler) handleStartReportCreation(
	ctx context.Context,
	in *StartReportCreationInput,
) (*StartReportCreationOutput, error) {
	return h.Backend.StartReportCreation(ctx, in)
}

func (h *Handler) handleDescribeReportCreation(
	ctx context.Context,
	_ *DescribeReportCreationInput,
) (*DescribeReportCreationOutput, error) {
	return h.Backend.DescribeReportCreation(ctx), nil
}

func (h *Handler) handleGetComplianceSummary(
	ctx context.Context,
	in *GetComplianceSummaryInput,
) (*GetComplianceSummaryOutput, error) {
	for _, g := range in.GroupBy {
		if !isValidGroupByValue(g) {
			return nil, fmt.Errorf(
				"%w: invalid GroupBy value %q; valid values are REGION, RESOURCE_TYPE, TARGET_ID",
				ErrValidation, g,
			)
		}
	}

	return h.Backend.GetComplianceSummary(ctx, in)
}

func (h *Handler) handleListRequiredTags(
	ctx context.Context,
	in *ListRequiredTagsInput,
) (*ListRequiredTagsOutput, error) {
	return h.Backend.ListRequiredTags(ctx, in), nil
}
