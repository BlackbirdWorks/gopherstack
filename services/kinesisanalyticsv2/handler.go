package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// targetPrefix is the X-Amz-Target prefix for the Kinesis Analytics v2 JSON protocol.
	targetPrefix = "KinesisAnalytics_20180523."
)

// Handler is the HTTP handler for the Kinesis Data Analytics v2 JSON API.
type Handler struct {
	Backend StorageBackend
	ops     map[string]func(context.Context, *echo.Context, []byte) error
}

// NewHandler creates a new Kinesis Data Analytics v2 handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears handler state by delegating to the backend if it supports it.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "KinesisAnalyticsV2" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"AddApplicationVpcConfiguration",
		"CreateApplication",
		"CreateApplicationPresignedUrl",
		"DeleteApplication",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
		"DeleteApplicationSnapshot",
		"DescribeApplication",
		"DescribeApplicationSnapshot",
		"ListApplications",
		"ListApplicationSnapshots",
		"ListTagsForResource",
		"StartApplication",
		"StopApplication",
		"TagResource",
		"UntagResource",
		"UpdateApplication",
		"CreateApplicationSnapshot",
		"DeleteApplicationReferenceDataSource",
		"DeleteApplicationVpcConfiguration",
		"DescribeApplicationOperation",
		"DescribeApplicationVersion",
		"DiscoverInputSchema",
		"ListApplicationOperations",
		"ListApplicationVersions",
		"RollbackApplication",
		"UpdateApplicationMaintenanceConfiguration",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "kinesisanalyticsv2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Kinesis Data Analytics v2 requests.
// The SDK uses X-Amz-Target: KinesisAnalytics_20180523.{Operation} with POST to /.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, targetPrefix) {
		return ""
	}

	return strings.TrimPrefix(target, targetPrefix)
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ApplicationName string `json:"ApplicationName"`
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ApplicationName
}

// Handler returns the Echo handler function for Kinesis Data Analytics v2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := h.contextWithRegion(c)
		log := logger.Load(ctx)

		op := h.ExtractOperation(c)
		if op == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "missing X-Amz-Target header")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "kinesisanalyticsv2: failed to read request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "kinesisanalyticsv2 request", "op", op)

		fn, ok := h.ops[op]
		if !ok {
			return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "unknown operation: "+op)
		}

		return fn(ctx, c, body)
	}
}

// contextWithRegion returns the request context with the resolved AWS region attached
// under regionContextKey so that backend operations are routed to the correct region.
// The SigV4 credential-scope region in the Authorization header (extracted by
// httputils.ExtractRegionFromRequest) takes precedence over the backend default.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

// buildOps constructs the dispatch map once at handler-creation time.
func (h *Handler) buildOps() map[string]func(context.Context, *echo.Context, []byte) error {
	return map[string]func(context.Context, *echo.Context, []byte) error{
		// Add operations
		"AddApplicationCloudWatchLoggingOption":      h.handleAddApplicationCloudWatchLoggingOption,
		"AddApplicationInput":                        h.handleAddApplicationInput,
		"AddApplicationInputProcessingConfiguration": h.handleAddApplicationInputProcessingConfiguration,
		"AddApplicationOutput":                       h.handleAddApplicationOutput,
		"AddApplicationReferenceDataSource":          h.handleAddApplicationReferenceDataSource,
		"AddApplicationVpcConfiguration":             h.handleAddApplicationVpcConfiguration,
		// Delete operations
		"DeleteApplicationCloudWatchLoggingOption":      h.handleDeleteApplicationCloudWatchLoggingOption,
		"DeleteApplicationInputProcessingConfiguration": h.handleDeleteApplicationInputProcessingConfiguration,
		"DeleteApplicationOutput":                       h.handleDeleteApplicationOutput,
		"DeleteApplication":                             h.handleDeleteApplication,
		"DeleteApplicationSnapshot":                     h.handleDeleteApplicationSnapshot,
		// Core operations
		"CreateApplication":             h.handleCreateApplication,
		"CreateApplicationPresignedUrl": h.handleCreateApplicationPresignedURL,
		"CreateApplicationSnapshot":     h.handleCreateApplicationSnapshot,
		"DescribeApplication":           h.handleDescribeApplication,
		"DescribeApplicationSnapshot":   h.handleDescribeApplicationSnapshot,
		"ListApplications":              h.handleListApplications,
		"ListApplicationSnapshots":      h.handleListApplicationSnapshots,
		"ListTagsForResource":           h.handleListTagsForResource,
		"StartApplication":              h.handleStartApplication,
		"StopApplication":               h.handleStopApplication,
		"TagResource":                   h.handleTagResource,
		"UntagResource":                 h.handleUntagResource,
		"UpdateApplication":             h.handleUpdateApplication,
		// New operations
		"DeleteApplicationReferenceDataSource":      h.handleDeleteApplicationReferenceDataSource,
		"DeleteApplicationVpcConfiguration":         h.handleDeleteApplicationVpcConfiguration,
		"DescribeApplicationOperation":              h.handleDescribeApplicationOperation,
		"DescribeApplicationVersion":                h.handleDescribeApplicationVersion,
		"DiscoverInputSchema":                       h.handleDiscoverInputSchema,
		"ListApplicationOperations":                 h.handleListApplicationOperations,
		"ListApplicationVersions":                   h.handleListApplicationVersions,
		"RollbackApplication":                       h.handleRollbackApplication,
		"UpdateApplicationMaintenanceConfiguration": h.handleUpdateApplicationMaintenanceConfiguration,
	}
}

type errorResponse struct {
	Message string `json:"message"`
	Code    string `json:"__type"`
}

func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, errorResponse{
		Message: message,
		Code:    code,
	})
}

// handleError maps a backend error to the appropriate HTTP response.
//
// ErrConcurrentModification wraps awserr.ErrInvalidParameter (see errors.go),
// so its case must be checked before the generic ErrInvalidParameter case
// below -- otherwise every optimistic-concurrency conflict would be reported
// to the client as the generic "InvalidArgumentException" instead of
// "ConcurrentModificationException", which is the exception name aws-sdk-go-v2
// switches on (via the __type response field) to construct
// *types.ConcurrentModificationException for caller retry logic. Sibling
// service kinesisanalytics (v1) maps the same condition to
// (400, "ConcurrentModificationException"); kinesisanalyticsv2 follows suit.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "ResourceInUseException", err.Error())
	case errors.Is(err, ErrConcurrentModification):
		return h.writeError(c, http.StatusBadRequest, "ConcurrentModificationException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}
