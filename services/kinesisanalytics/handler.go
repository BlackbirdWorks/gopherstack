package kinesisanalytics

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
	kinesisanalyticsTargetPrefix = "KinesisAnalytics_20150814."
	kinesisanalyticsService      = "kinesisanalytics"
	errInvalidArgumentException  = "InvalidArgumentException"
	errLimitExceededException    = "LimitExceededException"

	// nanosPerSecond converts nanoseconds to seconds as a float64 divisor.
	nanosPerSecond = 1e9
)

var (
	errUnknownAction   = errors.New("unknown action")
	errApplicationName = errors.New("ApplicationName is required")
	errResourceARN     = errors.New("ResourceARN is required")
	errInputID         = errors.New("InputId is required")
	errOutputID        = errors.New("OutputId is required")
	errReferenceID     = errors.New("ReferenceId is required")
	errCWLOptionID     = errors.New("CloudWatchLoggingOptionId is required")
)

// Handler is the HTTP handler for the Kinesis Analytics v1 API.
type Handler struct {
	ops           map[string]service.JSONOpFunc
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Kinesis Analytics handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears handler state (delegates to backend if supported).
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// buildOps constructs the dispatch map once at handler creation time.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateApplication":                     service.WrapOp(h.handleCreateApplication),
		"DeleteApplication":                     service.WrapOp(h.handleDeleteApplication),
		"DescribeApplication":                   service.WrapOp(h.handleDescribeApplication),
		"ListApplications":                      service.WrapOp(h.handleListApplications),
		"StartApplication":                      service.WrapOp(h.handleStartApplication),
		"StopApplication":                       service.WrapOp(h.handleStopApplication),
		"UpdateApplication":                     service.WrapOp(h.handleUpdateApplication),
		"ListTagsForResource":                   service.WrapOp(h.handleListTagsForResource),
		"TagResource":                           service.WrapOp(h.handleTagResource),
		"UntagResource":                         service.WrapOp(h.handleUntagResource),
		"AddApplicationCloudWatchLoggingOption": service.WrapOp(h.handleAddApplicationCloudWatchLoggingOption),
		"AddApplicationInput":                   service.WrapOp(h.handleAddApplicationInput),
		"AddApplicationInputProcessingConfiguration": service.WrapOp(
			h.handleAddApplicationInputProcessingConfiguration,
		),
		"AddApplicationOutput":              service.WrapOp(h.handleAddApplicationOutput),
		"AddApplicationReferenceDataSource": service.WrapOp(h.handleAddApplicationReferenceDataSource),
		"DeleteApplicationCloudWatchLoggingOption": service.WrapOp(
			h.handleDeleteApplicationCloudWatchLoggingOption,
		),
		"DeleteApplicationInputProcessingConfiguration": service.WrapOp(
			h.handleDeleteApplicationInputProcessingConfiguration,
		),
		"DeleteApplicationOutput":              service.WrapOp(h.handleDeleteApplicationOutput),
		"DeleteApplicationReferenceDataSource": service.WrapOp(h.handleDeleteApplicationReferenceDataSource),
		"DiscoverInputSchema":                  service.WrapOp(h.handleDiscoverInputSchema),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "KinesisAnalytics" }

// GetSupportedOperations returns the list of supported Kinesis Analytics operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"DeleteApplication",
		"DescribeApplication",
		"ListApplications",
		"StartApplication",
		"StopApplication",
		"UpdateApplication",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
		"DeleteApplicationReferenceDataSource",
		"DiscoverInputSchema",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return kinesisanalyticsService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Kinesis Analytics requests by X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), kinesisanalyticsTargetPrefix)
	}
}

// MatchPriority returns the routing priority for header-based matching.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Kinesis Analytics action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, kinesisanalyticsTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type applicationNameInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req applicationNameInput
	_ = json.Unmarshal(body, &req)

	return req.ApplicationName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"KinesisAnalytics", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
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

	var code string
	var status int

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		status = http.StatusNotFound
		code = "ResourceNotFoundException"
	case errors.Is(err, awserr.ErrAlreadyExists):
		status = http.StatusBadRequest
		code = "ResourceInUseException"
	case errors.Is(err, awserr.ErrInvalidParameter):
		status = http.StatusBadRequest
		code = errInvalidArgumentException
	case errors.Is(err, ErrConcurrentUpdate):
		status = http.StatusBadRequest
		code = "ConcurrentModificationException"
	case errors.Is(err, ErrTooManyTags):
		// Must precede the generic awserr.ErrConflict case below: AWS models tag-limit
		// overflow on CreateApplication/TagResource as a distinct TooManyTagsException,
		// not the generic LimitExceededException.
		status = http.StatusBadRequest
		code = "TooManyTagsException"
	case errors.Is(err, awserr.ErrConflict):
		status = http.StatusBadRequest
		code = errLimitExceededException
	case errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		status = http.StatusBadRequest
		code = errInvalidArgumentException
	case errors.Is(err, errApplicationName),
		errors.Is(err, errResourceARN),
		errors.Is(err, errInputID),
		errors.Is(err, errOutputID),
		errors.Is(err, errReferenceID),
		errors.Is(err, errCWLOptionID):
		status = http.StatusBadRequest
		code = errInvalidArgumentException
	default:
		status = http.StatusInternalServerError
		code = "InternalServiceException"
	}

	c.Response().Header().Set("X-Amzn-Errortype", code)

	return c.JSON(status, errorResponse{Type: code, Message: err.Error()})
}
