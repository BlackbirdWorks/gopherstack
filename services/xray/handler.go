package xray

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyTypeField = "__type"

	defaultResourcePoliciesPageSize = 25
	defaultGroupsPageSize           = 25
	defaultSamplingRulesPageSize    = 25
	defaultInsightEventsPageSize    = 50
	defaultInsightSummariesPageSize = 50
	defaultIndexingRulesPageSize    = 25
	defaultTraceSummariesPageSize   = 100
	defaultSamplingStatsPageSize    = 25

	maxTraceSegmentsPerCall = 50
	maxSegmentDocumentBytes = 64 * 1024

	timeRangeTypeTraceID = "TraceId"
	timeRangeTypeEvent   = "Event"
	timeRangeTypeService = "Service"
)

const (
	opGetEncryptionConfig      = "GetEncryptionConfig"
	errInvalidRequestException = "InvalidRequestException"
	keyMessageField            = "message"
	keyEncryptionConfig        = "EncryptionConfig"
	keyGroup                   = "Group"
	keyNextToken               = "NextToken"
	keySamplingRuleRecord      = "SamplingRuleRecord"
	keyServices                = "Services"
)

const (
	pathEncryptionConfig    = "/EncryptionConfig"
	pathPutEncryptionConfig = "/PutEncryptionConfig"
)

const (
	pathTraceSegments        = "/TraceSegments"
	pathTelemetryRecords     = "/TelemetryRecords"
	pathTraceSummaries       = "/TraceSummaries"
	pathTraces               = "/Traces"
	pathCreateGroup          = "/CreateGroup"
	pathGetGroup             = "/GetGroup"
	pathGroups               = "/Groups"
	pathUpdateGroup          = "/UpdateGroup"
	pathDeleteGroup          = "/DeleteGroup"
	pathCreateSamplingRule   = "/CreateSamplingRule"
	pathGetSamplingRules     = "/GetSamplingRules"
	pathUpdateSamplingRule   = "/UpdateSamplingRule"
	pathDeleteSamplingRule   = "/DeleteSamplingRule"
	pathCancelTraceRetrieval = "/CancelTraceRetrieval"
	pathDeleteResourcePolicy = "/DeleteResourcePolicy"
	pathListResourcePolicies = "/ListResourcePolicies"
	pathPutResourcePolicy    = "/PutResourcePolicy"
	pathGetIndexingRules     = "/GetIndexingRules"
	// pathGetInsight, pathGetInsightEvents, pathGetInsightImpactGraph,
	// pathGetInsightSummaries, pathGetSamplingStatisticSummaries, and
	// pathGetSamplingTargets deliberately do NOT start with "Get" -- these
	// are the actual REST paths the aws-sdk-go-v2 xray client serializers
	// send (verified against service/xray@v1.36.20 serializers.go), even
	// though the operation names themselves are prefixed with "Get".
	pathGetInsight                     = "/Insight"
	pathGetInsightEvents               = "/InsightEvents"
	pathGetInsightImpactGraph          = "/InsightImpactGraph"
	pathGetInsightSummaries            = "/InsightSummaries"
	pathGetRetrievedTracesGraph        = "/GetRetrievedTracesGraph"
	pathGetSamplingStatisticSummaries  = "/SamplingStatisticSummaries"
	pathGetSamplingTargets             = "/SamplingTargets"
	pathGetServiceGraph                = "/ServiceGraph"
	pathGetTimeSeriesServiceStatistics = "/TimeSeriesServiceStatistics"
	pathGetTraceGraph                  = "/TraceGraph"
	pathGetTraceSegmentDestination     = "/GetTraceSegmentDestination"
	pathListRetrievedTraces            = "/ListRetrievedTraces"
	pathListTagsForResource            = "/ListTagsForResource"
	pathStartTraceRetrieval            = "/StartTraceRetrieval"
	pathTagResource                    = "/TagResource"
	pathUntagResource                  = "/UntagResource"
	pathUpdateIndexingRule             = "/UpdateIndexingRule"
	pathUpdateTraceSegmentDestination  = "/UpdateTraceSegmentDestination"
)

var (
	errUnknownPath    = errors.New("unknown path")
	errInvalidRequest = errors.New("invalid request")
)

// xrayPaths is the set of supported X-Ray REST API paths.
var xrayPaths = map[string]bool{ //nolint:gochecknoglobals // package-level routing table
	pathTraceSegments:                  true,
	pathTelemetryRecords:               true,
	pathTraceSummaries:                 true,
	pathTraces:                         true,
	pathCreateGroup:                    true,
	pathGetGroup:                       true,
	pathGroups:                         true,
	pathUpdateGroup:                    true,
	pathDeleteGroup:                    true,
	pathCreateSamplingRule:             true,
	pathGetSamplingRules:               true,
	pathUpdateSamplingRule:             true,
	pathDeleteSamplingRule:             true,
	pathEncryptionConfig:               true,
	pathPutEncryptionConfig:            true,
	pathCancelTraceRetrieval:           true,
	pathDeleteResourcePolicy:           true,
	pathListResourcePolicies:           true,
	pathPutResourcePolicy:              true,
	pathGetIndexingRules:               true,
	pathGetInsight:                     true,
	pathGetInsightEvents:               true,
	pathGetInsightImpactGraph:          true,
	pathGetInsightSummaries:            true,
	pathGetRetrievedTracesGraph:        true,
	pathGetSamplingStatisticSummaries:  true,
	pathGetSamplingTargets:             true,
	pathGetServiceGraph:                true,
	pathGetTimeSeriesServiceStatistics: true,
	pathGetTraceGraph:                  true,
	pathGetTraceSegmentDestination:     true,
	pathListRetrievedTraces:            true,
	pathListTagsForResource:            true,
	pathStartTraceRetrieval:            true,
	pathTagResource:                    true,
	pathUntagResource:                  true,
	pathUpdateIndexingRule:             true,
	pathUpdateTraceSegmentDestination:  true,
}

// pathToOperation maps X-Ray REST API paths to operation names.
var pathToOperation = map[string]string{ //nolint:gochecknoglobals // package-level routing table
	pathTraceSegments:                  "PutTraceSegments",
	pathTelemetryRecords:               "PutTelemetryRecords",
	pathTraceSummaries:                 "GetTraceSummaries",
	pathTraces:                         "BatchGetTraces",
	pathCreateGroup:                    "CreateGroup",
	pathGetGroup:                       "GetGroup",
	pathGroups:                         "GetGroups",
	pathUpdateGroup:                    "UpdateGroup",
	pathDeleteGroup:                    "DeleteGroup",
	pathCreateSamplingRule:             "CreateSamplingRule",
	pathGetSamplingRules:               "GetSamplingRules",
	pathUpdateSamplingRule:             "UpdateSamplingRule",
	pathDeleteSamplingRule:             "DeleteSamplingRule",
	pathEncryptionConfig:               opGetEncryptionConfig,
	pathPutEncryptionConfig:            "PutEncryptionConfig",
	pathCancelTraceRetrieval:           "CancelTraceRetrieval",
	pathDeleteResourcePolicy:           "DeleteResourcePolicy",
	pathListResourcePolicies:           "ListResourcePolicies",
	pathPutResourcePolicy:              "PutResourcePolicy",
	pathGetIndexingRules:               "GetIndexingRules",
	pathGetInsight:                     "GetInsight",
	pathGetInsightEvents:               "GetInsightEvents",
	pathGetInsightImpactGraph:          "GetInsightImpactGraph",
	pathGetInsightSummaries:            "GetInsightSummaries",
	pathGetRetrievedTracesGraph:        "GetRetrievedTracesGraph",
	pathGetSamplingStatisticSummaries:  "GetSamplingStatisticSummaries",
	pathGetSamplingTargets:             "GetSamplingTargets",
	pathGetServiceGraph:                "GetServiceGraph",
	pathGetTimeSeriesServiceStatistics: "GetTimeSeriesServiceStatistics",
	pathGetTraceGraph:                  "GetTraceGraph",
	pathGetTraceSegmentDestination:     "GetTraceSegmentDestination",
	pathListRetrievedTraces:            "ListRetrievedTraces",
	pathListTagsForResource:            "ListTagsForResource",
	pathStartTraceRetrieval:            "StartTraceRetrieval",
	pathTagResource:                    "TagResource",
	pathUntagResource:                  "UntagResource",
	pathUpdateIndexingRule:             "UpdateIndexingRule",
	pathUpdateTraceSegmentDestination:  "UpdateTraceSegmentDestination",
}

// Handler is the Echo HTTP handler for AWS X-Ray operations.
type Handler struct {
	Backend StorageBackend
	janitor *Janitor
}

// NewHandler creates a new X-Ray handler backed by backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler.
// The optional taskTimeout variadic parameter sets TaskTimeout on the janitor.
// If Backend is not an *InMemoryBackend the call is a no-op.
func (h *Handler) WithJanitor(interval, ttl time.Duration, taskTimeout ...time.Duration) *Handler {
	concrete, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h
	}

	j := NewJanitor(concrete, interval, ttl)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}
	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string { return "Xray" }

// GetSupportedOperations returns the list of supported X-Ray operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchGetTraces",
		"CancelTraceRetrieval",
		"CreateGroup",
		"CreateSamplingRule",
		"DeleteGroup",
		"DeleteResourcePolicy",
		"DeleteSamplingRule",
		opGetEncryptionConfig,
		"GetGroup",
		"GetGroups",
		"GetIndexingRules",
		"GetInsight",
		"GetInsightEvents",
		"GetInsightImpactGraph",
		"GetInsightSummaries",
		"GetRetrievedTracesGraph",
		"GetSamplingRules",
		"GetSamplingStatisticSummaries",
		"GetSamplingTargets",
		"GetServiceGraph",
		"GetTimeSeriesServiceStatistics",
		"GetTraceGraph",
		"GetTraceSegmentDestination",
		"GetTraceSummaries",
		"ListResourcePolicies",
		"ListRetrievedTraces",
		"ListTagsForResource",
		"PutEncryptionConfig",
		"PutResourcePolicy",
		"PutTelemetryRecords",
		"PutTraceSegments",
		"StartTraceRetrieval",
		"TagResource",
		"UntagResource",
		"UpdateGroup",
		"UpdateIndexingRule",
		"UpdateSamplingRule",
		"UpdateTraceSegmentDestination",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "xray" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this X-Ray instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches X-Ray REST API requests.
// X-Ray uses POST with specific well-known paths, except /EncryptionConfig which also accepts GET.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		if !xrayPaths[path] {
			return false
		}

		// /EncryptionConfig accepts both GET (GetEncryptionConfig) and POST (PutEncryptionConfig).
		if path == pathEncryptionConfig {
			return c.Request().Method == http.MethodGet || c.Request().Method == http.MethodPost
		}

		return c.Request().Method == http.MethodPost
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the X-Ray operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path

	// POST /EncryptionConfig and GET /EncryptionConfig both map to
	// GetEncryptionConfig; PutEncryptionConfig uses POST /PutEncryptionConfig.
	if path == pathEncryptionConfig {
		return opGetEncryptionConfig
	}

	op, ok := pathToOperation[path]
	if !ok {
		return "Unknown"
	}

	return op
}

// ExtractResource extracts the primary resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		GroupName string `json:"GroupName"`
		RuleName  string `json:"RuleName"`
	}

	_ = json.Unmarshal(body, &req)

	if req.GroupName != "" {
		return req.GroupName
	}

	return req.RuleName
}

// Handler returns the Echo handler function for X-Ray requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		path := c.Request().URL.Path

		if !xrayPaths[path] {
			return h.handleError(c, path, fmt.Errorf("%w: %s", errUnknownPath, path))
		}

		// GET /EncryptionConfig → GetEncryptionConfig (no body). POST /EncryptionConfig
		// also maps to GetEncryptionConfig via the dispatch table; PutEncryptionConfig
		// is served from the distinct /PutEncryptionConfig path.
		if path == pathEncryptionConfig && c.Request().Method == http.MethodGet {
			return h.handleGetEncryptionConfig(c)
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read request body", "error", err)

			return h.handleError(c, path, err)
		}

		op := h.ExtractOperation(c)
		log.DebugContext(ctx, "xray request", "operation", op, "path", path)

		resp, dispatchErr := h.dispatch(ctx, path, body)
		if dispatchErr != nil {
			return h.handleError(c, op, dispatchErr)
		}

		c.Response().Header().Set("Content-Type", "application/json")

		return c.JSONBlob(http.StatusOK, resp)
	}
}

// xrayHandlerFn is the type for X-Ray path handler functions.
type xrayHandlerFn func(*Handler, context.Context, []byte) ([]byte, error)

// dispatchTable maps X-Ray paths to their handler functions (POST operations).
// This table-driven approach keeps the dispatch cyclomatic complexity at O(1).
var dispatchTable = map[string]xrayHandlerFn{ //nolint:gochecknoglobals // package-level dispatch table
	pathTraceSegments:                  (*Handler).handlePutTraceSegments,
	pathTelemetryRecords:               (*Handler).handlePutTelemetryRecords,
	pathTraceSummaries:                 (*Handler).handleGetTraceSummaries,
	pathTraces:                         (*Handler).handleBatchGetTraces,
	pathCreateGroup:                    (*Handler).handleCreateGroup,
	pathGetGroup:                       (*Handler).handleGetGroup,
	pathGroups:                         (*Handler).handleGetGroups,
	pathUpdateGroup:                    (*Handler).handleUpdateGroup,
	pathDeleteGroup:                    (*Handler).handleDeleteGroup,
	pathCreateSamplingRule:             (*Handler).handleCreateSamplingRule,
	pathGetSamplingRules:               (*Handler).handleGetSamplingRules,
	pathUpdateSamplingRule:             (*Handler).handleUpdateSamplingRule,
	pathDeleteSamplingRule:             (*Handler).handleDeleteSamplingRule,
	pathEncryptionConfig:               (*Handler).handleGetEncryptionConfigBody,
	pathPutEncryptionConfig:            (*Handler).handlePutEncryptionConfig,
	pathCancelTraceRetrieval:           (*Handler).handleCancelTraceRetrieval,
	pathDeleteResourcePolicy:           (*Handler).handleDeleteResourcePolicy,
	pathListResourcePolicies:           (*Handler).handleListResourcePolicies,
	pathPutResourcePolicy:              (*Handler).handlePutResourcePolicy,
	pathGetIndexingRules:               (*Handler).handleGetIndexingRules,
	pathGetInsight:                     (*Handler).handleGetInsight,
	pathGetInsightEvents:               (*Handler).handleGetInsightEvents,
	pathGetInsightImpactGraph:          (*Handler).handleGetInsightImpactGraph,
	pathGetInsightSummaries:            (*Handler).handleGetInsightSummaries,
	pathGetRetrievedTracesGraph:        (*Handler).handleGetRetrievedTracesGraph,
	pathGetSamplingStatisticSummaries:  (*Handler).handleGetSamplingStatisticSummaries,
	pathGetSamplingTargets:             (*Handler).handleGetSamplingTargets,
	pathGetServiceGraph:                (*Handler).handleGetServiceGraph,
	pathGetTimeSeriesServiceStatistics: (*Handler).handleGetTimeSeriesServiceStatistics,
	pathGetTraceGraph:                  (*Handler).handleGetTraceGraph,
	pathGetTraceSegmentDestination:     (*Handler).handleGetTraceSegmentDestination,
	pathListRetrievedTraces:            (*Handler).handleListRetrievedTraces,
	pathListTagsForResource:            (*Handler).handleListTagsForResource,
	pathStartTraceRetrieval:            (*Handler).handleStartTraceRetrieval,
	pathTagResource:                    (*Handler).handleTagResource,
	pathUntagResource:                  (*Handler).handleUntagResource,
	pathUpdateIndexingRule:             (*Handler).handleUpdateIndexingRule,
	pathUpdateTraceSegmentDestination:  (*Handler).handleUpdateTraceSegmentDestination,
}

func (h *Handler) dispatch(ctx context.Context, path string, body []byte) ([]byte, error) {
	fn, ok := dispatchTable[path]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownPath, path)
	}

	return fn(h, ctx, body)
}

// notFoundExceptionName returns the exception name for an awserr.ErrNotFound-class
// error. Unlike GetGroup/DeleteGroup/GetSamplingRules/GetInsight/etc. (which only ever
// model InvalidRequestException for "not found"), ErrIndexingRuleNotFound/
// ErrResourceNotFound/ErrTraceRetrievalNotFound's operations declare
// ResourceNotFoundException instead -- confirmed against aws-sdk-go-v2/service/xray's
// deserializers.go per-op error switch.
func notFoundExceptionName(err error) string {
	switch {
	case errors.Is(err, ErrIndexingRuleNotFound),
		errors.Is(err, ErrResourceNotFound),
		errors.Is(err, ErrTraceRetrievalNotFound):
		return "ResourceNotFoundException"
	default:
		return errInvalidRequestException
	}
}

// conflictExceptionName returns the exception name for an awserr.ErrConflict-class error.
// ErrGroupAlreadyExists/ErrSamplingRuleAlreadyExists fall through to the
// default (both are InvalidRequestException -- see their doc in errors.go):
// CreateGroup/CreateSamplingRule model no AlreadyExists-shaped exception.
func conflictExceptionName(err error) string {
	switch {
	case errors.Is(err, ErrInvalidPolicyRevisionID):
		return "InvalidPolicyRevisionIdException"
	default:
		return errInvalidRequestException
	}
}

// invalidParameterExceptionName returns the exception name for an
// awserr.ErrInvalidParameter-class error. ErrInvalidSamplingRule falls
// through to the default (InvalidRequestException -- see its doc in
// errors.go): CreateSamplingRule models no InvalidSamplingRuleException.
func invalidParameterExceptionName(err error) string {
	switch {
	case errors.Is(err, ErrMalformedPolicyDocument):
		return "MalformedPolicyDocumentException"
	case errors.Is(err, ErrTooManyPolicies):
		return "PolicyCountLimitExceededException"
	case errors.Is(err, ErrPolicySizeLimitExceeded):
		return "PolicySizeLimitExceededException"
	case errors.Is(err, ErrRuleLimitExceeded):
		return "RuleLimitExceededException"
	case errors.Is(err, ErrTooManyTags):
		return "TooManyTagsException"
	default:
		return errInvalidRequestException
	}
}

func (h *Handler) handleError(c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    notFoundExceptionName(err),
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    conflictExceptionName(err),
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    invalidParameterExceptionName(err),
			keyMessageField: err.Error(),
		})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownPath),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    errInvalidRequestException,
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalFailure",
			keyMessageField: err.Error(),
		})
	}
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
