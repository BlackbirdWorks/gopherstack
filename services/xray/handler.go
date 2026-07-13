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
	"github.com/blackbirdworks/gopherstack/pkgs/page"
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
			return c.String(http.StatusNotFound, "not found")
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

			return c.String(http.StatusInternalServerError, "internal server error")
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

func (h *Handler) handleError(c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    errInvalidRequestException,
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		typeName := errInvalidRequestException

		switch {
		case errors.Is(err, ErrGroupAlreadyExists):
			typeName = "GroupAlreadyExistsException"
		case errors.Is(err, ErrSamplingRuleAlreadyExists):
			typeName = "RuleAlreadyExistsException"
		case errors.Is(err, ErrInvalidPolicyRevisionID):
			typeName = "InvalidPolicyRevisionIdException"
		}

		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    typeName,
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter):
		typeName := errInvalidRequestException

		if errors.Is(err, ErrInvalidSamplingRule) {
			typeName = "InvalidSamplingRuleException"
		} else if errors.Is(err, ErrMalformedPolicyDocument) {
			typeName = "MalformedPolicyDocumentException"
		}

		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    typeName,
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
			keyTypeField:    "InternalServiceError",
			keyMessageField: err.Error(),
		})
	}
}

// --- Group views ---

type insightsConfigView struct {
	InsightsEnabled      bool `json:"InsightsEnabled"`
	NotificationsEnabled bool `json:"NotificationsEnabled"`
}

type groupView struct {
	GroupARN              string             `json:"GroupARN"`
	GroupName             string             `json:"GroupName"`
	FilterExpression      string             `json:"FilterExpression"`
	InsightsConfiguration insightsConfigView `json:"InsightsConfiguration"`
}

func toGroupView(g *Group) groupView {
	return groupView{
		GroupARN:         g.GroupARN,
		GroupName:        g.GroupName,
		FilterExpression: g.FilterExpression,
		InsightsConfiguration: insightsConfigView{
			InsightsEnabled:      g.InsightsConfiguration.InsightsEnabled,
			NotificationsEnabled: g.InsightsConfiguration.NotificationsEnabled,
		},
	}
}

// --- Group operations ---

type createGroupInput struct {
	GroupName             string             `json:"GroupName"`
	FilterExpression      string             `json:"FilterExpression"`
	InsightsConfiguration insightsConfigView `json:"InsightsConfiguration"`
}

func (h *Handler) handleCreateGroup(_ context.Context, body []byte) ([]byte, error) {
	var in createGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	// Validate: NotificationsEnabled=true requires InsightsEnabled=true.
	if in.InsightsConfiguration.NotificationsEnabled && !in.InsightsConfiguration.InsightsEnabled {
		return nil, fmt.Errorf("%w: NotificationsEnabled requires InsightsEnabled to be true", errInvalidRequest)
	}

	ic := InsightsConfiguration{
		InsightsEnabled:      in.InsightsConfiguration.InsightsEnabled,
		NotificationsEnabled: in.InsightsConfiguration.NotificationsEnabled,
	}

	g, err := h.Backend.CreateGroupWithInsights(in.GroupName, in.FilterExpression, ic)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyGroup: toGroupView(g),
	})
}

type getGroupInput struct {
	GroupName string `json:"GroupName"`
	GroupARN  string `json:"GroupARN"`
}

func (h *Handler) handleGetGroup(_ context.Context, body []byte) ([]byte, error) {
	var in getGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" && in.GroupARN == "" {
		return nil, fmt.Errorf("%w: GroupName or GroupARN is required", errInvalidRequest)
	}

	var (
		g   *Group
		err error
	)

	if in.GroupARN != "" {
		g, err = h.Backend.GetGroupByARN(in.GroupARN)
	} else {
		g, err = h.Backend.GetGroup(in.GroupName)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyGroup: toGroupView(g),
	})
}

type getGroupsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

func (h *Handler) handleGetGroups(_ context.Context, body []byte) ([]byte, error) {
	var in getGroupsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	groups := h.Backend.GetGroups()
	views := make([]groupView, 0, len(groups))

	for i := range groups {
		views = append(views, toGroupView(&groups[i]))
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultGroupsPageSize)
	resp := map[string]any{
		"Groups":     pg.Data,
		keyNextToken: pg.Next,
	}

	return json.Marshal(resp)
}

type updateGroupInput struct {
	GroupName             string             `json:"GroupName"`
	GroupARN              string             `json:"GroupARN"`
	FilterExpression      string             `json:"FilterExpression"`
	InsightsConfiguration insightsConfigView `json:"InsightsConfiguration"`
}

func (h *Handler) handleUpdateGroup(_ context.Context, body []byte) ([]byte, error) {
	var in updateGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" && in.GroupARN == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	g, err := h.Backend.UpdateGroupByARN(in.GroupName, in.GroupARN, in.FilterExpression)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyGroup: toGroupView(g),
	})
}

type deleteGroupInput struct {
	GroupName string `json:"GroupName"`
	GroupARN  string `json:"GroupARN"`
}

func (h *Handler) handleDeleteGroup(_ context.Context, body []byte) ([]byte, error) {
	var in deleteGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" && in.GroupARN == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteGroupByARN(in.GroupName, in.GroupARN); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// --- Sampling rule views ---

type samplingRuleView struct {
	Attributes    map[string]string `json:"Attributes,omitempty"`
	RuleARN       string            `json:"RuleARN"`
	RuleName      string            `json:"RuleName"`
	ResourceARN   string            `json:"ResourceARN"`
	ServiceName   string            `json:"ServiceName"`
	ServiceType   string            `json:"ServiceType"`
	Host          string            `json:"Host"`
	HTTPMethod    string            `json:"HTTPMethod"`
	URLPath       string            `json:"URLPath"`
	FixedRate     float64           `json:"FixedRate"`
	Priority      int32             `json:"Priority"`
	ReservoirSize int32             `json:"ReservoirSize"`
	Version       int               `json:"Version"`
}

type samplingRuleRecord struct {
	SamplingRule samplingRuleView `json:"SamplingRule"`
	CreatedAt    float64          `json:"CreatedAt"`
	ModifiedAt   float64          `json:"ModifiedAt"`
}

func toSamplingRuleView(r *SamplingRule) samplingRuleView {
	return samplingRuleView{
		RuleARN:       r.RuleARN,
		RuleName:      r.RuleName,
		ResourceARN:   r.ResourceARN,
		ServiceName:   r.ServiceName,
		ServiceType:   r.ServiceType,
		Host:          r.Host,
		HTTPMethod:    r.HTTPMethod,
		URLPath:       r.URLPath,
		FixedRate:     r.FixedRate,
		Priority:      r.Priority,
		ReservoirSize: r.ReservoirSize,
		Attributes:    r.Attributes,
		Version:       1,
	}
}

func toSamplingRuleRecord(r *SamplingRule) samplingRuleRecord {
	return samplingRuleRecord{
		SamplingRule: toSamplingRuleView(r),
		CreatedAt:    float64(r.CreatedAt.Unix()),
		ModifiedAt:   float64(r.ModifiedAt.Unix()),
	}
}

// --- Sampling rule operations ---

type samplingRuleInput struct {
	Attributes    map[string]string `json:"Attributes,omitempty"`
	RuleName      string            `json:"RuleName"`
	ResourceARN   string            `json:"ResourceARN"`
	ServiceName   string            `json:"ServiceName"`
	ServiceType   string            `json:"ServiceType"`
	Host          string            `json:"Host"`
	HTTPMethod    string            `json:"HTTPMethod"`
	URLPath       string            `json:"URLPath"`
	FixedRate     float64           `json:"FixedRate"`
	Priority      int32             `json:"Priority"`
	ReservoirSize int32             `json:"ReservoirSize"`
}

type createSamplingRuleInput struct {
	SamplingRule samplingRuleInput `json:"SamplingRule"`
}

func (h *Handler) handleCreateSamplingRule(_ context.Context, body []byte) ([]byte, error) {
	var in createSamplingRuleInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.SamplingRule.RuleName == "" {
		return nil, fmt.Errorf("%w: RuleName is required", errInvalidRequest)
	}

	rule := SamplingRule{
		RuleName:      in.SamplingRule.RuleName,
		ResourceARN:   in.SamplingRule.ResourceARN,
		ServiceName:   in.SamplingRule.ServiceName,
		ServiceType:   in.SamplingRule.ServiceType,
		Host:          in.SamplingRule.Host,
		HTTPMethod:    in.SamplingRule.HTTPMethod,
		URLPath:       in.SamplingRule.URLPath,
		FixedRate:     in.SamplingRule.FixedRate,
		Priority:      in.SamplingRule.Priority,
		ReservoirSize: in.SamplingRule.ReservoirSize,
		Attributes:    in.SamplingRule.Attributes,
	}

	if err := ValidateSamplingRule(rule); err != nil {
		return nil, err
	}

	r, err := h.Backend.CreateSamplingRule(rule)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keySamplingRuleRecord: toSamplingRuleRecord(r),
	})
}

type getSamplingRulesInput struct {
	NextToken string `json:"NextToken"`
}

func (h *Handler) handleGetSamplingRules(_ context.Context, body []byte) ([]byte, error) {
	var in getSamplingRulesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	rules := h.Backend.GetSamplingRules()
	records := make([]samplingRuleRecord, 0, len(rules))

	for i := range rules {
		records = append(records, toSamplingRuleRecord(&rules[i]))
	}

	pg := page.New(records, in.NextToken, 0, defaultSamplingRulesPageSize)

	return json.Marshal(map[string]any{
		"SamplingRuleRecords": pg.Data,
		keyNextToken:          pg.Next,
	})
}

// samplingRuleUpdateInput uses json.RawMessage so we can detect which fields
// were explicitly provided (even zero values like FixedRate=0).
type samplingRuleUpdateInput struct {
	ResourceARN   *string  `json:"ResourceARN"`
	ServiceName   *string  `json:"ServiceName"`
	ServiceType   *string  `json:"ServiceType"`
	Host          *string  `json:"Host"`
	HTTPMethod    *string  `json:"HTTPMethod"`
	URLPath       *string  `json:"URLPath"`
	FixedRate     *float64 `json:"FixedRate"`
	Priority      *int32   `json:"Priority"`
	ReservoirSize *int32   `json:"ReservoirSize"`
	RuleName      string   `json:"RuleName"`
}

type updateSamplingRuleInput struct {
	SamplingRuleUpdate samplingRuleUpdateInput `json:"SamplingRuleUpdate"`
}

func (h *Handler) handleUpdateSamplingRule(_ context.Context, body []byte) ([]byte, error) {
	var in updateSamplingRuleInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.SamplingRuleUpdate.RuleName == "" {
		return nil, fmt.Errorf("%w: RuleName is required", errInvalidRequest)
	}

	updates := SamplingRuleUpdate{
		ResourceARN:   in.SamplingRuleUpdate.ResourceARN,
		ServiceName:   in.SamplingRuleUpdate.ServiceName,
		ServiceType:   in.SamplingRuleUpdate.ServiceType,
		Host:          in.SamplingRuleUpdate.Host,
		HTTPMethod:    in.SamplingRuleUpdate.HTTPMethod,
		URLPath:       in.SamplingRuleUpdate.URLPath,
		FixedRate:     in.SamplingRuleUpdate.FixedRate,
		Priority:      in.SamplingRuleUpdate.Priority,
		ReservoirSize: in.SamplingRuleUpdate.ReservoirSize,
	}

	r, err := h.Backend.UpdateSamplingRuleWithPointers(in.SamplingRuleUpdate.RuleName, updates)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keySamplingRuleRecord: toSamplingRuleRecord(r),
	})
}

type deleteSamplingRuleInput struct {
	RuleName string `json:"RuleName"`
}

func (h *Handler) handleDeleteSamplingRule(_ context.Context, body []byte) ([]byte, error) {
	var in deleteSamplingRuleInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.RuleName == "" {
		return nil, fmt.Errorf("%w: RuleName is required", errInvalidRequest)
	}

	r, err := h.Backend.DeleteSamplingRule(in.RuleName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keySamplingRuleRecord: toSamplingRuleRecord(r),
	})
}

// --- Trace operations ---

type putTraceSegmentsInput struct {
	TraceSegmentDocuments []string `json:"TraceSegmentDocuments"`
}

func (h *Handler) handlePutTraceSegments(_ context.Context, body []byte) ([]byte, error) {
	var in putTraceSegmentsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if len(in.TraceSegmentDocuments) > maxTraceSegmentsPerCall {
		return nil, fmt.Errorf("%w: PutTraceSegments accepts at most %d documents per call, got %d",
			errInvalidRequest, maxTraceSegmentsPerCall, len(in.TraceSegmentDocuments))
	}

	for i, doc := range in.TraceSegmentDocuments {
		if len(doc) > maxSegmentDocumentBytes {
			return nil, fmt.Errorf("%w: segment document %d exceeds maximum size of %d bytes (got %d)",
				errInvalidRequest, i, maxSegmentDocumentBytes, len(doc))
		}
	}

	unprocessed := h.Backend.PutTraceSegments(in.TraceSegmentDocuments)

	type unprocessedSegment struct {
		ID        string `json:"Id"`
		ErrorCode string `json:"ErrorCode,omitempty"`
		Message   string `json:"Message,omitempty"`
	}

	out := make([]unprocessedSegment, 0, len(unprocessed))
	for _, id := range unprocessed {
		out = append(out, unprocessedSegment{ID: id, ErrorCode: "InvalidSegment", Message: "failed to parse segment"})
	}

	return json.Marshal(map[string]any{
		"UnprocessedTraceSegments": out,
	})
}

type telemetryRecordInput struct {
	Timestamp              float64 `json:"Timestamp"`
	SegmentsReceivedCount  int32   `json:"SegmentsReceivedCount"`
	SegmentsSentCount      int32   `json:"SegmentsSentCount"`
	SegmentsSpilloverCount int32   `json:"SegmentsSpilloverCount"`
	SegmentsRejectedCount  int32   `json:"SegmentsRejectedCount"`
}

type putTelemetryRecordsInput struct {
	TelemetryRecords []telemetryRecordInput `json:"TelemetryRecords"`
}

func (h *Handler) handlePutTelemetryRecords(_ context.Context, body []byte) ([]byte, error) {
	var in putTelemetryRecordsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	records := make([]TelemetryRecord, 0, len(in.TelemetryRecords))

	for _, r := range in.TelemetryRecords {
		ts := time.Now()
		if r.Timestamp > 0 {
			ts = time.Unix(int64(r.Timestamp), 0)
		}

		records = append(records, TelemetryRecord{
			Timestamp:              ts,
			SegmentsReceivedCount:  r.SegmentsReceivedCount,
			SegmentsSentCount:      r.SegmentsSentCount,
			SegmentsSpilloverCount: r.SegmentsSpilloverCount,
			SegmentsRejectedCount:  r.SegmentsRejectedCount,
		})
	}

	if len(records) > 0 {
		h.Backend.PutTelemetryRecords(records)
	}

	return json.Marshal(map[string]any{})
}

type getTraceSummariesInput struct {
	FilterExpression string  `json:"FilterExpression"`
	TimeRangeType    string  `json:"TimeRangeType"`
	NextToken        string  `json:"NextToken"`
	StartTime        float64 `json:"StartTime"`
	EndTime          float64 `json:"EndTime"`
	MaxResults       int32   `json:"MaxResults"`
	Sampling         bool    `json:"Sampling"`
}

type traceSummaryHTTPView struct {
	HTTPURL    string `json:"HttpURL,omitempty"`
	HTTPMethod string `json:"HttpMethod,omitempty"`
	ClientIP   string `json:"ClientIp,omitempty"`
	UserAgent  string `json:"UserAgent,omitempty"`
	HTTPStatus int    `json:"HttpStatus,omitempty"`
}

type traceSummaryServiceIDView struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

type traceSummaryForecastView struct{}

type traceSummary struct {
	HTTP               *traceSummaryHTTPView       `json:"Http,omitempty"`
	Annotations        map[string]any              `json:"Annotations,omitempty"`
	ForecastStatistics *traceSummaryForecastView   `json:"ForecastStatistics,omitempty"`
	EntryPoint         string                      `json:"EntryPoint,omitempty"`
	ID                 string                      `json:"Id"`
	ServiceIds         []traceSummaryServiceIDView `json:"ServiceIds,omitempty"` //nolint:revive // AWS API field name
	Users              []string                    `json:"Users,omitempty"`
	Duration           float64                     `json:"Duration"`
	ResponseTime       float64                     `json:"ResponseTime"`
	ApproximateTime    float64                     `json:"ApproximateTime"`
	Revision           int                         `json:"Revision"`
	HasFault           bool                        `json:"HasFault"`
	HasError           bool                        `json:"HasError"`
	HasThrottle        bool                        `json:"HasThrottle"`
	IsPartial          bool                        `json:"IsPartial"`
}

// buildTraceSummaryView converts a TraceSummaryData to the JSON view struct.
func buildTraceSummaryView(traceID string, sd TraceSummaryData) traceSummary {
	s := traceSummary{
		ID:              traceID,
		Duration:        sd.Duration,
		ResponseTime:    sd.ResponseTime,
		ApproximateTime: sd.ApproxTime,
		HasFault:        sd.HasFault,
		HasError:        sd.HasError,
		HasThrottle:     sd.HasThrottle,
		IsPartial:       sd.IsPartial,
		EntryPoint:      sd.EntryPoint,
		Revision:        sd.Revision,
		// ForecastStatistics is always present per AWS API (even as empty object).
		ForecastStatistics: &traceSummaryForecastView{},
	}

	if len(sd.Users) > 0 {
		s.Users = sd.Users
	}

	if sd.HTTP != nil {
		s.HTTP = &traceSummaryHTTPView{
			HTTPStatus: sd.HTTP.HTTPStatus,
			HTTPURL:    sd.HTTP.HTTPURL,
			HTTPMethod: sd.HTTP.HTTPMethod,
			ClientIP:   sd.HTTP.ClientIP,
			UserAgent:  sd.HTTP.UserAgent,
		}
	}

	if len(sd.Annotations) > 0 {
		s.Annotations = sd.Annotations
	}

	if len(sd.ServiceIDs) > 0 {
		s.ServiceIds = make([]traceSummaryServiceIDView, 0, len(sd.ServiceIDs))
		for _, svc := range sd.ServiceIDs {
			s.ServiceIds = append(s.ServiceIds, traceSummaryServiceIDView(svc))
		}
	}

	return s
}

func (h *Handler) handleGetTraceSummaries(_ context.Context, body []byte) ([]byte, error) {
	var in getTraceSummariesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.TimeRangeType != "" &&
		in.TimeRangeType != timeRangeTypeTraceID &&
		in.TimeRangeType != timeRangeTypeEvent &&
		in.TimeRangeType != timeRangeTypeService {
		return nil, fmt.Errorf("%w: TimeRangeType must be %q, %q, or %q, got %q",
			errInvalidRequest, timeRangeTypeTraceID, timeRangeTypeEvent, timeRangeTypeService, in.TimeRangeType)
	}

	traces := h.Backend.GetTraceSummaries()
	allSegs := h.Backend.GetAllParsedSegments()

	summaries := make([]traceSummary, 0, len(traces))

	for i := range traces {
		// Apply optional time window filter when both bounds are provided.
		if in.StartTime > 0 && in.EndTime > 0 {
			ts := float64(traces[i].StartTime.Unix())
			if ts < in.StartTime || ts > in.EndTime {
				continue
			}
		}

		segs := allSegs[traces[i].TraceID]
		sd := BuildTraceSummary(traces[i].TraceID, segs)

		if !evaluateFilter(in.FilterExpression, sd) {
			continue
		}

		summaries = append(summaries, buildTraceSummaryView(traces[i].TraceID, sd))
	}

	pg := page.New(summaries, in.NextToken, int(in.MaxResults), defaultTraceSummariesPageSize)

	return json.Marshal(map[string]any{
		"TraceSummaries":       pg.Data,
		"TracesProcessedCount": len(summaries),
		keyNextToken:           pg.Next,
	})
}

type batchGetTracesInput struct {
	TraceIDs []string `json:"TraceIds"`
}

type batchSegmentOutput struct {
	ID       string `json:"Id"`
	Document string `json:"Document"`
}

type traceOutput struct {
	ID       string               `json:"Id"`
	Segments []batchSegmentOutput `json:"Segments"`
	Duration float64              `json:"Duration"`
}

func (h *Handler) handleBatchGetTraces(_ context.Context, body []byte) ([]byte, error) {
	var in batchGetTracesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if len(in.TraceIDs) > maxBatchGetTraces {
		return nil, fmt.Errorf("%w: BatchGetTraces supports at most %d trace IDs, got %d",
			ErrBatchGetTracesLimit, maxBatchGetTraces, len(in.TraceIDs))
	}

	traces := make([]traceOutput, 0, len(in.TraceIDs))
	unprocessed := make([]string, 0, len(in.TraceIDs))

	for _, id := range in.TraceIDs {
		t := h.Backend.GetTrace(id)
		if t == nil {
			unprocessed = append(unprocessed, id)

			continue
		}

		segs := h.Backend.GetParsedSegments(id)
		sd := BuildTraceSummary(id, segs)

		segViews := make([]batchSegmentOutput, 0, len(segs))
		for _, seg := range segs {
			segViews = append(segViews, batchSegmentOutput{
				ID:       seg.ID,
				Document: seg.Document,
			})
		}

		// Fall back to raw segments when parsed segments are unavailable.
		if len(segViews) == 0 {
			for _, rawDoc := range t.Segments {
				var hdr struct {
					ID string `json:"id"`
				}

				if err := json.Unmarshal([]byte(rawDoc), &hdr); err == nil {
					segViews = append(segViews, batchSegmentOutput{
						ID:       hdr.ID,
						Document: rawDoc,
					})
				}
			}
		}

		traces = append(traces, traceOutput{
			ID:       t.TraceID,
			Duration: sd.Duration,
			Segments: segViews,
		})
	}

	return json.Marshal(map[string]any{
		"Traces":              traces,
		"UnprocessedTraceIds": unprocessed,
	})
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// --- Encryption config operations ---

type putEncryptionConfigInput struct {
	KeyID string `json:"KeyId,omitempty"`
	Type  string `json:"Type"`
}

func (h *Handler) handleGetEncryptionConfig(c *echo.Context) error {
	cfg := h.Backend.GetEncryptionConfig()

	return c.JSON(http.StatusOK, map[string]any{
		keyEncryptionConfig: cfg,
	})
}

// handleGetEncryptionConfigBody serves GetEncryptionConfig via the table-driven
// dispatch path. The AWS SDK sends GetEncryptionConfig as POST /EncryptionConfig
// (PutEncryptionConfig uses the distinct POST /PutEncryptionConfig path).
func (h *Handler) handleGetEncryptionConfigBody(_ context.Context, _ []byte) ([]byte, error) {
	cfg := h.Backend.GetEncryptionConfig()

	return json.Marshal(map[string]any{
		keyEncryptionConfig: cfg,
	})
}

func (h *Handler) handlePutEncryptionConfig(_ context.Context, body []byte) ([]byte, error) {
	var in putEncryptionConfigInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.Type == "" {
		in.Type = encTypeNone
	}

	if in.Type != encTypeNone && in.Type != encTypeKMS {
		return nil, fmt.Errorf("%w: Type must be %q or %q, got %q",
			errInvalidRequest, encTypeNone, encTypeKMS, in.Type)
	}

	if in.Type == encTypeKMS && in.KeyID == "" {
		return nil, fmt.Errorf("%w: KeyId is required when Type is %q", errInvalidRequest, encTypeKMS)
	}

	if in.Type == encTypeNone && in.KeyID != "" {
		return nil, fmt.Errorf("%w: KeyId must not be set when Type is %q", errInvalidRequest, encTypeNone)
	}

	cfg, err := h.Backend.PutEncryptionConfig(in.Type, in.KeyID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyEncryptionConfig: cfg,
	})
}

// --- CancelTraceRetrieval ---

type cancelTraceRetrievalInput struct {
	RetrievalToken string `json:"RetrievalToken"`
}

func (h *Handler) handleCancelTraceRetrieval(_ context.Context, body []byte) ([]byte, error) {
	var in cancelTraceRetrievalInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.RetrievalToken == "" {
		return nil, fmt.Errorf("%w: RetrievalToken is required", errInvalidRequest)
	}

	h.Backend.CancelTraceRetrieval(in.RetrievalToken)

	return json.Marshal(map[string]any{})
}

// --- DeleteResourcePolicy ---

type deleteResourcePolicyInput struct {
	PolicyName       string `json:"PolicyName"`
	PolicyRevisionID string `json:"PolicyRevisionId"`
}

func (h *Handler) handleDeleteResourcePolicy(_ context.Context, body []byte) ([]byte, error) {
	var in deleteResourcePolicyInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteResourcePolicy(in.PolicyName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// --- GetIndexingRules ---

type indexingRuleView struct {
	Name       string  `json:"Name"`
	ModifiedAt float64 `json:"ModifiedAt"`
}

type getIndexingRulesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

func (h *Handler) handleGetIndexingRules(_ context.Context, body []byte) ([]byte, error) {
	var in getIndexingRulesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	rules := h.Backend.GetIndexingRules()
	views := make([]indexingRuleView, 0, len(rules))

	for _, r := range rules {
		views = append(views, indexingRuleView{
			Name:       r.Name,
			ModifiedAt: float64(r.ModifiedAt.Unix()),
		})
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultIndexingRulesPageSize)

	return json.Marshal(map[string]any{
		"IndexingRules": pg.Data,
		keyNextToken:    pg.Next,
	})
}

// --- GetInsight ---

type getInsightInput struct {
	InsightID string `json:"InsightId"`
}

type insightView struct {
	InsightID  string   `json:"InsightId"`
	GroupARN   string   `json:"GroupARN"`
	GroupName  string   `json:"GroupName"`
	State      string   `json:"State"`
	Summary    string   `json:"Summary"`
	Categories []string `json:"Categories,omitempty"`
	StartTime  float64  `json:"StartTime"`
	EndTime    float64  `json:"EndTime,omitempty"`
}

func toInsightView(i *Insight) insightView {
	v := insightView{
		InsightID: i.InsightID,
		GroupARN:  i.GroupARN,
		GroupName: i.GroupName,
		State:     i.State,
		Summary:   i.Summary,
		StartTime: float64(i.StartTime.Unix()),
	}
	if !i.EndTime.IsZero() {
		v.EndTime = float64(i.EndTime.Unix())
	}

	return v
}

func (h *Handler) handleGetInsight(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.InsightID == "" {
		return nil, fmt.Errorf("%w: InsightId is required", errInvalidRequest)
	}

	i, err := h.Backend.GetInsight(in.InsightID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Insight": toInsightView(i),
	})
}

// --- GetInsightEvents ---

type getInsightEventsInput struct {
	InsightID  string `json:"InsightId"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type insightEventView struct {
	Summary   string  `json:"Summary"`
	EventTime float64 `json:"EventTime"`
}

func (h *Handler) handleGetInsightEvents(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightEventsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.InsightID == "" {
		return nil, fmt.Errorf("%w: InsightId is required", errInvalidRequest)
	}

	events, err := h.Backend.GetInsightEvents(in.InsightID)
	if err != nil {
		return nil, err
	}

	views := make([]insightEventView, 0, len(events))
	for _, e := range events {
		views = append(views, insightEventView{
			Summary:   e.Summary,
			EventTime: float64(e.EventTime.Unix()),
		})
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultInsightEventsPageSize)

	return json.Marshal(map[string]any{
		"InsightEvents": pg.Data,
		keyNextToken:    pg.Next,
	})
}

// --- GetInsightImpactGraph ---

type getInsightImpactGraphInput struct {
	InsightID string  `json:"InsightId"`
	NextToken string  `json:"NextToken"`
	StartTime float64 `json:"StartTime"`
	EndTime   float64 `json:"EndTime"`
}

func (h *Handler) handleGetInsightImpactGraph(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightImpactGraphInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.InsightID == "" {
		return nil, fmt.Errorf("%w: InsightId is required", errInvalidRequest)
	}

	// Validate the insight exists.
	if _, err := h.Backend.GetInsight(in.InsightID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"InsightId":             in.InsightID,
		keyServices:             []any{},
		"StartTime":             in.StartTime,
		"EndTime":               in.EndTime,
		"ServiceGraphStartTime": in.StartTime,
		"ServiceGraphEndTime":   in.EndTime,
		keyNextToken:            "",
	})
}

// --- GetInsightSummaries ---

type getInsightSummariesInput struct {
	GroupARN   string   `json:"GroupARN"`
	GroupName  string   `json:"GroupName"`
	NextToken  string   `json:"NextToken"`
	States     []string `json:"States"`
	StartTime  float64  `json:"StartTime"`
	EndTime    float64  `json:"EndTime"`
	MaxResults int32    `json:"MaxResults"`
}

func (h *Handler) handleGetInsightSummaries(_ context.Context, body []byte) ([]byte, error) {
	var in getInsightSummariesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	summaries, err := h.Backend.GetInsightSummaries(in.States)
	if err != nil {
		return nil, err
	}

	views := make([]insightView, 0, len(summaries))

	for i := range summaries {
		views = append(views, toInsightView(&summaries[i]))
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultInsightSummariesPageSize)

	return json.Marshal(map[string]any{
		"InsightSummaries": pg.Data,
		keyNextToken:       pg.Next,
	})
}

// --- GetRetrievedTracesGraph ---

type getRetrievedTracesGraphInput struct {
	RetrievalToken string `json:"RetrievalToken"`
	NextToken      string `json:"NextToken"`
}

func (h *Handler) handleGetRetrievedTracesGraph(_ context.Context, body []byte) ([]byte, error) {
	var in getRetrievedTracesGraphInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.RetrievalToken == "" {
		return nil, fmt.Errorf("%w: RetrievalToken is required", errInvalidRequest)
	}

	status, _ := h.Backend.GetRetrievedTracesGraph(in.RetrievalToken)

	return json.Marshal(map[string]any{
		"RetrievalStatus": status,
		keyServices:       []any{},
		keyNextToken:      "",
	})
}

// --- GetSamplingStatisticSummaries ---

type samplingStatisticSummaryView struct {
	RuleName     string  `json:"RuleName"`
	RequestCount int32   `json:"RequestCount"`
	SampledCount int32   `json:"SampledCount"`
	BorrowCount  int32   `json:"BorrowCount"`
	Timestamp    float64 `json:"Timestamp"`
}

type getSamplingStatisticSummariesInput struct {
	NextToken string `json:"NextToken"`
}

func (h *Handler) handleGetSamplingStatisticSummaries(_ context.Context, body []byte) ([]byte, error) {
	var in getSamplingStatisticSummariesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	summaries := h.Backend.GetSamplingStatisticSummaries()
	views := make([]samplingStatisticSummaryView, 0, len(summaries))

	for _, s := range summaries {
		views = append(views, samplingStatisticSummaryView{
			RuleName:     s.RuleName,
			RequestCount: s.RequestCount,
			SampledCount: s.SampledCount,
			BorrowCount:  s.BorrowCount,
			Timestamp:    float64(s.Timestamp.Unix()),
		})
	}

	pg := page.New(views, in.NextToken, 0, defaultSamplingStatsPageSize)

	return json.Marshal(map[string]any{
		"SamplingStatisticSummaries": pg.Data,
		keyNextToken:                 pg.Next,
	})
}

// --- GetSamplingTargets ---

type samplingStatisticsDocumentInput struct {
	RuleName     string `json:"RuleName"`
	ClientID     string `json:"ClientId"`
	RequestCount int32  `json:"RequestCount"`
	SampledCount int32  `json:"SampledCount"`
	BorrowCount  int32  `json:"BorrowCount"`
}

type getSamplingTargetsInput struct {
	SamplingStatisticsDocuments []samplingStatisticsDocumentInput `json:"SamplingStatisticsDocuments"`
}

type samplingTargetDocumentView struct {
	RuleName          string  `json:"RuleName"`
	ReservoirQuotaTTL float64 `json:"ReservoirQuotaTTL"`
	FixedRate         float64 `json:"FixedRate"`
	ReservoirQuota    int32   `json:"ReservoirQuota"`
	Interval          int32   `json:"Interval"`
}

type unprocessedStatisticsView struct {
	RuleName  string `json:"RuleName"`
	ErrorCode string `json:"ErrorCode"`
	Message   string `json:"Message"`
}

func (h *Handler) handleGetSamplingTargets(_ context.Context, body []byte) ([]byte, error) {
	var in getSamplingTargetsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	docs := make([]SamplingStatisticsDocument, 0, len(in.SamplingStatisticsDocuments))
	for _, d := range in.SamplingStatisticsDocuments {
		docs = append(docs, SamplingStatisticsDocument(d))
	}

	targets, unprocessed := h.Backend.GetSamplingTargets(docs)

	targetViews := make([]samplingTargetDocumentView, 0, len(targets))
	for _, t := range targets {
		targetViews = append(targetViews, samplingTargetDocumentView{
			RuleName:          t.RuleName,
			FixedRate:         t.FixedRate,
			ReservoirQuota:    t.ReservoirSize,
			ReservoirQuotaTTL: float64(t.ReservoirQuotaTTL.Unix()),
			Interval:          samplingTargetInterval,
		})
	}

	unprocessedViews := make([]unprocessedStatisticsView, 0, len(unprocessed))
	for _, u := range unprocessed {
		unprocessedViews = append(unprocessedViews, unprocessedStatisticsView(u))
	}

	lastMod := h.Backend.LastRuleModification()
	var lastModTS float64

	if !lastMod.IsZero() {
		lastModTS = float64(lastMod.Unix())
	} else {
		lastModTS = float64(time.Now().Unix())
	}

	return json.Marshal(map[string]any{
		"SamplingTargetDocuments": targetViews,
		"UnprocessedStatistics":   unprocessedViews,
		"LastRuleModification":    lastModTS,
	})
}

// --- ListResourcePolicies ---

type resourcePolicyView struct {
	PolicyName       string `json:"PolicyName"`
	PolicyDocument   string `json:"PolicyDocument"`
	PolicyRevisionID string `json:"PolicyRevisionId"`
}

func toResourcePolicyView(p *ResourcePolicy) resourcePolicyView {
	return resourcePolicyView{
		PolicyName:       p.PolicyName,
		PolicyDocument:   p.PolicyDocument,
		PolicyRevisionID: p.PolicyRevisionID,
	}
}

func (h *Handler) handleListResourcePolicies(_ context.Context, body []byte) ([]byte, error) {
	var in struct {
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	policies := h.Backend.ListResourcePolicies()
	views := make([]resourcePolicyView, 0, len(policies))
	for i := range policies {
		views = append(views, toResourcePolicyView(&policies[i]))
	}

	pg := page.New(views, in.NextToken, in.MaxResults, defaultResourcePoliciesPageSize)
	resp := map[string]any{"ResourcePolicies": pg.Data, keyNextToken: pg.Next}

	return json.Marshal(resp)
}

// --- PutResourcePolicy ---

type putResourcePolicyInput struct {
	PolicyName               string `json:"PolicyName"`
	PolicyDocument           string `json:"PolicyDocument"`
	PolicyRevisionID         string `json:"PolicyRevisionId"`
	BypassPolicyLockoutCheck bool   `json:"BypassPolicyLockoutCheck"`
}

func (h *Handler) handlePutResourcePolicy(_ context.Context, body []byte) ([]byte, error) {
	var in putResourcePolicyInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", errInvalidRequest)
	}

	if in.PolicyDocument == "" {
		return nil, fmt.Errorf("%w: PolicyDocument is required", errInvalidRequest)
	}

	p, err := h.Backend.PutResourcePolicy(in.PolicyName, in.PolicyDocument, in.PolicyRevisionID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ResourcePolicy": toResourcePolicyView(p),
	})
}
