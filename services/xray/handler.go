package xray

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var (
	errUnknownPath    = errors.New("unknown path")
	errInvalidRequest = errors.New("invalid request")
)

// xrayPaths is the set of supported X-Ray REST API paths.
var xrayPaths = map[string]bool{ //nolint:gochecknoglobals // package-level routing table
	"/TraceSegments":      true,
	"/TelemetryRecords":   true,
	"/TraceSummaries":     true,
	"/Traces":             true,
	"/CreateGroup":        true,
	"/GetGroup":           true,
	"/Groups":             true,
	"/UpdateGroup":        true,
	"/DeleteGroup":        true,
	"/CreateSamplingRule": true,
	"/GetSamplingRules":   true,
	"/UpdateSamplingRule": true,
	"/DeleteSamplingRule": true,
}

// pathToOperation maps X-Ray REST API paths to operation names.
var pathToOperation = map[string]string{ //nolint:gochecknoglobals // package-level routing table
	"/TraceSegments":      "PutTraceSegments",
	"/TelemetryRecords":   "PutTelemetryRecords",
	"/TraceSummaries":     "GetTraceSummaries",
	"/Traces":             "BatchGetTraces",
	"/CreateGroup":        "CreateGroup",
	"/GetGroup":           "GetGroup",
	"/Groups":             "GetGroups",
	"/UpdateGroup":        "UpdateGroup",
	"/DeleteGroup":        "DeleteGroup",
	"/CreateSamplingRule": "CreateSamplingRule",
	"/GetSamplingRules":   "GetSamplingRules",
	"/UpdateSamplingRule": "UpdateSamplingRule",
	"/DeleteSamplingRule": "DeleteSamplingRule",
}

// Handler is the Echo HTTP handler for AWS X-Ray operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new X-Ray handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Xray" }

// GetSupportedOperations returns the list of supported X-Ray operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"PutTraceSegments",
		"PutTelemetryRecords",
		"GetTraceSummaries",
		"BatchGetTraces",
		"CreateGroup",
		"GetGroup",
		"GetGroups",
		"UpdateGroup",
		"DeleteGroup",
		"CreateSamplingRule",
		"GetSamplingRules",
		"UpdateSamplingRule",
		"DeleteSamplingRule",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "xray" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this X-Ray instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches X-Ray REST API requests.
// X-Ray uses POST with specific well-known paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if c.Request().Method != http.MethodPost {
			return false
		}

		return xrayPaths[c.Request().URL.Path]
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the X-Ray operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, ok := pathToOperation[c.Request().URL.Path]
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

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := pathToOperation[path]
		log.DebugContext(ctx, "xray request", "operation", op, "path", path)

		resp, dispatchErr := h.dispatch(ctx, path, body)
		if dispatchErr != nil {
			return h.handleError(c, op, dispatchErr)
		}

		c.Response().Header().Set("Content-Type", "application/json")

		return c.JSONBlob(http.StatusOK, resp)
	}
}

func (h *Handler) dispatch(ctx context.Context, path string, body []byte) ([]byte, error) {
	switch path {
	case "/TraceSegments":
		return h.handlePutTraceSegments(ctx, body)
	case "/TelemetryRecords":
		return h.handlePutTelemetryRecords(ctx, body)
	case "/TraceSummaries":
		return h.handleGetTraceSummaries(ctx, body)
	case "/Traces":
		return h.handleBatchGetTraces(ctx, body)
	case "/CreateGroup":
		return h.handleCreateGroup(ctx, body)
	case "/GetGroup":
		return h.handleGetGroup(ctx, body)
	case "/Groups":
		return h.handleGetGroups(ctx, body)
	case "/UpdateGroup":
		return h.handleUpdateGroup(ctx, body)
	case "/DeleteGroup":
		return h.handleDeleteGroup(ctx, body)
	case "/CreateSamplingRule":
		return h.handleCreateSamplingRule(ctx, body)
	case "/GetSamplingRules":
		return h.handleGetSamplingRules(ctx, body)
	case "/UpdateSamplingRule":
		return h.handleUpdateSamplingRule(ctx, body)
	case "/DeleteSamplingRule":
		return h.handleDeleteSamplingRule(ctx, body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownPath, path)
	}
}

func (h *Handler) handleError(c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "InvalidRequestException",
			"message": err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		typeName := "InvalidRequestException"
		if errors.Is(err, ErrGroupAlreadyExists) {
			typeName = "GroupAlreadyExistsException"
		} else if errors.Is(err, ErrSamplingRuleAlreadyExists) {
			typeName = "RuleAlreadyExistsException"
		}

		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  typeName,
			"message": err.Error(),
		})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownPath),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "InvalidRequestException",
			"message": err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"__type":  "InternalServiceError",
			"message": err.Error(),
		})
	}
}

// --- Group views ---

type groupView struct {
	GroupARN         string `json:"GroupARN"`
	GroupName        string `json:"GroupName"`
	FilterExpression string `json:"FilterExpression"`
}

func toGroupView(g *Group) groupView {
	return groupView{
		GroupARN:         g.GroupARN,
		GroupName:        g.GroupName,
		FilterExpression: g.FilterExpression,
	}
}

// --- Group operations ---

type createGroupInput struct {
	GroupName        string `json:"GroupName"`
	FilterExpression string `json:"FilterExpression"`
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

	g, err := h.Backend.CreateGroup(in.GroupName, in.FilterExpression)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Group": toGroupView(g),
	})
}

type getGroupInput struct {
	GroupName string `json:"GroupName"`
}

func (h *Handler) handleGetGroup(_ context.Context, body []byte) ([]byte, error) {
	var in getGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	g, err := h.Backend.GetGroup(in.GroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Group": toGroupView(g),
	})
}

func (h *Handler) handleGetGroups(_ context.Context, _ []byte) ([]byte, error) {
	groups := h.Backend.GetGroups()
	views := make([]groupView, 0, len(groups))

	for i := range groups {
		views = append(views, toGroupView(&groups[i]))
	}

	return json.Marshal(map[string]any{
		"Groups":    views,
		"NextToken": "",
	})
}

type updateGroupInput struct {
	GroupName        string `json:"GroupName"`
	FilterExpression string `json:"FilterExpression"`
}

func (h *Handler) handleUpdateGroup(_ context.Context, body []byte) ([]byte, error) {
	var in updateGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	g, err := h.Backend.UpdateGroup(in.GroupName, in.FilterExpression)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Group": toGroupView(g),
	})
}

type deleteGroupInput struct {
	GroupName string `json:"GroupName"`
}

func (h *Handler) handleDeleteGroup(_ context.Context, body []byte) ([]byte, error) {
	var in deleteGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteGroup(in.GroupName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// --- Sampling rule views ---

type samplingRuleView struct {
	RuleARN       string  `json:"RuleARN"`
	RuleName      string  `json:"RuleName"`
	ResourceARN   string  `json:"ResourceARN"`
	ServiceName   string  `json:"ServiceName"`
	ServiceType   string  `json:"ServiceType"`
	Host          string  `json:"Host"`
	HTTPMethod    string  `json:"HTTPMethod"`
	URLPath       string  `json:"URLPath"`
	FixedRate     float64 `json:"FixedRate"`
	Priority      int32   `json:"Priority"`
	ReservoirSize int32   `json:"ReservoirSize"`
	Version       int     `json:"Version"`
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
		Version:       1,
	}
}

func toSamplingRuleRecord(r *SamplingRule) samplingRuleRecord {
	epoch := float64(r.CreatedAt.Unix())

	return samplingRuleRecord{
		SamplingRule: toSamplingRuleView(r),
		CreatedAt:    epoch,
		ModifiedAt:   epoch,
	}
}

// --- Sampling rule operations ---

type samplingRuleInput struct {
	RuleName      string  `json:"RuleName"`
	ResourceARN   string  `json:"ResourceARN"`
	ServiceName   string  `json:"ServiceName"`
	ServiceType   string  `json:"ServiceType"`
	Host          string  `json:"Host"`
	HTTPMethod    string  `json:"HTTPMethod"`
	URLPath       string  `json:"URLPath"`
	FixedRate     float64 `json:"FixedRate"`
	Priority      int32   `json:"Priority"`
	ReservoirSize int32   `json:"ReservoirSize"`
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
	}

	r, err := h.Backend.CreateSamplingRule(rule)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"SamplingRuleRecord": toSamplingRuleRecord(r),
	})
}

func (h *Handler) handleGetSamplingRules(_ context.Context, _ []byte) ([]byte, error) {
	rules := h.Backend.GetSamplingRules()
	records := make([]samplingRuleRecord, 0, len(rules))

	for i := range rules {
		records = append(records, toSamplingRuleRecord(&rules[i]))
	}

	return json.Marshal(map[string]any{
		"SamplingRuleRecords": records,
		"NextToken":           "",
	})
}

type samplingRuleUpdate struct {
	RuleName      string  `json:"RuleName"`
	ResourceARN   string  `json:"ResourceARN"`
	ServiceName   string  `json:"ServiceName"`
	ServiceType   string  `json:"ServiceType"`
	Host          string  `json:"Host"`
	HTTPMethod    string  `json:"HTTPMethod"`
	URLPath       string  `json:"URLPath"`
	FixedRate     float64 `json:"FixedRate"`
	Priority      int32   `json:"Priority"`
	ReservoirSize int32   `json:"ReservoirSize"`
}

type updateSamplingRuleInput struct {
	SamplingRuleUpdate samplingRuleUpdate `json:"SamplingRuleUpdate"`
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

	updates := SamplingRule{
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

	r, err := h.Backend.UpdateSamplingRule(in.SamplingRuleUpdate.RuleName, updates)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"SamplingRuleRecord": toSamplingRuleRecord(r),
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
		"SamplingRuleRecord": toSamplingRuleRecord(r),
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

func (h *Handler) handlePutTelemetryRecords(_ context.Context, _ []byte) ([]byte, error) {
	return json.Marshal(map[string]any{})
}

type getTraceSummariesInput struct {
	StartTime float64 `json:"StartTime"`
	EndTime   float64 `json:"EndTime"`
}

type traceSummary struct {
	ID       string  `json:"Id"`
	Duration float64 `json:"Duration"`
}

func (h *Handler) handleGetTraceSummaries(_ context.Context, body []byte) ([]byte, error) {
	var in getTraceSummariesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	traces := h.Backend.GetTraceSummaries()
	summaries := make([]traceSummary, 0, len(traces))

	for i := range traces {
		summaries = append(summaries, traceSummary{
			ID:       traces[i].TraceID,
			Duration: 0,
		})
	}

	return json.Marshal(map[string]any{
		"TraceSummaries":       summaries,
		"TracesProcessedCount": len(summaries),
		"NextToken":            "",
	})
}

type batchGetTracesInput struct {
	TraceIDs []string `json:"TraceIds"`
}

type traceOutput struct {
	ID       string   `json:"Id"`
	Segments []string `json:"Segments"`
	Duration float64  `json:"Duration"`
}

func (h *Handler) handleBatchGetTraces(_ context.Context, body []byte) ([]byte, error) {
	var in batchGetTracesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	traces := make([]traceOutput, 0, len(in.TraceIDs))
	unprocessed := make([]string, 0)

	for _, id := range in.TraceIDs {
		t := h.Backend.GetTrace(id)
		if t == nil {
			unprocessed = append(unprocessed, id)

			continue
		}

		traces = append(traces, traceOutput{
			ID:       t.TraceID,
			Duration: 0,
			Segments: t.Segments,
		})
	}

	return json.Marshal(map[string]any{
		"Traces":              traces,
		"UnprocessedTraceIds": unprocessed,
	})
}
