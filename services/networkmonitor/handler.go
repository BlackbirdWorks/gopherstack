package networkmonitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opCreateMonitor       = "CreateMonitor"
	opDeleteMonitor       = "DeleteMonitor"
	opGetMonitor          = "GetMonitor"
	opUpdateMonitor       = "UpdateMonitor"
	opListMonitors        = "ListMonitors"
	opCreateProbe         = "CreateProbe"
	opDeleteProbe         = "DeleteProbe"
	opGetProbe            = "GetProbe"
	opUpdateProbe         = "UpdateProbe"
	opListTagsForResource = "ListTagsForResource"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
)

const (
	nmService       = "networkmonitor"
	nmMatchPriority = 88
	nmPathMonitors  = "/monitors"
	nmPathTags      = "/tags/"
	opUnknown       = "Unknown"
	splitTwo        = 2
	splitThree      = 3
	splitFour       = 4
)

var errUnknownAction = errors.New("unknown action")

// Handler is the HTTP handler for the CloudWatch Network Monitor REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Network Monitor handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears handler state (delegates to backend if supported).
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Name returns the service name.
func (h *Handler) Name() string { return "NetworkMonitor" }

// GetSupportedOperations returns the list of supported Network Monitor operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateMonitor,
		opDeleteMonitor,
		opGetMonitor,
		opUpdateMonitor,
		opListMonitors,
		opCreateProbe,
		opDeleteProbe,
		opGetProbe,
		opUpdateProbe,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return nmService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Network Monitor requests by service + path.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != nmService {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, nmPathMonitors) || strings.HasPrefix(path, nmPathTags)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return nmMatchPriority }

// ExtractOperation determines the operation name from the HTTP request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	if strings.HasPrefix(path, nmPathTags) {
		return extractTagOp(method)
	}

	return extractMonitorOp(method, path)
}

func extractTagOp(method string) string {
	switch method {
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	}

	return opUnknown
}

func extractMonitorOp(method, path string) string {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(path, "/monitors"), "/")
	segments := strings.SplitN(strings.TrimPrefix(trimmed, "/"), "/", splitFour)

	if trimmed == "" || (len(segments) == 1 && segments[0] == "") {
		return extractMonitorListOp(method)
	}

	if len(segments) == 1 {
		return extractMonitorCRUDOp(method)
	}

	if len(segments) >= 2 && segments[1] == "probes" {
		return extractProbeOp(method, segments)
	}

	return opUnknown
}

func extractMonitorListOp(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateMonitor
	case http.MethodGet:
		return opListMonitors
	}

	return opUnknown
}

func extractMonitorCRUDOp(method string) string {
	switch method {
	case http.MethodGet:
		return opGetMonitor
	case http.MethodDelete:
		return opDeleteMonitor
	case http.MethodPatch:
		return opUpdateMonitor
	}

	return opUnknown
}

func extractProbeOp(method string, segments []string) string {
	if len(segments) == 2 || segments[2] == "" {
		if method == http.MethodPost {
			return opCreateProbe
		}

		return opUnknown
	}

	switch method {
	case http.MethodGet:
		return opGetProbe
	case http.MethodDelete:
		return opDeleteProbe
	case http.MethodPatch:
		return opUpdateProbe
	}

	return opUnknown
}

// ExtractResource extracts the monitor name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	trimmed := strings.TrimPrefix(path, "/monitors/")

	if trimmed == path {
		return ""
	}

	parts := strings.SplitN(trimmed, "/", splitTwo)

	return parts[0]
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		log := logger.Load(ctx)

		path := c.Request().URL.Path
		query := c.Request().URL.Query()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "networkmonitor: failed to read request body", "error", err)

			return h.handleError(c, err)
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, path, query, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(
	ctx context.Context,
	op, path string,
	query url.Values,
	body []byte,
) ([]byte, error) {
	switch op {
	case opCreateMonitor:
		return h.handleCreateMonitor(ctx, body)
	case opDeleteMonitor:
		return h.handleDeleteMonitor(ctx, path)
	case opGetMonitor:
		return h.handleGetMonitor(ctx, path)
	case opUpdateMonitor:
		return h.handleUpdateMonitor(ctx, path, body)
	case opListMonitors:
		return h.handleListMonitors(ctx, query)
	case opCreateProbe:
		return h.handleCreateProbe(ctx, path, body)
	case opDeleteProbe:
		return h.handleDeleteProbe(ctx, path)
	case opGetProbe:
		return h.handleGetProbe(ctx, path)
	case opUpdateProbe:
		return h.handleUpdateProbe(ctx, path, body)
	case opListTagsForResource:
		return h.handleListTagsForResource(ctx, path)
	case opTagResource:
		return h.handleTagResource(ctx, path, body)
	case opUntagResource:
		return h.handleUntagResource(ctx, path, query)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	var code string
	var status int

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		status = http.StatusNotFound
		code = "ResourceNotFoundException"
	case errors.Is(err, awserr.ErrAlreadyExists):
		status = http.StatusConflict
		code = "ConflictException"
	case errors.Is(err, ErrServiceQuotaExceeded):
		status = http.StatusPaymentRequired
		code = "ServiceQuotaExceededException"
	case errors.Is(err, awserr.ErrInvalidParameter):
		status = http.StatusBadRequest
		code = "ValidationException"
	case errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		status = http.StatusBadRequest
		code = "ValidationException"
	default:
		status = http.StatusInternalServerError
		code = "InternalServerException"
	}

	c.Response().Header().Set("X-Amzn-Errortype", code)

	return c.JSON(status, errorResponse{Message: err.Error()})
}

// epochSeconds converts a *time.Time to a pointer to fractional epoch seconds,
// matching the AWS networkmonitor wire format (Iso8601Timestamp = JSON Number).
func epochSeconds(t *time.Time) *float64 {
	if t == nil {
		return nil
	}

	secs := float64(t.UnixNano()) / float64(time.Second)

	return &secs
}

// toProbeWire converts a *Probe to probeWireBody with epoch-second timestamps.
func toProbeWire(p *Probe) *probeWireBody {
	if p == nil {
		return nil
	}

	return &probeWireBody{
		CreatedAt:       epochSeconds(p.CreatedAt),
		ModifiedAt:      epochSeconds(p.ModifiedAt),
		Tags:            p.Tags,
		PacketSize:      p.PacketSize,
		DestinationPort: p.DestinationPort,
		Destination:     p.Destination,
		SourceArn:       p.SourceArn,
		Protocol:        p.Protocol,
		State:           p.State,
		AddressFamily:   p.AddressFamily,
		VpcID:           p.VpcID,
		ProbeID:         p.ProbeID,
		ProbeArn:        p.ProbeArn,
	}
}

// extractMonitorName extracts the monitor name from /monitors/{name}[/...].
func extractMonitorName(path string) string {
	trimmed := strings.TrimPrefix(path, "/monitors/")
	if trimmed == path {
		return ""
	}

	parts := strings.SplitN(trimmed, "/", splitTwo)

	return parts[0]
}

// extractProbeID extracts the probe ID from /monitors/{name}/probes/{probeId}.
func extractProbeID(path string) string {
	// path format: /monitors/{name}/probes/{probeId}
	trimmed := strings.TrimPrefix(path, "/monitors/")
	if trimmed == path {
		return ""
	}

	parts := strings.SplitN(trimmed, "/", splitThree)
	if len(parts) < 3 || parts[1] != "probes" {
		return ""
	}

	return parts[2]
}

// extractTagResourceARN extracts the resource ARN from /tags/{resourceArn}.
func extractTagResourceARN(path string) string {
	trimmed := strings.TrimPrefix(path, nmPathTags)
	if trimmed == path {
		return ""
	}

	return trimmed
}

func (h *Handler) handleCreateMonitor(ctx context.Context, body []byte) ([]byte, error) {
	var req createMonitorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.MonitorName == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	m, err := h.Backend.CreateMonitor(
		ctx,
		req.MonitorName,
		req.AggregationPeriod,
		req.Probes,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	period := m.AggregationPeriod
	resp := createMonitorResponse{
		MonitorArn:        m.MonitorArn,
		MonitorName:       m.MonitorName,
		State:             m.State,
		AggregationPeriod: &period,
		Tags:              m.Tags,
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteMonitor(ctx context.Context, path string) ([]byte, error) {
	name := extractMonitorName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	if err := h.Backend.DeleteMonitor(ctx, name); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleGetMonitor(ctx context.Context, path string) ([]byte, error) {
	name := extractMonitorName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	m, err := h.Backend.GetMonitor(ctx, name)
	if err != nil {
		return nil, err
	}

	probes := make([]*probeWireBody, len(m.Probes))
	for i, p := range m.Probes {
		probes[i] = toProbeWire(p)
	}

	resp := getMonitorResponse{
		MonitorArn:        m.MonitorArn,
		MonitorName:       m.MonitorName,
		State:             m.State,
		AggregationPeriod: m.AggregationPeriod,
		Probes:            probes,
		Tags:              m.Tags,
		CreatedAt:         epochSeconds(m.CreatedAt),
		ModifiedAt:        epochSeconds(m.ModifiedAt),
	}

	return json.Marshal(resp)
}

func (h *Handler) handleUpdateMonitor(
	ctx context.Context,
	path string,
	body []byte,
) ([]byte, error) {
	name := extractMonitorName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	var req updateMonitorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	m, err := h.Backend.UpdateMonitor(ctx, name, req.AggregationPeriod)
	if err != nil {
		return nil, err
	}

	period := m.AggregationPeriod
	resp := updateMonitorResponse{
		MonitorArn:        m.MonitorArn,
		MonitorName:       m.MonitorName,
		State:             m.State,
		AggregationPeriod: &period,
		Tags:              m.Tags,
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListMonitors(ctx context.Context, query url.Values) ([]byte, error) {
	state := query.Get("state")
	nextToken := query.Get("nextToken")
	maxResults := 0

	if mr := query.Get("maxResults"); mr != "" {
		if _, err := fmt.Sscanf(mr, "%d", &maxResults); err != nil {
			maxResults = 0
		}
	}

	summaries, outToken, err := h.Backend.ListMonitors(ctx, state, nextToken, maxResults)
	if err != nil {
		return nil, err
	}

	resp := listMonitorsResponse{
		Monitors:  summaries,
		NextToken: outToken,
	}

	return json.Marshal(resp)
}

func (h *Handler) handleCreateProbe(ctx context.Context, path string, body []byte) ([]byte, error) {
	monitorName := extractMonitorName(path)
	if monitorName == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	var req createProbeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.Probe == nil {
		return nil, fmt.Errorf("%w: probe is required", ErrValidation)
	}

	probe, err := h.Backend.CreateProbe(ctx, monitorName, req.Probe, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toProbeWire(probe))
}

func (h *Handler) handleDeleteProbe(ctx context.Context, path string) ([]byte, error) {
	monitorName := extractMonitorName(path)
	probeID := extractProbeID(path)

	if monitorName == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	if probeID == "" {
		return nil, fmt.Errorf("%w: probeId is required", ErrValidation)
	}

	if err := h.Backend.DeleteProbe(ctx, monitorName, probeID); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleGetProbe(ctx context.Context, path string) ([]byte, error) {
	monitorName := extractMonitorName(path)
	probeID := extractProbeID(path)

	if monitorName == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	if probeID == "" {
		return nil, fmt.Errorf("%w: probeId is required", ErrValidation)
	}

	probe, err := h.Backend.GetProbe(ctx, monitorName, probeID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toProbeWire(probe))
}

func (h *Handler) handleUpdateProbe(ctx context.Context, path string, body []byte) ([]byte, error) {
	monitorName := extractMonitorName(path)
	probeID := extractProbeID(path)

	if monitorName == "" {
		return nil, fmt.Errorf("%w: monitorName is required", ErrValidation)
	}

	if probeID == "" {
		return nil, fmt.Errorf("%w: probeId is required", ErrValidation)
	}

	var req updateProbeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	probe, err := h.Backend.UpdateProbe(ctx, monitorName, probeID, &req)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toProbeWire(probe))
}

func (h *Handler) handleListTagsForResource(ctx context.Context, path string) ([]byte, error) {
	resourceARN := extractTagResourceARN(path)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", ErrValidation)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return nil, err
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return json.Marshal(listTagsForResourceResponse{Tags: tags})
}

func (h *Handler) handleTagResource(ctx context.Context, path string, body []byte) ([]byte, error) {
	resourceARN := extractTagResourceARN(path)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", ErrValidation)
	}

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(ctx, resourceARN, req.Tags); err != nil {
		return nil, err
	}

	return nil, nil
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	path string,
	query url.Values,
) ([]byte, error) {
	resourceARN := extractTagResourceARN(path)
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", ErrValidation)
	}

	tagKeys := query["tagKeys"]

	if err := h.Backend.UntagResource(ctx, resourceARN, tagKeys); err != nil {
		return nil, err
	}

	return nil, nil
}
