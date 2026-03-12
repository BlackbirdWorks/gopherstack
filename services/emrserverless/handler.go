package emrserverless

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	pathApplications     = "/applications"
	pathTags             = "/tags/"
	emrServerlessService = "emr-serverless"
	emrMatchPriority     = 87
)

// Handler is the Echo HTTP handler for EMR Serverless operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new EMR Serverless handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "EmrServerless" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"UpdateApplication",
		"DeleteApplication",
		"StartApplication",
		"StopApplication",
		"StartJobRun",
		"GetJobRun",
		"ListJobRuns",
		"CancelJobRun",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "emr-serverless" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches EMR Serverless requests.
// For /applications paths, it additionally checks the Authorization header
// service name to distinguish from AppConfig (which also uses /applications).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, pathTags+"arn:aws:emr-serverless:") {
			return true
		}

		if path == pathApplications || strings.HasPrefix(path, pathApplications+"/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == emrServerlessService
		}

		return false
	}
}

// MatchPriority returns the routing priority.
// Uses 87 to be evaluated before AppConfig (priority 86) which also uses /applications paths.
func (h *Handler) MatchPriority() int { return emrMatchPriority }

const (
	pathPartsApplication = 1
	pathPartsWithSub     = 2
	pathPartsWithJobRun  = 3
)

// emrRoute holds the parsed route information.
type emrRoute struct {
	applicationID string
	jobRunID      string
	resourceARN   string
	operation     string
}

// parseEMRPath maps HTTP method + path to an operation and resource identifiers.
func parseEMRPath(method, rawPath string) emrRoute {
	path, _ := url.PathUnescape(rawPath)

	if after, ok := strings.CutPrefix(path, pathTags); ok {
		return parseTagRoute(method, after)
	}

	if path == pathApplications {
		return parseApplicationsCollection(method)
	}

	suffix := strings.TrimPrefix(path, pathApplications+"/")
	parts := strings.SplitN(suffix, "/", pathPartsWithJobRun)

	switch len(parts) {
	case pathPartsApplication:
		return parseSingleAppRoute(method, parts[0])
	case pathPartsWithSub:
		return parseAppSubRoute(method, parts[0], parts[1])
	case pathPartsWithJobRun:
		return parseJobRunRoute(method, parts[0], parts[1], parts[2])
	}

	return emrRoute{operation: "Unknown"}
}

func parseTagRoute(method, resourceARN string) emrRoute {
	switch method {
	case http.MethodGet:
		return emrRoute{operation: "ListTagsForResource", resourceARN: resourceARN}
	case http.MethodPost:
		return emrRoute{operation: "TagResource", resourceARN: resourceARN}
	case http.MethodDelete:
		return emrRoute{operation: "UntagResource", resourceARN: resourceARN}
	}

	return emrRoute{operation: "Unknown"}
}

func parseApplicationsCollection(method string) emrRoute {
	switch method {
	case http.MethodPost:
		return emrRoute{operation: "CreateApplication"}
	case http.MethodGet:
		return emrRoute{operation: "ListApplications"}
	}

	return emrRoute{operation: "Unknown"}
}

func parseSingleAppRoute(method, appID string) emrRoute {
	switch method {
	case http.MethodGet:
		return emrRoute{operation: "GetApplication", applicationID: appID}
	case http.MethodPatch:
		return emrRoute{operation: "UpdateApplication", applicationID: appID}
	case http.MethodDelete:
		return emrRoute{operation: "DeleteApplication", applicationID: appID}
	}

	return emrRoute{operation: "Unknown"}
}

func parseAppSubRoute(method, appID, sub string) emrRoute {
	switch sub {
	case "start":
		if method == http.MethodPost {
			return emrRoute{operation: "StartApplication", applicationID: appID}
		}
	case "stop":
		if method == http.MethodPost {
			return emrRoute{operation: "StopApplication", applicationID: appID}
		}
	case "jobruns":
		switch method {
		case http.MethodPost:
			return emrRoute{operation: "StartJobRun", applicationID: appID}
		case http.MethodGet:
			return emrRoute{operation: "ListJobRuns", applicationID: appID}
		}
	}

	return emrRoute{operation: "Unknown"}
}

func parseJobRunRoute(method, appID, sub, jobRunID string) emrRoute {
	if sub != "jobruns" {
		return emrRoute{operation: "Unknown"}
	}

	switch method {
	case http.MethodGet:
		return emrRoute{operation: "GetJobRun", applicationID: appID, jobRunID: jobRunID}
	case http.MethodDelete:
		return emrRoute{operation: "CancelJobRun", applicationID: appID, jobRunID: jobRunID}
	}

	return emrRoute{operation: "Unknown"}
}

// ExtractOperation returns the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	route := parseEMRPath(c.Request().Method, c.Request().URL.Path)

	return route.operation
}

// ExtractResource extracts a resource identifier from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	route := parseEMRPath(c.Request().Method, c.Request().URL.Path)

	if route.resourceARN != "" {
		return route.resourceARN
	}

	if route.jobRunID != "" {
		return route.applicationID + "/" + route.jobRunID
	}

	return route.applicationID
}

// Handler returns the Echo handler function for EMR Serverless requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		log := logger.Load(r.Context())
		route := parseEMRPath(r.Method, r.URL.Path)

		log.Debug("emrserverless request", "operation", route.operation)

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "emrserverless: failed to read request body", "error", err)

			return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", "internal server error"))
		}

		return h.dispatch(c, route, body)
	}
}

//nolint:cyclop // dispatch table for 14 REST operations is inherently wide
func (h *Handler) dispatch(c *echo.Context, route emrRoute, body []byte) error {
	switch route.operation {
	case "CreateApplication":
		return h.handleCreateApplication(c, body)
	case "GetApplication":
		return h.handleGetApplication(c, route.applicationID)
	case "ListApplications":
		return h.handleListApplications(c)
	case "UpdateApplication":
		return h.handleUpdateApplication(c, route.applicationID, body)
	case "DeleteApplication":
		return h.handleDeleteApplication(c, route.applicationID)
	case "StartApplication":
		return h.handleStartApplication(c, route.applicationID)
	case "StopApplication":
		return h.handleStopApplication(c, route.applicationID)
	case "StartJobRun":
		return h.handleStartJobRun(c, route.applicationID, body)
	case "GetJobRun":
		return h.handleGetJobRun(c, route.applicationID, route.jobRunID)
	case "ListJobRuns":
		return h.handleListJobRuns(c, route.applicationID)
	case "CancelJobRun":
		return h.handleCancelJobRun(c, route.applicationID, route.jobRunID)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, route.resourceARN)
	case "TagResource":
		return h.handleTagResource(c, route.resourceARN, body)
	case "UntagResource":
		return h.handleUntagResource(c, route.resourceARN, c.Request().URL.Query())
	default:
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("ConflictException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"code": code, "message": msg}
}

// epochSeconds converts a [time.Time] to a float64 Unix epoch seconds value,
// matching the AWS REST-JSON timestamp serialization format.
func epochSeconds(ts interface{ Unix() int64 }) float64 {
	return float64(ts.Unix())
}

// applicationToMap converts an Application to a map with float64 timestamps
// for correct AWS REST-JSON serialization. Returns a map representation with
// createdAt/updatedAt as float64 Unix epoch seconds values.
func applicationToMap(app *Application) map[string]any {
	m := map[string]any{
		"applicationId": app.ApplicationID,
		"arn":           app.Arn,
		"name":          app.Name,
		"type":          app.Type,
		"releaseLabel":  app.ReleaseLabel,
		"state":         app.State,
		"createdAt":     epochSeconds(app.CreatedAt),
		"updatedAt":     epochSeconds(app.UpdatedAt),
	}

	if len(app.Tags) > 0 {
		m["tags"] = app.Tags
	}

	return m
}

// jobRunToMap converts a JobRun to a map with float64 timestamps
// for correct AWS REST-JSON serialization. Returns a map representation with
// createdAt/updatedAt as float64 Unix epoch seconds values.
func jobRunToMap(jr *JobRun) map[string]any {
	m := map[string]any{
		"applicationId":    jr.ApplicationID,
		"jobRunId":         jr.JobRunID,
		"arn":              jr.Arn,
		"name":             jr.Name,
		"state":            jr.State,
		"executionRoleArn": jr.ExecutionRoleArn,
		"createdAt":        epochSeconds(jr.CreatedAt),
		"updatedAt":        epochSeconds(jr.UpdatedAt),
	}

	if len(jr.Tags) > 0 {
		m["tags"] = jr.Tags
	}

	return m
}

// --- Application handlers ---

type createApplicationBody struct {
	Tags         map[string]string `json:"tags"`
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	ReleaseLabel string            `json:"releaseLabel"`
}

type createApplicationResponse struct {
	ApplicationID string `json:"applicationId"`
	Arn           string `json:"arn"`
	Name          string `json:"name"`
}

func (h *Handler) handleCreateApplication(c *echo.Context, body []byte) error {
	var in createApplicationBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	app, err := h.Backend.CreateApplication(in.Name, in.Type, in.ReleaseLabel, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createApplicationResponse{
		ApplicationID: app.ApplicationID,
		Arn:           app.Arn,
		Name:          app.Name,
	})
}

func (h *Handler) handleGetApplication(c *echo.Context, applicationID string) error {
	app, err := h.Backend.GetApplication(applicationID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"application": applicationToMap(app)})
}

func (h *Handler) handleListApplications(c *echo.Context) error {
	apps := h.Backend.ListApplications()
	list := make([]map[string]any, 0, len(apps))

	for _, app := range apps {
		list = append(list, applicationToMap(app))
	}

	return c.JSON(http.StatusOK, map[string]any{"applications": list})
}

type updateApplicationBody struct {
	ReleaseLabel string `json:"releaseLabel"`
}

func (h *Handler) handleUpdateApplication(c *echo.Context, applicationID string, body []byte) error {
	var in updateApplicationBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	app, err := h.Backend.UpdateApplication(applicationID, func(a *Application) {
		if in.ReleaseLabel != "" {
			a.ReleaseLabel = in.ReleaseLabel
		}
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"application": applicationToMap(app)})
}

func (h *Handler) handleDeleteApplication(c *echo.Context, applicationID string) error {
	if err := h.Backend.DeleteApplication(applicationID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStartApplication(c *echo.Context, applicationID string) error {
	if err := h.Backend.StartApplication(applicationID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStopApplication(c *echo.Context, applicationID string) error {
	if err := h.Backend.StopApplication(applicationID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- JobRun handlers ---

type startJobRunBody struct {
	Tags             map[string]string `json:"tags"`
	ExecutionRoleArn string            `json:"executionRoleArn"`
	Name             string            `json:"name"`
}

type startJobRunResponse struct {
	ApplicationID string `json:"applicationId"`
	Arn           string `json:"arn"`
	JobRunID      string `json:"jobRunId"`
}

func (h *Handler) handleStartJobRun(c *echo.Context, applicationID string, body []byte) error {
	var in startJobRunBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	jr, err := h.Backend.StartJobRun(applicationID, in.ExecutionRoleArn, in.Name, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, startJobRunResponse{
		ApplicationID: jr.ApplicationID,
		Arn:           jr.Arn,
		JobRunID:      jr.JobRunID,
	})
}

func (h *Handler) handleGetJobRun(c *echo.Context, applicationID, jobRunID string) error {
	jr, err := h.Backend.GetJobRun(applicationID, jobRunID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"jobRun": jobRunToMap(jr)})
}

func (h *Handler) handleListJobRuns(c *echo.Context, applicationID string) error {
	runs, err := h.Backend.ListJobRuns(applicationID)
	if err != nil {
		return h.handleError(c, err)
	}

	list := make([]map[string]any, 0, len(runs))

	for _, jr := range runs {
		list = append(list, jobRunToMap(jr))
	}

	return c.JSON(http.StatusOK, map[string]any{"jobRuns": list})
}

func (h *Handler) handleCancelJobRun(c *echo.Context, applicationID, jobRunID string) error {
	jr, err := h.Backend.CancelJobRun(applicationID, jobRunID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"applicationId": jr.ApplicationID,
		"jobRunId":      jr.JobRunID,
	})
}

// --- Tags handlers ---

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

type tagResourceBody struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	var in tagResourceBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	if err := h.Backend.TagResource(resourceARN, in.Tags); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, query url.Values) error {
	tagKeys := query["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
