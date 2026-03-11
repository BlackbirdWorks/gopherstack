package elastictranscoder

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	etMatchPriority = service.PriorityPathVersioned
	pathPrefix      = "/2012-09-25/"
	pipelinesPath   = "/2012-09-25/pipelines"
	presetsPath     = "/2012-09-25/presets"
	jobsPath        = "/2012-09-25/jobs"
)

// Handler is the Echo HTTP handler for Amazon Elastic Transcoder operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Elastic Transcoder handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ElasticTranscoder" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreatePipeline",
		"ReadPipeline",
		"ListPipelines",
		"UpdatePipeline",
		"DeletePipeline",
		"CreatePreset",
		"ReadPreset",
		"ListPresets",
		"DeletePreset",
		"CreateJob",
		"ReadJob",
		"ListJobsByPipeline",
		"CancelJob",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "elastictranscoder" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Elastic Transcoder requests.
// Elastic Transcoder uses REST paths prefixed with /2012-09-25/.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, pathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return etMatchPriority }

// ExtractOperation returns the operation name from the request path and method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).operation
}

// ExtractResource extracts a resource ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		route := parseRoute(r.Method, r.URL.Path)

		return h.dispatch(c, route)
	}
}

// dispatch routes the request to the appropriate handler based on the parsed route.
func (h *Handler) dispatch(c *echo.Context, route etRoute) error {
	r := c.Request()
	log := logger.Load(r.Context())

	readBody := func() ([]byte, bool) {
		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "elastictranscoder: failed to read request body", "error", err)

			return nil, false
		}

		return body, true
	}

	switch route.operation {
	case "ListPipelines":
		return h.handleListPipelines(c)
	case "ReadPipeline":
		return h.handleReadPipeline(c, route.resource)
	case "DeletePipeline":
		return h.handleDeletePipeline(c, route.resource)
	case "ListPresets":
		return h.handleListPresets(c)
	case "ReadPreset":
		return h.handleReadPreset(c, route.resource)
	case "DeletePreset":
		return h.handleDeletePreset(c, route.resource)
	case "ReadJob":
		return h.handleReadJob(c, route.resource)
	case "CancelJob":
		return h.handleCancelJob(c, route.resource)
	case "ListJobsByPipeline":
		return h.handleListJobsByPipeline(c, route.resource)
	}

	return h.dispatchMutating(c, route, readBody)
}

// dispatchMutating handles write operations that require reading a request body.
func (h *Handler) dispatchMutating(c *echo.Context, route etRoute, readBody func() ([]byte, bool)) error {
	body, ok := readBody()
	if !ok {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	switch route.operation {
	case "CreatePipeline":
		return h.handleCreatePipeline(c, body)
	case "UpdatePipeline":
		return h.handleUpdatePipeline(c, route.resource, body)
	case "CreatePreset":
		return h.handleCreatePreset(c, body)
	case "CreateJob":
		return h.handleCreateJob(c, body)
	}

	return c.JSON(
		http.StatusNotFound,
		errorResponse("UnknownOperationException", "unknown operation: "+c.Request().URL.Path),
	)
}

// etRoute holds the parsed route information.
type etRoute struct {
	resource  string
	operation string
}

// parseRoute maps HTTP method + path to an operation name and resource ID.
func parseRoute(method, path string) etRoute {
	// Check jobsByPipeline BEFORE jobs to avoid prefix collision.
	switch {
	case strings.HasPrefix(path, "/2012-09-25/jobsByPipeline/"):
		id := strings.TrimPrefix(path, "/2012-09-25/jobsByPipeline/")

		return etRoute{operation: "ListJobsByPipeline", resource: id}
	case strings.HasPrefix(path, pipelinesPath):
		return parsePipelineRoute(method, strings.TrimPrefix(path, pipelinesPath))
	case strings.HasPrefix(path, presetsPath):
		return parsePresetRoute(method, strings.TrimPrefix(path, presetsPath))
	case strings.HasPrefix(path, jobsPath):
		return parseJobRoute(method, strings.TrimPrefix(path, jobsPath))
	}

	return etRoute{operation: "Unknown"}
}

func parsePipelineRoute(method, suffix string) etRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return etRoute{operation: "ListPipelines"}
		case http.MethodPost:
			return etRoute{operation: "CreatePipeline"}
		}
	}

	switch method {
	case http.MethodGet:
		return etRoute{operation: "ReadPipeline", resource: id}
	case http.MethodPut:
		return etRoute{operation: "UpdatePipeline", resource: id}
	case http.MethodDelete:
		return etRoute{operation: "DeletePipeline", resource: id}
	}

	return etRoute{operation: "Unknown"}
}

func parsePresetRoute(method, suffix string) etRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return etRoute{operation: "ListPresets"}
		case http.MethodPost:
			return etRoute{operation: "CreatePreset"}
		}
	}

	switch method {
	case http.MethodGet:
		return etRoute{operation: "ReadPreset", resource: id}
	case http.MethodDelete:
		return etRoute{operation: "DeletePreset", resource: id}
	}

	return etRoute{operation: "Unknown"}
}

func parseJobRoute(method, suffix string) etRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" && method == http.MethodPost {
		return etRoute{operation: "CreateJob"}
	}

	switch method {
	case http.MethodGet:
		return etRoute{operation: "ReadJob", resource: id}
	case http.MethodDelete:
		return etRoute{operation: "CancelJob", resource: id}
	}

	return etRoute{operation: "Unknown"}
}

// --- Pipeline handlers ---

type createPipelineInput struct {
	Name         string `json:"Name"`
	InputBucket  string `json:"InputBucket"`
	OutputBucket string `json:"OutputBucket,omitempty"`
	Role         string `json:"Role"`
}

type pipelineWrapper struct {
	Pipeline *Pipeline `json:"Pipeline"`
}

type pipelinesListOutput struct {
	Pipelines []*Pipeline `json:"Pipelines"`
}

func (h *Handler) handleCreatePipeline(c *echo.Context, body []byte) error {
	var in createPipelineInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if in.Name == "" || in.InputBucket == "" || in.Role == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "Name, InputBucket and Role are required"),
		)
	}

	p, err := h.Backend.CreatePipeline(in.Name, in.InputBucket, in.OutputBucket, in.Role)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, pipelineWrapper{Pipeline: p})
}

func (h *Handler) handleReadPipeline(c *echo.Context, id string) error {
	p, err := h.Backend.ReadPipeline(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, pipelineWrapper{Pipeline: p})
}

func (h *Handler) handleListPipelines(c *echo.Context) error {
	pipelines := h.Backend.ListPipelines()
	if pipelines == nil {
		pipelines = []*Pipeline{}
	}

	return c.JSON(http.StatusOK, pipelinesListOutput{Pipelines: pipelines})
}

type updatePipelineInput struct {
	Name         string `json:"Name"`
	InputBucket  string `json:"InputBucket,omitempty"`
	OutputBucket string `json:"OutputBucket,omitempty"`
	Role         string `json:"Role,omitempty"`
}

func (h *Handler) handleUpdatePipeline(c *echo.Context, id string, body []byte) error {
	var in updatePipelineInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	p, err := h.Backend.UpdatePipeline(id, in.Name, in.InputBucket, in.OutputBucket, in.Role)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, pipelineWrapper{Pipeline: p})
}

func (h *Handler) handleDeletePipeline(c *echo.Context, id string) error {
	if err := h.Backend.DeletePipeline(id); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusAccepted, struct{}{})
}

// --- Preset handlers ---

type createPresetInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
	Container   string `json:"Container"`
}

type presetWrapper struct {
	Preset *Preset `json:"Preset"`
}

type presetsListOutput struct {
	Presets []*Preset `json:"Presets"`
}

func (h *Handler) handleCreatePreset(c *echo.Context, body []byte) error {
	var in createPresetInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if in.Name == "" || in.Container == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "Name and Container are required"))
	}

	p, err := h.Backend.CreatePreset(in.Name, in.Description, in.Container)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, presetWrapper{Preset: p})
}

func (h *Handler) handleReadPreset(c *echo.Context, id string) error {
	p, err := h.Backend.ReadPreset(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, presetWrapper{Preset: p})
}

func (h *Handler) handleListPresets(c *echo.Context) error {
	presets := h.Backend.ListPresets()
	if presets == nil {
		presets = []*Preset{}
	}

	return c.JSON(http.StatusOK, presetsListOutput{Presets: presets})
}

func (h *Handler) handleDeletePreset(c *echo.Context, id string) error {
	if err := h.Backend.DeletePreset(id); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusAccepted, struct{}{})
}

// --- Job handlers ---

type createJobInput struct {
	PipelineID string `json:"PipelineId"`
}

type jobWrapper struct {
	Job *Job `json:"Job"`
}

type jobsListOutput struct {
	Jobs []*Job `json:"Jobs"`
}

func (h *Handler) handleCreateJob(c *echo.Context, body []byte) error {
	var in createJobInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if in.PipelineID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "PipelineId is required"))
	}

	j, err := h.Backend.CreateJob(in.PipelineID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, jobWrapper{Job: j})
}

func (h *Handler) handleReadJob(c *echo.Context, id string) error {
	j, err := h.Backend.ReadJob(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobWrapper{Job: j})
}

func (h *Handler) handleListJobsByPipeline(c *echo.Context, pipelineID string) error {
	jobs := h.Backend.ListJobsByPipeline(pipelineID)
	if jobs == nil {
		jobs = []*Job{}
	}

	return c.JSON(http.StatusOK, jobsListOutput{Jobs: jobs})
}

func (h *Handler) handleCancelJob(c *echo.Context, id string) error {
	if err := h.Backend.CancelJob(id); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusAccepted, struct{}{})
}

// --- Error handling ---

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ResourceInUseException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"Error": code, "Message": msg}
}
