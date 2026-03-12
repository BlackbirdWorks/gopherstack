package mediaconvert

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
	mcMatchPriority  = service.PriorityPathVersioned
	pathPrefix       = "/2017-08-29/"
	queuesPath       = "/2017-08-29/queues"
	jobTemplatesPath = "/2017-08-29/jobTemplates"
	jobsPath         = "/2017-08-29/jobs"
	endpointsPath    = "/2017-08-29/endpoints"
	tagsPath         = "/2017-08-29/tags"
)

// Handler is the Echo HTTP handler for Amazon MediaConvert operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new MediaConvert handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaConvert" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateQueue",
		"GetQueue",
		"ListQueues",
		"UpdateQueue",
		"DeleteQueue",
		"CreateJobTemplate",
		"GetJobTemplate",
		"ListJobTemplates",
		"UpdateJobTemplate",
		"DeleteJobTemplate",
		"CreateJob",
		"GetJob",
		"ListJobs",
		"CancelJob",
		"DescribeEndpoints",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "mediaconvert" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches MediaConvert requests.
// MediaConvert uses REST paths prefixed with /2017-08-29/.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, pathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return mcMatchPriority }

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
func (h *Handler) dispatch(c *echo.Context, route mcRoute) error {
	r := c.Request()
	log := logger.Load(r.Context())

	readBody := func() ([]byte, bool) {
		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "mediaconvert: failed to read request body", "error", err)

			return nil, false
		}

		return body, true
	}

	switch route.operation {
	case "ListQueues":
		return h.handleListQueues(c)
	case "GetQueue":
		return h.handleGetQueue(c, route.resource)
	case "DeleteQueue":
		return h.handleDeleteQueue(c, route.resource)
	case "ListJobTemplates":
		return h.handleListJobTemplates(c)
	case "GetJobTemplate":
		return h.handleGetJobTemplate(c, route.resource)
	case "DeleteJobTemplate":
		return h.handleDeleteJobTemplate(c, route.resource)
	case "ListJobs":
		return h.handleListJobs(c)
	case "GetJob":
		return h.handleGetJob(c, route.resource)
	case "CancelJob":
		return h.handleCancelJob(c, route.resource)
	case "DescribeEndpoints":
		return h.handleDescribeEndpoints(c)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, route.resource)
	case "UntagResource":
		return h.handleUntagResource(c, route.resource)
	}

	return h.dispatchMutating(c, route, readBody)
}

// dispatchMutating handles write operations that require reading a request body.
func (h *Handler) dispatchMutating(c *echo.Context, route mcRoute, readBody func() ([]byte, bool)) error {
	body, ok := readBody()
	if !ok {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalError", "internal server error"))
	}

	switch route.operation {
	case "CreateQueue":
		return h.handleCreateQueue(c, body)
	case "UpdateQueue":
		return h.handleUpdateQueue(c, route.resource, body)
	case "CreateJobTemplate":
		return h.handleCreateJobTemplate(c, body)
	case "UpdateJobTemplate":
		return h.handleUpdateJobTemplate(c, route.resource, body)
	case "CreateJob":
		return h.handleCreateJob(c, body)
	case "TagResource":
		return h.handleTagResource(c, route.resource, body)
	}

	return c.JSON(
		http.StatusNotFound,
		errorResponse("NotFoundException", "unknown operation: "+c.Request().URL.Path),
	)
}

// mcRoute holds the parsed route information.
type mcRoute struct {
	resource  string
	operation string
}

// parseRoute maps HTTP method + path to an operation name and resource ID.
func parseRoute(method, path string) mcRoute {
	switch {
	case strings.HasPrefix(path, queuesPath):
		return parseQueueRoute(method, strings.TrimPrefix(path, queuesPath))
	case strings.HasPrefix(path, jobTemplatesPath):
		return parseJobTemplateRoute(method, strings.TrimPrefix(path, jobTemplatesPath))
	case strings.HasPrefix(path, jobsPath):
		return parseJobRoute(method, strings.TrimPrefix(path, jobsPath))
	case strings.HasPrefix(path, tagsPath):
		return parseTagRoute(method, strings.TrimPrefix(path, tagsPath))
	case path == endpointsPath:
		return mcRoute{operation: "DescribeEndpoints"}
	}

	return mcRoute{operation: "Unknown"}
}

func parseQueueRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: "ListQueues"}
		case http.MethodPost:
			return mcRoute{operation: "CreateQueue"}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: "GetQueue", resource: name}
	case http.MethodPut:
		return mcRoute{operation: "UpdateQueue", resource: name}
	case http.MethodDelete:
		return mcRoute{operation: "DeleteQueue", resource: name}
	}

	return mcRoute{operation: "Unknown"}
}

func parseJobTemplateRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: "ListJobTemplates"}
		case http.MethodPost:
			return mcRoute{operation: "CreateJobTemplate"}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: "GetJobTemplate", resource: name}
	case http.MethodPut:
		return mcRoute{operation: "UpdateJobTemplate", resource: name}
	case http.MethodDelete:
		return mcRoute{operation: "DeleteJobTemplate", resource: name}
	}

	return mcRoute{operation: "Unknown"}
}

func parseJobRoute(method, suffix string) mcRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: "ListJobs"}
		case http.MethodPost:
			return mcRoute{operation: "CreateJob"}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: "GetJob", resource: id}
	case http.MethodDelete:
		return mcRoute{operation: "CancelJob", resource: id}
	}

	return mcRoute{operation: "Unknown"}
}

func parseTagRoute(method, suffix string) mcRoute {
	resourceARN := strings.TrimPrefix(suffix, "/")

	switch method {
	case http.MethodGet:
		return mcRoute{operation: "ListTagsForResource", resource: resourceARN}
	case http.MethodPost:
		return mcRoute{operation: "TagResource", resource: resourceARN}
	case http.MethodDelete:
		return mcRoute{operation: "UntagResource", resource: resourceARN}
	}

	return mcRoute{operation: "Unknown"}
}

// --- Queue handlers ---

type createQueueInput struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	PricingPlan string `json:"pricingPlan,omitempty"`
	Status      string `json:"status,omitempty"`
}

type queueWrapper struct {
	Queue *Queue `json:"queue"`
}

type queuesListOutput struct {
	Queues []*Queue `json:"queues"`
}

func (h *Handler) handleCreateQueue(c *echo.Context, body []byte) error {
	var in createQueueInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	q, err := h.Backend.CreateQueue(in.Name, in.Description, in.PricingPlan, in.Status)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, queueWrapper{Queue: q})
}

func (h *Handler) handleGetQueue(c *echo.Context, name string) error {
	q, err := h.Backend.GetQueue(name)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, queueWrapper{Queue: q})
}

func (h *Handler) handleListQueues(c *echo.Context) error {
	queues := h.Backend.ListQueues()
	if queues == nil {
		queues = []*Queue{}
	}

	return c.JSON(http.StatusOK, queuesListOutput{Queues: queues})
}

type updateQueueInput struct {
	Description string `json:"description,omitempty"`
	Status      string `json:"status,omitempty"`
}

func (h *Handler) handleUpdateQueue(c *echo.Context, name string, body []byte) error {
	var in updateQueueInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	q, err := h.Backend.UpdateQueue(name, in.Description, in.Status)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, queueWrapper{Queue: q})
}

func (h *Handler) handleDeleteQueue(c *echo.Context, name string) error {
	if err := h.Backend.DeleteQueue(name); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Job Template handlers ---

type createJobTemplateInput struct {
	Settings    map[string]any `json:"settings,omitempty"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Category    string         `json:"category,omitempty"`
	Queue       string         `json:"queue,omitempty"`
	Priority    int            `json:"priority"`
}

type jobTemplateWrapper struct {
	JobTemplate *JobTemplate `json:"jobTemplate"`
}

type jobTemplatesListOutput struct {
	JobTemplates []*JobTemplate `json:"jobTemplates"`
}

func (h *Handler) handleCreateJobTemplate(c *echo.Context, body []byte) error {
	var in createJobTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	jt, err := h.Backend.CreateJobTemplate(in.Name, in.Description, in.Category, in.Queue, in.Priority, in.Settings)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, jobTemplateWrapper{JobTemplate: jt})
}

func (h *Handler) handleGetJobTemplate(c *echo.Context, name string) error {
	jt, err := h.Backend.GetJobTemplate(name)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobTemplateWrapper{JobTemplate: jt})
}

func (h *Handler) handleListJobTemplates(c *echo.Context) error {
	templates := h.Backend.ListJobTemplates()
	if templates == nil {
		templates = []*JobTemplate{}
	}

	return c.JSON(http.StatusOK, jobTemplatesListOutput{JobTemplates: templates})
}

type updateJobTemplateInput struct {
	Priority    *int           `json:"priority,omitempty"`
	Settings    map[string]any `json:"settings,omitempty"`
	Description string         `json:"description,omitempty"`
	Category    string         `json:"category,omitempty"`
	Queue       string         `json:"queue,omitempty"`
}

func (h *Handler) handleUpdateJobTemplate(c *echo.Context, name string, body []byte) error {
	var in updateJobTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	jt, err := h.Backend.UpdateJobTemplate(name, in.Description, in.Category, in.Queue, in.Priority, in.Settings)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobTemplateWrapper{JobTemplate: jt})
}

func (h *Handler) handleDeleteJobTemplate(c *echo.Context, name string) error {
	if err := h.Backend.DeleteJobTemplate(name); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Job handlers ---

type createJobInput struct {
	Settings    map[string]any `json:"settings,omitempty"`
	Role        string         `json:"role"`
	Queue       string         `json:"queue,omitempty"`
	JobTemplate string         `json:"jobTemplate,omitempty"`
}

type jobWrapper struct {
	Job *Job `json:"job"`
}

type jobsListOutput struct {
	Jobs []*Job `json:"jobs"`
}

func (h *Handler) handleCreateJob(c *echo.Context, body []byte) error {
	var in createJobInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Role == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "role is required"))
	}

	j, err := h.Backend.CreateJob(in.Role, in.Queue, in.JobTemplate, in.Settings)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, jobWrapper{Job: j})
}

func (h *Handler) handleGetJob(c *echo.Context, id string) error {
	j, err := h.Backend.GetJob(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobWrapper{Job: j})
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	jobs := h.Backend.ListJobs()
	if jobs == nil {
		jobs = []*Job{}
	}

	return c.JSON(http.StatusOK, jobsListOutput{Jobs: jobs})
}

func (h *Handler) handleCancelJob(c *echo.Context, id string) error {
	if err := h.Backend.CancelJob(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Endpoints handler ---

type endpointsOutput struct {
	Endpoints []endpointEntry `json:"endpoints"`
}

type endpointEntry struct {
	URL string `json:"url"`
}

func (h *Handler) handleDescribeEndpoints(c *echo.Context) error {
	r := c.Request()
	scheme := "http"

	if r.TLS != nil {
		scheme = "https"
	}

	url := scheme + "://" + r.Host
	out := endpointsOutput{
		Endpoints: []endpointEntry{{URL: url}},
	}

	return c.JSON(http.StatusOK, out)
}

// --- Tags handlers ---

type resourceTagsOutput struct {
	ResourceTags resourceTagsEntry `json:"resourceTags"`
}

type resourceTagsEntry struct {
	Tags map[string]string `json:"tags"`
	Arn  string            `json:"arn"`
}

type tagResourceInput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags := h.Backend.GetTags(resourceARN)
	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, resourceTagsOutput{
		ResourceTags: resourceTagsEntry{
			Arn:  resourceARN,
			Tags: tags,
		},
	})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	h.Backend.TagResource(resourceARN, in.Tags)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]
	h.Backend.UntagResource(resourceARN, tagKeys)

	return c.NoContent(http.StatusNoContent)
}

// --- Error handling ---

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalError", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}
