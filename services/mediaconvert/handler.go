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
	mcMatchPriority    = service.PriorityPathVersioned
	pathPrefix         = "/2017-08-29/"
	queuesPath         = "/2017-08-29/queues"
	jobTemplatesPath   = "/2017-08-29/jobTemplates"
	jobsPath           = "/2017-08-29/jobs"
	endpointsPath      = "/2017-08-29/endpoints"
	tagsPath           = "/2017-08-29/tags"
	presetsPath        = "/2017-08-29/presets"
	policyPath         = "/2017-08-29/policy"
	certificatesPath   = "/2017-08-29/certificates"
	jobsQueriesPath    = "/2017-08-29/jobsQueries"
	resourceSharesPath = "/2017-08-29/resourceShares"
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
		"AssociateCertificate",
		"CancelJob",
		"CreateJob",
		"CreateJobTemplate",
		"CreatePreset",
		"CreateQueue",
		"CreateResourceShare",
		"DeleteJobTemplate",
		"DeletePolicy",
		"DeletePreset",
		"DeleteQueue",
		"DescribeEndpoints",
		"DisassociateCertificate",
		"GetJob",
		"GetJobTemplate",
		"GetJobsQueryResults",
		"GetPolicy",
		"GetPreset",
		"GetQueue",
		"ListJobTemplates",
		"ListJobs",
		"ListPresets",
		"ListQueues",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"UpdateJobTemplate",
		"UpdateQueue",
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

	if handled, result := h.dispatchReadOnly(c, route); handled {
		return result
	}

	return h.dispatchMutating(c, route, readBody)
}

// dispatchReadOnly handles operations that do not require a request body.
// This includes both read operations and DELETE operations where no body is needed.
func (h *Handler) dispatchReadOnly(c *echo.Context, route mcRoute) (bool, error) {
	switch route.operation {
	case "ListQueues":
		return true, h.handleListQueues(c)
	case "GetQueue":
		return true, h.handleGetQueue(c, route.resource)
	case "DeleteQueue":
		return true, h.handleDeleteQueue(c, route.resource)
	case "ListJobTemplates":
		return true, h.handleListJobTemplates(c)
	case "GetJobTemplate":
		return true, h.handleGetJobTemplate(c, route.resource)
	case "DeleteJobTemplate":
		return true, h.handleDeleteJobTemplate(c, route.resource)
	case "ListJobs":
		return true, h.handleListJobs(c)
	case "GetJob":
		return true, h.handleGetJob(c, route.resource)
	case "CancelJob":
		return true, h.handleCancelJob(c, route.resource)
	case "DescribeEndpoints":
		return true, h.handleDescribeEndpoints(c)
	case "ListTagsForResource":
		return true, h.handleListTagsForResource(c, route.resource)
	case "UntagResource":
		return true, h.handleUntagResource(c, route.resource)
	}

	return h.dispatchReadOnlyNewOps(c, route)
}

// dispatchReadOnlyNewOps handles newer operations that do not require a request body.
// This includes both read operations and DELETE operations where no body is needed.
func (h *Handler) dispatchReadOnlyNewOps(c *echo.Context, route mcRoute) (bool, error) {
	switch route.operation {
	case "ListPresets":
		return true, h.handleListPresets(c)
	case "GetPreset":
		return true, h.handleGetPreset(c, route.resource)
	case "DeletePreset":
		return true, h.handleDeletePreset(c, route.resource)
	case "GetPolicy":
		return true, h.handleGetPolicy(c)
	case "DeletePolicy":
		return true, h.handleDeletePolicy(c)
	case "DisassociateCertificate":
		return true, h.handleDisassociateCertificate(c, route.resource)
	case "GetJobsQueryResults":
		return true, h.handleGetJobsQueryResults(c, route.resource)
	}

	return false, nil
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
	case "CreatePreset":
		return h.handleCreatePreset(c, body)
	case "AssociateCertificate":
		return h.handleAssociateCertificate(c, body)
	case "CreateResourceShare":
		return h.handleCreateResourceShare(c, body)
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
	case strings.HasPrefix(path, jobsQueriesPath):
		return parseJobsQueriesRoute(method, strings.TrimPrefix(path, jobsQueriesPath))
	case strings.HasPrefix(path, jobsPath):
		return parseJobRoute(method, strings.TrimPrefix(path, jobsPath))
	case strings.HasPrefix(path, presetsPath):
		return parsePresetRoute(method, strings.TrimPrefix(path, presetsPath))
	case strings.HasPrefix(path, tagsPath):
		return parseTagRoute(method, strings.TrimPrefix(path, tagsPath))
	case strings.HasPrefix(path, certificatesPath):
		return parseCertificateRoute(method, strings.TrimPrefix(path, certificatesPath))
	case path == policyPath:
		return parsePolicyRoute(method)
	case path == endpointsPath:
		return mcRoute{operation: "DescribeEndpoints"}
	case path == resourceSharesPath:
		if method == http.MethodPost {
			return mcRoute{operation: "CreateResourceShare"}
		}
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

func parsePresetRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: "ListPresets"}
		case http.MethodPost:
			return mcRoute{operation: "CreatePreset"}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: "GetPreset", resource: name}
	case http.MethodDelete:
		return mcRoute{operation: "DeletePreset", resource: name}
	}

	return mcRoute{operation: "Unknown"}
}

func parsePolicyRoute(method string) mcRoute {
	switch method {
	case http.MethodGet:
		return mcRoute{operation: "GetPolicy"}
	case http.MethodDelete:
		return mcRoute{operation: "DeletePolicy"}
	}

	return mcRoute{operation: "Unknown"}
}

func parseCertificateRoute(method, suffix string) mcRoute {
	certARN := strings.TrimPrefix(suffix, "/")

	if certARN == "" {
		if method == http.MethodPost {
			return mcRoute{operation: "AssociateCertificate"}
		}

		return mcRoute{operation: "Unknown"}
	}

	if method == http.MethodDelete {
		return mcRoute{operation: "DisassociateCertificate", resource: certARN}
	}

	return mcRoute{operation: "Unknown"}
}

func parseJobsQueriesRoute(method, suffix string) mcRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id != "" && method == http.MethodGet {
		return mcRoute{operation: "GetJobsQueryResults", resource: id}
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

// --- Preset handlers ---

type createPresetInput struct {
	Settings    map[string]any `json:"settings,omitempty"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Category    string         `json:"category,omitempty"`
}

type presetWrapper struct {
	Preset *Preset `json:"preset"`
}

type presetsListOutput struct {
	Presets []*Preset `json:"presets"`
}

func (h *Handler) handleCreatePreset(c *echo.Context, body []byte) error {
	var in createPresetInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	p, err := h.Backend.CreatePreset(in.Name, in.Description, in.Category, in.Settings)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, presetWrapper{Preset: p})
}

func (h *Handler) handleGetPreset(c *echo.Context, name string) error {
	p, err := h.Backend.GetPreset(name)
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

func (h *Handler) handleDeletePreset(c *echo.Context, name string) error {
	if err := h.Backend.DeletePreset(name); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Policy handlers ---

type policyWrapper struct {
	Policy *Policy `json:"policy"`
}

func (h *Handler) handleGetPolicy(c *echo.Context) error {
	p, err := h.Backend.GetPolicy()
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, policyWrapper{Policy: p})
}

func (h *Handler) handleDeletePolicy(c *echo.Context) error {
	if err := h.Backend.DeletePolicy(); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Certificate handlers ---

type associateCertificateInput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleAssociateCertificate(c *echo.Context, body []byte) error {
	var in associateCertificateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Arn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "arn is required"))
	}

	if err := h.Backend.AssociateCertificate(in.Arn); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDisassociateCertificate(c *echo.Context, certARN string) error {
	if err := h.Backend.DisassociateCertificate(certARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Jobs query handlers ---

type jobsQueryResultsOutput struct {
	Jobs []*Job `json:"jobs"`
}

func (h *Handler) handleGetJobsQueryResults(c *echo.Context, queryID string) error {
	jobs := h.Backend.GetJobsQueryResults(queryID)

	return c.JSON(http.StatusOK, jobsQueryResultsOutput{Jobs: jobs})
}

// --- Resource share handlers ---

type createResourceShareInput struct {
	JobID string `json:"jobId"`
}

func (h *Handler) handleCreateResourceShare(c *echo.Context, body []byte) error {
	var in createResourceShareInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.JobID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "jobId is required"))
	}

	if _, err := h.Backend.CreateResourceShare(in.JobID); err != nil {
		return h.writeError(c, err)
	}

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
