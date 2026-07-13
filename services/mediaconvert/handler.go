package mediaconvert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opUnknown = "Unknown"

	defaultJobsPageSize = 20
)

const (
	orderDescending = "DESCENDING"
)

const (
	opAssociateCertificate    = "AssociateCertificate"
	opCancelJob               = "CancelJob"
	opCreateJob               = "CreateJob"
	opCreateJobTemplate       = "CreateJobTemplate"
	opCreatePreset            = "CreatePreset"
	opCreateQueue             = "CreateQueue"
	opCreateResourceShare     = "CreateResourceShare"
	opDeleteJobTemplate       = "DeleteJobTemplate"
	opDeletePolicy            = "DeletePolicy"
	opDeletePreset            = "DeletePreset"
	opDeleteQueue             = "DeleteQueue"
	opDescribeEndpoints       = "DescribeEndpoints"
	opDisassociateCertificate = "DisassociateCertificate"
	opGetJob                  = "GetJob"
	opGetJobTemplate          = "GetJobTemplate"
	opGetJobsQueryResults     = "GetJobsQueryResults"
	opGetPolicy               = "GetPolicy"
	opGetPreset               = "GetPreset"
	opGetQueue                = "GetQueue"
	opListJobTemplates        = "ListJobTemplates"
	opListJobs                = "ListJobs"
	opListPresets             = "ListPresets"
	opListQueues              = "ListQueues"
	opListTagsForResource     = "ListTagsForResource"
	opPutPolicy               = "PutPolicy"
	opTagResource             = "TagResource"
	opUntagResource           = "UntagResource"
	opUpdateJob               = "UpdateJob"
	opUpdateJobTemplate       = "UpdateJobTemplate"
	opUpdatePreset            = "UpdatePreset"
	opUpdateQueue             = "UpdateQueue"
	opListVersions            = "ListVersions"
	opProbe                   = "Probe"
	opSearchJobs              = "SearchJobs"
	opStartJobsQuery          = "StartJobsQuery"
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
	versionsPath       = "/2017-08-29/versions"
	probePath          = "/2017-08-29/probe"
	searchPath         = "/2017-08-29/search"
)

// Handler is the Echo HTTP handler for Amazon MediaConvert operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new MediaConvert handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state. Implements service.Resettable.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Without this delegation, cli.go's setupPersistence type-asserts the
// service.Registerable value returned by Provider.Init (this Handler, not
// InMemoryBackend) against a Snapshot/Restore interface -- since Handler
// itself never exposed either method, InMemoryBackend.Snapshot/Restore
// (persistence.go) were dead code and this service was never actually
// persisted, despite StorageBackend already declaring the Persistable
// contract.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaConvert" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opAssociateCertificate,
		opCancelJob,
		opCreateJob,
		opCreateJobTemplate,
		opCreatePreset,
		opCreateQueue,
		opCreateResourceShare,
		opDeleteJobTemplate,
		opDeletePolicy,
		opDeletePreset,
		opDeleteQueue,
		opDescribeEndpoints,
		opDisassociateCertificate,
		opGetJob,
		opGetJobTemplate,
		opGetJobsQueryResults,
		opGetPolicy,
		opGetPreset,
		opGetQueue,
		opListJobTemplates,
		opListJobs,
		opListPresets,
		opListQueues,
		opListTagsForResource,
		opPutPolicy,
		opTagResource,
		opUntagResource,
		opUpdateJob,
		opUpdateJobTemplate,
		opUpdatePreset,
		opUpdateQueue,
		opListVersions,
		opProbe,
		opSearchJobs,
		opStartJobsQuery,
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
	case opListQueues:
		return true, h.handleListQueues(c)
	case opGetQueue:
		return true, h.handleGetQueue(c, route.resource)
	case opDeleteQueue:
		return true, h.handleDeleteQueue(c, route.resource)
	case opListJobTemplates:
		return true, h.handleListJobTemplates(c)
	case opGetJobTemplate:
		return true, h.handleGetJobTemplate(c, route.resource)
	case opDeleteJobTemplate:
		return true, h.handleDeleteJobTemplate(c, route.resource)
	case opListJobs:
		return true, h.handleListJobs(c)
	case opGetJob:
		return true, h.handleGetJob(c, route.resource)
	case opCancelJob:
		return true, h.handleCancelJob(c, route.resource)
	case opDescribeEndpoints:
		return true, h.handleDescribeEndpoints(c)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c, route.resource)
	}

	return h.dispatchReadOnlyNewOps(c, route)
}

// dispatchReadOnlyNewOps handles newer operations that do not require a request body.
// These are the operations added after the initial implementation.
func (h *Handler) dispatchReadOnlyNewOps(c *echo.Context, route mcRoute) (bool, error) {
	switch route.operation {
	case opListPresets:
		return true, h.handleListPresets(c)
	case opGetPreset:
		return true, h.handleGetPreset(c, route.resource)
	case opDeletePreset:
		return true, h.handleDeletePreset(c, route.resource)
	case opGetPolicy:
		return true, h.handleGetPolicy(c)
	case opDeletePolicy:
		return true, h.handleDeletePolicy(c)
	case opDisassociateCertificate:
		return true, h.handleDisassociateCertificate(c, route.resource)
	case opGetJobsQueryResults:
		return true, h.handleGetJobsQueryResults(c, route.resource)
	case opListVersions:
		return true, h.handleListVersions(c)
	case opSearchJobs:
		return true, h.handleSearchJobs(c)
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
	case opCreateQueue:
		return h.handleCreateQueue(c, body)
	case opUpdateQueue:
		return h.handleUpdateQueue(c, route.resource, body)
	case opCreateJobTemplate:
		return h.handleCreateJobTemplate(c, body)
	case opUpdateJobTemplate:
		return h.handleUpdateJobTemplate(c, route.resource, body)
	case opCreateJob:
		return h.handleCreateJob(c, body)
	case opUpdateJob:
		return h.handleUpdateJob(c, route.resource, body)
	case opTagResource:
		return h.handleTagResource(c, body)
	case opUntagResource:
		return h.handleUntagResource(c, route.resource, body)
	}

	return h.dispatchMutatingNewOps(c, route, body)
}

// dispatchMutatingNewOps handles write operations added after the initial implementation.
func (h *Handler) dispatchMutatingNewOps(c *echo.Context, route mcRoute, body []byte) error {
	switch route.operation {
	case opCreatePreset:
		return h.handleCreatePreset(c, body)
	case opUpdatePreset:
		return h.handleUpdatePreset(c, route.resource, body)
	case opPutPolicy:
		return h.handlePutPolicy(c, body)
	case opAssociateCertificate:
		return h.handleAssociateCertificate(c, body)
	case opCreateResourceShare:
		return h.handleCreateResourceShare(c, body)
	case opProbe:
		return h.handleProbe(c, body)
	case opStartJobsQuery:
		return h.handleStartJobsQuery(c, body)
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
	}

	return parseStaticPathRoute(method, path)
}

// parseStaticPathRoute handles non-prefix (exact-match) path routes.
func parseStaticPathRoute(method, path string) mcRoute {
	switch path {
	case policyPath:
		return parsePolicyRoute(method)
	case endpointsPath:
		return mcRoute{operation: opDescribeEndpoints}
	case resourceSharesPath:
		if method == http.MethodPost {
			return mcRoute{operation: opCreateResourceShare}
		}
	case versionsPath:
		if method == http.MethodGet {
			return mcRoute{operation: opListVersions}
		}
	case probePath:
		if method == http.MethodPost {
			return mcRoute{operation: opProbe}
		}
	case searchPath:
		if method == http.MethodGet {
			return mcRoute{operation: opSearchJobs}
		}
	}

	return mcRoute{operation: opUnknown}
}

func parseQueueRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListQueues}
		case http.MethodPost:
			return mcRoute{operation: opCreateQueue}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetQueue, resource: name}
	case http.MethodPut:
		return mcRoute{operation: opUpdateQueue, resource: name}
	case http.MethodDelete:
		return mcRoute{operation: opDeleteQueue, resource: name}
	}

	return mcRoute{operation: opUnknown}
}

func parseJobTemplateRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListJobTemplates}
		case http.MethodPost:
			return mcRoute{operation: opCreateJobTemplate}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetJobTemplate, resource: name}
	case http.MethodPut:
		return mcRoute{operation: opUpdateJobTemplate, resource: name}
	case http.MethodDelete:
		return mcRoute{operation: opDeleteJobTemplate, resource: name}
	}

	return mcRoute{operation: opUnknown}
}

func parseJobRoute(method, suffix string) mcRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListJobs}
		case http.MethodPost:
			return mcRoute{operation: opCreateJob}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetJob, resource: id}
	case http.MethodPut:
		return mcRoute{operation: opUpdateJob, resource: id}
	case http.MethodDelete:
		return mcRoute{operation: opCancelJob, resource: id}
	}

	return mcRoute{operation: opUnknown}
}

// parseTagRoute maps tag-path requests to operations. Per the real
// MediaConvert restjson1 model: TagResource is POST /tags with the target
// ARN carried in the JSON body (not the URL), ListTagsForResource is
// GET /tags/{Arn}, and UntagResource is PUT /tags/{Arn} (not DELETE) with
// tagKeys carried in the JSON body.
func parseTagRoute(method, suffix string) mcRoute {
	resourceARN := strings.TrimPrefix(suffix, "/")

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opListTagsForResource, resource: resourceARN}
	case http.MethodPost:
		return mcRoute{operation: opTagResource}
	case http.MethodPut:
		return mcRoute{operation: opUntagResource, resource: resourceARN}
	}

	return mcRoute{operation: opUnknown}
}

func parsePresetRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListPresets}
		case http.MethodPost:
			return mcRoute{operation: opCreatePreset}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetPreset, resource: name}
	case http.MethodPut:
		return mcRoute{operation: opUpdatePreset, resource: name}
	case http.MethodDelete:
		return mcRoute{operation: opDeletePreset, resource: name}
	}

	return mcRoute{operation: opUnknown}
}

func parsePolicyRoute(method string) mcRoute {
	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetPolicy}
	case http.MethodPut:
		return mcRoute{operation: opPutPolicy}
	case http.MethodDelete:
		return mcRoute{operation: opDeletePolicy}
	}

	return mcRoute{operation: opUnknown}
}

func parseCertificateRoute(method, suffix string) mcRoute {
	certARN := strings.TrimPrefix(suffix, "/")

	if certARN == "" {
		if method == http.MethodPost {
			return mcRoute{operation: opAssociateCertificate}
		}

		return mcRoute{operation: opUnknown}
	}

	if method == http.MethodDelete {
		return mcRoute{operation: opDisassociateCertificate, resource: certARN}
	}

	return mcRoute{operation: opUnknown}
}

func parseJobsQueriesRoute(method, suffix string) mcRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id != "" && method == http.MethodGet {
		return mcRoute{operation: opGetJobsQueryResults, resource: id}
	}

	if id == "" && method == http.MethodPost {
		return mcRoute{operation: opStartJobsQuery}
	}

	return mcRoute{operation: opUnknown}
}

// --- Queue handlers ---

type createQueueInput struct {
	// ReservationPlanSettings is the wire field name the real MediaConvert
	// API uses on CreateQueueInput/UpdateQueueInput (it becomes
	// ReservationPlan on the Queue output resource -- the request and
	// response field names differ).
	ReservationPlanSettings *ReservationPlan  `json:"reservationPlanSettings,omitempty"`
	ServiceOverrides        map[string]any    `json:"serviceOverrides,omitempty"`
	Tags                    map[string]string `json:"tags,omitempty"`
	Name                    string            `json:"name"`
	Description             string            `json:"description,omitempty"`
	PricingPlan             string            `json:"pricingPlan,omitempty"`
	Status                  string            `json:"status,omitempty"`
	ConcurrentJobs          int               `json:"concurrentJobs,omitempty"`
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

	q, err := h.Backend.CreateQueueFull(
		in.Name, in.Description, in.PricingPlan, in.Status,
		in.Tags, in.ConcurrentJobs, in.ReservationPlanSettings, in.ServiceOverrides,
	)
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

	q := c.Request().URL.Query()

	if q.Get("order") == orderDescending {
		reverseSlice(queues)
	}

	return c.JSON(http.StatusOK, queuesListOutput{Queues: limitSlice(queues, parseMaxResults(q.Get("maxResults")))})
}

type updateQueueInput struct {
	ReservationPlanSettings *ReservationPlan `json:"reservationPlanSettings,omitempty"`
	ConcurrentJobs          *int             `json:"concurrentJobs,omitempty"`
	Description             string           `json:"description,omitempty"`
	Status                  string           `json:"status,omitempty"`
}

func (h *Handler) handleUpdateQueue(c *echo.Context, name string, body []byte) error {
	var in updateQueueInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	q, err := h.Backend.UpdateQueue(name, in.Description, in.Status, in.ConcurrentJobs, in.ReservationPlanSettings)
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
	Settings    map[string]any    `json:"settings,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Category    string            `json:"category,omitempty"`
	Queue       string            `json:"queue,omitempty"`
	Priority    int               `json:"priority"`
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

	jt, err := h.Backend.CreateJobTemplate(
		in.Name,
		in.Description,
		in.Category,
		in.Queue,
		in.Priority,
		in.Settings,
		in.Tags,
	)
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

	q := c.Request().URL.Query()
	category := q.Get("category")

	if category != "" {
		filtered := templates[:0:0]

		for _, t := range templates {
			if t.Category == category {
				filtered = append(filtered, t)
			}
		}

		templates = filtered
	}

	if q.Get("order") == orderDescending {
		reverseSlice(templates)
	}

	out := jobTemplatesListOutput{JobTemplates: limitSlice(templates, parseMaxResults(q.Get("maxResults")))}

	return c.JSON(http.StatusOK, out)
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
	AccelerationSettings *AccelerationSettings `json:"accelerationSettings,omitempty"`
	Settings             map[string]any        `json:"settings,omitempty"`
	Tags                 map[string]string     `json:"tags,omitempty"`
	UserMetadata         map[string]string     `json:"userMetadata,omitempty"`
	Role                 string                `json:"role"`
	Queue                string                `json:"queue,omitempty"`
	JobTemplate          string                `json:"jobTemplate,omitempty"`
	BillingTagsSource    string                `json:"billingTagsSource,omitempty"`
	ClientRequestToken   string                `json:"clientRequestToken,omitempty"`
	// JobEngineVersion is the wire field name the real MediaConvert API uses
	// on CreateJobInput (it becomes JobEngineVersionRequested on the Job
	// output resource -- the request and response field names differ).
	JobEngineVersion string           `json:"jobEngineVersion,omitempty"`
	HopDestinations  []HopDestination `json:"hopDestinations,omitempty"`
	Priority         int              `json:"priority,omitempty"`
}

type jobWrapper struct {
	Job *Job `json:"job"`
}

type jobsListOutput struct {
	NextToken  string `json:"nextToken,omitempty"`
	Jobs       []*Job `json:"jobs"`
	TotalCount int    `json:"totalCount"`
}

func (h *Handler) handleCreateJob(c *echo.Context, body []byte) error {
	var in createJobInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Role == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "role is required"))
	}

	accelMode := ""
	if in.AccelerationSettings != nil {
		accelMode = in.AccelerationSettings.Mode
	}

	j, err := h.Backend.CreateJobFull(
		in.Role,
		in.Queue,
		in.JobTemplate,
		in.Settings,
		in.Tags,
		in.UserMetadata,
		in.BillingTagsSource,
		in.ClientRequestToken,
		accelMode,
		in.JobEngineVersion,
		in.Priority,
		in.HopDestinations,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, jobWrapper{Job: j})
}

type updateJobInput struct {
	Priority        *int             `json:"priority,omitempty"`
	Queue           string           `json:"queue,omitempty"`
	HopDestinations []HopDestination `json:"hopDestinations,omitempty"`
}

func (h *Handler) handleUpdateJob(c *echo.Context, id string, body []byte) error {
	var in updateJobInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	j, err := h.Backend.UpdateJob(id, in.Queue, in.Priority, in.HopDestinations)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobWrapper{Job: j})
}

func (h *Handler) handleGetJob(c *echo.Context, id string) error {
	j, err := h.Backend.GetJob(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobWrapper{Job: j})
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	q := c.Request().URL.Query()
	statusFilter := q.Get("status")
	queueFilter := q.Get("queue")
	order := q.Get("order")
	maxResults := parseMaxResults(q.Get("maxResults"))

	jobs := h.Backend.ListJobsFiltered(statusFilter, queueFilter, order)
	if jobs == nil {
		jobs = []*Job{}
	}

	nextTokenIn := q.Get("nextToken")
	pg := page.New(jobs, nextTokenIn, maxResults, defaultJobsPageSize)

	out := jobsListOutput{Jobs: pg.Data, TotalCount: len(jobs)}
	if pg.Next != "" {
		out.NextToken = pg.Next
	}

	return c.JSON(http.StatusOK, out)
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

// tagResourceInput mirrors the real TagResourceInput wire shape: the target
// ARN is carried in the JSON body ("arn"), not in the URL path -- the real
// TagResource endpoint is POST /2017-08-29/tags with no ARN suffix.
type tagResourceInput struct {
	Tags map[string]string `json:"tags"`
	Arn  string            `json:"arn"`
}

// untagResourceInput mirrors the real UntagResourceInput wire shape: the
// keys to remove are carried in the JSON body ("tagKeys"), not as a
// repeated query parameter.
type untagResourceInput struct {
	TagKeys []string `json:"tagKeys"`
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

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Arn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "arn is required"))
	}

	h.Backend.TagResource(in.Arn, in.Tags)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, body []byte) error {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	h.Backend.UntagResource(resourceARN, in.TagKeys)

	return c.NoContent(http.StatusNoContent)
}

// --- Preset handlers ---

type createPresetInput struct {
	Settings    map[string]any    `json:"settings,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Category    string            `json:"category,omitempty"`
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

	p, err := h.Backend.CreatePreset(in.Name, in.Description, in.Category, in.Settings, in.Tags)
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

	q := c.Request().URL.Query()
	category := q.Get("category")

	if category != "" {
		filtered := presets[:0:0]

		for _, p := range presets {
			if p.Category == category {
				filtered = append(filtered, p)
			}
		}

		presets = filtered
	}

	if q.Get("order") == orderDescending {
		reverseSlice(presets)
	}

	return c.JSON(http.StatusOK, presetsListOutput{Presets: limitSlice(presets, parseMaxResults(q.Get("maxResults")))})
}

func (h *Handler) handleDeletePreset(c *echo.Context, name string) error {
	if err := h.Backend.DeletePreset(name); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type updatePresetInput struct {
	Settings    map[string]any `json:"settings,omitempty"`
	Description string         `json:"description,omitempty"`
	Category    string         `json:"category,omitempty"`
}

func (h *Handler) handleUpdatePreset(c *echo.Context, name string, body []byte) error {
	var in updatePresetInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	p, err := h.Backend.UpdatePreset(name, in.Description, in.Category, in.Settings)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, presetWrapper{Preset: p})
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

type putPolicyInput struct {
	Policy *policyInputEntry `json:"policy"`
}

type policyInputEntry struct {
	HTTPInputs  string `json:"httpInputs,omitempty"`
	HTTPSInputs string `json:"httpsInputs,omitempty"`
	S3Inputs    string `json:"s3Inputs,omitempty"`
}

func (h *Handler) handlePutPolicy(c *echo.Context, body []byte) error {
	var in putPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	var httpInputs, httpsInputs, s3Inputs string
	if in.Policy != nil {
		httpInputs = in.Policy.HTTPInputs
		httpsInputs = in.Policy.HTTPSInputs
		s3Inputs = in.Policy.S3Inputs
	}

	p := h.Backend.PutPolicy(httpInputs, httpsInputs, s3Inputs)

	return c.JSON(http.StatusOK, policyWrapper{Policy: p})
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

// jobsQueryStatusComplete is the JobsQueryStatus value reported for every
// GetJobsQueryResults response. Queries in this backend are resolved
// synchronously at GetJobsQueryResults time (see StartJobsQuery/
// GetJobsQueryResults in backend.go), so results are always immediately
// available -- COMPLETE is therefore always the accurate status, unlike
// real AWS where a query can still be SUBMITTED/PROGRESSING/ERROR.
const jobsQueryStatusComplete = "COMPLETE"

type jobsQueryResultsOutput struct {
	Status string `json:"status"`
	Jobs   []*Job `json:"jobs"`
}

func (h *Handler) handleGetJobsQueryResults(c *echo.Context, queryID string) error {
	jobs := h.Backend.GetJobsQueryResults(queryID)

	return c.JSON(http.StatusOK, jobsQueryResultsOutput{Jobs: jobs, Status: jobsQueryStatusComplete})
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

// --- ListVersions handler ---

type listVersionsOutput struct {
	NextToken string             `json:"nextToken,omitempty"`
	Versions  []jobEngineVersion `json:"versions"`
}

// jobEngineVersion mirrors the AWS MediaConvert JobEngineVersion type.
type jobEngineVersion struct {
	ExpirationDate *float64 `json:"expirationDate,omitempty"`
	Version        string   `json:"version"`
}

func (h *Handler) handleListVersions(c *echo.Context) error {
	out := listVersionsOutput{
		Versions: []jobEngineVersion{
			{Version: "2017-08-29"},
		},
	}

	return c.JSON(http.StatusOK, out)
}

// --- Probe handler ---

type probeInput struct {
	InputFiles []map[string]any `json:"inputFiles,omitempty"`
}

type probeOutput struct {
	ProbeResults []map[string]any `json:"probeResults"`
}

func (h *Handler) handleProbe(c *echo.Context, body []byte) error {
	var in probeInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	results := make([]map[string]any, 0, len(in.InputFiles))
	for _, f := range in.InputFiles {
		result := map[string]any{
			"probeResult": map[string]any{
				"container": map[string]any{"format": "mp4"},
				"inputFile": f,
			},
		}
		results = append(results, result)
	}

	return c.JSON(http.StatusOK, probeOutput{ProbeResults: results})
}

// --- SearchJobs handler ---

type searchJobsOutput struct {
	NextToken string `json:"nextToken,omitempty"`
	Jobs      []*Job `json:"jobs"`
}

func (h *Handler) handleSearchJobs(c *echo.Context) error {
	q := c.Request().URL.Query()
	statusFilter := q.Get("status")
	queueFilter := q.Get("queue")
	order := q.Get("order")
	maxResults := parseMaxResults(q.Get("maxResults"))

	jobs := h.Backend.ListJobsFiltered(statusFilter, queueFilter, order)
	if jobs == nil {
		jobs = []*Job{}
	}

	nextTokenIn := q.Get("nextToken")
	pg := page.New(jobs, nextTokenIn, maxResults, defaultJobsPageSize)

	out := searchJobsOutput{Jobs: pg.Data}
	if pg.Next != "" {
		out.NextToken = pg.Next
	}

	return c.JSON(http.StatusOK, out)
}

// --- StartJobsQuery handler ---

type startJobsQueryInput struct {
	Order      string           `json:"order,omitempty"`
	MaxResults *int             `json:"maxResults,omitempty"`
	FilterList []map[string]any `json:"filterList,omitempty"`
}

// startJobsQueryOutput mirrors the real StartJobsQueryOutput wire shape,
// whose sole member is "id" -- not "queryId".
type startJobsQueryOutput struct {
	QueryID string `json:"id"`
}

func (h *Handler) handleStartJobsQuery(c *echo.Context, body []byte) error {
	var in startJobsQueryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	maxResults := 0
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	queryID, err := h.Backend.StartJobsQuery(in.FilterList, maxResults, in.Order)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, startJobsQueryOutput{QueryID: queryID})
}

// reverseSlice reverses items in-place.
func reverseSlice[T any](items []T) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

// limitSlice returns at most maxResults items; 0 means no limit.
func limitSlice[T any](items []T, maxResults int) []T {
	if maxResults > 0 && len(items) > maxResults {
		return items[:maxResults]
	}

	return items
}

// parseMaxResults converts a query-parameter string to a non-negative int,
// returning 0 (no limit) when the string is empty or unparseable.
func parseMaxResults(s string) int {
	if s == "" {
		return 0
	}

	var n int

	if _, err := fmt.Sscanf(s, "%d", &n); err != nil || n < 0 {
		return 0
	}

	return n
}

// --- Error handling ---

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalError", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}
