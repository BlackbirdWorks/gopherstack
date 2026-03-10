package batch

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	v1Prefix        = "/v1/"
	tagsPrefix      = "/v1/tags/"
	appsyncV1Prefix = "/v1/apis"
)

// Handler is the Echo HTTP handler for AWS Batch operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Batch handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Batch" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateComputeEnvironment",
		"DescribeComputeEnvironments",
		"UpdateComputeEnvironment",
		"DeleteComputeEnvironment",
		"CreateJobQueue",
		"DescribeJobQueues",
		"UpdateJobQueue",
		"DeleteJobQueue",
		"RegisterJobDefinition",
		"DescribeJobDefinitions",
		"DeregisterJobDefinition",
		"ListJobs",
		"DescribeJobs",
		"SubmitJob",
		"TerminateJob",
		"CancelJob",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "batch" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Batch requests.
// It matches /v1/ paths but explicitly excludes /v1/apis (AppSync)
// to prevent routing conflicts when both services use PriorityPathVersioned.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		// Exclude AppSync paths (/v1/apis) which share the /v1/ prefix.
		return strings.HasPrefix(path, v1Prefix) && !strings.HasPrefix(path, appsyncV1Prefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation returns the operation name from the request path and method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	if strings.HasPrefix(path, tagsPrefix) {
		switch method {
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	}

	return pathToOperation(path)
}

// ExtractResource extracts a resource identifier from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	if after, ok := strings.CutPrefix(path, tagsPrefix); ok {
		decoded, err := url.PathUnescape(after)
		if err != nil {
			return after
		}

		return decoded
	}

	return ""
}

// Handler returns the Echo handler function for Batch requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		log := logger.Load(r.Context())

		if strings.HasPrefix(path, tagsPrefix) {
			return h.handleTags(c, log)
		}

		if r.Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "batch: failed to read request body", "error", err)

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		fn, ok := h.dispatchTable()[path]
		if !ok {
			return c.JSON(
				http.StatusNotFound,
				errorResponse("UnknownOperationException", "unknown operation for path: "+path),
			)
		}

		result, opErr := fn(r.Context(), body)
		if opErr != nil {
			return h.writeError(c, opErr)
		}

		out, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			log.ErrorContext(r.Context(), "batch: failed to marshal response", "error", marshalErr)

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		return c.JSONBlob(http.StatusOK, out)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"/v1/createcomputeenvironment":    service.WrapOp(h.handleCreateComputeEnvironment),
		"/v1/describecomputeenvironments": service.WrapOp(h.handleDescribeComputeEnvironments),
		"/v1/updatecomputeenvironment":    service.WrapOp(h.handleUpdateComputeEnvironment),
		"/v1/deletecomputeenvironment":    service.WrapOp(h.handleDeleteComputeEnvironment),
		"/v1/createjobqueue":              service.WrapOp(h.handleCreateJobQueue),
		"/v1/describejobqueues":           service.WrapOp(h.handleDescribeJobQueues),
		"/v1/updatejobqueue":              service.WrapOp(h.handleUpdateJobQueue),
		"/v1/deletejobqueue":              service.WrapOp(h.handleDeleteJobQueue),
		"/v1/registerjobdefinition":       service.WrapOp(h.handleRegisterJobDefinition),
		"/v1/describejobdefinitions":      service.WrapOp(h.handleDescribeJobDefinitions),
		"/v1/deregisterjobdefinition":     service.WrapOp(h.handleDeregisterJobDefinition),
		"/v1/listjobs":                    service.WrapOp(h.handleListJobs),
		"/v1/describejobs":                service.WrapOp(h.handleDescribeJobs),
		"/v1/submitjob":                   service.WrapOp(h.handleSubmitJob),
		"/v1/terminatejob":                service.WrapOp(h.handleTerminateJob),
		"/v1/canceljob":                   service.WrapOp(h.handleCancelJob),
	}
}

func (h *Handler) handleTags(c *echo.Context, log *slog.Logger) error {
	r := c.Request()
	resourceARN, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, tagsPrefix))

	if err != nil || resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN in path"))
	}

	switch r.Method {
	case http.MethodGet:
		return h.handleListTagsForResource(c, resourceARN)
	case http.MethodPost:
		body, readErr := httputils.ReadBody(r)
		if readErr != nil {
			log.ErrorContext(r.Context(), "batch: failed to read tags body", "error", readErr)

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		return h.handleTagResource(c, resourceARN, body)
	case http.MethodDelete:
		return h.handleUntagResource(c, resourceARN, r.URL.Query())
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
	}
}

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("ClientException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

func pathToOperation(path string) string {
	ops := map[string]string{
		"/v1/createcomputeenvironment":    "CreateComputeEnvironment",
		"/v1/describecomputeenvironments": "DescribeComputeEnvironments",
		"/v1/updatecomputeenvironment":    "UpdateComputeEnvironment",
		"/v1/deletecomputeenvironment":    "DeleteComputeEnvironment",
		"/v1/createjobqueue":              "CreateJobQueue",
		"/v1/describejobqueues":           "DescribeJobQueues",
		"/v1/updatejobqueue":              "UpdateJobQueue",
		"/v1/deletejobqueue":              "DeleteJobQueue",
		"/v1/registerjobdefinition":       "RegisterJobDefinition",
		"/v1/describejobdefinitions":      "DescribeJobDefinitions",
		"/v1/deregisterjobdefinition":     "DeregisterJobDefinition",
		"/v1/listjobs":                    "ListJobs",
		"/v1/describejobs":                "DescribeJobs",
		"/v1/submitjob":                   "SubmitJob",
		"/v1/terminatejob":                "TerminateJob",
		"/v1/canceljob":                   "CancelJob",
	}

	if op, ok := ops[path]; ok {
		return op
	}

	return "Unknown"
}

// --- Input / Output types ---

type createComputeEnvironmentInput struct {
	Tags                   map[string]string `json:"tags"`
	ComputeEnvironmentName string            `json:"computeEnvironmentName"`
	Type                   string            `json:"type"`
	State                  string            `json:"state"`
}

type createComputeEnvironmentOutput struct {
	ComputeEnvironmentArn  string `json:"computeEnvironmentArn"`
	ComputeEnvironmentName string `json:"computeEnvironmentName"`
}

func (h *Handler) handleCreateComputeEnvironment(
	_ context.Context,
	in *createComputeEnvironmentInput,
) (*createComputeEnvironmentOutput, error) {
	state := in.State
	if state == "" {
		state = "ENABLED"
	}

	ce, err := h.Backend.CreateComputeEnvironment(in.ComputeEnvironmentName, in.Type, state, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createComputeEnvironmentOutput{
		ComputeEnvironmentArn:  ce.ComputeEnvironmentArn,
		ComputeEnvironmentName: ce.ComputeEnvironmentName,
	}, nil
}

type describeComputeEnvironmentsInput struct {
	ComputeEnvironments []string `json:"computeEnvironments"`
}

type describeComputeEnvironmentsOutput struct {
	ComputeEnvironments []*ComputeEnvironment `json:"computeEnvironments"`
}

func (h *Handler) handleDescribeComputeEnvironments(
	_ context.Context,
	in *describeComputeEnvironmentsInput,
) (*describeComputeEnvironmentsOutput, error) {
	ces := h.Backend.DescribeComputeEnvironments(in.ComputeEnvironments)

	return &describeComputeEnvironmentsOutput{ComputeEnvironments: ces}, nil
}

type updateComputeEnvironmentInput struct {
	ComputeEnvironment string `json:"computeEnvironment"`
	State              string `json:"state"`
}

type updateComputeEnvironmentOutput struct {
	ComputeEnvironmentArn  string `json:"computeEnvironmentArn"`
	ComputeEnvironmentName string `json:"computeEnvironmentName"`
}

func (h *Handler) handleUpdateComputeEnvironment(
	_ context.Context,
	in *updateComputeEnvironmentInput,
) (*updateComputeEnvironmentOutput, error) {
	ce, err := h.Backend.UpdateComputeEnvironment(in.ComputeEnvironment, in.State)
	if err != nil {
		return nil, err
	}

	return &updateComputeEnvironmentOutput{
		ComputeEnvironmentArn:  ce.ComputeEnvironmentArn,
		ComputeEnvironmentName: ce.ComputeEnvironmentName,
	}, nil
}

type deleteComputeEnvironmentInput struct {
	ComputeEnvironment string `json:"computeEnvironment"`
}

type emptyOutput struct{}

func (h *Handler) handleDeleteComputeEnvironment(
	_ context.Context,
	in *deleteComputeEnvironmentInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteComputeEnvironment(in.ComputeEnvironment); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type createJobQueueInput struct {
	Tags                    map[string]string         `json:"tags"`
	JobQueueName            string                    `json:"jobQueueName"`
	State                   string                    `json:"state"`
	ComputeEnvironmentOrder []ComputeEnvironmentOrder `json:"computeEnvironmentOrder"`
	Priority                int32                     `json:"priority"`
}

type createJobQueueOutput struct {
	JobQueueArn  string `json:"jobQueueArn"`
	JobQueueName string `json:"jobQueueName"`
}

func (h *Handler) handleCreateJobQueue(
	_ context.Context,
	in *createJobQueueInput,
) (*createJobQueueOutput, error) {
	state := in.State
	if state == "" {
		state = "ENABLED"
	}

	jq, err := h.Backend.CreateJobQueue(in.JobQueueName, in.Priority, state, in.ComputeEnvironmentOrder, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createJobQueueOutput{
		JobQueueArn:  jq.JobQueueArn,
		JobQueueName: jq.JobQueueName,
	}, nil
}

type describeJobQueuesInput struct {
	JobQueues []string `json:"jobQueues"`
}

type describeJobQueuesOutput struct {
	JobQueues []*JobQueue `json:"jobQueues"`
}

func (h *Handler) handleDescribeJobQueues(
	_ context.Context,
	in *describeJobQueuesInput,
) (*describeJobQueuesOutput, error) {
	jqs := h.Backend.DescribeJobQueues(in.JobQueues)

	return &describeJobQueuesOutput{JobQueues: jqs}, nil
}

type updateJobQueueInput struct {
	Priority *int32 `json:"priority,omitempty"`
	JobQueue string `json:"jobQueue"`
	State    string `json:"state"`
}

type updateJobQueueOutput struct {
	JobQueueArn  string `json:"jobQueueArn"`
	JobQueueName string `json:"jobQueueName"`
}

func (h *Handler) handleUpdateJobQueue(
	_ context.Context,
	in *updateJobQueueInput,
) (*updateJobQueueOutput, error) {
	jq, err := h.Backend.UpdateJobQueue(in.JobQueue, in.Priority, in.State)
	if err != nil {
		return nil, err
	}

	return &updateJobQueueOutput{
		JobQueueArn:  jq.JobQueueArn,
		JobQueueName: jq.JobQueueName,
	}, nil
}

type deleteJobQueueInput struct {
	JobQueue string `json:"jobQueue"`
}

func (h *Handler) handleDeleteJobQueue(
	_ context.Context,
	in *deleteJobQueueInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteJobQueue(in.JobQueue); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type registerJobDefinitionInput struct {
	Tags              map[string]string `json:"tags"`
	JobDefinitionName string            `json:"jobDefinitionName"`
	Type              string            `json:"type"`
}

type registerJobDefinitionOutput struct {
	JobDefinitionArn  string `json:"jobDefinitionArn"`
	JobDefinitionName string `json:"jobDefinitionName"`
	Revision          int32  `json:"revision"`
}

func (h *Handler) handleRegisterJobDefinition(
	_ context.Context,
	in *registerJobDefinitionInput,
) (*registerJobDefinitionOutput, error) {
	jd, err := h.Backend.RegisterJobDefinition(in.JobDefinitionName, in.Type, in.Tags)
	if err != nil {
		return nil, err
	}

	return &registerJobDefinitionOutput{
		JobDefinitionArn:  jd.JobDefinitionArn,
		JobDefinitionName: jd.JobDefinitionName,
		Revision:          jd.Revision,
	}, nil
}

type describeJobDefinitionsInput struct {
	JobDefinitions []string `json:"jobDefinitions"`
}

type describeJobDefinitionsOutput struct {
	JobDefinitions []*JobDefinition `json:"jobDefinitions"`
}

func (h *Handler) handleDescribeJobDefinitions(
	_ context.Context,
	in *describeJobDefinitionsInput,
) (*describeJobDefinitionsOutput, error) {
	jds := h.Backend.DescribeJobDefinitions(in.JobDefinitions)

	return &describeJobDefinitionsOutput{JobDefinitions: jds}, nil
}

type deregisterJobDefinitionInput struct {
	JobDefinition string `json:"jobDefinition"`
}

func (h *Handler) handleDeregisterJobDefinition(
	_ context.Context,
	in *deregisterJobDefinitionInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeregisterJobDefinition(in.JobDefinition); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Stub handlers for job operations ---

type listJobsInput struct {
	JobQueue string `json:"jobQueue"`
}

type listJobsOutput struct {
	JobSummaryList []any `json:"jobSummaryList"`
}

func (h *Handler) handleListJobs(_ context.Context, _ *listJobsInput) (*listJobsOutput, error) {
	return &listJobsOutput{JobSummaryList: []any{}}, nil
}

type describeJobsInput struct {
	Jobs []string `json:"jobs"`
}

type describeJobsOutput struct {
	Jobs []any `json:"jobs"`
}

func (h *Handler) handleDescribeJobs(_ context.Context, _ *describeJobsInput) (*describeJobsOutput, error) {
	return &describeJobsOutput{Jobs: []any{}}, nil
}

type submitJobInput struct {
	JobName       string `json:"jobName"`
	JobQueue      string `json:"jobQueue"`
	JobDefinition string `json:"jobDefinition"`
}

type submitJobOutput struct {
	JobID   string `json:"jobId"`
	JobName string `json:"jobName"`
}

func (h *Handler) handleSubmitJob(_ context.Context, in *submitJobInput) (*submitJobOutput, error) {
	return &submitJobOutput{
		JobID:   "00000000-0000-0000-0000-000000000000",
		JobName: in.JobName,
	}, nil
}

type terminateJobInput struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

func (h *Handler) handleTerminateJob(_ context.Context, _ *terminateJobInput) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

type cancelJobInput struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

func (h *Handler) handleCancelJob(_ context.Context, _ *cancelJobInput) (*emptyOutput, error) {
	return &emptyOutput{}, nil
}

// --- Tags handlers ---

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.writeError(c, err)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, listTagsForResourceOutput{Tags: tags})
}

type tagResourceInput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	var in tagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
		}
	}

	if err := h.Backend.TagResource(resourceARN, in.Tags); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, emptyOutput{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, query url.Values) error {
	tagKeys := query["tagKeys"]
	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, emptyOutput{})
}
