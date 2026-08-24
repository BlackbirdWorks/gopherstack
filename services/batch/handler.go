package batch

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// opListTagsForResource, opTagResource, and opUntagResource name the three
	// tag operations shared by the main Handler's GetSupportedOperations/
	// ExtractOperation and TagsRouter's GetSupportedOperations (tags_route.go).
	opListTagsForResource = "ListTagsForResource"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"

	v1Prefix              = "/v1/"
	tagsPrefix            = "/v1/tags/"
	appsyncV1Prefix       = "/v1/apis"
	codeartifactDomain    = "/v1/domain"
	codeartifactRepos     = "/v1/repository"
	codeartifactAuthToken = "/v1/authorization-token" //nolint:gosec // not a credential
	// kafkaClustersPrefix and kafkaConfigurationsPrefix are MSK Kafka paths that share
	// the /v1/ prefix; exclude them to avoid routing Kafka requests to Batch.
	kafkaClustersPrefix       = "/v1/clusters"
	kafkaConfigurationsPrefix = "/v1/configurations"
)

// Handler is the Echo HTTP handler for AWS Batch operations.
type Handler struct {
	Backend *InMemoryBackend
	janitor *Janitor
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Batch handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// Zero values for interval, inactiveJobDefTTL, or completedJobTTL use defaults.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(
	interval, inactiveJobDefTTL, completedJobTTL time.Duration,
	taskTimeout ...time.Duration,
) *Handler {
	j := NewJanitor(h.Backend, interval, inactiveJobDefTTL, completedJobTTL)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}

	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
// It always returns nil; the error return satisfies the service.BackgroundWorker interface.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
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
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		"CreateConsumableResource",
		"DeleteConsumableResource",
		"DescribeConsumableResource",
		"CreateSchedulingPolicy",
		"DeleteSchedulingPolicy",
		"CreateServiceEnvironment",
		"DeleteServiceEnvironment",
		"UpdateConsumableResource",
		"ListConsumableResources",
		"DescribeSchedulingPolicies",
		"ListSchedulingPolicies",
		"UpdateSchedulingPolicy",
		"DescribeServiceEnvironments",
		"UpdateServiceEnvironment",
		"DescribeServiceJob",
		"GetJobQueueSnapshot",
		"ListJobsByConsumableResource",
		"ListServiceJobs",
		"SubmitServiceJob",
		"TerminateServiceJob",
		"UpdateServiceJob",
		"CreateQuotaShare",
		"DescribeQuotaShare",
		"UpdateQuotaShare",
		"DeleteQuotaShare",
		"ListQuotaShares",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "batch" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Batch requests.
// It matches /v1/ paths but explicitly excludes /v1/apis (AppSync),
// CodeArtifact paths, and Kafka paths to prevent routing conflicts when
// multiple services use PriorityPathVersioned. The tags path is scoped by
// ARN via isBatchTagPath instead of excluded outright, since Batch owns its
// own ARNs there too (see isAppSyncTagPath in services/appsync/handler.go
// for the mirrored guard that stops AppSync's tag-path matcher from
// claiming Batch's ARNs in the first place).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, tagsPrefix) {
			return isBatchTagPath(path)
		}
		// Exclude AppSync paths (/v1/apis) which share the /v1/ prefix.
		if strings.HasPrefix(path, appsyncV1Prefix) {
			return false
		}
		// Exclude CodeArtifact paths which share the /v1/ prefix.
		if strings.HasPrefix(path, codeartifactDomain) ||
			strings.HasPrefix(path, codeartifactRepos) ||
			strings.HasPrefix(path, codeartifactAuthToken) ||
			path == "/v1/tags" ||
			path == "/v1/tag" ||
			path == "/v1/untag" {
			return false
		}
		// Exclude Kafka (MSK) paths which share the /v1/ prefix.
		if strings.HasPrefix(path, kafkaClustersPrefix) ||
			strings.HasPrefix(path, kafkaConfigurationsPrefix) {
			return false
		}

		return strings.HasPrefix(path, v1Prefix)
	}
}

// arnSplitNForService is the n passed to [strings.SplitN] when parsing an ARN to
// reach the service name at index 2 (arn:partition:service:...).
const arnSplitNForService = 4

// isBatchTagPath reports whether path is a /v1/tags/{arn} path for a Batch ARN.
// Used as an inclusion guard so this handler only claims its own ARNs -- it can't
// steal Kafka/CodeArtifact/AppSync/MQ/Pinpoint/Polly's tag requests, all of which
// share this same "/v1/tags/{arn}" prefix.
func isBatchTagPath(path string) bool {
	if !strings.HasPrefix(path, tagsPrefix) {
		return false
	}

	encodedARN := path[len(tagsPrefix):]
	if encodedARN == "" {
		return false
	}

	decodedARN, err := url.PathUnescape(encodedARN)
	if err != nil {
		return false
	}

	// arn:partition:service:region:account:resource — check the service segment.
	parts := strings.SplitN(decodedARN, ":", arnSplitNForService)

	return len(parts) >= 3 && parts[2] == "batch"
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
			return opListTagsForResource
		case http.MethodPost:
			return opTagResource
		case http.MethodDelete:
			return opUntagResource
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

// contextWithRegion returns the request context with the resolved AWS region attached
// under regionContextKey so that backend operations are routed to the correct region.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

// Handler returns the Echo handler function for Batch requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		ctx := h.contextWithRegion(c)
		log := logger.Load(ctx)

		if strings.HasPrefix(path, tagsPrefix) {
			return h.handleTags(ctx, c, log)
		}

		if r.Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(ctx, "batch: failed to read request body", "error", err)

			return c.JSON(http.StatusInternalServerError, errorResponse("ServerException", "internal server error"))
		}

		fn, ok := h.ops[path]
		if !ok {
			return c.JSON(
				http.StatusNotFound,
				errorResponse("UnknownOperationException", "unknown operation for path: "+path),
			)
		}

		result, opErr := fn(ctx, body)
		if opErr != nil {
			return h.writeError(c, opErr)
		}

		out, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			log.ErrorContext(ctx, "batch: failed to marshal response", "error", marshalErr)

			return c.JSON(http.StatusInternalServerError, errorResponse("ServerException", "internal server error"))
		}

		return c.JSONBlob(http.StatusOK, out)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"/v1/createcomputeenvironment":     service.WrapOp(h.handleCreateComputeEnvironment),
		"/v1/describecomputeenvironments":  service.WrapOp(h.handleDescribeComputeEnvironments),
		"/v1/updatecomputeenvironment":     service.WrapOp(h.handleUpdateComputeEnvironment),
		"/v1/deletecomputeenvironment":     service.WrapOp(h.handleDeleteComputeEnvironment),
		"/v1/createjobqueue":               service.WrapOp(h.handleCreateJobQueue),
		"/v1/describejobqueues":            service.WrapOp(h.handleDescribeJobQueues),
		"/v1/updatejobqueue":               service.WrapOp(h.handleUpdateJobQueue),
		"/v1/deletejobqueue":               service.WrapOp(h.handleDeleteJobQueue),
		"/v1/registerjobdefinition":        service.WrapOp(h.handleRegisterJobDefinition),
		"/v1/describejobdefinitions":       service.WrapOp(h.handleDescribeJobDefinitions),
		"/v1/deregisterjobdefinition":      service.WrapOp(h.handleDeregisterJobDefinition),
		"/v1/listjobs":                     service.WrapOp(h.handleListJobs),
		"/v1/describejobs":                 service.WrapOp(h.handleDescribeJobs),
		"/v1/submitjob":                    service.WrapOp(h.handleSubmitJob),
		"/v1/terminatejob":                 service.WrapOp(h.handleTerminateJob),
		"/v1/canceljob":                    service.WrapOp(h.handleCancelJob),
		"/v1/createconsumableresource":     service.WrapOp(h.handleCreateConsumableResource),
		"/v1/deleteconsumableresource":     service.WrapOp(h.handleDeleteConsumableResource),
		"/v1/describeconsumableresource":   service.WrapOp(h.handleDescribeConsumableResource),
		"/v1/updateconsumableresource":     service.WrapOp(h.handleUpdateConsumableResource),
		"/v1/listconsumableresources":      service.WrapOp(h.handleListConsumableResources),
		"/v1/createschedulingpolicy":       service.WrapOp(h.handleCreateSchedulingPolicy),
		"/v1/deleteschedulingpolicy":       service.WrapOp(h.handleDeleteSchedulingPolicy),
		"/v1/describeschedulingpolicies":   service.WrapOp(h.handleDescribeSchedulingPolicies),
		"/v1/listschedulingpolicies":       service.WrapOp(h.handleListSchedulingPolicies),
		"/v1/updateschedulingpolicy":       service.WrapOp(h.handleUpdateSchedulingPolicy),
		"/v1/createserviceenvironment":     service.WrapOp(h.handleCreateServiceEnvironment),
		"/v1/deleteserviceenvironment":     service.WrapOp(h.handleDeleteServiceEnvironment),
		"/v1/describeserviceenvironments":  service.WrapOp(h.handleDescribeServiceEnvironments),
		"/v1/updateserviceenvironment":     service.WrapOp(h.handleUpdateServiceEnvironment),
		"/v1/describeservicejob":           service.WrapOp(h.handleDescribeServiceJob),
		"/v1/getjobqueuesnapshot":          service.WrapOp(h.handleGetJobQueueSnapshot),
		"/v1/listjobsbyconsumableresource": service.WrapOp(h.handleListJobsByConsumableResource),
		"/v1/listservicejobs":              service.WrapOp(h.handleListServiceJobs),
		"/v1/submitservicejob":             service.WrapOp(h.handleSubmitServiceJob),
		"/v1/terminateservicejob":          service.WrapOp(h.handleTerminateServiceJob),
		"/v1/updateservicejob":             service.WrapOp(h.handleUpdateServiceJob),
		"/v1/createquotashare":             service.WrapOp(h.handleCreateQuotaShare),
		"/v1/describequotashare":           service.WrapOp(h.handleDescribeQuotaShare),
		"/v1/updatequotashare":             service.WrapOp(h.handleUpdateQuotaShare),
		"/v1/deletequotashare":             service.WrapOp(h.handleDeleteQuotaShare),
		"/v1/listquotashares":              service.WrapOp(h.handleListQuotaShares),
	}
}

func (h *Handler) handleTags(ctx context.Context, c *echo.Context, log *slog.Logger) error {
	r := c.Request()
	resourceARN, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, tagsPrefix))

	if err != nil || resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN in path"))
	}

	switch r.Method {
	case http.MethodGet:
		return h.handleListTagsForResource(ctx, c, resourceARN)
	case http.MethodPost:
		body, readErr := httputils.ReadBody(r)
		if readErr != nil {
			log.ErrorContext(ctx, "batch: failed to read tags body", "error", readErr)

			return c.JSON(http.StatusInternalServerError, errorResponse("ServerException", "internal server error"))
		}

		return h.handleTagResource(ctx, c, resourceARN, body)
	case http.MethodDelete:
		return h.handleUntagResource(ctx, c, resourceARN, r.URL.Query())
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
	}
}

// batch@v1.68.4's types/errors.go models exactly two exceptions, ClientException
// and ServerException, wired into all 44 of batch's own deserializeOpError
// switches. "InternalFailure" (the prior default-branch code, also used at every
// other 500 site in this file) is not modeled anywhere, so it deserialized as an
// untyped smithy.GenericAPIError instead of *types.ServerException.
func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorResponse("ClientException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("ServerException", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// emptyOutput is the wire shape for AWS Batch operations that return no
// fields on success (e.g. delete/terminate/cancel operations).
type emptyOutput struct{}

func tagsOrEmpty(tags map[string]string) map[string]string {
	if tags == nil {
		return map[string]string{}
	}

	return tags
}

// int64OrZero dereferences p, or returns 0 if p is nil. Used for required
// output timestamp members (e.g. JobDetail.StartedAt) that this backend
// tracks as a nilable pointer internally but must always emit -- AWS marks
// them required even for a job that hasn't reached that state yet.
func int64OrZero(p *int64) int64 {
	if p == nil {
		return 0
	}

	return *p
}

func pathToOperation(path string) string {
	ops := map[string]string{
		"/v1/createcomputeenvironment":     "CreateComputeEnvironment",
		"/v1/describecomputeenvironments":  "DescribeComputeEnvironments",
		"/v1/updatecomputeenvironment":     "UpdateComputeEnvironment",
		"/v1/deletecomputeenvironment":     "DeleteComputeEnvironment",
		"/v1/createjobqueue":               "CreateJobQueue",
		"/v1/describejobqueues":            "DescribeJobQueues",
		"/v1/updatejobqueue":               "UpdateJobQueue",
		"/v1/deletejobqueue":               "DeleteJobQueue",
		"/v1/registerjobdefinition":        "RegisterJobDefinition",
		"/v1/describejobdefinitions":       "DescribeJobDefinitions",
		"/v1/deregisterjobdefinition":      "DeregisterJobDefinition",
		"/v1/listjobs":                     "ListJobs",
		"/v1/describejobs":                 "DescribeJobs",
		"/v1/submitjob":                    "SubmitJob",
		"/v1/terminatejob":                 "TerminateJob",
		"/v1/canceljob":                    "CancelJob",
		"/v1/createconsumableresource":     "CreateConsumableResource",
		"/v1/deleteconsumableresource":     "DeleteConsumableResource",
		"/v1/describeconsumableresource":   "DescribeConsumableResource",
		"/v1/updateconsumableresource":     "UpdateConsumableResource",
		"/v1/listconsumableresources":      "ListConsumableResources",
		"/v1/createschedulingpolicy":       "CreateSchedulingPolicy",
		"/v1/deleteschedulingpolicy":       "DeleteSchedulingPolicy",
		"/v1/describeschedulingpolicies":   "DescribeSchedulingPolicies",
		"/v1/listschedulingpolicies":       "ListSchedulingPolicies",
		"/v1/updateschedulingpolicy":       "UpdateSchedulingPolicy",
		"/v1/createserviceenvironment":     "CreateServiceEnvironment",
		"/v1/deleteserviceenvironment":     "DeleteServiceEnvironment",
		"/v1/describeserviceenvironments":  "DescribeServiceEnvironments",
		"/v1/updateserviceenvironment":     "UpdateServiceEnvironment",
		"/v1/describeservicejob":           "DescribeServiceJob",
		"/v1/getjobqueuesnapshot":          "GetJobQueueSnapshot",
		"/v1/listjobsbyconsumableresource": "ListJobsByConsumableResource",
		"/v1/listservicejobs":              "ListServiceJobs",
		"/v1/submitservicejob":             "SubmitServiceJob",
		"/v1/terminateservicejob":          "TerminateServiceJob",
		"/v1/updateservicejob":             "UpdateServiceJob",
		"/v1/createquotashare":             "CreateQuotaShare",
		"/v1/describequotashare":           "DescribeQuotaShare",
		"/v1/updatequotashare":             "UpdateQuotaShare",
		"/v1/deletequotashare":             "DeleteQuotaShare",
		"/v1/listquotashares":              "ListQuotaShares",
	}

	if op, ok := ops[path]; ok {
		return op
	}

	return "Unknown"
}
