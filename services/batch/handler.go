package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
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
// CodeArtifact paths, Kafka paths, and Kafka tag resource paths to
// prevent routing conflicts when multiple services use PriorityPathVersioned.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
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
		// Exclude Kafka (MSK) paths which share the /v1/ prefix.
		if strings.HasPrefix(path, kafkaClustersPrefix) ||
			strings.HasPrefix(path, kafkaConfigurationsPrefix) {
			return false
		}
		// Exclude Kafka tag resource paths (/v1/tags/{kafka-arn}).
		if isKafkaTagPath(path) {
			return false
		}

		return strings.HasPrefix(path, v1Prefix)
	}
}

// arnSplitNForService is the n passed to [strings.SplitN] when parsing an ARN to
// reach the service name at index 2 (arn:partition:service:...).
const arnSplitNForService = 4

// isKafkaTagPath reports whether path is a /v1/tags/{arn} path for a Kafka ARN.
// Kafka's RouteMatcher handles these; the Batch handler must not intercept them.
func isKafkaTagPath(path string) bool {
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

	return len(parts) >= 3 && parts[2] == "kafka"
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

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
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

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
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

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		return h.handleTagResource(ctx, c, resourceARN, body)
	case http.MethodDelete:
		return h.handleUntagResource(ctx, c, resourceARN, r.URL.Query())
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
	}
}

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorResponse("ClientException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

func tagsOrEmpty(tags map[string]string) map[string]string {
	if tags == nil {
		return map[string]string{}
	}

	return tags
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
	}

	if op, ok := ops[path]; ok {
		return op
	}

	return "Unknown"
}

// --- Input / Output types ---

type launchTemplateOverrideInput struct {
	LaunchTemplateName  string   `json:"launchTemplateName,omitempty"`
	LaunchTemplateID    string   `json:"launchTemplateId,omitempty"`
	Version             string   `json:"version,omitempty"`
	TargetInstanceTypes []string `json:"targetInstanceTypes,omitempty"`
}

type launchTemplateInput struct {
	LaunchTemplateName string                        `json:"launchTemplateName,omitempty"`
	LaunchTemplateID   string                        `json:"launchTemplateId,omitempty"`
	Version            string                        `json:"version,omitempty"`
	Overrides          []launchTemplateOverrideInput `json:"overrides,omitempty"`
}

type ec2ConfigurationInput struct {
	ImageType              string `json:"imageType"`
	ImageIDOverride        string `json:"imageIdOverride,omitempty"`
	ImageKubernetesVersion string `json:"imageKubernetesVersion,omitempty"`
}

type computeResourcesInput struct {
	Type               string                  `json:"type,omitempty"`
	AllocationStrategy string                  `json:"allocationStrategy,omitempty"`
	InstanceRole       string                  `json:"instanceRole,omitempty"`
	Ec2KeyPair         string                  `json:"ec2KeyPair,omitempty"`
	ImageID            string                  `json:"imageId,omitempty"`
	PlacementGroup     string                  `json:"placementGroup,omitempty"`
	SpotIamFleetRole   string                  `json:"spotIamFleetRole,omitempty"`
	InstanceTypes      []string                `json:"instanceTypes,omitempty"`
	Subnets            []string                `json:"subnets,omitempty"`
	SecurityGroupIDs   []string                `json:"securityGroupIds,omitempty"`
	Tags               map[string]string       `json:"tags,omitempty"`
	LaunchTemplate     *launchTemplateInput    `json:"launchTemplate,omitempty"`
	Ec2Configuration   []ec2ConfigurationInput `json:"ec2Configuration,omitempty"`
	MinvCpus           int32                   `json:"minvCpus,omitempty"`
	MaxvCpus           int32                   `json:"maxvCpus,omitempty"`
	DesiredvCpus       int32                   `json:"desiredvCpus,omitempty"`
	BidPercentage      int32                   `json:"bidPercentage,omitempty"`
}

type eksConfigurationInput struct {
	EksClusterArn       string `json:"eksClusterArn"`
	KubernetesNamespace string `json:"kubernetesNamespace"`
}

type updatePolicyInput struct {
	TerminateJobsOnUpdate      bool  `json:"terminateJobsOnUpdate,omitempty"`
	JobExecutionTimeoutMinutes int64 `json:"jobExecutionTimeoutMinutes,omitempty"`
}

type createComputeEnvironmentInput struct {
	Tags                   map[string]string      `json:"tags"`
	ComputeResources       *computeResourcesInput `json:"computeResources,omitempty"`
	EksConfiguration       *eksConfigurationInput `json:"eksConfiguration,omitempty"`
	UpdatePolicy           *updatePolicyInput     `json:"updatePolicy,omitempty"`
	ComputeEnvironmentName string                 `json:"computeEnvironmentName"`
	Type                   string                 `json:"type"`
	State                  string                 `json:"state"`
	ServiceRole            string                 `json:"serviceRole,omitempty"`
}

type createComputeEnvironmentOutput struct {
	ComputeEnvironmentArn  string `json:"computeEnvironmentArn"`
	ComputeEnvironmentName string `json:"computeEnvironmentName"`
}

func computeResourcesFromInput(in *computeResourcesInput) *ComputeResources {
	if in == nil {
		return nil
	}

	cr := &ComputeResources{
		Type:               in.Type,
		AllocationStrategy: in.AllocationStrategy,
		InstanceRole:       in.InstanceRole,
		Ec2KeyPair:         in.Ec2KeyPair,
		ImageID:            in.ImageID,
		PlacementGroup:     in.PlacementGroup,
		SpotIamFleetRole:   in.SpotIamFleetRole,
		InstanceTypes:      in.InstanceTypes,
		Subnets:            in.Subnets,
		SecurityGroupIDs:   in.SecurityGroupIDs,
		Tags:               in.Tags,
		MinvCpus:           in.MinvCpus,
		MaxvCpus:           in.MaxvCpus,
		DesiredvCpus:       in.DesiredvCpus,
		BidPercentage:      in.BidPercentage,
	}

	if in.LaunchTemplate != nil {
		lt := &LaunchTemplate{
			LaunchTemplateName: in.LaunchTemplate.LaunchTemplateName,
			LaunchTemplateID:   in.LaunchTemplate.LaunchTemplateID,
			Version:            in.LaunchTemplate.Version,
		}

		for _, o := range in.LaunchTemplate.Overrides {
			lt.Overrides = append(lt.Overrides, LaunchTemplateOverride(o))
		}

		cr.LaunchTemplate = lt
	}

	for _, ec2 := range in.Ec2Configuration {
		cr.Ec2Configuration = append(cr.Ec2Configuration, Ec2Configuration(ec2))
	}

	return cr
}

func eksConfigFromInput(in *eksConfigurationInput) *EksConfiguration {
	if in == nil {
		return nil
	}

	return &EksConfiguration{
		EksClusterArn:       in.EksClusterArn,
		KubernetesNamespace: in.KubernetesNamespace,
	}
}

func updatePolicyFromInput(in *updatePolicyInput) *UpdatePolicy {
	if in == nil {
		return nil
	}

	return &UpdatePolicy{
		TerminateJobsOnUpdate:      in.TerminateJobsOnUpdate,
		JobExecutionTimeoutMinutes: in.JobExecutionTimeoutMinutes,
	}
}

func (h *Handler) handleCreateComputeEnvironment(
	ctx context.Context,
	in *createComputeEnvironmentInput,
) (*createComputeEnvironmentOutput, error) {
	state := in.State
	if state == "" {
		state = stateEnabled
	}

	ce, err := h.Backend.CreateComputeEnvironment(
		ctx,
		in.ComputeEnvironmentName, in.Type, state, in.Tags, in.ServiceRole,
		computeResourcesFromInput(in.ComputeResources),
		eksConfigFromInput(in.EksConfiguration),
		updatePolicyFromInput(in.UpdatePolicy),
	)
	if err != nil {
		return nil, err
	}

	return &createComputeEnvironmentOutput{
		ComputeEnvironmentArn:  ce.ComputeEnvironmentArn,
		ComputeEnvironmentName: ce.ComputeEnvironmentName,
	}, nil
}

type describeComputeEnvironmentsInput struct {
	MaxResults          *int32   `json:"maxResults,omitempty"`
	NextToken           *string  `json:"nextToken,omitempty"`
	ComputeEnvironments []string `json:"computeEnvironments"`
}

type describeComputeEnvironmentsOutput struct {
	NextToken           *string               `json:"nextToken,omitempty"`
	ComputeEnvironments []*ComputeEnvironment `json:"computeEnvironments"`
}

func (h *Handler) handleDescribeComputeEnvironments(
	ctx context.Context,
	in *describeComputeEnvironmentsInput,
) (*describeComputeEnvironmentsOutput, error) {
	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	ces, outToken := h.Backend.DescribeComputeEnvironments(ctx, in.ComputeEnvironments, maxResults, nextToken)
	out := &describeComputeEnvironmentsOutput{ComputeEnvironments: ces}

	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type updateComputeEnvironmentInput struct {
	ComputeResources   *computeResourcesInput `json:"computeResources,omitempty"`
	UpdatePolicy       *updatePolicyInput     `json:"updatePolicy,omitempty"`
	ComputeEnvironment string                 `json:"computeEnvironment"`
	State              string                 `json:"state"`
	ServiceRole        string                 `json:"serviceRole,omitempty"`
}

type updateComputeEnvironmentOutput struct {
	ComputeEnvironmentArn  string `json:"computeEnvironmentArn"`
	ComputeEnvironmentName string `json:"computeEnvironmentName"`
}

func (h *Handler) handleUpdateComputeEnvironment(
	ctx context.Context,
	in *updateComputeEnvironmentInput,
) (*updateComputeEnvironmentOutput, error) {
	ce, err := h.Backend.UpdateComputeEnvironment(
		ctx,
		in.ComputeEnvironment, in.State, in.ServiceRole,
		computeResourcesFromInput(in.ComputeResources),
		updatePolicyFromInput(in.UpdatePolicy),
	)
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
	ctx context.Context,
	in *deleteComputeEnvironmentInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteComputeEnvironment(ctx, in.ComputeEnvironment); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type jobStateTimeLimitActionInput struct {
	Reason         string `json:"reason"`
	State          string `json:"state"`
	Action         string `json:"action"`
	MaxTimeSeconds int32  `json:"maxTimeSeconds"`
}

type createJobQueueInput struct {
	Tags                     map[string]string              `json:"tags"`
	JobQueueName             string                         `json:"jobQueueName"`
	State                    string                         `json:"state"`
	SchedulingPolicyArn      string                         `json:"schedulingPolicyArn,omitempty"`
	ComputeEnvironmentOrder  []ComputeEnvironmentOrder      `json:"computeEnvironmentOrder"`
	JobStateTimeLimitActions []jobStateTimeLimitActionInput `json:"jobStateTimeLimitActions,omitempty"`
	Priority                 int32                          `json:"priority"`
}

type createJobQueueOutput struct {
	JobQueueArn  string `json:"jobQueueArn"`
	JobQueueName string `json:"jobQueueName"`
}

func jobStateTimeLimitActionsFromInput(in []jobStateTimeLimitActionInput) []JobStateTimeLimitAction {
	if len(in) == 0 {
		return nil
	}

	out := make([]JobStateTimeLimitAction, len(in))
	for i, a := range in {
		out[i] = JobStateTimeLimitAction(a)
	}

	return out
}

func (h *Handler) handleCreateJobQueue(
	ctx context.Context,
	in *createJobQueueInput,
) (*createJobQueueOutput, error) {
	state := in.State
	if state == "" {
		state = stateEnabled
	}

	jq, err := h.Backend.CreateJobQueue(
		ctx,
		in.JobQueueName,
		in.Priority,
		state,
		in.ComputeEnvironmentOrder,
		in.Tags,
		in.SchedulingPolicyArn,
		jobStateTimeLimitActionsFromInput(in.JobStateTimeLimitActions),
	)
	if err != nil {
		return nil, err
	}

	return &createJobQueueOutput{
		JobQueueArn:  jq.JobQueueArn,
		JobQueueName: jq.JobQueueName,
	}, nil
}

type describeJobQueuesInput struct {
	MaxResults *int32   `json:"maxResults,omitempty"`
	NextToken  *string  `json:"nextToken,omitempty"`
	JobQueues  []string `json:"jobQueues"`
}

type describeJobQueuesOutput struct {
	NextToken *string     `json:"nextToken,omitempty"`
	JobQueues []*JobQueue `json:"jobQueues"`
}

func (h *Handler) handleDescribeJobQueues(
	ctx context.Context,
	in *describeJobQueuesInput,
) (*describeJobQueuesOutput, error) {
	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	jqs, outToken := h.Backend.DescribeJobQueues(ctx, in.JobQueues, maxResults, nextToken)
	out := &describeJobQueuesOutput{JobQueues: jqs}

	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type updateJobQueueInput struct {
	Priority                 *int32                         `json:"priority,omitempty"`
	JobQueue                 string                         `json:"jobQueue"`
	State                    string                         `json:"state"`
	SchedulingPolicyArn      string                         `json:"schedulingPolicyArn,omitempty"`
	ComputeEnvironmentOrder  []ComputeEnvironmentOrder      `json:"computeEnvironmentOrder,omitempty"`
	JobStateTimeLimitActions []jobStateTimeLimitActionInput `json:"jobStateTimeLimitActions,omitempty"`
}

type updateJobQueueOutput struct {
	JobQueueArn  string `json:"jobQueueArn"`
	JobQueueName string `json:"jobQueueName"`
}

func (h *Handler) handleUpdateJobQueue(
	ctx context.Context,
	in *updateJobQueueInput,
) (*updateJobQueueOutput, error) {
	jq, err := h.Backend.UpdateJobQueue(
		ctx,
		in.JobQueue, in.Priority, in.State, in.ComputeEnvironmentOrder,
		jobStateTimeLimitActionsFromInput(in.JobStateTimeLimitActions),
	)
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
	ctx context.Context,
	in *deleteJobQueueInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteJobQueue(ctx, in.JobQueue); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type jobDefinitionTimeout struct {
	AttemptDurationSeconds int32 `json:"attemptDurationSeconds,omitempty"`
}

type resourceRequirementInput struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type keyValuePairInput struct {
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

type mountPointInput struct {
	ContainerPath string `json:"containerPath,omitempty"`
	SourceVolume  string `json:"sourceVolume,omitempty"`
	ReadOnly      bool   `json:"readOnly,omitempty"`
}

type hostVolumeInput struct {
	SourcePath string `json:"sourcePath,omitempty"`
}

type volumeInput struct {
	Host *hostVolumeInput `json:"host,omitempty"`
	Name string           `json:"name"`
}

type ulimitInput struct {
	Name      string `json:"name"`
	SoftLimit int32  `json:"softLimit"`
	HardLimit int32  `json:"hardLimit"`
}

type logConfigurationInput struct {
	Options   map[string]string `json:"options,omitempty"`
	LogDriver string            `json:"logDriver"`
}

type networkConfigurationInput struct {
	AssignPublicIP string `json:"assignPublicIp,omitempty"`
}

type fargatePlatformConfigInput struct {
	PlatformVersion string `json:"platformVersion,omitempty"`
}

type ephemeralStorageInput struct {
	SizeInGiB int32 `json:"sizeInGiB"`
}

type runtimePlatformInput struct {
	OperatingSystemFamily string `json:"operatingSystemFamily,omitempty"`
	CPUArchitecture       string `json:"cpuArchitecture,omitempty"`
}

type repositoryCredentialsInput struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

type secretInput struct {
	Name      string `json:"name"`
	ValueFrom string `json:"valueFrom"`
}

type deviceInput struct {
	HostPath      string   `json:"hostPath"`
	ContainerPath string   `json:"containerPath,omitempty"`
	Permissions   []string `json:"permissions,omitempty"`
}

type tmpfsInput struct {
	ContainerPath string   `json:"containerPath"`
	MountOptions  []string `json:"mountOptions,omitempty"`
	Size          int32    `json:"size"`
}

type linuxParametersInput struct {
	Devices            []deviceInput `json:"devices,omitempty"`
	Tmpfs              []tmpfsInput  `json:"tmpfs,omitempty"`
	InitProcessEnabled bool          `json:"initProcessEnabled,omitempty"`
	SharedMemorySize   int32         `json:"sharedMemorySize,omitempty"`
	MaxSwap            int32         `json:"maxSwap,omitempty"`
	Swappiness         int32         `json:"swappiness,omitempty"`
}

type containerPropertiesInput struct {
	LinuxParameters              *linuxParametersInput       `json:"linuxParameters,omitempty"`
	RepositoryCredentials        *repositoryCredentialsInput `json:"repositoryCredentials,omitempty"`
	RuntimePlatform              *runtimePlatformInput       `json:"runtimePlatform,omitempty"`
	EphemeralStorage             *ephemeralStorageInput      `json:"ephemeralStorage,omitempty"`
	FargatePlatformConfiguration *fargatePlatformConfigInput `json:"fargatePlatformConfiguration,omitempty"`
	NetworkConfiguration         *networkConfigurationInput  `json:"networkConfiguration,omitempty"`
	LogConfiguration             *logConfigurationInput      `json:"logConfiguration,omitempty"`
	JobRoleArn                   string                      `json:"jobRoleArn,omitempty"`
	ExecutionRoleArn             string                      `json:"executionRoleArn,omitempty"`
	User                         string                      `json:"user,omitempty"`
	InstanceType                 string                      `json:"instanceType,omitempty"`
	Image                        string                      `json:"image,omitempty"`
	Command                      []string                    `json:"command,omitempty"`
	Secrets                      []secretInput               `json:"secrets,omitempty"`
	ResourceRequirements         []resourceRequirementInput  `json:"resourceRequirements,omitempty"`
	Ulimits                      []ulimitInput               `json:"ulimits,omitempty"`
	MountPoints                  []mountPointInput           `json:"mountPoints,omitempty"`
	Volumes                      []volumeInput               `json:"volumes,omitempty"`
	Environment                  []keyValuePairInput         `json:"environment,omitempty"`
	Vcpus                        int32                       `json:"vcpus,omitempty"`
	Memory                       int32                       `json:"memory,omitempty"`
	ReadonlyRootFilesystem       bool                        `json:"readonlyRootFilesystem,omitempty"`
	Privileged                   bool                        `json:"privileged,omitempty"`
}

type consumableResourcePropertyInput struct {
	ConsumableResource string  `json:"consumableResource"`
	Quantity           float64 `json:"quantity"`
}

type nodeRangePropertyInput struct {
	ContainerProperties *containerPropertiesInput `json:"containerProperties,omitempty"`
	TargetNodes         string                    `json:"targetNodes"`
}

type nodePropertiesInput struct {
	NodeRangeProperties []nodeRangePropertyInput `json:"nodeRangeProperties"`
	NumNodes            int32                    `json:"numNodes"`
	MainNode            int32                    `json:"mainNode"`
}

type registerJobDefinitionInput struct {
	Tags                         map[string]string                 `json:"tags"`
	Parameters                   map[string]string                 `json:"parameters,omitempty"`
	Timeout                      *jobDefinitionTimeout             `json:"timeout,omitempty"`
	ContainerProperties          *containerPropertiesInput         `json:"containerProperties,omitempty"`
	NodeProperties               *nodePropertiesInput              `json:"nodeProperties,omitempty"`
	RuntimePlatform              *runtimePlatformInput             `json:"runtimePlatform,omitempty"`
	ConsumableResourceProperties []consumableResourcePropertyInput `json:"consumableResourceProperties,omitempty"`
	JobDefinitionName            string                            `json:"jobDefinitionName"`
	Type                         string                            `json:"type"`
	PlatformCapabilities         []string                          `json:"platformCapabilities,omitempty"`
	SchedulingPriority           int32                             `json:"schedulingPriority,omitempty"`
	PropagateTags                bool                              `json:"propagateTags,omitempty"`
}

type registerJobDefinitionOutput struct {
	JobDefinitionArn  string `json:"jobDefinitionArn"`
	JobDefinitionName string `json:"jobDefinitionName"`
	Revision          int32  `json:"revision"`
	TimeoutSeconds    int32  `json:"timeout,omitempty"`
}

//nolint:cyclop,funlen // Too complex to refactor given time constraints
func containerPropertiesFromInput(in *containerPropertiesInput) *ContainerProperties {
	if in == nil {
		return nil
	}

	cp := &ContainerProperties{
		Image:                  in.Image,
		JobRoleArn:             in.JobRoleArn,
		ExecutionRoleArn:       in.ExecutionRoleArn,
		User:                   in.User,
		InstanceType:           in.InstanceType,
		Command:                in.Command,
		Vcpus:                  in.Vcpus,
		Memory:                 in.Memory,
		ReadonlyRootFilesystem: in.ReadonlyRootFilesystem,
		Privileged:             in.Privileged,
	}

	for _, e := range in.Environment {
		cp.Environment = append(cp.Environment, KeyValuePair(e))
	}

	for _, v := range in.Volumes {
		vol := Volume{Name: v.Name}
		if v.Host != nil {
			vol.Host = &HostVolume{SourcePath: v.Host.SourcePath}
		}

		cp.Volumes = append(cp.Volumes, vol)
	}

	for _, m := range in.MountPoints {
		cp.MountPoints = append(cp.MountPoints, MountPoint(m))
	}

	for _, u := range in.Ulimits {
		cp.Ulimits = append(cp.Ulimits, Ulimit(u))
	}

	for _, rr := range in.ResourceRequirements {
		cp.ResourceRequirements = append(cp.ResourceRequirements, ResourceRequirement(rr))
	}

	for _, s := range in.Secrets {
		cp.Secrets = append(cp.Secrets, Secret(s))
	}

	if in.LinuxParameters != nil {
		lp := &LinuxParameters{
			InitProcessEnabled: in.LinuxParameters.InitProcessEnabled,
			SharedMemorySize:   in.LinuxParameters.SharedMemorySize,
			MaxSwap:            in.LinuxParameters.MaxSwap,
			Swappiness:         in.LinuxParameters.Swappiness,
		}

		for _, d := range in.LinuxParameters.Devices {
			lp.Devices = append(
				lp.Devices,
				Device(d),
			)
		}

		for _, t := range in.LinuxParameters.Tmpfs {
			lp.Tmpfs = append(
				lp.Tmpfs,
				Tmpfs(t),
			)
		}

		cp.LinuxParameters = lp
	}

	if in.LogConfiguration != nil {
		cp.LogConfiguration = &LogConfiguration{
			LogDriver: in.LogConfiguration.LogDriver,
			Options:   in.LogConfiguration.Options,
		}
	}

	if in.NetworkConfiguration != nil {
		cp.NetworkConfiguration = &NetworkConfiguration{AssignPublicIP: in.NetworkConfiguration.AssignPublicIP}
	}

	if in.FargatePlatformConfiguration != nil {
		cp.FargatePlatformConfiguration = &FargatePlatformConfiguration{
			PlatformVersion: in.FargatePlatformConfiguration.PlatformVersion,
		}
	}

	if in.EphemeralStorage != nil {
		cp.EphemeralStorage = &EphemeralStorage{SizeInGiB: in.EphemeralStorage.SizeInGiB}
	}

	if in.RuntimePlatform != nil {
		cp.RuntimePlatform = &RuntimePlatform{
			OperatingSystemFamily: in.RuntimePlatform.OperatingSystemFamily,
			CPUArchitecture:       in.RuntimePlatform.CPUArchitecture,
		}
	}

	if in.RepositoryCredentials != nil {
		cp.RepositoryCredentials = &RepositoryCredentials{
			CredentialsParameter: in.RepositoryCredentials.CredentialsParameter,
		}
	}

	return cp
}

func nodePropertiesFromInput(in *nodePropertiesInput) *NodeProperties {
	if in == nil {
		return nil
	}

	np := &NodeProperties{
		NumNodes: in.NumNodes,
		MainNode: in.MainNode,
	}

	for _, nrp := range in.NodeRangeProperties {
		np.NodeRangeProperties = append(np.NodeRangeProperties, NodeRangeProperty{
			TargetNodes:         nrp.TargetNodes,
			ContainerProperties: containerPropertiesFromInput(nrp.ContainerProperties),
		})
	}

	return np
}

func runtimePlatformFromInput(in *runtimePlatformInput) *RuntimePlatform {
	if in == nil {
		return nil
	}

	return &RuntimePlatform{
		OperatingSystemFamily: in.OperatingSystemFamily,
		CPUArchitecture:       in.CPUArchitecture,
	}
}

func consumableResourcePropertiesFromInput(in []consumableResourcePropertyInput) []ConsumableResourceProperty {
	if len(in) == 0 {
		return nil
	}

	out := make([]ConsumableResourceProperty, len(in))
	for i, c := range in {
		out[i] = ConsumableResourceProperty(c)
	}

	return out
}

func (h *Handler) handleRegisterJobDefinition(
	ctx context.Context,
	in *registerJobDefinitionInput,
) (*registerJobDefinitionOutput, error) {
	var timeoutSeconds int32
	if in.Timeout != nil {
		timeoutSeconds = in.Timeout.AttemptDurationSeconds
	}

	jd, err := h.Backend.RegisterJobDefinition(
		ctx,
		in.JobDefinitionName,
		in.Type,
		in.Tags,
		in.PlatformCapabilities,
		timeoutSeconds,
		in.SchedulingPriority,
		containerPropertiesFromInput(in.ContainerProperties),
		nodePropertiesFromInput(in.NodeProperties),
		nil, // EksProperties not yet wired through handler input
		runtimePlatformFromInput(in.RuntimePlatform),
		consumableResourcePropertiesFromInput(in.ConsumableResourceProperties),
		in.Parameters,
		in.PropagateTags,
	)
	if err != nil {
		return nil, err
	}

	return &registerJobDefinitionOutput{
		JobDefinitionArn:  jd.JobDefinitionArn,
		JobDefinitionName: jd.JobDefinitionName,
		Revision:          jd.Revision,
		TimeoutSeconds:    jd.TimeoutSeconds,
	}, nil
}

type describeJobDefinitionsInput struct {
	MaxResults        *int32   `json:"maxResults,omitempty"`
	NextToken         *string  `json:"nextToken,omitempty"`
	JobDefinitionName string   `json:"jobDefinitionName,omitempty"`
	Status            string   `json:"status,omitempty"`
	JobDefinitions    []string `json:"jobDefinitions"`
}

type describeJobDefinitionsOutput struct {
	NextToken      *string          `json:"nextToken,omitempty"`
	JobDefinitions []*JobDefinition `json:"jobDefinitions"`
}

func (h *Handler) handleDescribeJobDefinitions(
	ctx context.Context,
	in *describeJobDefinitionsInput,
) (*describeJobDefinitionsOutput, error) {
	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	jds, outToken := h.Backend.DescribeJobDefinitions(
		ctx,
		in.JobDefinitions,
		in.Status,
		in.JobDefinitionName,
		maxResults,
		nextToken,
	)

	out := &describeJobDefinitionsOutput{JobDefinitions: jds}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type deregisterJobDefinitionInput struct {
	JobDefinition string `json:"jobDefinition"`
}

func (h *Handler) handleDeregisterJobDefinition(
	ctx context.Context,
	in *deregisterJobDefinitionInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeregisterJobDefinition(ctx, in.JobDefinition); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Job operation handlers ---

type listJobsInput struct {
	MaxResults *int32  `json:"maxResults,omitempty"`
	NextToken  *string `json:"nextToken,omitempty"`
	JobQueue   string  `json:"jobQueue"`
	JobStatus  string  `json:"jobStatus"`
}

type jobSummary struct {
	JobID        string `json:"jobId"`
	JobName      string `json:"jobName"`
	Status       string `json:"status"`
	StatusReason string `json:"statusReason,omitempty"`
	CreatedAt    int64  `json:"createdAt"`
}

type listJobsOutput struct {
	NextToken      *string      `json:"nextToken,omitempty"`
	JobSummaryList []jobSummary `json:"jobSummaryList"`
}

func (h *Handler) handleListJobs(ctx context.Context, in *listJobsInput) (*listJobsOutput, error) {
	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	jobs, outToken, err := h.Backend.ListJobs(ctx, in.JobQueue, in.JobStatus, nextToken, maxResults)
	if err != nil {
		return nil, err
	}

	summaries := make([]jobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, jobSummary{
			JobID:        j.JobID,
			JobName:      j.JobName,
			Status:       j.Status,
			CreatedAt:    j.CreatedAt,
			StatusReason: j.StatusReason,
		})
	}

	out := &listJobsOutput{JobSummaryList: summaries}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type describeJobsInput struct {
	Jobs []string `json:"jobs"`
}

type jobDetail struct {
	StoppedAt     *int64            `json:"stoppedAt,omitempty"`
	StartedAt     *int64            `json:"startedAt,omitempty"`
	Tags          map[string]string `json:"tags"`
	JobID         string            `json:"jobId"`
	JobARN        string            `json:"jobArn,omitempty"`
	JobName       string            `json:"jobName"`
	JobQueue      string            `json:"jobQueue"`
	JobDefinition string            `json:"jobDefinition"`
	Status        string            `json:"status"`
	StatusReason  string            `json:"statusReason,omitempty"`
	CreatedAt     int64             `json:"createdAt"`
}

type describeJobsOutput struct {
	Jobs []jobDetail `json:"jobs"`
}

func (h *Handler) handleDescribeJobs(ctx context.Context, in *describeJobsInput) (*describeJobsOutput, error) {
	jobs := h.Backend.DescribeJobs(ctx, in.Jobs)

	details := make([]jobDetail, 0, len(jobs))
	for _, j := range jobs {
		details = append(details, jobDetail{
			JobID:         j.JobID,
			JobARN:        j.JobARN,
			JobName:       j.JobName,
			JobQueue:      j.JobQueue,
			JobDefinition: j.JobDefinition,
			Status:        j.Status,
			StatusReason:  j.StatusReason,
			CreatedAt:     j.CreatedAt,
			StartedAt:     j.StartedAt,
			StoppedAt:     j.StoppedAt,
			Tags:          tagsOrEmpty(j.Tags),
		})
	}

	return &describeJobsOutput{Jobs: details}, nil
}

type containerOverridesInput struct {
	Environment []keyValuePair `json:"environment,omitempty"`
	Command     []string       `json:"command,omitempty"`
}

type keyValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type submitJobInput struct {
	Tags               map[string]string        `json:"tags"`
	Parameters         map[string]string        `json:"parameters,omitempty"`
	RetryStrategy      *RetryStrategy           `json:"retryStrategy,omitempty"`
	Timeout            *JobTimeout              `json:"timeout,omitempty"`
	ContainerOverrides *containerOverridesInput `json:"containerOverrides,omitempty"`
	JobName            string                   `json:"jobName"`
	JobQueue           string                   `json:"jobQueue"`
	JobDefinition      string                   `json:"jobDefinition"`
	DependsOn          []JobDependency          `json:"dependsOn,omitempty"`
}

type submitJobOutput struct {
	JobID   string `json:"jobId"`
	JobName string `json:"jobName"`
}

func (h *Handler) handleSubmitJob(ctx context.Context, in *submitJobInput) (*submitJobOutput, error) {
	var overrides *ContainerOverrides
	if in.ContainerOverrides != nil {
		env := make([]KeyValuePair, len(in.ContainerOverrides.Environment))
		for i, kv := range in.ContainerOverrides.Environment {
			env[i] = KeyValuePair(kv)
		}
		overrides = &ContainerOverrides{
			Command:     in.ContainerOverrides.Command,
			Environment: env,
		}
	}

	j, err := h.Backend.SubmitJob(
		ctx,
		in.JobName,
		in.JobQueue,
		in.JobDefinition,
		in.Tags,
		in.Parameters,
		in.DependsOn,
		in.RetryStrategy,
		in.Timeout,
		nil, // arrayProperties
		overrides,
		nil,   // consumableResourceProperties
		"",    // shareIdentifier
		0,     // schedulingPriorityOverride
		false, // propagateTags
	)
	if err != nil {
		return nil, err
	}

	return &submitJobOutput{
		JobID:   j.JobID,
		JobName: j.JobName,
	}, nil
}

type terminateJobInput struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

func (h *Handler) handleTerminateJob(ctx context.Context, in *terminateJobInput) (*emptyOutput, error) {
	if err := h.Backend.TerminateJob(ctx, in.JobID, in.Reason); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type cancelJobInput struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

func (h *Handler) handleCancelJob(ctx context.Context, in *cancelJobInput) (*emptyOutput, error) {
	if err := h.Backend.CancelJob(ctx, in.JobID, in.Reason); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Tags handlers ---

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
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

func (h *Handler) handleTagResource(ctx context.Context, c *echo.Context, resourceARN string, body []byte) error {
	var in tagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
		}
	}

	if err := h.Backend.TagResource(ctx, resourceARN, in.Tags); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, emptyOutput{})
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	c *echo.Context,
	resourceARN string,
	query url.Values,
) error {
	tagKeys := query["tagKeys"]
	if err := h.Backend.UntagResource(ctx, resourceARN, tagKeys); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, emptyOutput{})
}

// --- ConsumableResource handlers ---

type createConsumableResourceInput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType"`
	TotalQuantity          int64             `json:"totalQuantity"`
}

type createConsumableResourceOutput struct {
	ConsumableResourceArn  string `json:"consumableResourceArn"`
	ConsumableResourceName string `json:"consumableResourceName"`
}

func (h *Handler) handleCreateConsumableResource(
	ctx context.Context,
	in *createConsumableResourceInput,
) (*createConsumableResourceOutput, error) {
	if in.ConsumableResourceName == "" {
		return nil, fmt.Errorf("%w: consumableResourceName is required", ErrValidation)
	}

	cr, err := h.Backend.CreateConsumableResource(
		ctx,
		in.ConsumableResourceName,
		in.ResourceType,
		in.TotalQuantity,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
	}, nil
}

type deleteConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

func (h *Handler) handleDeleteConsumableResource(
	ctx context.Context,
	in *deleteConsumableResourceInput,
) (*emptyOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	if err := h.Backend.DeleteConsumableResource(ctx, in.ConsumableResource); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type describeConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

type describeConsumableResourceOutput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

func (h *Handler) handleDescribeConsumableResource(
	ctx context.Context,
	in *describeConsumableResourceInput,
) (*describeConsumableResourceOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	cr, err := h.Backend.DescribeConsumableResource(ctx, in.ConsumableResource)
	if err != nil {
		return nil, err
	}

	return &describeConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
		ResourceType:           cr.ResourceType,
		Tags:                   tagsOrEmpty(cr.Tags),
		CreatedAt:              cr.CreatedAt,
		TotalQuantity:          cr.TotalQuantity,
		AvailableQuantity:      cr.AvailableQuantity,
		InUseQuantity:          cr.InUseQuantity,
	}, nil
}

// --- SchedulingPolicy handlers ---

type createSchedulingPolicyInput struct {
	Tags map[string]string `json:"tags"`
	Name string            `json:"name"`
}

type createSchedulingPolicyOutput struct {
	Arn  string `json:"arn"`
	Name string `json:"name"`
}

func (h *Handler) handleCreateSchedulingPolicy(
	ctx context.Context,
	in *createSchedulingPolicyInput,
) (*createSchedulingPolicyOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	sp, err := h.Backend.CreateSchedulingPolicy(ctx, in.Name, in.Tags, nil)
	if err != nil {
		return nil, err
	}

	return &createSchedulingPolicyOutput{
		Arn:  sp.Arn,
		Name: sp.Name,
	}, nil
}

type deleteSchedulingPolicyInput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleDeleteSchedulingPolicy(
	ctx context.Context,
	in *deleteSchedulingPolicyInput,
) (*emptyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if err := h.Backend.DeleteSchedulingPolicy(ctx, in.Arn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- ServiceEnvironment handlers ---

type createServiceEnvironmentInput struct {
	Tags                   map[string]string `json:"tags"`
	ServiceEnvironmentName string            `json:"serviceEnvironmentName"`
	ServiceEnvironmentType string            `json:"serviceEnvironmentType"`
	State                  string            `json:"state"`
}

type createServiceEnvironmentOutput struct {
	ServiceEnvironmentArn  string `json:"serviceEnvironmentArn"`
	ServiceEnvironmentName string `json:"serviceEnvironmentName"`
}

func (h *Handler) handleCreateServiceEnvironment(
	ctx context.Context,
	in *createServiceEnvironmentInput,
) (*createServiceEnvironmentOutput, error) {
	if in.ServiceEnvironmentName == "" {
		return nil, fmt.Errorf("%w: serviceEnvironmentName is required", ErrValidation)
	}

	if in.ServiceEnvironmentType == "" {
		return nil, fmt.Errorf("%w: serviceEnvironmentType is required", ErrValidation)
	}

	se, err := h.Backend.CreateServiceEnvironment(
		ctx,
		in.ServiceEnvironmentName,
		in.ServiceEnvironmentType,
		in.State,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createServiceEnvironmentOutput{
		ServiceEnvironmentArn:  se.ServiceEnvironmentArn,
		ServiceEnvironmentName: se.ServiceEnvironmentName,
	}, nil
}

type deleteServiceEnvironmentInput struct {
	ServiceEnvironment string `json:"serviceEnvironment"`
}

func (h *Handler) handleDeleteServiceEnvironment(
	ctx context.Context,
	in *deleteServiceEnvironmentInput,
) (*emptyOutput, error) {
	if in.ServiceEnvironment == "" {
		return nil, fmt.Errorf("%w: serviceEnvironment is required", ErrValidation)
	}

	if err := h.Backend.DeleteServiceEnvironment(ctx, in.ServiceEnvironment); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- UpdateConsumableResource handler ---

type updateConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
	Operation          string `json:"operation"`
	Quantity           int64  `json:"quantity"`
}

type updateConsumableResourceOutput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

func (h *Handler) handleUpdateConsumableResource(
	ctx context.Context,
	in *updateConsumableResourceInput,
) (*updateConsumableResourceOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	cr, err := h.Backend.UpdateConsumableResource(ctx, in.ConsumableResource, in.Operation, in.Quantity)
	if err != nil {
		return nil, err
	}

	return &updateConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
		ResourceType:           cr.ResourceType,
		Tags:                   tagsOrEmpty(cr.Tags),
		CreatedAt:              cr.CreatedAt,
		TotalQuantity:          cr.TotalQuantity,
		AvailableQuantity:      cr.AvailableQuantity,
		InUseQuantity:          cr.InUseQuantity,
	}, nil
}

// --- ListConsumableResources handler ---

type listConsumableResourcesOutput struct {
	ConsumableResourceSummaryList []*ConsumableResource `json:"consumableResourceSummaryList"`
}

func (h *Handler) handleListConsumableResources(
	ctx context.Context,
	_ *struct{},
) (*listConsumableResourcesOutput, error) {
	list := h.Backend.ListConsumableResources(ctx)

	return &listConsumableResourcesOutput{ConsumableResourceSummaryList: list}, nil
}

// --- DescribeSchedulingPolicies handler ---

type describeSchedulingPoliciesInput struct {
	Arns []string `json:"arns"`
}

type describeSchedulingPoliciesOutput struct {
	SchedulingPolicies []*SchedulingPolicy `json:"schedulingPolicies"`
}

func (h *Handler) handleDescribeSchedulingPolicies(
	ctx context.Context,
	in *describeSchedulingPoliciesInput,
) (*describeSchedulingPoliciesOutput, error) {
	list := h.Backend.DescribeSchedulingPolicies(ctx, in.Arns)

	return &describeSchedulingPoliciesOutput{SchedulingPolicies: list}, nil
}

// --- ListSchedulingPolicies handler ---

type listSchedulingPoliciesOutput struct {
	SchedulingPolicies []*SchedulingPolicy `json:"schedulingPolicies"`
}

func (h *Handler) handleListSchedulingPolicies(
	ctx context.Context,
	_ *struct{},
) (*listSchedulingPoliciesOutput, error) {
	list := h.Backend.ListSchedulingPolicies(ctx)

	return &listSchedulingPoliciesOutput{SchedulingPolicies: list}, nil
}

// --- UpdateSchedulingPolicy handler ---

type updateSchedulingPolicyInput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleUpdateSchedulingPolicy(
	ctx context.Context,
	in *updateSchedulingPolicyInput,
) (*emptyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if err := h.Backend.UpdateSchedulingPolicy(ctx, in.Arn, nil); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DescribeServiceEnvironments handler ---

type describeServiceEnvironmentsInput struct {
	ServiceEnvironments []string `json:"serviceEnvironments"`
}

type describeServiceEnvironmentsOutput struct {
	ServiceEnvironments []*ServiceEnvironment `json:"serviceEnvironments"`
}

func (h *Handler) handleDescribeServiceEnvironments(
	ctx context.Context,
	in *describeServiceEnvironmentsInput,
) (*describeServiceEnvironmentsOutput, error) {
	list := h.Backend.DescribeServiceEnvironments(ctx, in.ServiceEnvironments)

	return &describeServiceEnvironmentsOutput{ServiceEnvironments: list}, nil
}

// --- UpdateServiceEnvironment handler ---

type updateServiceEnvironmentInput struct {
	ServiceEnvironment string `json:"serviceEnvironment"`
	State              string `json:"state"`
}

type updateServiceEnvironmentOutput struct {
	ServiceEnvironmentArn  string `json:"serviceEnvironmentArn"`
	ServiceEnvironmentName string `json:"serviceEnvironmentName"`
}

func (h *Handler) handleUpdateServiceEnvironment(
	ctx context.Context,
	in *updateServiceEnvironmentInput,
) (*updateServiceEnvironmentOutput, error) {
	if in.ServiceEnvironment == "" {
		return nil, fmt.Errorf("%w: serviceEnvironment is required", ErrValidation)
	}

	se, err := h.Backend.UpdateServiceEnvironment(ctx, in.ServiceEnvironment, in.State)
	if err != nil {
		return nil, err
	}

	return &updateServiceEnvironmentOutput{
		ServiceEnvironmentArn:  se.ServiceEnvironmentArn,
		ServiceEnvironmentName: se.ServiceEnvironmentName,
	}, nil
}

// --- ServiceJob handlers ---

type submitServiceJobInput struct {
	Tags               map[string]string `json:"tags"`
	ServiceJobName     string            `json:"serviceJobName"`
	ServiceEnvironment string            `json:"serviceEnvironment"`
}

type submitServiceJobOutput struct {
	ServiceJobArn  string `json:"serviceJobArn"`
	ServiceJobName string `json:"serviceJobName"`
}

func (h *Handler) handleSubmitServiceJob(
	ctx context.Context,
	in *submitServiceJobInput,
) (*submitServiceJobOutput, error) {
	if in.ServiceJobName == "" {
		return nil, fmt.Errorf("%w: serviceJobName is required", ErrValidation)
	}

	sj, err := h.Backend.SubmitServiceJob(ctx, in.ServiceJobName, in.ServiceEnvironment, in.Tags)
	if err != nil {
		return nil, err
	}

	return &submitServiceJobOutput{
		ServiceJobArn:  sj.ServiceJobArn,
		ServiceJobName: sj.ServiceJobName,
	}, nil
}

type describeServiceJobInput struct {
	ServiceJob string `json:"serviceJob"`
}

type describeServiceJobOutput struct {
	Tags               map[string]string `json:"tags"`
	StartedAt          *int64            `json:"startedAt,omitempty"`
	StoppedAt          *int64            `json:"stoppedAt,omitempty"`
	ServiceJobID       string            `json:"serviceJobId"`
	ServiceJobArn      string            `json:"serviceJobArn"`
	ServiceJobName     string            `json:"serviceJobName"`
	ServiceEnvironment string            `json:"serviceEnvironment"`
	Status             string            `json:"status"`
	StatusReason       string            `json:"statusReason,omitempty"`
	CreatedAt          int64             `json:"createdAt"`
}

func (h *Handler) handleDescribeServiceJob(
	ctx context.Context,
	in *describeServiceJobInput,
) (*describeServiceJobOutput, error) {
	if in.ServiceJob == "" {
		return nil, fmt.Errorf("%w: serviceJob is required", ErrValidation)
	}

	sj, err := h.Backend.DescribeServiceJob(ctx, in.ServiceJob)
	if err != nil {
		return nil, err
	}

	return &describeServiceJobOutput{
		ServiceJobID:       sj.ServiceJobID,
		ServiceJobArn:      sj.ServiceJobArn,
		ServiceJobName:     sj.ServiceJobName,
		ServiceEnvironment: sj.ServiceEnvironment,
		Status:             sj.Status,
		StatusReason:       sj.StatusReason,
		CreatedAt:          sj.CreatedAt,
		StartedAt:          sj.StartedAt,
		StoppedAt:          sj.StoppedAt,
		Tags:               tagsOrEmpty(sj.Tags),
	}, nil
}

type listServiceJobsInput struct {
	ServiceEnvironment string `json:"serviceEnvironment,omitempty"`
}

type listServiceJobsOutput struct {
	ServiceJobs []*ServiceJob `json:"serviceJobs"`
}

func (h *Handler) handleListServiceJobs(ctx context.Context, in *listServiceJobsInput) (*listServiceJobsOutput, error) {
	list, err := h.Backend.ListServiceJobs(ctx, in.ServiceEnvironment)
	if err != nil {
		return nil, err
	}

	return &listServiceJobsOutput{ServiceJobs: list}, nil
}

type terminateServiceJobInput struct {
	ServiceJob string `json:"serviceJob"`
	Reason     string `json:"reason"`
}

func (h *Handler) handleTerminateServiceJob(ctx context.Context, in *terminateServiceJobInput) (*emptyOutput, error) {
	if in.ServiceJob == "" {
		return nil, fmt.Errorf("%w: serviceJob is required", ErrValidation)
	}

	if err := h.Backend.TerminateServiceJob(ctx, in.ServiceJob, in.Reason); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getJobQueueSnapshotInput struct {
	JobQueue string `json:"jobQueue"`
}

func (h *Handler) handleGetJobQueueSnapshot(
	ctx context.Context,
	in *getJobQueueSnapshotInput,
) (*JobQueueSnapshot, error) {
	if in.JobQueue == "" {
		return nil, fmt.Errorf("%w: jobQueue is required", ErrValidation)
	}

	return h.Backend.GetJobQueueSnapshot(ctx, in.JobQueue)
}

type listJobsByConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

type listJobsByConsumableResourceOutput struct {
	Jobs []*Job `json:"jobs"`
}

func (h *Handler) handleListJobsByConsumableResource(
	ctx context.Context,
	in *listJobsByConsumableResourceInput,
) (*listJobsByConsumableResourceOutput, error) {
	jobs, err := h.Backend.ListJobsByConsumableResource(ctx, in.ConsumableResource)
	if err != nil {
		return nil, err
	}

	return &listJobsByConsumableResourceOutput{Jobs: jobs}, nil
}
