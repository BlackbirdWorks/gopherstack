package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyMessageField = "message"
)

const (
	ecsTargetPrefix   = "AmazonEC2ContainerServiceV20141113."
	unknownActionName = "Unknown"
	statusMissing     = "MISSING"
	transportTCP      = "tcp"
	keyTypeField      = "__type"
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for ECS operations.
type Handler struct {
	Backend Backend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new ECS handler.
func NewHandler(backend Backend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "ECS" }

// GetSupportedOperations returns the list of supported ECS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"ListClusters",
		"DescribeClusters",
		"DeleteCluster",
		"RegisterTaskDefinition",
		"DescribeTaskDefinition",
		"DeregisterTaskDefinition",
		"ListTaskDefinitions",
		"DeleteTaskDefinitions",
		"CreateService",
		"DescribeServices",
		"UpdateService",
		"DeleteService",
		"ListServices",
		"RunTask",
		"DescribeTasks",
		"StopTask",
		"ListTasks",
		"RegisterContainerInstance",
		"DeregisterContainerInstance",
		"DescribeContainerInstances",
		"ListContainerInstances",
		"UpdateContainerInstancesState",
		"UpdateContainerAgent",
		"CreateTaskSet",
		"DeleteTaskSet",
		"DescribeTaskSets",
		"UpdateTaskSet",
		"UpdateServicePrimaryTaskSet",
		"ExecuteCommand",
		"CreateCapacityProvider",
		"DeleteCapacityProvider",
		"DescribeCapacityProviders",
		"PutClusterCapacityProviders",
		"DeleteAccountSetting",
		"ListAccountSettings",
		"PutAccountSetting",
		"PutAccountSettingDefault",
		"DeleteAttributes",
		"ListAttributes",
		"PutAttributes",
		"UpdateClusterSettings",
		"DescribeServiceDeployments",
		"CreateExpressGatewayService",
		"DeleteExpressGatewayService",
		"DescribeExpressGatewayService",
		"UpdateExpressGatewayService",
		"GetTaskProtection",
		"UpdateTaskProtection",
		"UpdateCluster",
		"UpdateCapacityProvider",
		"ListTaskDefinitionFamilies",
		"StartTask",
		"ListServicesByNamespace",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"ListServiceDeployments",
		"StopServiceDeployment",
		// Daemon operations (stub implementations).
		"CreateDaemon",
		"DeleteDaemon",
		"DeleteDaemonTaskDefinition",
		"DescribeDaemon",
		"DescribeDaemonDeployments",
		"DescribeDaemonRevisions",
		"DescribeDaemonTaskDefinition",
		"ListDaemonDeployments",
		"ListDaemonTaskDefinitions",
		"ListDaemons",
		"RegisterDaemonTaskDefinition",
		"UpdateDaemon",
		// Service revisions.
		"DescribeServiceRevisions",
		// Internal agent endpoint.
		"DiscoverPollEndpoint",
		// State change submissions (internal container agent protocol).
		"SubmitAttachmentStateChanges",
		"SubmitContainerStateChange",
		"SubmitTaskStateChange",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ecs" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ECS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches ECS requests via X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, ecsTargetPrefix)
	}
}

// MatchPriority returns the routing priority for ECS.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the ECS action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, ecsTargetPrefix)

	if action == "" || action == target {
		return unknownActionName
	}

	return action
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		Cluster           string `json:"cluster"`
		Service           string `json:"service"`
		ServiceName       string `json:"serviceName"`
		ClusterName       string `json:"clusterName"`
		Family            string `json:"family"`
		TaskArn           string `json:"task"`
		ContainerInstance string `json:"containerInstance"`
		TaskSet           string `json:"taskSet"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ClusterName != "":
		return req.ClusterName
	case req.Cluster != "":
		return req.Cluster
	case req.ServiceName != "":
		return req.ServiceName
	case req.Service != "":
		return req.Service
	case req.Family != "":
		return req.Family
	case req.TaskArn != "":
		return req.TaskArn
	case req.ContainerInstance != "":
		return req.ContainerInstance
	case req.TaskSet != "":
		return req.TaskSet
	}

	return ""
}

// Handler returns the Echo handler function for ECS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ECS", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCluster":            service.WrapOp(h.handleCreateCluster),
		"ListClusters":             service.WrapOp(h.handleListClusters),
		"DescribeClusters":         service.WrapOp(h.handleDescribeClusters),
		"DeleteCluster":            service.WrapOp(h.handleDeleteCluster),
		"RegisterTaskDefinition":   service.WrapOp(h.handleRegisterTaskDefinition),
		"DescribeTaskDefinition":   service.WrapOp(h.handleDescribeTaskDefinition),
		"DeregisterTaskDefinition": service.WrapOp(h.handleDeregisterTaskDefinition),
		"ListTaskDefinitions":      service.WrapOp(h.handleListTaskDefinitions),
		"CreateService":            service.WrapOp(h.handleCreateService),
		"DescribeServices":         service.WrapOp(h.handleDescribeServices),
		"UpdateService":            service.WrapOp(h.handleUpdateService),
		"DeleteService":            service.WrapOp(h.handleDeleteService),
		"ListServices":             service.WrapOp(h.handleListServices),
		"RunTask":                  service.WrapOp(h.handleRunTask),
		"DescribeTasks":            service.WrapOp(h.handleDescribeTasks),
		"StopTask":                 service.WrapOp(h.handleStopTask),
		"ListTasks":                service.WrapOp(h.handleListTasks),
		// Container instances
		"RegisterContainerInstance":     service.WrapOp(h.handleRegisterContainerInstance),
		"DeregisterContainerInstance":   service.WrapOp(h.handleDeregisterContainerInstance),
		"DescribeContainerInstances":    service.WrapOp(h.handleDescribeContainerInstances),
		"ListContainerInstances":        service.WrapOp(h.handleListContainerInstances),
		"UpdateContainerInstancesState": service.WrapOp(h.handleUpdateContainerInstancesState),
		"UpdateContainerAgent":          service.WrapOp(h.handleUpdateContainerAgent),
		// Task sets
		"CreateTaskSet":               service.WrapOp(h.handleCreateTaskSet),
		"DeleteTaskSet":               service.WrapOp(h.handleDeleteTaskSet),
		"DescribeTaskSets":            service.WrapOp(h.handleDescribeTaskSets),
		"UpdateTaskSet":               service.WrapOp(h.handleUpdateTaskSet),
		"UpdateServicePrimaryTaskSet": service.WrapOp(h.handleUpdateServicePrimaryTaskSet),
		// ECS Exec
		"ExecuteCommand": service.WrapOp(h.handleExecuteCommand),
		// Capacity providers
		"CreateCapacityProvider":      service.WrapOp(h.handleCreateCapacityProvider),
		"DeleteCapacityProvider":      service.WrapOp(h.handleDeleteCapacityProvider),
		"DescribeCapacityProviders":   service.WrapOp(h.handleDescribeCapacityProviders),
		"PutClusterCapacityProviders": service.WrapOp(h.handlePutClusterCapacityProviders),
		// Account settings
		"DeleteAccountSetting":     service.WrapOp(h.handleDeleteAccountSetting),
		"ListAccountSettings":      service.WrapOp(h.handleListAccountSettings),
		"PutAccountSetting":        service.WrapOp(h.handlePutAccountSetting),
		"PutAccountSettingDefault": service.WrapOp(h.handlePutAccountSettingDefault),
		// Attributes
		"DeleteAttributes": service.WrapOp(h.handleDeleteAttributes),
		"ListAttributes":   service.WrapOp(h.handleListAttributes),
		"PutAttributes":    service.WrapOp(h.handlePutAttributes),
		// Cluster settings
		"UpdateClusterSettings": service.WrapOp(h.handleUpdateClusterSettings),
		// Task definitions (batch delete)
		"DeleteTaskDefinitions": service.WrapOp(h.handleDeleteTaskDefinitions),
		// Service deployments
		"DescribeServiceDeployments": service.WrapOp(h.handleDescribeServiceDeployments),
		// Express gateway services
		"CreateExpressGatewayService":   service.WrapOp(h.handleCreateExpressGatewayService),
		"DeleteExpressGatewayService":   service.WrapOp(h.handleDeleteExpressGatewayService),
		"DescribeExpressGatewayService": service.WrapOp(h.handleDescribeExpressGatewayService),
		"UpdateExpressGatewayService":   service.WrapOp(h.handleUpdateExpressGatewayService),
		// Task protection
		"GetTaskProtection":    service.WrapOp(h.handleGetTaskProtection),
		"UpdateTaskProtection": service.WrapOp(h.handleUpdateTaskProtection),
		// Cluster management
		"UpdateCluster": service.WrapOp(h.handleUpdateCluster),
		// Capacity provider management
		"UpdateCapacityProvider": service.WrapOp(h.handleUpdateCapacityProvider),
		// Task definition families
		"ListTaskDefinitionFamilies": service.WrapOp(h.handleListTaskDefinitionFamilies),
		// Task placement
		"StartTask": service.WrapOp(h.handleStartTask),
		// Namespace-scoped service listing
		"ListServicesByNamespace": service.WrapOp(h.handleListServicesByNamespace),
		// Tagging
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		// Service deployments
		"ListServiceDeployments": service.WrapOp(h.handleListServiceDeployments),
		"StopServiceDeployment":  service.WrapOp(h.handleStopServiceDeployment),
		// Daemon stubs
		"CreateDaemon":                 service.WrapOp(h.handleCreateDaemon),
		"DeleteDaemon":                 service.WrapOp(h.handleDeleteDaemon),
		"DeleteDaemonTaskDefinition":   service.WrapOp(h.handleDeleteDaemonTaskDefinition),
		"DescribeDaemon":               service.WrapOp(h.handleDescribeDaemon),
		"DescribeDaemonDeployments":    service.WrapOp(h.handleDescribeDaemonDeployments),
		"DescribeDaemonRevisions":      service.WrapOp(h.handleDescribeDaemonRevisions),
		"DescribeDaemonTaskDefinition": service.WrapOp(h.handleDescribeDaemonTaskDefinition),
		"ListDaemonDeployments":        service.WrapOp(h.handleListDaemonDeployments),
		"ListDaemonTaskDefinitions":    service.WrapOp(h.handleListDaemonTaskDefinitions),
		"ListDaemons":                  service.WrapOp(h.handleListDaemons),
		"RegisterDaemonTaskDefinition": service.WrapOp(h.handleRegisterDaemonTaskDefinition),
		"UpdateDaemon":                 service.WrapOp(h.handleUpdateDaemon),
		// Service revisions
		"DescribeServiceRevisions": service.WrapOp(h.handleDescribeServiceRevisions),
		// Internal agent endpoints
		"DiscoverPollEndpoint":         service.WrapOp(h.handleDiscoverPollEndpoint),
		"SubmitAttachmentStateChanges": service.WrapOp(h.handleSubmitAttachmentStateChanges),
		"SubmitContainerStateChange":   service.WrapOp(h.handleSubmitContainerStateChange),
		"SubmitTaskStateChange":        service.WrapOp(h.handleSubmitTaskStateChange),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// Reset clears the backend state.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		code := errorCode(err)

		return c.JSON(http.StatusBadRequest, map[string]string{keyTypeField: code, keyMessageField: err.Error()})
	case errors.Is(err, awserr.ErrAlreadyExists):
		code := errorCode(err)

		return c.JSON(http.StatusBadRequest, map[string]string{keyTypeField: code, keyMessageField: err.Error()})
	case errors.Is(err, errUnknownAction):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyTypeField: "UnknownOperationException", keyMessageField: err.Error()},
		)
	case errors.Is(err, awserr.ErrInvalidParameter), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		code := errorCode(err)

		return c.JSON(http.StatusBadRequest, map[string]string{keyTypeField: code, keyMessageField: err.Error()})
	default:
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyTypeField: "ServerException", keyMessageField: err.Error()},
		)
	}
}

// errorCode extracts the AWS-style error code from a wrapped error.
// It walks the error chain and returns the first message that is not a sentinel.
func errorCode(err error) string {
	// isSentinel returns true for AWS error sentinel messages that should not be used as error codes.
	isSentinel := func(msg string) bool {
		switch msg {
		case "resource not found", "resource already exists", "invalid parameter", "conflict":
			return true
		}

		return false
	}

	for currentErr := err; currentErr != nil; currentErr = errors.Unwrap(currentErr) {
		msg := currentErr.Error()
		if !isSentinel(msg) {
			return msg
		}
	}

	return "ServerException"
}

// ----- Cluster handlers -----

type createClusterInput struct {
	ClusterName string               `json:"clusterName"`
	Settings    []clusterSettingView `json:"settings,omitempty"`
}

type createClusterOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleCreateCluster(_ context.Context, in *createClusterInput) (*createClusterOutput, error) {
	settings := make([]ClusterSetting, 0, len(in.Settings))
	for _, s := range in.Settings {
		settings = append(settings, ClusterSetting(s))
	}

	cluster, err := h.Backend.CreateCluster(CreateClusterInput{
		ClusterName: in.ClusterName,
		Settings:    settings,
	})
	if err != nil {
		return nil, err
	}

	return &createClusterOutput{Cluster: toClusterView(*cluster)}, nil
}

type listClustersInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listClustersOutput struct {
	NextToken   string   `json:"nextToken,omitempty"`
	ClusterArns []string `json:"clusterArns"`
}

func (h *Handler) handleListClusters(_ context.Context, in *listClustersInput) (*listClustersOutput, error) {
	clusters, err := h.Backend.ListClusters()
	if err != nil {
		return nil, err
	}

	arns := make([]string, 0, len(clusters))
	for _, c := range clusters {
		arns = append(arns, c.ClusterArn)
	}

	sort.Strings(arns)

	arns, nextToken := applyNextTokenSlice(arns, in.NextToken, in.MaxResults)

	return &listClustersOutput{ClusterArns: arns, NextToken: nextToken}, nil
}

type describeClustersInput struct {
	Clusters []string `json:"clusters"`
}

type describeClustersOutput struct {
	Clusters []clusterView `json:"clusters"`
	Failures []failureView `json:"failures"`
}

func (h *Handler) handleDescribeClusters(
	_ context.Context,
	in *describeClustersInput,
) (*describeClustersOutput, error) {
	clusters, failures, err := h.Backend.DescribeClusters(in.Clusters)
	if err != nil {
		return nil, err
	}

	views := make([]clusterView, 0, len(clusters))
	for _, c := range clusters {
		views = append(views, toClusterView(c))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeClustersOutput{Clusters: views, Failures: failViews}, nil
}

type deleteClusterInput struct {
	Cluster string `json:"cluster"`
}

type deleteClusterOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleDeleteCluster(_ context.Context, in *deleteClusterInput) (*deleteClusterOutput, error) {
	cluster, err := h.Backend.DeleteCluster(in.Cluster)
	if err != nil {
		return nil, err
	}

	return &deleteClusterOutput{Cluster: toClusterView(*cluster)}, nil
}

// ----- Task definition handlers -----

type placementConstraintInput struct {
	Type       string `json:"type,omitempty"`
	Expression string `json:"expression,omitempty"`
}

type placementStrategyInput struct {
	Type  string `json:"type,omitempty"`
	Field string `json:"field,omitempty"`
}

type deploymentCircuitBreakerInput struct {
	Enable   bool `json:"enable"`
	Rollback bool `json:"rollback"`
}

type deploymentConfigurationInput struct {
	DeploymentCircuitBreaker *deploymentCircuitBreakerInput `json:"deploymentCircuitBreaker,omitempty"`
	MinimumHealthyPercent    *int                           `json:"minimumHealthyPercent,omitempty"`
	MaximumPercent           *int                           `json:"maximumPercent,omitempty"`
}

type deploymentControllerInput struct {
	Type string `json:"type"`
}

type awsvpcConfigurationInput struct {
	AssignPublicIP string   `json:"assignPublicIp,omitempty"`
	Subnets        []string `json:"subnets"`
	SecurityGroups []string `json:"securityGroups,omitempty"`
}

type networkConfigurationInput struct {
	AwsvpcConfiguration *awsvpcConfigurationInput `json:"awsvpcConfiguration,omitempty"`
}

type registerTaskDefinitionInput struct {
	RuntimePlatform         *RuntimePlatform           `json:"runtimePlatform,omitempty"`
	EphemeralStorage        *EphemeralStorage          `json:"ephemeralStorage,omitempty"`
	Family                  string                     `json:"family"`
	TaskRoleArn             string                     `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn        string                     `json:"executionRoleArn,omitempty"`
	NetworkMode             string                     `json:"networkMode,omitempty"`
	CPU                     string                     `json:"cpu,omitempty"`
	Memory                  string                     `json:"memory,omitempty"`
	PlatformFamily          string                     `json:"platformFamily,omitempty"`
	Tags                    []Tag                      `json:"tags,omitempty"`
	ContainerDefinitions    []ContainerDefinition      `json:"containerDefinitions"`
	Volumes                 []Volume                   `json:"volumes,omitempty"`
	PlacementConstraints    []placementConstraintInput `json:"placementConstraints,omitempty"`
	RequiresCompatibilities []string                   `json:"requiresCompatibilities,omitempty"`
	InferenceAccelerators   []InferenceAccelerator     `json:"inferenceAccelerators,omitempty"`
}

type registerTaskDefinitionOutput struct {
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

func (h *Handler) handleRegisterTaskDefinition(
	_ context.Context,
	in *registerTaskDefinitionInput,
) (*registerTaskDefinitionOutput, error) {
	td, err := h.Backend.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  in.Family,
		TaskRoleArn:             in.TaskRoleArn,
		ExecutionRoleArn:        in.ExecutionRoleArn,
		NetworkMode:             in.NetworkMode,
		CPU:                     in.CPU,
		Memory:                  in.Memory,
		PlatformFamily:          in.PlatformFamily,
		ContainerDefinitions:    in.ContainerDefinitions,
		Volumes:                 in.Volumes,
		PlacementConstraints:    toPlacementConstraints(in.PlacementConstraints),
		RequiresCompatibilities: in.RequiresCompatibilities,
		Tags:                    in.Tags,
		RuntimePlatform:         in.RuntimePlatform,
		EphemeralStorage:        in.EphemeralStorage,
		InferenceAccelerators:   in.InferenceAccelerators,
	})
	if err != nil {
		return nil, err
	}

	return &registerTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td)}, nil
}

type describeTaskDefinitionInput struct {
	TaskDefinition string   `json:"taskDefinition"`
	Include        []string `json:"include,omitempty"`
}

type describeTaskDefinitionOutput struct {
	Tags           []Tag              `json:"tags,omitempty"`
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

// describeTaskIncludeTags is the AWS-defined value for the `include` parameter
// that requests resource tags be returned alongside the TaskDefinition.
const describeTaskIncludeTags = "TAGS"

func (h *Handler) handleDescribeTaskDefinition(
	_ context.Context,
	in *describeTaskDefinitionInput,
) (*describeTaskDefinitionOutput, error) {
	td, err := h.Backend.DescribeTaskDefinition(in.TaskDefinition)
	if err != nil {
		return nil, err
	}

	out := &describeTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td)}

	for _, opt := range in.Include {
		if !strings.EqualFold(opt, describeTaskIncludeTags) {
			continue
		}

		tags, terr := h.Backend.ListTagsForResource(td.TaskDefinitionArn)
		if terr != nil {
			return nil, terr
		}

		out.Tags = tags

		break
	}

	return out, nil
}

type deregisterTaskDefinitionInput struct {
	TaskDefinition string `json:"taskDefinition"`
}

type deregisterTaskDefinitionOutput struct {
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

func (h *Handler) handleDeregisterTaskDefinition(
	_ context.Context,
	in *deregisterTaskDefinitionInput,
) (*deregisterTaskDefinitionOutput, error) {
	td, err := h.Backend.DeregisterTaskDefinition(in.TaskDefinition)
	if err != nil {
		return nil, err
	}

	return &deregisterTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td)}, nil
}

type listTaskDefinitionsInput struct {
	FamilyPrefix string `json:"familyPrefix,omitempty"`
	Status       string `json:"status,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
}

type listTaskDefinitionsOutput struct {
	NextToken          string   `json:"nextToken,omitempty"`
	TaskDefinitionArns []string `json:"taskDefinitionArns"`
}

func (h *Handler) handleListTaskDefinitions(
	_ context.Context,
	in *listTaskDefinitionsInput,
) (*listTaskDefinitionsOutput, error) {
	arns, err := h.Backend.ListTaskDefinitionsFiltered(ListTaskDefinitionsInput{
		FamilyPrefix: in.FamilyPrefix,
		Status:       in.Status,
	})
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	sort.Strings(arns)

	arns, nextToken := applyNextTokenSlice(arns, in.NextToken, in.MaxResults)

	return &listTaskDefinitionsOutput{TaskDefinitionArns: arns, NextToken: nextToken}, nil
}

// ----- Service handlers -----

type serviceConnectClientAliasInput struct {
	DNSName string `json:"dnsName,omitempty"`
	Port    int    `json:"port"`
}

type serviceConnectServiceInput struct {
	PortName      string                           `json:"portName"`
	DiscoveryName string                           `json:"discoveryName,omitempty"`
	ClientAliases []serviceConnectClientAliasInput `json:"clientAliases,omitempty"`
}

type serviceConnectConfigurationInput struct {
	Namespace string                       `json:"namespace,omitempty"`
	Services  []serviceConnectServiceInput `json:"services,omitempty"`
	Enabled   bool                         `json:"enabled"`
}

type loadBalancerInput struct {
	TargetGroupArn   string `json:"targetGroupArn,omitempty"`
	LoadBalancerName string `json:"loadBalancerName,omitempty"`
	ContainerName    string `json:"containerName,omitempty"`
	ContainerPort    int    `json:"containerPort,omitempty"`
}

type serviceRegistryInput struct {
	RegistryArn   string `json:"registryArn,omitempty"`
	ContainerName string `json:"containerName,omitempty"`
	Port          int    `json:"port,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
}

type createServiceInput struct {
	DeploymentConfiguration     *deploymentConfigurationInput     `json:"deploymentConfiguration,omitempty"`
	DeploymentController        *deploymentControllerInput        `json:"deploymentController,omitempty"`
	NetworkConfiguration        *networkConfigurationInput        `json:"networkConfiguration,omitempty"`
	ServiceConnectConfiguration *serviceConnectConfigurationInput `json:"serviceConnectConfiguration,omitempty"`
	ServiceName                 string                            `json:"serviceName"`
	Cluster                     string                            `json:"cluster,omitempty"`
	TaskDefinition              string                            `json:"taskDefinition"`
	LaunchType                  string                            `json:"launchType,omitempty"`
	SchedulingStrategy          string                            `json:"schedulingStrategy,omitempty"`
	PropagateTags               string                            `json:"propagateTags,omitempty"`
	Tags                        []Tag                             `json:"tags,omitempty"`
	LoadBalancers               []loadBalancerInput               `json:"loadBalancers,omitempty"`
	ServiceRegistries           []serviceRegistryInput            `json:"serviceRegistries,omitempty"`
	CapacityProviderStrategy    []cpStrategyItemInput             `json:"capacityProviderStrategy,omitempty"`
	PlacementConstraints        []placementConstraintInput        `json:"placementConstraints,omitempty"`
	PlacementStrategy           []placementStrategyInput          `json:"placementStrategy,omitempty"`
	DesiredCount                int                               `json:"desiredCount"`
	EnableExecuteCommand        bool                              `json:"enableExecuteCommand,omitempty"`
}

type createServiceOutput struct {
	Service serviceView `json:"service"`
}

func (h *Handler) handleCreateService(_ context.Context, in *createServiceInput) (*createServiceOutput, error) {
	svc, err := h.Backend.CreateService(CreateServiceInput{
		ServiceName:                 in.ServiceName,
		Cluster:                     in.Cluster,
		TaskDefinition:              in.TaskDefinition,
		LaunchType:                  in.LaunchType,
		SchedulingStrategy:          in.SchedulingStrategy,
		PropagateTags:               in.PropagateTags,
		Tags:                        in.Tags,
		LoadBalancers:               toLoadBalancers(in.LoadBalancers),
		ServiceRegistries:           toServiceRegistries(in.ServiceRegistries),
		DeploymentConfiguration:     toDeploymentConfiguration(in.DeploymentConfiguration),
		DeploymentController:        toDeploymentController(in.DeploymentController),
		NetworkConfiguration:        toNetworkConfiguration(in.NetworkConfiguration),
		CapacityProviderStrategy:    toCPStrategyItems(in.CapacityProviderStrategy),
		PlacementConstraints:        toPlacementConstraints(in.PlacementConstraints),
		PlacementStrategy:           toPlacementStrategies(in.PlacementStrategy),
		ServiceConnectConfiguration: toServiceConnectConfiguration(in.ServiceConnectConfiguration),
		DesiredCount:                in.DesiredCount,
		EnableExecuteCommand:        in.EnableExecuteCommand,
	})
	if err != nil {
		return nil, err
	}

	return &createServiceOutput{Service: toServiceView(*svc)}, nil
}

type describeServicesInput struct {
	Cluster  string   `json:"cluster,omitempty"`
	Services []string `json:"services"`
}

type describeServicesOutput struct {
	Services []serviceView `json:"services"`
	Failures []failureView `json:"failures"`
}

func (h *Handler) handleDescribeServices(
	_ context.Context,
	in *describeServicesInput,
) (*describeServicesOutput, error) {
	svcs, failures, err := h.Backend.DescribeServices(in.Cluster, in.Services)
	if err != nil {
		return nil, err
	}

	views := make([]serviceView, 0, len(svcs))
	for _, s := range svcs {
		views = append(views, toServiceView(s))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeServicesOutput{Services: views, Failures: failViews}, nil
}

type updateServiceInput struct {
	EnableExecuteCommand        *bool                             `json:"enableExecuteCommand,omitempty"`
	DesiredCount                *int                              `json:"desiredCount,omitempty"`
	DeploymentConfiguration     *deploymentConfigurationInput     `json:"deploymentConfiguration,omitempty"`
	NetworkConfiguration        *networkConfigurationInput        `json:"networkConfiguration,omitempty"`
	ServiceConnectConfiguration *serviceConnectConfigurationInput `json:"serviceConnectConfiguration,omitempty"`
	Cluster                     string                            `json:"cluster,omitempty"`
	Service                     string                            `json:"service"`
	TaskDefinition              string                            `json:"taskDefinition,omitempty"`
	PropagateTags               string                            `json:"propagateTags,omitempty"`
	LoadBalancers               []loadBalancerInput               `json:"loadBalancers,omitempty"`
	CapacityProviderStrategy    []cpStrategyItemInput             `json:"capacityProviderStrategy,omitempty"`
	PlacementConstraints        []placementConstraintInput        `json:"placementConstraints,omitempty"`
	PlacementStrategy           []placementStrategyInput          `json:"placementStrategy,omitempty"`
}

type updateServiceOutput struct {
	Service serviceView `json:"service"`
}

func (h *Handler) handleUpdateService(_ context.Context, in *updateServiceInput) (*updateServiceOutput, error) {
	svc, err := h.Backend.UpdateService(UpdateServiceInput{
		Cluster:                     in.Cluster,
		Service:                     in.Service,
		PropagateTags:               in.PropagateTags,
		LoadBalancers:               toLoadBalancers(in.LoadBalancers),
		NetworkConfiguration:        toNetworkConfiguration(in.NetworkConfiguration),
		TaskDefinition:              in.TaskDefinition,
		DesiredCount:                in.DesiredCount,
		DeploymentConfiguration:     toDeploymentConfiguration(in.DeploymentConfiguration),
		CapacityProviderStrategy:    toCPStrategyItems(in.CapacityProviderStrategy),
		PlacementConstraints:        toPlacementConstraints(in.PlacementConstraints),
		PlacementStrategy:           toPlacementStrategies(in.PlacementStrategy),
		ServiceConnectConfiguration: toServiceConnectConfiguration(in.ServiceConnectConfiguration),
		EnableExecuteCommand:        in.EnableExecuteCommand,
	})
	if err != nil {
		return nil, err
	}

	return &updateServiceOutput{Service: toServiceView(*svc)}, nil
}

type deleteServiceInput struct {
	Cluster string `json:"cluster,omitempty"`
	Service string `json:"service"`
}

type deleteServiceOutput struct {
	Service serviceView `json:"service"`
}

func (h *Handler) handleDeleteService(_ context.Context, in *deleteServiceInput) (*deleteServiceOutput, error) {
	svc, err := h.Backend.DeleteService(in.Cluster, in.Service)
	if err != nil {
		return nil, err
	}

	return &deleteServiceOutput{Service: toServiceView(*svc)}, nil
}

type listServicesInput struct {
	Cluster            string `json:"cluster,omitempty"`
	LaunchType         string `json:"launchType,omitempty"`
	SchedulingStrategy string `json:"schedulingStrategy,omitempty"`
}

type listServicesOutput struct {
	ServiceArns []string `json:"serviceArns"`
}

func (h *Handler) handleListServices(_ context.Context, in *listServicesInput) (*listServicesOutput, error) {
	arns, err := h.Backend.ListServices(in.Cluster, in.LaunchType, in.SchedulingStrategy)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	return &listServicesOutput{ServiceArns: arns}, nil
}

// ----- Task handlers -----

type containerOverrideInput struct {
	CPU         *int              `json:"cpu,omitempty"`
	Memory      *int              `json:"memory,omitempty"`
	Name        string            `json:"name"`
	Command     []string          `json:"command,omitempty"`
	Environment []KeyValuePair    `json:"environment,omitempty"`
	Secrets     []SecretReference `json:"secrets,omitempty"`
}

type taskOverrideInput struct {
	TaskRoleArn        string                   `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn   string                   `json:"executionRoleArn,omitempty"`
	CPU                string                   `json:"cpu,omitempty"`
	Memory             string                   `json:"memory,omitempty"`
	ContainerOverrides []containerOverrideInput `json:"containerOverrides,omitempty"`
}

type runTaskInput struct {
	Overrides            *taskOverrideInput         `json:"overrides,omitempty"`
	NetworkConfiguration *networkConfigurationInput `json:"networkConfiguration,omitempty"`
	Cluster              string                     `json:"cluster,omitempty"`
	TaskDefinition       string                     `json:"taskDefinition"`
	LaunchType           string                     `json:"launchType,omitempty"`
	Group                string                     `json:"group,omitempty"`
	StartedBy            string                     `json:"startedBy,omitempty"`
	PlatformVersion      string                     `json:"platformVersion,omitempty"`
	PropagateTags        string                     `json:"propagateTags,omitempty"`
	Tags                 []Tag                      `json:"tags,omitempty"`
	Count                int                        `json:"count,omitempty"`
	EnableECSManagedTags bool                       `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand bool                       `json:"enableExecuteCommand,omitempty"`
}

type runTaskOutput struct {
	Tasks []taskView `json:"tasks"`
}

func (h *Handler) handleRunTask(_ context.Context, in *runTaskInput) (*runTaskOutput, error) {
	tasks, err := h.Backend.RunTask(RunTaskInput{
		Cluster:              in.Cluster,
		TaskDefinition:       in.TaskDefinition,
		Count:                in.Count,
		LaunchType:           in.LaunchType,
		Group:                in.Group,
		StartedBy:            in.StartedBy,
		PlatformVersion:      in.PlatformVersion,
		PropagateTags:        in.PropagateTags,
		EnableECSManagedTags: in.EnableECSManagedTags,
		Tags:                 in.Tags,
		Overrides:            toTaskOverride(in.Overrides),
		NetworkConfiguration: toNetworkConfiguration(in.NetworkConfiguration),
		EnableExecuteCommand: in.EnableExecuteCommand,
	})
	if err != nil {
		return nil, err
	}

	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}

	return &runTaskOutput{Tasks: views}, nil
}

type describeTasksInput struct {
	Cluster string   `json:"cluster,omitempty"`
	Tasks   []string `json:"tasks"`
}

type describeTasksOutput struct {
	Tasks    []taskView    `json:"tasks"`
	Failures []failureView `json:"failures"`
}

func (h *Handler) handleDescribeTasks(_ context.Context, in *describeTasksInput) (*describeTasksOutput, error) {
	tasks, failures, err := h.Backend.DescribeTasks(in.Cluster, in.Tasks)
	if err != nil {
		return nil, err
	}

	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeTasksOutput{Tasks: views, Failures: failViews}, nil
}

type stopTaskInput struct {
	Cluster string `json:"cluster,omitempty"`
	Task    string `json:"task"`
	Reason  string `json:"reason,omitempty"`
}

type stopTaskOutput struct {
	Task taskView `json:"task"`
}

func (h *Handler) handleStopTask(_ context.Context, in *stopTaskInput) (*stopTaskOutput, error) {
	task, err := h.Backend.StopTask(in.Cluster, in.Task, in.Reason)
	if err != nil {
		return nil, err
	}

	return &stopTaskOutput{Task: toTaskView(*task)}, nil
}

type listTasksInput struct {
	Cluster           string `json:"cluster,omitempty"`
	ContainerInstance string `json:"containerInstance,omitempty"`
	Family            string `json:"family,omitempty"`
	ServiceName       string `json:"serviceName,omitempty"`
	DesiredStatus     string `json:"desiredStatus,omitempty"`
	LaunchType        string `json:"launchType,omitempty"`
	StartedBy         string `json:"startedBy,omitempty"`
	NextToken         string `json:"nextToken,omitempty"`
	MaxResults        int    `json:"maxResults,omitempty"`
}

type listTasksOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	TaskArns  []string `json:"taskArns"`
}

func (h *Handler) handleListTasks(_ context.Context, in *listTasksInput) (*listTasksOutput, error) {
	arns, err := h.Backend.ListTasksFiltered(ListTasksInput{
		Cluster:           in.Cluster,
		ContainerInstance: in.ContainerInstance,
		Family:            in.Family,
		ServiceName:       in.ServiceName,
		DesiredStatus:     in.DesiredStatus,
		LaunchType:        in.LaunchType,
		StartedBy:         in.StartedBy,
	})
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	sort.Strings(arns)

	arns, nextToken := applyNextTokenSlice(arns, in.NextToken, in.MaxResults)

	return &listTasksOutput{TaskArns: arns, NextToken: nextToken}, nil
}

// ----- View types (JSON serialization) -----

type clusterView struct {
	ClusterArn                        string                `json:"clusterArn"`
	ClusterName                       string                `json:"clusterName"`
	Status                            string                `json:"status"`
	DefaultCapacityProviderStrategy   []cpStrategyItemInput `json:"defaultCapacityProviderStrategy"`
	Settings                          []clusterSettingView  `json:"settings,omitempty"`
	CapacityProviders                 []string              `json:"capacityProviders"`
	CreatedAt                         float64               `json:"createdAt"`
	ActiveServicesCount               int                   `json:"activeServicesCount"`
	PendingTasksCount                 int                   `json:"pendingTasksCount"`
	RegisteredContainerInstancesCount int                   `json:"registeredContainerInstancesCount"`
	RunningTasksCount                 int                   `json:"runningTasksCount"`
}

func toClusterView(c Cluster) clusterView {
	v := clusterView{
		ClusterArn:                        c.ClusterArn,
		ClusterName:                       c.ClusterName,
		Status:                            c.Status,
		CreatedAt:                         float64(c.CreatedAt.Unix()),
		ActiveServicesCount:               c.ActiveServicesCount,
		PendingTasksCount:                 c.PendingTasksCount,
		RegisteredContainerInstancesCount: c.RegisteredContainerInstancesCount,
		RunningTasksCount:                 c.RunningTasksCount,
		DefaultCapacityProviderStrategy:   []cpStrategyItemInput{},
	}

	if c.CapacityProviders != nil {
		v.CapacityProviders = c.CapacityProviders
	} else {
		v.CapacityProviders = []string{}
	}

	for _, item := range c.DefaultCapacityProviderStrategy {
		v.DefaultCapacityProviderStrategy = append(v.DefaultCapacityProviderStrategy,
			cpStrategyItemInput(item))
	}

	for _, s := range c.Settings {
		v.Settings = append(v.Settings, clusterSettingView(s))
	}

	return v
}

type placementConstraintView struct {
	Type       string `json:"type,omitempty"`
	Expression string `json:"expression,omitempty"`
}

type placementStrategyView struct {
	Type  string `json:"type,omitempty"`
	Field string `json:"field,omitempty"`
}

type deploymentCircuitBreakerView struct {
	Enable   bool `json:"enable"`
	Rollback bool `json:"rollback"`
}

type deploymentConfigurationView struct {
	DeploymentCircuitBreaker *deploymentCircuitBreakerView `json:"deploymentCircuitBreaker,omitempty"`
	MinimumHealthyPercent    *int                          `json:"minimumHealthyPercent,omitempty"`
	MaximumPercent           *int                          `json:"maximumPercent,omitempty"`
}

type deploymentControllerView struct {
	Type string `json:"type"`
}

type awsvpcConfigurationView struct {
	AssignPublicIP string   `json:"assignPublicIp,omitempty"`
	Subnets        []string `json:"subnets"`
	SecurityGroups []string `json:"securityGroups,omitempty"`
}

type networkConfigurationView struct {
	AwsvpcConfiguration *awsvpcConfigurationView `json:"awsvpcConfiguration,omitempty"`
}

type loadBalancerView struct {
	TargetGroupArn   string `json:"targetGroupArn,omitempty"`
	LoadBalancerName string `json:"loadBalancerName,omitempty"`
	ContainerName    string `json:"containerName,omitempty"`
	ContainerPort    int    `json:"containerPort,omitempty"`
}

type serviceRegistryView struct {
	RegistryArn   string `json:"registryArn,omitempty"`
	ContainerName string `json:"containerName,omitempty"`
	Port          int    `json:"port,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
}

type taskDefinitionView struct {
	RuntimePlatform         *RuntimePlatform          `json:"runtimePlatform,omitempty"`
	EphemeralStorage        *EphemeralStorage         `json:"ephemeralStorage,omitempty"`
	TaskDefinitionArn       string                    `json:"taskDefinitionArn"`
	Family                  string                    `json:"family"`
	TaskRoleArn             string                    `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn        string                    `json:"executionRoleArn,omitempty"`
	NetworkMode             string                    `json:"networkMode,omitempty"`
	Status                  string                    `json:"status"`
	CPU                     string                    `json:"cpu,omitempty"`
	Memory                  string                    `json:"memory,omitempty"`
	PlatformFamily          string                    `json:"platformFamily,omitempty"`
	ContainerDefinitions    []ContainerDefinition     `json:"containerDefinitions"`
	Volumes                 []Volume                  `json:"volumes"`
	PlacementConstraints    []placementConstraintView `json:"placementConstraints,omitempty"`
	RequiresCompatibilities []string                  `json:"requiresCompatibilities,omitempty"`
	InferenceAccelerators   []InferenceAccelerator    `json:"inferenceAccelerators,omitempty"`
	RegisteredAt            float64                   `json:"registeredAt"`
	Revision                int                       `json:"revision"`
}

func toTaskDefinitionView(td TaskDefinition) taskDefinitionView {
	volumes := td.Volumes
	if volumes == nil {
		volumes = []Volume{}
	}

	v := taskDefinitionView{
		TaskDefinitionArn:       td.TaskDefinitionArn,
		Family:                  td.Family,
		TaskRoleArn:             td.TaskRoleArn,
		ExecutionRoleArn:        td.ExecutionRoleArn,
		NetworkMode:             td.NetworkMode,
		Status:                  td.Status,
		CPU:                     td.CPU,
		Memory:                  td.Memory,
		PlatformFamily:          td.PlatformFamily,
		ContainerDefinitions:    td.ContainerDefinitions,
		Volumes:                 volumes,
		RequiresCompatibilities: td.RequiresCompatibilities,
		RegisteredAt:            float64(td.RegisteredAt.Unix()),
		Revision:                td.Revision,
		RuntimePlatform:         td.RuntimePlatform,
		EphemeralStorage:        td.EphemeralStorage,
		InferenceAccelerators:   td.InferenceAccelerators,
	}

	for _, c := range td.PlacementConstraints {
		v.PlacementConstraints = append(v.PlacementConstraints, placementConstraintView(c))
	}

	return v
}

func toDeploymentConfigurationView(dc *DeploymentConfiguration) *deploymentConfigurationView {
	if dc == nil {
		return nil
	}

	v := &deploymentConfigurationView{
		MinimumHealthyPercent: dc.MinimumHealthyPercent,
		MaximumPercent:        dc.MaximumPercent,
	}

	if dc.DeploymentCircuitBreaker != nil {
		v.DeploymentCircuitBreaker = &deploymentCircuitBreakerView{
			Enable:   dc.DeploymentCircuitBreaker.Enable,
			Rollback: dc.DeploymentCircuitBreaker.Rollback,
		}
	}

	return v
}

type serviceConnectClientAliasView struct {
	DNSName string `json:"dnsName,omitempty"`
	Port    int    `json:"port"`
}

type serviceConnectServiceView struct {
	PortName      string                          `json:"portName"`
	DiscoveryName string                          `json:"discoveryName,omitempty"`
	ClientAliases []serviceConnectClientAliasView `json:"clientAliases,omitempty"`
}

type serviceConnectConfigurationView struct {
	Namespace string                      `json:"namespace,omitempty"`
	Services  []serviceConnectServiceView `json:"services,omitempty"`
	Enabled   bool                        `json:"enabled"`
}

type serviceView struct {
	ServiceConnectConfiguration *serviceConnectConfigurationView `json:"serviceConnectConfiguration,omitempty"`
	DeploymentConfiguration     *deploymentConfigurationView     `json:"deploymentConfiguration,omitempty"`
	DeploymentController        *deploymentControllerView        `json:"deploymentController,omitempty"`
	NetworkConfiguration        *networkConfigurationView        `json:"networkConfiguration,omitempty"`
	ClusterArn                  string                           `json:"clusterArn"`
	TaskDefinition              string                           `json:"taskDefinition"`
	Status                      string                           `json:"status"`
	LaunchType                  string                           `json:"launchType,omitempty"`
	SchedulingStrategy          string                           `json:"schedulingStrategy,omitempty"`
	PropagateTags               string                           `json:"propagateTags,omitempty"`
	ServiceArn                  string                           `json:"serviceArn"`
	ServiceName                 string                           `json:"serviceName"`
	LoadBalancers               []loadBalancerView               `json:"loadBalancers"`
	ServiceRegistries           []serviceRegistryView            `json:"serviceRegistries"`
	CapacityProviderStrategy    []cpStrategyItemInput            `json:"capacityProviderStrategy,omitempty"`
	PlacementConstraints        []placementConstraintView        `json:"placementConstraints,omitempty"`
	PlacementStrategy           []placementStrategyView          `json:"placementStrategy,omitempty"`
	Deployments                 []deploymentView                 `json:"deployments,omitempty"`
	Tags                        []Tag                            `json:"tags,omitempty"`
	CreatedAt                   float64                          `json:"createdAt"`
	DesiredCount                int                              `json:"desiredCount"`
	PendingCount                int                              `json:"pendingCount"`
	RunningCount                int                              `json:"runningCount"`
	EnableExecuteCommand        bool                             `json:"enableExecuteCommand,omitempty"`
}

func toServiceView(s Service) serviceView {
	v := serviceView{
		ServiceArn:                  s.ServiceArn,
		ServiceName:                 s.ServiceName,
		ClusterArn:                  s.ClusterArn,
		TaskDefinition:              s.TaskDefinition,
		Status:                      s.Status,
		LaunchType:                  s.LaunchType,
		SchedulingStrategy:          s.SchedulingStrategy,
		PropagateTags:               s.PropagateTags,
		CreatedAt:                   float64(s.CreatedAt.Unix()),
		Tags:                        s.Tags,
		DeploymentConfiguration:     toDeploymentConfigurationView(s.DeploymentConfiguration),
		ServiceConnectConfiguration: toServiceConnectConfigurationView(s.ServiceConnectConfiguration),
		NetworkConfiguration:        toNetworkConfigurationView(s.NetworkConfiguration),
		DesiredCount:                s.DesiredCount,
		PendingCount:                s.PendingCount,
		RunningCount:                s.RunningCount,
		EnableExecuteCommand:        s.EnableExecuteCommand,
	}

	if s.DeploymentController != nil {
		v.DeploymentController = &deploymentControllerView{Type: s.DeploymentController.Type}
	}

	v.LoadBalancers = []loadBalancerView{}
	for _, lb := range s.LoadBalancers {
		v.LoadBalancers = append(v.LoadBalancers, loadBalancerView(lb))
	}

	v.ServiceRegistries = []serviceRegistryView{}
	for _, sr := range s.ServiceRegistries {
		v.ServiceRegistries = append(v.ServiceRegistries, serviceRegistryView(sr))
	}

	for _, item := range s.CapacityProviderStrategy {
		v.CapacityProviderStrategy = append(v.CapacityProviderStrategy, cpStrategyItemInput(item))
	}

	for _, c := range s.PlacementConstraints {
		v.PlacementConstraints = append(v.PlacementConstraints, placementConstraintView(c))
	}

	for _, ps := range s.PlacementStrategy {
		v.PlacementStrategy = append(v.PlacementStrategy, placementStrategyView(ps))
	}

	for _, d := range s.Deployments {
		v.Deployments = append(v.Deployments, toDeploymentView(d))
	}

	return v
}

// toDeploymentView maps a backend Deployment onto the wire view. ServiceRevisionArn
// is tracked internally (for DescribeServiceRevisions) but is not part of the real
// AWS Deployment wire shape embedded in DescribeServices/DescribeService responses,
// so it is intentionally omitted here.
func toDeploymentView(d Deployment) deploymentView {
	return deploymentView{
		CreatedAt:          d.CreatedAt,
		UpdatedAt:          d.UpdatedAt,
		ID:                 d.ID,
		Status:             d.Status,
		TaskDefinition:     d.TaskDefinition,
		LaunchType:         d.LaunchType,
		PlatformVersion:    d.PlatformVersion,
		RolloutState:       d.RolloutState,
		RolloutStateReason: d.RolloutStateReason,
		DesiredCount:       d.DesiredCount,
		PendingCount:       d.PendingCount,
		RunningCount:       d.RunningCount,
		FailedTasks:        d.FailedTasks,
	}
}

type taskAttachmentDetailView struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type taskAttachmentView struct {
	ID      string                     `json:"id"`
	Type    string                     `json:"type"`
	Status  string                     `json:"status"`
	Details []taskAttachmentDetailView `json:"details,omitempty"`
}

type containerOverrideView struct {
	CPU         *int              `json:"cpu,omitempty"`
	Memory      *int              `json:"memory,omitempty"`
	Name        string            `json:"name"`
	Command     []string          `json:"command,omitempty"`
	Environment []KeyValuePair    `json:"environment,omitempty"`
	Secrets     []SecretReference `json:"secrets,omitempty"`
}

type taskOverrideView struct {
	TaskRoleArn        string                  `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn   string                  `json:"executionRoleArn,omitempty"`
	CPU                string                  `json:"cpu,omitempty"`
	Memory             string                  `json:"memory,omitempty"`
	ContainerOverrides []containerOverrideView `json:"containerOverrides,omitempty"`
}

// containerNetworkInterfaceView is the handler view of a container's network interface.
type containerNetworkInterfaceView struct {
	AttachmentID       string `json:"attachmentId,omitempty"`
	PrivateIpv4Address string `json:"privateIpv4Address,omitempty"`
	Ipv6Address        string `json:"ipv6Address,omitempty"`
}

// containerNetworkBindingView is the handler view of a container's port binding.
type containerNetworkBindingView struct {
	BindIP        string `json:"bindIP,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
	HostPort      int    `json:"hostPort,omitempty"`
}

// containerView is the handler view of a runtime container within a task.
type containerView struct {
	ContainerArn      string                          `json:"containerArn,omitempty"`
	TaskArn           string                          `json:"taskArn,omitempty"`
	Name              string                          `json:"name"`
	Image             string                          `json:"image,omitempty"`
	ImageDigest       string                          `json:"imageDigest,omitempty"`
	RuntimeID         string                          `json:"runtimeId,omitempty"`
	LastStatus        string                          `json:"lastStatus"`
	HealthStatus      string                          `json:"healthStatus,omitempty"`
	CPU               string                          `json:"cpu,omitempty"`
	Memory            string                          `json:"memory,omitempty"`
	MemoryReservation string                          `json:"memoryReservation,omitempty"`
	Reason            string                          `json:"reason,omitempty"`
	ExitCode          *int                            `json:"exitCode,omitempty"`
	NetworkInterfaces []containerNetworkInterfaceView `json:"networkInterfaces,omitempty"`
	NetworkBindings   []containerNetworkBindingView   `json:"networkBindings,omitempty"`
}

// deploymentView is the handler view of a service deployment record.
type deploymentView struct {
	CreatedAt          *float64 `json:"createdAt,omitempty"`
	UpdatedAt          *float64 `json:"updatedAt,omitempty"`
	ID                 string   `json:"id"`
	Status             string   `json:"status"`
	TaskDefinition     string   `json:"taskDefinition"`
	LaunchType         string   `json:"launchType,omitempty"`
	PlatformVersion    string   `json:"platformVersion,omitempty"`
	RolloutState       string   `json:"rolloutState,omitempty"`
	RolloutStateReason string   `json:"rolloutStateReason,omitempty"`
	DesiredCount       int      `json:"desiredCount"`
	PendingCount       int      `json:"pendingCount"`
	RunningCount       int      `json:"runningCount"`
	FailedTasks        int      `json:"failedTasks"`
}

type taskView struct {
	Overrides            *taskOverrideView         `json:"overrides,omitempty"`
	NetworkConfiguration *networkConfigurationView `json:"networkConfiguration,omitempty"`
	TaskArn              string                    `json:"taskArn"`
	ClusterArn           string                    `json:"clusterArn"`
	TaskDefinitionArn    string                    `json:"taskDefinitionArn"`
	LastStatus           string                    `json:"lastStatus"`
	DesiredStatus        string                    `json:"desiredStatus"`
	Connectivity         string                    `json:"connectivity,omitempty"`
	StoppedReason        string                    `json:"stoppedReason,omitempty"`
	Group                string                    `json:"group,omitempty"`
	LaunchType           string                    `json:"launchType,omitempty"`
	ContainerInstanceArn string                    `json:"containerInstanceArn,omitempty"`
	StartedBy            string                    `json:"startedBy,omitempty"`
	PlatformVersion      string                    `json:"platformVersion,omitempty"`
	PlatformFamily       string                    `json:"platformFamily,omitempty"`
	RuntimeID            string                    `json:"runtimeId,omitempty"`
	PropagateTags        string                    `json:"propagateTags,omitempty"`
	Attachments          []taskAttachmentView      `json:"attachments,omitempty"`
	Containers           []containerView           `json:"containers,omitempty"`
	Tags                 []Tag                     `json:"tags,omitempty"`
	StartedAt            float64                   `json:"startedAt,omitempty"`
	StoppedAt            float64                   `json:"stoppedAt,omitempty"`
	ConnectivityAt       float64                   `json:"connectivityAt,omitempty"`
	EnableExecuteCommand bool                      `json:"enableExecuteCommand,omitempty"`
}

func toTaskView(t Task) taskView {
	v := taskView{
		TaskArn:              t.TaskArn,
		ClusterArn:           t.ClusterArn,
		TaskDefinitionArn:    t.TaskDefinitionArn,
		LastStatus:           t.LastStatus,
		DesiredStatus:        t.DesiredStatus,
		Connectivity:         t.Connectivity,
		StoppedReason:        t.StoppedReason,
		Group:                t.Group,
		LaunchType:           t.LaunchType,
		ContainerInstanceArn: t.ContainerInstanceArn,
		StartedBy:            t.StartedBy,
		PlatformVersion:      t.PlatformVersion,
		PlatformFamily:       t.PlatformFamily,
		RuntimeID:            t.RuntimeID,
		PropagateTags:        t.PropagateTags,
		Tags:                 t.Tags,
		Overrides:            toTaskOverrideView(t.Overrides),
		NetworkConfiguration: toNetworkConfigurationView(t.NetworkConfiguration),
		EnableExecuteCommand: t.EnableExecuteCommand,
	}

	for _, a := range t.Attachments {
		details := make([]taskAttachmentDetailView, 0, len(a.Details))
		for _, d := range a.Details {
			details = append(details, taskAttachmentDetailView(d))
		}

		v.Attachments = append(v.Attachments, taskAttachmentView{
			ID:      a.ID,
			Type:    a.Type,
			Status:  a.Status,
			Details: details,
		})
	}

	for _, c := range t.Containers {
		cv := containerView{
			ContainerArn:      c.ContainerArn,
			TaskArn:           c.TaskArn,
			Name:              c.Name,
			Image:             c.Image,
			ImageDigest:       c.ImageDigest,
			RuntimeID:         c.RuntimeID,
			LastStatus:        c.LastStatus,
			HealthStatus:      c.HealthStatus,
			CPU:               c.CPU,
			Memory:            c.Memory,
			MemoryReservation: c.MemoryReservation,
			Reason:            c.Reason,
			ExitCode:          c.ExitCode,
		}

		for _, ni := range c.NetworkInterfaces {
			cv.NetworkInterfaces = append(cv.NetworkInterfaces, containerNetworkInterfaceView(ni))
		}

		for _, nb := range c.NetworkBindings {
			cv.NetworkBindings = append(cv.NetworkBindings, containerNetworkBindingView(nb))
		}

		v.Containers = append(v.Containers, cv)
	}

	if t.StartedAt != nil {
		v.StartedAt = float64(t.StartedAt.Unix())
	}

	if t.StoppedAt != nil {
		v.StoppedAt = float64(t.StoppedAt.Unix())
	}

	if t.ConnectivityAt != nil {
		v.ConnectivityAt = float64(t.ConnectivityAt.Unix())
	}

	return v
}

// toDeploymentConfiguration converts handler input to backend type.
func toDeploymentConfiguration(in *deploymentConfigurationInput) *DeploymentConfiguration {
	if in == nil {
		return nil
	}

	dc := &DeploymentConfiguration{
		MinimumHealthyPercent: in.MinimumHealthyPercent,
		MaximumPercent:        in.MaximumPercent,
	}

	if in.DeploymentCircuitBreaker != nil {
		dc.DeploymentCircuitBreaker = &DeploymentCircuitBreaker{
			Enable:   in.DeploymentCircuitBreaker.Enable,
			Rollback: in.DeploymentCircuitBreaker.Rollback,
		}
	}

	return dc
}

// toDeploymentController converts handler input to backend type.
func toDeploymentController(in *deploymentControllerInput) *DeploymentController {
	if in == nil {
		return nil
	}

	return &DeploymentController{Type: in.Type}
}

// toNetworkConfiguration converts handler input to backend type.
func toNetworkConfiguration(in *networkConfigurationInput) *NetworkConfiguration {
	if in == nil {
		return nil
	}

	nc := &NetworkConfiguration{}

	if in.AwsvpcConfiguration != nil {
		nc.AwsvpcConfiguration = &AwsvpcConfiguration{
			Subnets:        in.AwsvpcConfiguration.Subnets,
			SecurityGroups: in.AwsvpcConfiguration.SecurityGroups,
			AssignPublicIP: in.AwsvpcConfiguration.AssignPublicIP,
		}
	}

	return nc
}

// toNetworkConfigurationView converts backend type to handler view.
func toNetworkConfigurationView(nc *NetworkConfiguration) *networkConfigurationView {
	if nc == nil {
		return nil
	}

	v := &networkConfigurationView{}

	if nc.AwsvpcConfiguration != nil {
		v.AwsvpcConfiguration = &awsvpcConfigurationView{
			Subnets:        nc.AwsvpcConfiguration.Subnets,
			SecurityGroups: nc.AwsvpcConfiguration.SecurityGroups,
			AssignPublicIP: nc.AwsvpcConfiguration.AssignPublicIP,
		}
	}

	return v
}

// toLoadBalancers converts handler input to backend type.
func toLoadBalancers(in []loadBalancerInput) []LoadBalancer {
	if len(in) == 0 {
		return nil
	}

	out := make([]LoadBalancer, len(in))
	for i, lb := range in {
		out[i] = LoadBalancer(lb)
	}

	return out
}

// toServiceRegistries converts handler input to backend type.
func toServiceRegistries(in []serviceRegistryInput) []ServiceRegistry {
	if len(in) == 0 {
		return nil
	}

	out := make([]ServiceRegistry, len(in))
	for i, sr := range in {
		out[i] = ServiceRegistry(sr)
	}

	return out
}

// toCPStrategyItems converts handler cpStrategyItemInput to backend CapacityProviderStrategyItem.
func toCPStrategyItems(in []cpStrategyItemInput) []CapacityProviderStrategyItem {
	if len(in) == 0 {
		return nil
	}

	out := make([]CapacityProviderStrategyItem, len(in))
	for i, item := range in {
		out[i] = CapacityProviderStrategyItem(item)
	}

	return out
}

// toPlacementConstraints converts handler input to backend type.
func toPlacementConstraints(in []placementConstraintInput) []PlacementConstraint {
	if len(in) == 0 {
		return nil
	}

	out := make([]PlacementConstraint, len(in))
	for i, c := range in {
		out[i] = PlacementConstraint(c)
	}

	return out
}

// toPlacementStrategies converts handler input to backend type.
func toPlacementStrategies(in []placementStrategyInput) []PlacementStrategy {
	if len(in) == 0 {
		return nil
	}

	out := make([]PlacementStrategy, len(in))
	for i, s := range in {
		out[i] = PlacementStrategy(s)
	}

	return out
}

// toServiceConnectConfiguration converts handler input to backend type.
func toServiceConnectConfiguration(in *serviceConnectConfigurationInput) *ServiceConnectConfiguration {
	if in == nil {
		return nil
	}

	out := &ServiceConnectConfiguration{
		Namespace: in.Namespace,
		Enabled:   in.Enabled,
	}

	for _, s := range in.Services {
		svc := ServiceConnectService{
			PortName:      s.PortName,
			DiscoveryName: s.DiscoveryName,
		}

		for _, a := range s.ClientAliases {
			svc.ClientAliases = append(svc.ClientAliases, ServiceConnectClientAlias(a))
		}

		out.Services = append(out.Services, svc)
	}

	return out
}

// toServiceConnectConfigurationView converts backend type to view.
func toServiceConnectConfigurationView(in *ServiceConnectConfiguration) *serviceConnectConfigurationView {
	if in == nil {
		return nil
	}

	out := &serviceConnectConfigurationView{
		Namespace: in.Namespace,
		Enabled:   in.Enabled,
	}

	for _, s := range in.Services {
		svc := serviceConnectServiceView{
			PortName:      s.PortName,
			DiscoveryName: s.DiscoveryName,
		}

		for _, a := range s.ClientAliases {
			svc.ClientAliases = append(svc.ClientAliases, serviceConnectClientAliasView(a))
		}

		out.Services = append(out.Services, svc)
	}

	return out
}

// toTaskOverride converts handler input to backend type.
func toTaskOverride(in *taskOverrideInput) *TaskOverride {
	if in == nil {
		return nil
	}

	out := &TaskOverride{
		TaskRoleArn:      in.TaskRoleArn,
		ExecutionRoleArn: in.ExecutionRoleArn,
		CPU:              in.CPU,
		Memory:           in.Memory,
	}

	for _, co := range in.ContainerOverrides {
		out.ContainerOverrides = append(out.ContainerOverrides, ContainerOverride(co))
	}

	return out
}

// toTaskOverrideView converts backend type to view.
func toTaskOverrideView(in *TaskOverride) *taskOverrideView {
	if in == nil {
		return nil
	}

	out := &taskOverrideView{
		TaskRoleArn:      in.TaskRoleArn,
		ExecutionRoleArn: in.ExecutionRoleArn,
		CPU:              in.CPU,
		Memory:           in.Memory,
	}

	for _, co := range in.ContainerOverrides {
		out.ContainerOverrides = append(out.ContainerOverrides, containerOverrideView(co))
	}

	return out
}

// Purge implements service.Purgeable by removing all ECS resources older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}
}

const defaultECSMaxResults = 100

// applyNextTokenSlice applies nextToken-based pagination to a string slice
// using the shared pkgs/page opaque token format.
func applyNextTokenSlice(items []string, nextToken string, maxResults int) ([]string, string) {
	p := page.New(items, nextToken, maxResults, defaultECSMaxResults)

	return p.Data, p.Next
}
