package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	ecsTargetPrefix   = "AmazonEC2ContainerServiceV20141113."
	unknownActionName = "Unknown"
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

		return c.JSON(http.StatusBadRequest, map[string]string{"__type": code, "message": err.Error()})
	case errors.Is(err, awserr.ErrAlreadyExists):
		code := errorCode(err)

		return c.JSON(http.StatusBadRequest, map[string]string{"__type": code, "message": err.Error()})
	case errors.Is(err, errUnknownAction):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{"__type": "UnknownOperationException", "message": err.Error()},
		)
	case errors.Is(err, awserr.ErrInvalidParameter), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		code := errorCode(err)

		return c.JSON(http.StatusBadRequest, map[string]string{"__type": code, "message": err.Error()})
	default:
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{"__type": "ServerException", "message": err.Error()},
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
	ClusterName string `json:"clusterName"`
}

type createClusterOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleCreateCluster(_ context.Context, in *createClusterInput) (*createClusterOutput, error) {
	cluster, err := h.Backend.CreateCluster(CreateClusterInput{ClusterName: in.ClusterName})
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
}

func (h *Handler) handleDescribeClusters(
	_ context.Context,
	in *describeClustersInput,
) (*describeClustersOutput, error) {
	clusters, err := h.Backend.DescribeClusters(in.Clusters)
	if err != nil {
		return nil, err
	}

	views := make([]clusterView, 0, len(clusters))
	for _, c := range clusters {
		views = append(views, toClusterView(c))
	}

	return &describeClustersOutput{Clusters: views}, nil
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

type registerTaskDefinitionInput struct {
	Family               string                `json:"family"`
	NetworkMode          string                `json:"networkMode,omitempty"`
	CPU                  string                `json:"cpu,omitempty"`
	Memory               string                `json:"memory,omitempty"`
	PlatformFamily       string                `json:"platformFamily,omitempty"`
	ContainerDefinitions []ContainerDefinition `json:"containerDefinitions"`
}

type registerTaskDefinitionOutput struct {
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

func (h *Handler) handleRegisterTaskDefinition(
	_ context.Context,
	in *registerTaskDefinitionInput,
) (*registerTaskDefinitionOutput, error) {
	td, err := h.Backend.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               in.Family,
		NetworkMode:          in.NetworkMode,
		CPU:                  in.CPU,
		Memory:               in.Memory,
		PlatformFamily:       in.PlatformFamily,
		ContainerDefinitions: in.ContainerDefinitions,
	})
	if err != nil {
		return nil, err
	}

	return &registerTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td)}, nil
}

type describeTaskDefinitionInput struct {
	TaskDefinition string `json:"taskDefinition"`
}

type describeTaskDefinitionOutput struct {
	TaskDefinition taskDefinitionView `json:"taskDefinition"`
}

func (h *Handler) handleDescribeTaskDefinition(
	_ context.Context,
	in *describeTaskDefinitionInput,
) (*describeTaskDefinitionOutput, error) {
	td, err := h.Backend.DescribeTaskDefinition(in.TaskDefinition)
	if err != nil {
		return nil, err
	}

	return &describeTaskDefinitionOutput{TaskDefinition: toTaskDefinitionView(*td)}, nil
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
	arns, err := h.Backend.ListTaskDefinitions(in.FamilyPrefix)
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

type createServiceInput struct {
	ServiceName        string `json:"serviceName"`
	Cluster            string `json:"cluster,omitempty"`
	TaskDefinition     string `json:"taskDefinition"`
	LaunchType         string `json:"launchType,omitempty"`
	SchedulingStrategy string `json:"schedulingStrategy,omitempty"`
	Tags               []Tag  `json:"tags,omitempty"`
	DesiredCount       int    `json:"desiredCount"`
}

type createServiceOutput struct {
	Service serviceView `json:"service"`
}

func (h *Handler) handleCreateService(_ context.Context, in *createServiceInput) (*createServiceOutput, error) {
	svc, err := h.Backend.CreateService(CreateServiceInput{
		ServiceName:        in.ServiceName,
		Cluster:            in.Cluster,
		TaskDefinition:     in.TaskDefinition,
		LaunchType:         in.LaunchType,
		SchedulingStrategy: in.SchedulingStrategy,
		Tags:               in.Tags,
		DesiredCount:       in.DesiredCount,
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
}

func (h *Handler) handleDescribeServices(
	_ context.Context,
	in *describeServicesInput,
) (*describeServicesOutput, error) {
	svcs, err := h.Backend.DescribeServices(in.Cluster, in.Services)
	if err != nil {
		return nil, err
	}

	views := make([]serviceView, 0, len(svcs))
	for _, s := range svcs {
		views = append(views, toServiceView(s))
	}

	return &describeServicesOutput{Services: views}, nil
}

type updateServiceInput struct {
	DesiredCount   *int   `json:"desiredCount,omitempty"`
	Cluster        string `json:"cluster,omitempty"`
	Service        string `json:"service"`
	TaskDefinition string `json:"taskDefinition,omitempty"`
}

type updateServiceOutput struct {
	Service serviceView `json:"service"`
}

func (h *Handler) handleUpdateService(_ context.Context, in *updateServiceInput) (*updateServiceOutput, error) {
	svc, err := h.Backend.UpdateService(UpdateServiceInput{
		Cluster:        in.Cluster,
		Service:        in.Service,
		TaskDefinition: in.TaskDefinition,
		DesiredCount:   in.DesiredCount,
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

type runTaskInput struct {
	Cluster              string `json:"cluster,omitempty"`
	TaskDefinition       string `json:"taskDefinition"`
	LaunchType           string `json:"launchType,omitempty"`
	Group                string `json:"group,omitempty"`
	StartedBy            string `json:"startedBy,omitempty"`
	PlatformVersion      string `json:"platformVersion,omitempty"`
	EnableECSManagedTags bool   `json:"enableECSManagedTags,omitempty"`
	Count                int    `json:"count,omitempty"`
	Tags                 []Tag  `json:"tags,omitempty"`
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
		EnableECSManagedTags: in.EnableECSManagedTags,
		Tags:                 in.Tags,
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
	Tasks []taskView `json:"tasks"`
}

func (h *Handler) handleDescribeTasks(_ context.Context, in *describeTasksInput) (*describeTasksOutput, error) {
	tasks, err := h.Backend.DescribeTasks(in.Cluster, in.Tasks)
	if err != nil {
		return nil, err
	}

	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}

	return &describeTasksOutput{Tasks: views}, nil
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
	DefaultCapacityProviderStrategy   []cpStrategyItemInput `json:"defaultCapacityProviderStrategy,omitempty"`
	Settings                          []clusterSettingView  `json:"settings,omitempty"`
	CapacityProviders                 []string              `json:"capacityProviders,omitempty"`
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
		CapacityProviders:                 c.CapacityProviders,
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

type taskDefinitionView struct {
	TaskDefinitionArn    string                `json:"taskDefinitionArn"`
	Family               string                `json:"family"`
	NetworkMode          string                `json:"networkMode,omitempty"`
	Status               string                `json:"status"`
	CPU                  string                `json:"cpu,omitempty"`
	Memory               string                `json:"memory,omitempty"`
	PlatformFamily       string                `json:"platformFamily,omitempty"`
	ContainerDefinitions []ContainerDefinition `json:"containerDefinitions"`
	RegisteredAt         float64               `json:"registeredAt"`
	Revision             int                   `json:"revision"`
}

func toTaskDefinitionView(td TaskDefinition) taskDefinitionView {
	return taskDefinitionView{
		TaskDefinitionArn:    td.TaskDefinitionArn,
		Family:               td.Family,
		NetworkMode:          td.NetworkMode,
		Status:               td.Status,
		CPU:                  td.CPU,
		Memory:               td.Memory,
		PlatformFamily:       td.PlatformFamily,
		ContainerDefinitions: td.ContainerDefinitions,
		RegisteredAt:         float64(td.RegisteredAt.Unix()),
		Revision:             td.Revision,
	}
}

type serviceView struct {
	ServiceArn         string   `json:"serviceArn"`
	ServiceName        string   `json:"serviceName"`
	ClusterArn         string   `json:"clusterArn"`
	TaskDefinition     string   `json:"taskDefinition"`
	Status             string   `json:"status"`
	LaunchType         string   `json:"launchType,omitempty"`
	SchedulingStrategy string   `json:"schedulingStrategy,omitempty"`
	CreatedAt          float64  `json:"createdAt"`
	Tags               []Tag    `json:"tags,omitempty"`
	DesiredCount       int      `json:"desiredCount"`
	PendingCount       int      `json:"pendingCount"`
	RunningCount       int      `json:"runningCount"`
}

func toServiceView(s Service) serviceView {
	return serviceView{
		ServiceArn:         s.ServiceArn,
		ServiceName:        s.ServiceName,
		ClusterArn:         s.ClusterArn,
		TaskDefinition:     s.TaskDefinition,
		Status:             s.Status,
		LaunchType:         s.LaunchType,
		SchedulingStrategy: s.SchedulingStrategy,
		CreatedAt:          float64(s.CreatedAt.Unix()),
		Tags:               s.Tags,
		DesiredCount:       s.DesiredCount,
		PendingCount:       s.PendingCount,
		RunningCount:       s.RunningCount,
	}
}

type taskView struct {
	TaskArn              string  `json:"taskArn"`
	ClusterArn           string  `json:"clusterArn"`
	TaskDefinitionArn    string  `json:"taskDefinitionArn"`
	LastStatus           string  `json:"lastStatus"`
	DesiredStatus        string  `json:"desiredStatus"`
	StoppedReason        string  `json:"stoppedReason,omitempty"`
	Group                string  `json:"group,omitempty"`
	LaunchType           string  `json:"launchType,omitempty"`
	ContainerInstanceArn string  `json:"containerInstanceArn,omitempty"`
	StartedBy            string  `json:"startedBy,omitempty"`
	PlatformVersion      string  `json:"platformVersion,omitempty"`
	PlatformFamily       string  `json:"platformFamily,omitempty"`
	RuntimeID            string  `json:"runtimeId,omitempty"`
	StartedAt            float64 `json:"startedAt,omitempty"`
	StoppedAt            float64 `json:"stoppedAt,omitempty"`
}

func toTaskView(t Task) taskView {
	v := taskView{
		TaskArn:              t.TaskArn,
		ClusterArn:           t.ClusterArn,
		TaskDefinitionArn:    t.TaskDefinitionArn,
		LastStatus:           t.LastStatus,
		DesiredStatus:        t.DesiredStatus,
		StoppedReason:        t.StoppedReason,
		Group:                t.Group,
		LaunchType:           t.LaunchType,
		ContainerInstanceArn: t.ContainerInstanceArn,
		StartedBy:            t.StartedBy,
		PlatformVersion:      t.PlatformVersion,
		PlatformFamily:       t.PlatformFamily,
		RuntimeID:            t.RuntimeID,
	}

	if t.StartedAt != nil {
		v.StartedAt = float64(t.StartedAt.Unix())
	}

	if t.StoppedAt != nil {
		v.StoppedAt = float64(t.StoppedAt.Unix())
	}

	return v
}

const defaultECSMaxResults = 100

// applyNextTokenSlice applies nextToken-based pagination to a string slice
// using the shared pkgs/page opaque token format.
func applyNextTokenSlice(items []string, nextToken string, maxResults int) ([]string, string) {
	p := page.New(items, nextToken, maxResults, defaultECSMaxResults)

	return p.Data, p.Next
}
