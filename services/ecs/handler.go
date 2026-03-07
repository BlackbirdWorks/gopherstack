package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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
}

// NewHandler creates a new ECS handler.
func NewHandler(backend Backend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ECS" }

// GetSupportedOperations returns the list of supported ECS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"DescribeClusters",
		"DeleteCluster",
		"RegisterTaskDefinition",
		"DescribeTaskDefinition",
		"DeregisterTaskDefinition",
		"ListTaskDefinitions",
		"CreateService",
		"DescribeServices",
		"UpdateService",
		"DeleteService",
		"RunTask",
		"DescribeTasks",
		"StopTask",
		"ListTasks",
	}
}

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
		Cluster     string `json:"cluster"`
		Service     string `json:"service"`
		ServiceName string `json:"serviceName"`
		ClusterName string `json:"clusterName"`
		Family      string `json:"family"`
		TaskArn     string `json:"task"`
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCluster":            service.WrapOp(h.handleCreateCluster),
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
		"RunTask":                  service.WrapOp(h.handleRunTask),
		"DescribeTasks":            service.WrapOp(h.handleDescribeTasks),
		"StopTask":                 service.WrapOp(h.handleStopTask),
		"ListTasks":                service.WrapOp(h.handleListTasks),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
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

	e := err

	for e != nil {
		msg := e.Error()
		if !isSentinel(msg) {
			return msg
		}

		e = errors.Unwrap(e)
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
}

type listTaskDefinitionsOutput struct {
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

	return &listTaskDefinitionsOutput{TaskDefinitionArns: arns}, nil
}

// ----- Service handlers -----

type createServiceInput struct {
	ServiceName    string `json:"serviceName"`
	Cluster        string `json:"cluster,omitempty"`
	TaskDefinition string `json:"taskDefinition"`
	LaunchType     string `json:"launchType,omitempty"`
	DesiredCount   int    `json:"desiredCount"`
}

type createServiceOutput struct {
	Service serviceView `json:"service"`
}

func (h *Handler) handleCreateService(_ context.Context, in *createServiceInput) (*createServiceOutput, error) {
	svc, err := h.Backend.CreateService(CreateServiceInput{
		ServiceName:    in.ServiceName,
		Cluster:        in.Cluster,
		TaskDefinition: in.TaskDefinition,
		LaunchType:     in.LaunchType,
		DesiredCount:   in.DesiredCount,
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

// ----- Task handlers -----

type runTaskInput struct {
	Cluster        string `json:"cluster,omitempty"`
	TaskDefinition string `json:"taskDefinition"`
	LaunchType     string `json:"launchType,omitempty"`
	Group          string `json:"group,omitempty"`
	Count          int    `json:"count,omitempty"`
}

type runTaskOutput struct {
	Tasks []taskView `json:"tasks"`
}

func (h *Handler) handleRunTask(_ context.Context, in *runTaskInput) (*runTaskOutput, error) {
	tasks, err := h.Backend.RunTask(RunTaskInput{
		Cluster:        in.Cluster,
		TaskDefinition: in.TaskDefinition,
		Count:          in.Count,
		LaunchType:     in.LaunchType,
		Group:          in.Group,
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
	Cluster string `json:"cluster,omitempty"`
}

type listTasksOutput struct {
	TaskArns []string `json:"taskArns"`
}

func (h *Handler) handleListTasks(_ context.Context, in *listTasksInput) (*listTasksOutput, error) {
	arns, err := h.Backend.ListTasks(in.Cluster)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	return &listTasksOutput{TaskArns: arns}, nil
}

// ----- View types (JSON serialization) -----

type clusterView struct {
	ClusterArn                        string  `json:"clusterArn"`
	ClusterName                       string  `json:"clusterName"`
	Status                            string  `json:"status"`
	CreatedAt                         float64 `json:"createdAt"`
	ActiveServicesCount               int     `json:"activeServicesCount"`
	PendingTasksCount                 int     `json:"pendingTasksCount"`
	RegisteredContainerInstancesCount int     `json:"registeredContainerInstancesCount"`
	RunningTasksCount                 int     `json:"runningTasksCount"`
}

func toClusterView(c Cluster) clusterView {
	return clusterView{
		ClusterArn:                        c.ClusterArn,
		ClusterName:                       c.ClusterName,
		Status:                            c.Status,
		CreatedAt:                         float64(c.CreatedAt.Unix()),
		ActiveServicesCount:               c.ActiveServicesCount,
		PendingTasksCount:                 c.PendingTasksCount,
		RegisteredContainerInstancesCount: c.RegisteredContainerInstancesCount,
		RunningTasksCount:                 c.RunningTasksCount,
	}
}

type taskDefinitionView struct {
	TaskDefinitionArn    string                `json:"taskDefinitionArn"`
	Family               string                `json:"family"`
	NetworkMode          string                `json:"networkMode,omitempty"`
	Status               string                `json:"status"`
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
		ContainerDefinitions: td.ContainerDefinitions,
		RegisteredAt:         float64(td.RegisteredAt.Unix()),
		Revision:             td.Revision,
	}
}

type serviceView struct {
	ServiceArn     string  `json:"serviceArn"`
	ServiceName    string  `json:"serviceName"`
	ClusterArn     string  `json:"clusterArn"`
	TaskDefinition string  `json:"taskDefinition"`
	Status         string  `json:"status"`
	LaunchType     string  `json:"launchType,omitempty"`
	CreatedAt      float64 `json:"createdAt"`
	DesiredCount   int     `json:"desiredCount"`
	PendingCount   int     `json:"pendingCount"`
	RunningCount   int     `json:"runningCount"`
}

func toServiceView(s Service) serviceView {
	return serviceView{
		ServiceArn:     s.ServiceArn,
		ServiceName:    s.ServiceName,
		ClusterArn:     s.ClusterArn,
		TaskDefinition: s.TaskDefinition,
		Status:         s.Status,
		LaunchType:     s.LaunchType,
		CreatedAt:      float64(s.CreatedAt.Unix()),
		DesiredCount:   s.DesiredCount,
		PendingCount:   s.PendingCount,
		RunningCount:   s.RunningCount,
	}
}

type taskView struct {
	TaskArn           string  `json:"taskArn"`
	ClusterArn        string  `json:"clusterArn"`
	TaskDefinitionArn string  `json:"taskDefinitionArn"`
	LastStatus        string  `json:"lastStatus"`
	DesiredStatus     string  `json:"desiredStatus"`
	StoppedReason     string  `json:"stoppedReason,omitempty"`
	Group             string  `json:"group,omitempty"`
	LaunchType        string  `json:"launchType,omitempty"`
	StartedAt         float64 `json:"startedAt,omitempty"`
	StoppedAt         float64 `json:"stoppedAt,omitempty"`
}

func toTaskView(t Task) taskView {
	v := taskView{
		TaskArn:           t.TaskArn,
		ClusterArn:        t.ClusterArn,
		TaskDefinitionArn: t.TaskDefinitionArn,
		LastStatus:        t.LastStatus,
		DesiredStatus:     t.DesiredStatus,
		StoppedReason:     t.StoppedReason,
		Group:             t.Group,
		LaunchType:        t.LaunchType,
	}

	if t.StartedAt != nil {
		v.StartedAt = float64(t.StartedAt.Unix())
	}

	if t.StoppedAt != nil {
		v.StoppedAt = float64(t.StoppedAt.Unix())
	}

	return v
}
