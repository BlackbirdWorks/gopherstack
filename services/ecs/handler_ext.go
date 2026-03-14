package ecs

import "context"

// ----- Container instance handlers -----

type registerContainerInstanceInput struct {
	Cluster       string `json:"cluster,omitempty"`
	EC2InstanceID string `json:"ec2InstanceId"`
}

type registerContainerInstanceOutput struct {
	ContainerInstance containerInstanceView `json:"containerInstance"`
}

func (h *Handler) handleRegisterContainerInstance(
	_ context.Context,
	in *registerContainerInstanceInput,
) (*registerContainerInstanceOutput, error) {
	ci, err := h.Backend.RegisterContainerInstance(in.Cluster, in.EC2InstanceID)
	if err != nil {
		return nil, err
	}

	return &registerContainerInstanceOutput{ContainerInstance: toContainerInstanceView(*ci)}, nil
}

type deregisterContainerInstanceInput struct {
	Cluster           string `json:"cluster,omitempty"`
	ContainerInstance string `json:"containerInstance"`
	Force             bool   `json:"force,omitempty"`
}

type deregisterContainerInstanceOutput struct {
	ContainerInstance containerInstanceView `json:"containerInstance"`
}

func (h *Handler) handleDeregisterContainerInstance(
	_ context.Context,
	in *deregisterContainerInstanceInput,
) (*deregisterContainerInstanceOutput, error) {
	ci, err := h.Backend.DeregisterContainerInstance(in.Cluster, in.ContainerInstance, in.Force)
	if err != nil {
		return nil, err
	}

	return &deregisterContainerInstanceOutput{ContainerInstance: toContainerInstanceView(*ci)}, nil
}

type describeContainerInstancesInput struct {
	Cluster            string   `json:"cluster,omitempty"`
	ContainerInstances []string `json:"containerInstances"`
}

type describeContainerInstancesOutput struct {
	ContainerInstances []containerInstanceView `json:"containerInstances"`
}

func (h *Handler) handleDescribeContainerInstances(
	_ context.Context,
	in *describeContainerInstancesInput,
) (*describeContainerInstancesOutput, error) {
	cis, err := h.Backend.DescribeContainerInstances(in.Cluster, in.ContainerInstances)
	if err != nil {
		return nil, err
	}

	views := make([]containerInstanceView, 0, len(cis))
	for _, ci := range cis {
		views = append(views, toContainerInstanceView(ci))
	}

	return &describeContainerInstancesOutput{ContainerInstances: views}, nil
}

type listContainerInstancesInput struct {
	Cluster string `json:"cluster,omitempty"`
}

type listContainerInstancesOutput struct {
	ContainerInstanceArns []string `json:"containerInstanceArns"`
}

func (h *Handler) handleListContainerInstances(
	_ context.Context,
	in *listContainerInstancesInput,
) (*listContainerInstancesOutput, error) {
	arns, err := h.Backend.ListContainerInstances(in.Cluster)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	return &listContainerInstancesOutput{ContainerInstanceArns: arns}, nil
}

type updateContainerInstancesStateInput struct {
	Cluster            string   `json:"cluster,omitempty"`
	Status             string   `json:"status"`
	ContainerInstances []string `json:"containerInstances"`
}

type updateContainerInstancesStateOutput struct {
	ContainerInstances []containerInstanceView `json:"containerInstances"`
}

func (h *Handler) handleUpdateContainerInstancesState(
	_ context.Context,
	in *updateContainerInstancesStateInput,
) (*updateContainerInstancesStateOutput, error) {
	cis, err := h.Backend.UpdateContainerInstancesState(in.Cluster, in.ContainerInstances, in.Status)
	if err != nil {
		return nil, err
	}

	views := make([]containerInstanceView, 0, len(cis))
	for _, ci := range cis {
		views = append(views, toContainerInstanceView(ci))
	}

	return &updateContainerInstancesStateOutput{ContainerInstances: views}, nil
}

// ----- Task set handlers -----

type createTaskSetInput struct {
	Scale           *taskSetScale `json:"scale,omitempty"`
	Cluster         string        `json:"cluster,omitempty"`
	Service         string        `json:"service"`
	TaskDefinition  string        `json:"taskDefinition"`
	ExternalID      string        `json:"externalId,omitempty"`
	PlatformVersion string        `json:"platformVersion,omitempty"`
	LaunchType      string        `json:"launchType,omitempty"`
}

type createTaskSetOutput struct {
	TaskSet taskSetView `json:"taskSet"`
}

func (h *Handler) handleCreateTaskSet(_ context.Context, in *createTaskSetInput) (*createTaskSetOutput, error) {
	var scale *TaskSetScale
	if in.Scale != nil {
		scale = &TaskSetScale{Unit: in.Scale.Unit, Value: in.Scale.Value}
	}

	ts, err := h.Backend.CreateTaskSet(CreateTaskSetInput{
		Cluster:         in.Cluster,
		Service:         in.Service,
		TaskDefinition:  in.TaskDefinition,
		ExternalID:      in.ExternalID,
		PlatformVersion: in.PlatformVersion,
		LaunchType:      in.LaunchType,
		Scale:           scale,
	})
	if err != nil {
		return nil, err
	}

	return &createTaskSetOutput{TaskSet: toTaskSetView(*ts)}, nil
}

type deleteTaskSetInput struct {
	Cluster string `json:"cluster,omitempty"`
	Service string `json:"service"`
	TaskSet string `json:"taskSet"`
}

type deleteTaskSetOutput struct {
	TaskSet taskSetView `json:"taskSet"`
}

func (h *Handler) handleDeleteTaskSet(_ context.Context, in *deleteTaskSetInput) (*deleteTaskSetOutput, error) {
	ts, err := h.Backend.DeleteTaskSet(in.Cluster, in.Service, in.TaskSet)
	if err != nil {
		return nil, err
	}

	return &deleteTaskSetOutput{TaskSet: toTaskSetView(*ts)}, nil
}

type describeTaskSetsInput struct {
	Cluster  string   `json:"cluster,omitempty"`
	Service  string   `json:"service"`
	TaskSets []string `json:"taskSets,omitempty"`
}

type describeTaskSetsOutput struct {
	TaskSets []taskSetView `json:"taskSets"`
}

func (h *Handler) handleDescribeTaskSets(
	_ context.Context,
	in *describeTaskSetsInput,
) (*describeTaskSetsOutput, error) {
	sets, err := h.Backend.DescribeTaskSets(in.Cluster, in.Service, in.TaskSets)
	if err != nil {
		return nil, err
	}

	views := make([]taskSetView, 0, len(sets))
	for _, ts := range sets {
		views = append(views, toTaskSetView(ts))
	}

	return &describeTaskSetsOutput{TaskSets: views}, nil
}

type updateTaskSetInput struct {
	Cluster string       `json:"cluster,omitempty"`
	Service string       `json:"service"`
	TaskSet string       `json:"taskSet"`
	Scale   taskSetScale `json:"scale"`
}

type updateTaskSetOutput struct {
	TaskSet taskSetView `json:"taskSet"`
}

func (h *Handler) handleUpdateTaskSet(_ context.Context, in *updateTaskSetInput) (*updateTaskSetOutput, error) {
	ts, err := h.Backend.UpdateTaskSet(in.Cluster, in.Service, in.TaskSet, TaskSetScale{
		Unit:  in.Scale.Unit,
		Value: in.Scale.Value,
	})
	if err != nil {
		return nil, err
	}

	return &updateTaskSetOutput{TaskSet: toTaskSetView(*ts)}, nil
}

type updateServicePrimaryTaskSetInput struct {
	Cluster        string `json:"cluster,omitempty"`
	Service        string `json:"service"`
	PrimaryTaskSet string `json:"primaryTaskSet"`
}

type updateServicePrimaryTaskSetOutput struct {
	TaskSet taskSetView `json:"taskSet"`
}

func (h *Handler) handleUpdateServicePrimaryTaskSet(
	_ context.Context,
	in *updateServicePrimaryTaskSetInput,
) (*updateServicePrimaryTaskSetOutput, error) {
	ts, err := h.Backend.UpdateServicePrimaryTaskSet(in.Cluster, in.Service, in.PrimaryTaskSet)
	if err != nil {
		return nil, err
	}

	return &updateServicePrimaryTaskSetOutput{TaskSet: toTaskSetView(*ts)}, nil
}

// ----- ExecuteCommand handler -----

type executeCommandInput struct {
	Cluster     string `json:"cluster,omitempty"`
	Task        string `json:"task"`
	Container   string `json:"container,omitempty"`
	Command     string `json:"command"`
	Interactive bool   `json:"interactive,omitempty"`
}

type executeCommandOutput struct {
	ClusterArn    string  `json:"clusterArn"`
	ContainerArn  string  `json:"containerArn"`
	ContainerName string  `json:"containerName"`
	TaskArn       string  `json:"taskArn"`
	Session       session `json:"session"`
	Interactive   bool    `json:"interactive"`
}

type session struct {
	SessionID  string `json:"sessionId"`
	StreamURL  string `json:"streamUrl"`
	TokenValue string `json:"tokenValue"`
}

func (h *Handler) handleExecuteCommand(_ context.Context, in *executeCommandInput) (*executeCommandOutput, error) {
	out, err := h.Backend.ExecuteCommand(in.Cluster, in.Task, in.Container, in.Command, in.Interactive)
	if err != nil {
		return nil, err
	}

	return &executeCommandOutput{
		ClusterArn:    out.ClusterArn,
		ContainerArn:  out.ContainerArn,
		ContainerName: out.ContainerName,
		TaskArn:       out.TaskArn,
		Interactive:   out.Interactive,
		Session: session{
			SessionID:  out.Session.SessionID,
			StreamURL:  out.Session.StreamURL,
			TokenValue: out.Session.TokenValue,
		},
	}, nil
}

// ----- View types -----

type containerInstanceView struct {
	ContainerInstanceArn string  `json:"containerInstanceArn"`
	EC2InstanceID        string  `json:"ec2InstanceId"`
	ClusterArn           string  `json:"clusterArn"`
	Status               string  `json:"status"`
	AgentUpdateStatus    string  `json:"agentUpdateStatus,omitempty"`
	RegisteredAt         float64 `json:"registeredAt"`
	Version              int64   `json:"version"`
	RunningTasksCount    int     `json:"runningTasksCount"`
	PendingTasksCount    int     `json:"pendingTasksCount"`
	AgentConnected       bool    `json:"agentConnected"`
}

func toContainerInstanceView(ci ContainerInstance) containerInstanceView {
	return containerInstanceView{
		ContainerInstanceArn: ci.ContainerInstanceArn,
		EC2InstanceID:        ci.EC2InstanceID,
		ClusterArn:           ci.ClusterArn,
		Status:               ci.Status,
		AgentUpdateStatus:    ci.AgentUpdateStatus,
		RegisteredAt:         float64(ci.RegisteredAt.Unix()),
		Version:              ci.Version,
		RunningTasksCount:    ci.RunningTasksCount,
		PendingTasksCount:    ci.PendingTasksCount,
		AgentConnected:       ci.AgentConnected,
	}
}

type taskSetScale struct {
	Unit  string  `json:"unit"`
	Value float64 `json:"value"`
}

type taskSetView struct {
	TaskSetArn        string       `json:"taskSetArn"`
	ID                string       `json:"id"`
	ServiceArn        string       `json:"serviceArn"`
	ClusterArn        string       `json:"clusterArn"`
	TaskDefinition    string       `json:"taskDefinition"`
	Status            string       `json:"status"`
	ExternalID        string       `json:"externalId,omitempty"`
	PlatformVersion   string       `json:"platformVersion,omitempty"`
	LaunchType        string       `json:"launchType,omitempty"`
	StabilityStatus   string       `json:"stabilityStatus,omitempty"`
	Scale             taskSetScale `json:"scale"`
	CreatedAt         float64      `json:"createdAt"`
	UpdatedAt         float64      `json:"updatedAt"`
	StabilityStatusAt float64      `json:"stabilityStatusAt"`
}

func toTaskSetView(ts TaskSet) taskSetView {
	return taskSetView{
		TaskSetArn:        ts.TaskSetArn,
		ID:                ts.ID,
		ServiceArn:        ts.ServiceArn,
		ClusterArn:        ts.ClusterArn,
		TaskDefinition:    ts.TaskDefinition,
		Status:            ts.Status,
		ExternalID:        ts.ExternalID,
		PlatformVersion:   ts.PlatformVersion,
		LaunchType:        ts.LaunchType,
		StabilityStatus:   ts.StabilityStatus,
		Scale:             taskSetScale{Unit: ts.Scale.Unit, Value: ts.Scale.Value},
		CreatedAt:         float64(ts.CreatedAt.Unix()),
		UpdatedAt:         float64(ts.UpdatedAt.Unix()),
		StabilityStatusAt: float64(ts.StabilityStatusAt.Unix()),
	}
}
