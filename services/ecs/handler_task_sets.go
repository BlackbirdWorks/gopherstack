package ecs

import "context"

// ----- Task set handlers -----

type createTaskSetInput struct {
	NetworkConfiguration *networkConfigurationInput `json:"networkConfiguration,omitempty"`
	Scale                *taskSetScale              `json:"scale,omitempty"`
	Cluster              string                     `json:"cluster,omitempty"`
	Service              string                     `json:"service"`
	TaskDefinition       string                     `json:"taskDefinition"`
	ExternalID           string                     `json:"externalId,omitempty"`
	PlatformVersion      string                     `json:"platformVersion,omitempty"`
	LaunchType           string                     `json:"launchType,omitempty"`
	LoadBalancers        []loadBalancerInput        `json:"loadBalancers,omitempty"`
	ServiceRegistries    []serviceRegistryInput     `json:"serviceRegistries,omitempty"`
}

type createTaskSetOutput struct {
	TaskSet taskSetView `json:"taskSet"`
}

func (h *Handler) handleCreateTaskSet(
	_ context.Context,
	in *createTaskSetInput,
) (*createTaskSetOutput, error) {
	var scale *TaskSetScale
	if in.Scale != nil {
		scale = &TaskSetScale{Unit: in.Scale.Unit, Value: in.Scale.Value}
	}

	ts, err := h.Backend.CreateTaskSet(CreateTaskSetInput{
		Cluster:              in.Cluster,
		Service:              in.Service,
		TaskDefinition:       in.TaskDefinition,
		ExternalID:           in.ExternalID,
		PlatformVersion:      in.PlatformVersion,
		LaunchType:           in.LaunchType,
		Scale:                scale,
		LoadBalancers:        toLoadBalancers(in.LoadBalancers),
		ServiceRegistries:    toServiceRegistries(in.ServiceRegistries),
		NetworkConfiguration: toNetworkConfiguration(in.NetworkConfiguration),
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

func (h *Handler) handleDeleteTaskSet(
	_ context.Context,
	in *deleteTaskSetInput,
) (*deleteTaskSetOutput, error) {
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

func (h *Handler) handleUpdateTaskSet(
	_ context.Context,
	in *updateTaskSetInput,
) (*updateTaskSetOutput, error) {
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

// ----- View types -----

type taskSetScale struct {
	Unit  string  `json:"unit"`
	Value float64 `json:"value"`
}

type taskSetView struct {
	NetworkConfiguration *networkConfigurationView `json:"networkConfiguration,omitempty"`
	TaskSetArn           string                    `json:"taskSetArn"`
	ID                   string                    `json:"id"`
	ServiceArn           string                    `json:"serviceArn"`
	ClusterArn           string                    `json:"clusterArn"`
	TaskDefinition       string                    `json:"taskDefinition"`
	Status               string                    `json:"status"`
	ExternalID           string                    `json:"externalId,omitempty"`
	PlatformVersion      string                    `json:"platformVersion,omitempty"`
	LaunchType           string                    `json:"launchType,omitempty"`
	StabilityStatus      string                    `json:"stabilityStatus,omitempty"`
	Scale                taskSetScale              `json:"scale"`
	LoadBalancers        []loadBalancerView        `json:"loadBalancers,omitempty"`
	ServiceRegistries    []serviceRegistryView     `json:"serviceRegistries,omitempty"`
	CreatedAt            float64                   `json:"createdAt"`
	UpdatedAt            float64                   `json:"updatedAt"`
	StabilityStatusAt    float64                   `json:"stabilityStatusAt"`
}

func toTaskSetView(ts TaskSet) taskSetView {
	v := taskSetView{
		TaskSetArn:           ts.TaskSetArn,
		ID:                   ts.ID,
		ServiceArn:           ts.ServiceArn,
		ClusterArn:           ts.ClusterArn,
		TaskDefinition:       ts.TaskDefinition,
		Status:               ts.Status,
		ExternalID:           ts.ExternalID,
		PlatformVersion:      ts.PlatformVersion,
		LaunchType:           ts.LaunchType,
		StabilityStatus:      ts.StabilityStatus,
		Scale:                taskSetScale{Unit: ts.Scale.Unit, Value: ts.Scale.Value},
		CreatedAt:            float64(ts.CreatedAt.Unix()),
		UpdatedAt:            float64(ts.UpdatedAt.Unix()),
		StabilityStatusAt:    float64(ts.StabilityStatusAt.Unix()),
		NetworkConfiguration: toNetworkConfigurationView(ts.NetworkConfiguration),
	}

	for _, lb := range ts.LoadBalancers {
		v.LoadBalancers = append(v.LoadBalancers, loadBalancerView(lb))
	}

	for _, sr := range ts.ServiceRegistries {
		v.ServiceRegistries = append(v.ServiceRegistries, serviceRegistryView(sr))
	}

	return v
}
