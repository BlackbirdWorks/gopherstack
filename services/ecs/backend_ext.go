package ecs

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrContainerInstanceNotFound is returned when a container instance does not exist.
	ErrContainerInstanceNotFound = awserr.New("ContainerInstanceNotFoundException", awserr.ErrNotFound)
	// ErrTaskSetNotFound is returned when a task set does not exist.
	ErrTaskSetNotFound = awserr.New("TaskSetNotFoundException", awserr.ErrNotFound)
)

const defaultTaskSetScaleValue = 100.0

// ContainerInstance represents a registered ECS container instance.
type ContainerInstance struct {
	RegisteredAt         time.Time `json:"registeredAt"`
	ContainerInstanceArn string    `json:"containerInstanceArn"`
	EC2InstanceID        string    `json:"ec2InstanceId"`
	ClusterArn           string    `json:"clusterArn"`
	Status               string    `json:"status"`
	AgentUpdateStatus    string    `json:"agentUpdateStatus,omitempty"`
	Version              int64     `json:"version"`
	RunningTasksCount    int       `json:"runningTasksCount"`
	PendingTasksCount    int       `json:"pendingTasksCount"`
	AgentConnected       bool      `json:"agentConnected"`
}

// TaskSetScale specifies a scale for a task set.
type TaskSetScale struct {
	Unit  string  `json:"unit"`
	Value float64 `json:"value"`
}

// TaskSet represents an ECS task set within a service.
type TaskSet struct {
	CreatedAt         time.Time    `json:"createdAt"`
	UpdatedAt         time.Time    `json:"updatedAt"`
	StabilityStatusAt time.Time    `json:"stabilityStatusAt"`
	Status            string       `json:"status"`
	TaskSetArn        string       `json:"taskSetArn"`
	ID                string       `json:"id"`
	ServiceArn        string       `json:"serviceArn"`
	ClusterArn        string       `json:"clusterArn"`
	TaskDefinition    string       `json:"taskDefinition"`
	ExternalID        string       `json:"externalId,omitempty"`
	PlatformVersion   string       `json:"platformVersion,omitempty"`
	LaunchType        string       `json:"launchType,omitempty"`
	StabilityStatus   string       `json:"stabilityStatus,omitempty"`
	Scale             TaskSetScale `json:"scale"`
}

// Session represents an ECS Exec interactive session.
type Session struct {
	SessionID  string `json:"sessionId"`
	StreamURL  string `json:"streamUrl"`
	TokenValue string `json:"tokenValue"`
}

// ExecuteCommandOutput is the output from ExecuteCommand.
type ExecuteCommandOutput struct {
	Session       Session `json:"session"`
	ClusterArn    string  `json:"clusterArn"`
	ContainerArn  string  `json:"containerArn"`
	ContainerName string  `json:"containerName"`
	TaskArn       string  `json:"taskArn"`
	Interactive   bool    `json:"interactive"`
}

// CreateTaskSetInput holds input for CreateTaskSet.
type CreateTaskSetInput struct {
	Scale           *TaskSetScale
	Cluster         string
	Service         string
	TaskDefinition  string
	ExternalID      string
	PlatformVersion string
	LaunchType      string
}

// RegisterContainerInstance registers a container instance to a cluster.
func (b *InMemoryBackend) RegisterContainerInstance(cluster, ec2InstanceID string) (*ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("RegisterContainerInstance")
	defer b.mu.Unlock()

	b.ensureClusterLocked(clusterName)

	clusterObj, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	instanceArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:container-instance/%s/%s",
		b.region, b.accountID, clusterName, uuid.NewString(),
	)

	ci := &ContainerInstance{
		RegisteredAt:         time.Now(),
		ContainerInstanceArn: instanceArn,
		EC2InstanceID:        ec2InstanceID,
		ClusterArn:           clusterObj.ClusterArn,
		Status:               statusActive,
		AgentConnected:       true,
		Version:              1,
	}

	b.containerInstances[clusterName][instanceArn] = ci

	cp := *ci

	return &cp, nil
}

// DeregisterContainerInstance removes a container instance from a cluster.
func (b *InMemoryBackend) DeregisterContainerInstance(
	cluster, containerInstance string,
	force bool,
) (*ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("DeregisterContainerInstance")
	defer b.mu.Unlock()

	instances, ok := b.containerInstances[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	ci, ok := instances[containerInstance]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrContainerInstanceNotFound, containerInstance)
	}

	if !force {
		for _, t := range b.tasks[clusterName] {
			if t.ContainerInstanceArn == containerInstance && t.LastStatus == statusRunning {
				return nil, fmt.Errorf(
					"%w: container instance has running tasks; use force=true to override",
					ErrInvalidParameter,
				)
			}
		}
	}

	delete(instances, containerInstance)

	cp := *ci
	cp.Status = statusInactive

	return &cp, nil
}

// DescribeContainerInstances returns container instances for a cluster.
func (b *InMemoryBackend) DescribeContainerInstances(
	cluster string,
	containerInstances []string,
) ([]ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeContainerInstances")
	defer b.mu.RUnlock()

	instances, ok := b.containerInstances[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(containerInstances) == 0 {
		out := make([]ContainerInstance, 0, len(instances))
		for _, ci := range instances {
			out = append(out, b.enrichContainerInstance(ci, clusterName))
		}

		return out, nil
	}

	out := make([]ContainerInstance, 0, len(containerInstances))

	for _, ref := range containerInstances {
		ci, found := instances[ref]
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrContainerInstanceNotFound, ref)
		}

		out = append(out, b.enrichContainerInstance(ci, clusterName))
	}

	return out, nil
}

// enrichContainerInstance fills in running/pending task counts.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) enrichContainerInstance(ci *ContainerInstance, clusterName string) ContainerInstance {
	cp := *ci

	running := 0
	pending := 0

	for _, t := range b.tasks[clusterName] {
		if t.ContainerInstanceArn == ci.ContainerInstanceArn {
			switch t.LastStatus {
			case statusRunning:
				running++
			case statusPending, statusProvisioning:
				pending++
			}
		}
	}

	cp.RunningTasksCount = running
	cp.PendingTasksCount = pending

	return cp
}

// ListContainerInstances returns container instance ARNs for a cluster.
func (b *InMemoryBackend) ListContainerInstances(cluster string) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListContainerInstances")
	defer b.mu.RUnlock()

	instances, ok := b.containerInstances[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	arns := make([]string, 0, len(instances))
	for arn := range instances {
		arns = append(arns, arn)
	}

	return arns, nil
}

// UpdateContainerInstancesState updates the status of container instances.
func (b *InMemoryBackend) UpdateContainerInstancesState(
	cluster string,
	containerInstances []string,
	status string,
) ([]ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateContainerInstancesState")
	defer b.mu.Unlock()

	instances, ok := b.containerInstances[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	out := make([]ContainerInstance, 0, len(containerInstances))

	for _, ref := range containerInstances {
		ci, found := instances[ref]
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrContainerInstanceNotFound, ref)
		}

		ci.Status = status
		ci.Version++

		cp := *ci
		out = append(out, cp)
	}

	return out, nil
}

// CreateTaskSet creates a task set within a service.
func (b *InMemoryBackend) CreateTaskSet(input CreateTaskSetInput) (*TaskSet, error) {
	if input.Service == "" {
		return nil, fmt.Errorf("%w: service is required", ErrInvalidParameter)
	}

	if input.TaskDefinition == "" {
		return nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("CreateTaskSet")
	defer b.mu.Unlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	svcKey := serviceKey(input.Service)

	svc, ok := svcs[svcKey]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, input.Service)
	}

	td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()[:8]
	taskSetArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:task-set/%s/%s/ecs-svc-%s",
		b.region, b.accountID, clusterName, svcKey, id,
	)

	launchType := input.LaunchType
	if launchType == "" {
		launchType = launchTypeFargate
	}

	platformVersion := input.PlatformVersion
	if platformVersion == "" {
		platformVersion = "LATEST"
	}

	scale := TaskSetScale{Unit: "PERCENT", Value: defaultTaskSetScaleValue}
	if input.Scale != nil {
		scale = *input.Scale
	}

	now := time.Now()
	ts := &TaskSet{
		TaskSetArn:        taskSetArn,
		ID:                "ecs-svc-" + id,
		ServiceArn:        svc.ServiceArn,
		ClusterArn:        fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterName),
		TaskDefinition:    td.TaskDefinitionArn,
		Status:            statusActive,
		ExternalID:        input.ExternalID,
		PlatformVersion:   platformVersion,
		LaunchType:        launchType,
		Scale:             scale,
		StabilityStatus:   "STEADY_STATE",
		StabilityStatusAt: now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	serviceArn := svc.ServiceArn
	if b.taskSets[serviceArn] == nil {
		b.taskSets[serviceArn] = make(map[string]*TaskSet)
	}

	b.taskSets[serviceArn][taskSetArn] = ts

	cp := *ts

	return &cp, nil
}

// DeleteTaskSet removes a task set.
func (b *InMemoryBackend) DeleteTaskSet(cluster, service, taskSet string) (*TaskSet, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("DeleteTaskSet")
	defer b.mu.Unlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svcKey := serviceKey(service)

	svc, ok := svcs[svcKey]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	sets, ok := b.taskSets[svc.ServiceArn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, taskSet)
	}

	ts, ok := sets[taskSet]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, taskSet)
	}

	delete(sets, taskSet)

	cp := *ts

	return &cp, nil
}

// DescribeTaskSets returns task sets for a service.
func (b *InMemoryBackend) DescribeTaskSets(cluster, service string, taskSets []string) ([]TaskSet, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeTaskSets")
	defer b.mu.RUnlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svcKey := serviceKey(service)

	svc, ok := svcs[svcKey]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	sets := b.taskSets[svc.ServiceArn]

	if len(taskSets) == 0 {
		out := make([]TaskSet, 0, len(sets))
		for _, ts := range sets {
			out = append(out, *ts)
		}

		return out, nil
	}

	out := make([]TaskSet, 0, len(taskSets))

	for _, ref := range taskSets {
		ts, found := sets[ref]
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, ref)
		}

		out = append(out, *ts)
	}

	return out, nil
}

// UpdateTaskSet updates the scale of a task set.
func (b *InMemoryBackend) UpdateTaskSet(cluster, service, taskSet string, scale TaskSetScale) (*TaskSet, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateTaskSet")
	defer b.mu.Unlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svcKey := serviceKey(service)

	svc, ok := svcs[svcKey]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	sets, ok := b.taskSets[svc.ServiceArn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, taskSet)
	}

	ts, ok := sets[taskSet]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, taskSet)
	}

	ts.Scale = scale
	ts.UpdatedAt = time.Now()

	cp := *ts

	return &cp, nil
}

// UpdateServicePrimaryTaskSet sets the primary task set for a service.
func (b *InMemoryBackend) UpdateServicePrimaryTaskSet(cluster, service, primaryTaskSet string) (*TaskSet, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateServicePrimaryTaskSet")
	defer b.mu.Unlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svcKey := serviceKey(service)

	svc, ok := svcs[svcKey]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, service)
	}

	sets := b.taskSets[svc.ServiceArn]
	if _, found := sets[primaryTaskSet]; !found {
		return nil, fmt.Errorf("%w: %s", ErrTaskSetNotFound, primaryTaskSet)
	}

	now := time.Now()

	for arn, ts := range sets {
		if arn == primaryTaskSet {
			ts.Status = "PRIMARY"
		} else {
			ts.Status = statusActive
		}

		ts.UpdatedAt = now
	}

	cp := *sets[primaryTaskSet]

	return &cp, nil
}

// ExecuteCommand simulates an ECS Exec session.
func (b *InMemoryBackend) ExecuteCommand(
	cluster, task, container, command string,
	interactive bool,
) (*ExecuteCommandOutput, error) {
	if task == "" {
		return nil, fmt.Errorf("%w: task is required", ErrInvalidParameter)
	}

	if command == "" {
		return nil, fmt.Errorf("%w: command is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ExecuteCommand")
	defer b.mu.RUnlock()

	clusterTasks, ok := b.tasks[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	t, ok := clusterTasks[task]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, task)
	}

	if t.LastStatus != statusRunning {
		return nil, fmt.Errorf("%w: task %s is not in RUNNING state", ErrInvalidParameter, task)
	}

	clusterObj := b.clusters[clusterName]
	sessionID := uuid.NewString()

	return &ExecuteCommandOutput{
		ClusterArn:    clusterObj.ClusterArn,
		ContainerArn:  fmt.Sprintf("arn:aws:ecs:%s:%s:container/%s", b.region, b.accountID, uuid.NewString()),
		ContainerName: container,
		TaskArn:       t.TaskArn,
		Interactive:   interactive,
		Session: Session{
			SessionID:  sessionID,
			StreamURL:  fmt.Sprintf("wss://ssmmessages.%s.amazonaws.com/v1/data-channel/%s", b.region, sessionID),
			TokenValue: uuid.NewString(),
		},
	}, nil
}

// ListServices returns service ARNs for a cluster.
func (b *InMemoryBackend) ListServices(cluster string) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	arns := make([]string, 0, len(svcs))
	for _, svc := range svcs {
		arns = append(arns, svc.ServiceArn)
	}

	return arns, nil
}
