package ecs

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusRunning      = "RUNNING"
	statusStopped      = "STOPPED"
	statusActive       = "ACTIVE"
	statusInactive     = "INACTIVE"
	statusProvisioning = "PROVISIONING"
	statusPending      = "PENDING"
	launchTypeFargate  = "FARGATE"
	defaultCluster     = "default"
)

var (
	// ErrClusterNotFound is returned when a cluster does not exist.
	ErrClusterNotFound = awserr.New("ClusterNotFoundException", awserr.ErrNotFound)
	// ErrClusterAlreadyExists is returned when a cluster already exists.
	ErrClusterAlreadyExists = awserr.New("ClusterAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrTaskDefinitionNotFound is returned when a task definition does not exist.
	ErrTaskDefinitionNotFound = awserr.New("TaskDefinitionNotFoundException", awserr.ErrNotFound)
	// ErrServiceNotFound is returned when a service does not exist.
	ErrServiceNotFound = awserr.New("ServiceNotFoundException", awserr.ErrNotFound)
	// ErrServiceAlreadyExists is returned when a service already exists.
	ErrServiceAlreadyExists = awserr.New("ServiceAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrTaskNotFound is returned when a task does not exist.
	ErrTaskNotFound = awserr.New("TaskNotFoundException", awserr.ErrNotFound)
	// ErrInvalidParameter is returned when a required parameter is missing or invalid.
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
)

// Cluster represents an ECS cluster.
type Cluster struct {
	CreatedAt                         time.Time `json:"createdAt"`
	ClusterArn                        string    `json:"clusterArn"`
	ClusterName                       string    `json:"clusterName"`
	Status                            string    `json:"status"`
	ActiveServicesCount               int       `json:"activeServicesCount"`
	PendingTasksCount                 int       `json:"pendingTasksCount"`
	RegisteredContainerInstancesCount int       `json:"registeredContainerInstancesCount"`
	RunningTasksCount                 int       `json:"runningTasksCount"`
}

// ContainerDefinition represents a container definition in a task definition.
type ContainerDefinition struct {
	Image        string         `json:"image"`
	Name         string         `json:"name"`
	Environment  []KeyValuePair `json:"environment,omitempty"`
	PortMappings []PortMapping  `json:"portMappings,omitempty"`
	Essential    bool           `json:"essential"`
	Memory       int            `json:"memory,omitempty"`
	CPU          int            `json:"cpu,omitempty"`
}

// KeyValuePair is a name/value pair.
type KeyValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// PortMapping maps a container port to a host port.
type PortMapping struct {
	Protocol      string `json:"protocol,omitempty"`
	ContainerPort int    `json:"containerPort"`
	HostPort      int    `json:"hostPort,omitempty"`
}

// TaskDefinition represents an ECS task definition.
type TaskDefinition struct {
	RegisteredAt         time.Time             `json:"registeredAt"`
	TaskDefinitionArn    string                `json:"taskDefinitionArn"`
	Family               string                `json:"family"`
	NetworkMode          string                `json:"networkMode,omitempty"`
	Status               string                `json:"status"`
	ContainerDefinitions []ContainerDefinition `json:"containerDefinitions"`
	Revision             int                   `json:"revision"`
}

// Service represents an ECS service.
type Service struct {
	CreatedAt          time.Time `json:"createdAt"`
	ServiceArn         string    `json:"serviceArn"`
	ServiceName        string    `json:"serviceName"`
	ClusterArn         string    `json:"clusterArn"`
	TaskDefinition     string    `json:"taskDefinition"`
	Status             string    `json:"status"`
	LaunchType         string    `json:"launchType,omitempty"`
	SchedulingStrategy string    `json:"schedulingStrategy,omitempty"`
	DesiredCount       int       `json:"desiredCount"`
	PendingCount       int       `json:"pendingCount"`
	RunningCount       int       `json:"runningCount"`
}

// Task represents an ECS task.
type Task struct {
	StartedAt            *time.Time `json:"startedAt,omitempty"`
	StoppedAt            *time.Time `json:"stoppedAt,omitempty"`
	TaskArn              string     `json:"taskArn"`
	ClusterArn           string     `json:"clusterArn"`
	TaskDefinitionArn    string     `json:"taskDefinitionArn"`
	LastStatus           string     `json:"lastStatus"`
	DesiredStatus        string     `json:"desiredStatus"`
	StoppedReason        string     `json:"stoppedReason,omitempty"`
	Group                string     `json:"group,omitempty"`
	LaunchType           string     `json:"launchType,omitempty"`
	ContainerInstanceArn string     `json:"containerInstanceArn,omitempty"`
}

// CreateClusterInput holds input for CreateCluster.
type CreateClusterInput struct {
	ClusterName string `json:"clusterName"`
}

// RegisterTaskDefinitionInput holds input for RegisterTaskDefinition.
type RegisterTaskDefinitionInput struct {
	Family               string                `json:"family"`
	NetworkMode          string                `json:"networkMode,omitempty"`
	ContainerDefinitions []ContainerDefinition `json:"containerDefinitions"`
}

// CreateServiceInput holds input for CreateService.
type CreateServiceInput struct {
	ServiceName        string `json:"serviceName"`
	Cluster            string `json:"cluster,omitempty"`
	TaskDefinition     string `json:"taskDefinition"`
	LaunchType         string `json:"launchType,omitempty"`
	SchedulingStrategy string `json:"schedulingStrategy,omitempty"`
	DesiredCount       int    `json:"desiredCount"`
}

// UpdateServiceInput holds input for UpdateService.
type UpdateServiceInput struct {
	DesiredCount   *int   `json:"desiredCount,omitempty"`
	Cluster        string `json:"cluster,omitempty"`
	Service        string `json:"service"`
	TaskDefinition string `json:"taskDefinition,omitempty"`
}

// RunTaskInput holds input for RunTask.
type RunTaskInput struct {
	Cluster        string `json:"cluster,omitempty"`
	TaskDefinition string `json:"taskDefinition"`
	LaunchType     string `json:"launchType,omitempty"`
	Group          string `json:"group,omitempty"`
	Count          int    `json:"count,omitempty"`
}

// compile-time assertion.
var _ Backend = (*InMemoryBackend)(nil)

// InMemoryBackend stores ECS state in memory.
type InMemoryBackend struct {
	runner             TaskRunner
	clusters           map[string]*Cluster
	taskDefinitions    map[string][]*TaskDefinition
	services           map[string]map[string]*Service
	tasks              map[string]map[string]*Task
	containerInstances map[string]map[string]*ContainerInstance
	taskSets           map[string]map[string]*TaskSet
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
}

// TaskRunner is the interface for launching container tasks.
// The no-op implementation is used when no runtime is configured.
type TaskRunner interface {
	RunTask(task *Task, td *TaskDefinition) error
	StopTask(task *Task) error
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string, runner TaskRunner) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:           make(map[string]*Cluster),
		taskDefinitions:    make(map[string][]*TaskDefinition),
		services:           make(map[string]map[string]*Service),
		tasks:              make(map[string]map[string]*Task),
		containerInstances: make(map[string]map[string]*ContainerInstance),
		taskSets:           make(map[string]map[string]*TaskSet),
		mu:                 lockmetrics.New("ecs"),
		accountID:          accountID,
		region:             region,
		runner:             runner,
	}
}

// resolveCluster returns the cluster ARN/name to use, defaulting to "default".
func (b *InMemoryBackend) resolveCluster(cluster string) string {
	if cluster == "" {
		return defaultCluster
	}

	return cluster
}

// clusterKey extracts the cluster name from either a full ARN or a bare name.
func clusterKey(clusterRef string) string {
	if !strings.HasPrefix(clusterRef, "arn:") {
		return clusterRef
	}

	for i := len(clusterRef) - 1; i >= 0; i-- {
		if clusterRef[i] == '/' {
			return clusterRef[i+1:]
		}
	}

	return clusterRef
}

// CreateCluster creates a new ECS cluster.
func (b *InMemoryBackend) CreateCluster(input CreateClusterInput) (*Cluster, error) {
	name := input.ClusterName
	if name == "" {
		name = defaultCluster
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, ok := b.clusters[name]; ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterAlreadyExists, name)
	}

	cluster := &Cluster{
		CreatedAt:   time.Now(),
		ClusterArn:  fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, name),
		ClusterName: name,
		Status:      statusActive,
	}
	b.clusters[name] = cluster
	b.services[name] = make(map[string]*Service)
	b.tasks[name] = make(map[string]*Task)
	b.containerInstances[name] = make(map[string]*ContainerInstance)

	cp := *cluster

	return &cp, nil
}

// DescribeClusters returns cluster metadata.
func (b *InMemoryBackend) DescribeClusters(clusterNames []string) ([]Cluster, error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if len(clusterNames) == 0 {
		out := make([]Cluster, 0, len(b.clusters))
		for _, c := range b.clusters {
			out = append(out, b.enrichCluster(c))
		}

		return out, nil
	}

	out := make([]Cluster, 0, len(clusterNames))

	for _, name := range clusterNames {
		key := clusterKey(name)

		c, ok := b.clusters[key]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, name)
		}

		out = append(out, b.enrichCluster(c))
	}

	return out, nil
}

// enrichCluster fills in runtime-computed counts for a cluster.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) enrichCluster(c *Cluster) Cluster {
	cp := *c

	cp.ActiveServicesCount = len(b.services[c.ClusterName])

	running := 0

	for _, t := range b.tasks[c.ClusterName] {
		if t.LastStatus == statusRunning {
			running++
		}
	}

	cp.RunningTasksCount = running

	return cp
}

// DeleteCluster removes a cluster.
func (b *InMemoryBackend) DeleteCluster(clusterName string) (*Cluster, error) {
	key := clusterKey(clusterName)

	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	// Stop all Docker containers for running tasks in this cluster before
	// removing the task map, preventing container leaks on cluster deletion.
	if b.runner != nil {
		for _, task := range b.tasks[key] {
			_ = b.runner.StopTask(task)
		}
	}

	// Delete task sets for all services in this cluster before removing the services map,
	// preventing stale task set entries on cluster recreation.
	if svcs, exists := b.services[key]; exists {
		for _, svc := range svcs {
			delete(b.taskSets, svc.ServiceArn)
		}
	}

	delete(b.clusters, key)
	delete(b.services, key)
	delete(b.tasks, key)
	delete(b.containerInstances, key)

	cp := *c

	return &cp, nil
}

// RegisterTaskDefinition registers a new task definition revision.
func (b *InMemoryBackend) RegisterTaskDefinition(input RegisterTaskDefinitionInput) (*TaskDefinition, error) {
	if input.Family == "" {
		return nil, fmt.Errorf("%w: family is required", ErrInvalidParameter)
	}

	b.mu.Lock("RegisterTaskDefinition")
	defer b.mu.Unlock()

	revisions := b.taskDefinitions[input.Family]
	revision := len(revisions) + 1

	td := &TaskDefinition{
		RegisteredAt: time.Now(),
		TaskDefinitionArn: fmt.Sprintf(
			"arn:aws:ecs:%s:%s:task-definition/%s:%d",
			b.region,
			b.accountID,
			input.Family,
			revision,
		),
		Family:               input.Family,
		NetworkMode:          input.NetworkMode,
		Status:               statusActive,
		ContainerDefinitions: input.ContainerDefinitions,
		Revision:             revision,
	}

	b.taskDefinitions[input.Family] = append(revisions, td)

	cp := *td

	return &cp, nil
}

// DescribeTaskDefinition returns the latest revision of a task definition by family or ARN.
func (b *InMemoryBackend) DescribeTaskDefinition(familyOrArn string) (*TaskDefinition, error) {
	b.mu.RLock("DescribeTaskDefinition")
	defer b.mu.RUnlock()

	return b.findTaskDefinitionLocked(familyOrArn)
}

// findTaskDefinitionLocked finds a task definition. Must be called with lock held.
func (b *InMemoryBackend) findTaskDefinitionLocked(familyOrArn string) (*TaskDefinition, error) {
	// Try direct family lookup (latest revision).
	if revs, ok := b.taskDefinitions[familyOrArn]; ok && len(revs) > 0 {
		cp := *revs[len(revs)-1]

		return &cp, nil
	}

	// Try ARN / family:revision lookup.
	for _, revs := range b.taskDefinitions {
		for _, td := range revs {
			if td.TaskDefinitionArn == familyOrArn {
				cp := *td

				return &cp, nil
			}

			// Support "family:revision" shorthand.
			short := fmt.Sprintf("%s:%d", td.Family, td.Revision)
			if short == familyOrArn {
				cp := *td

				return &cp, nil
			}
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrTaskDefinitionNotFound, familyOrArn)
}

// DeregisterTaskDefinition marks a task definition revision as INACTIVE.
func (b *InMemoryBackend) DeregisterTaskDefinition(taskDefinitionArn string) (*TaskDefinition, error) {
	b.mu.Lock("DeregisterTaskDefinition")
	defer b.mu.Unlock()

	td, err := b.findTaskDefinitionLocked(taskDefinitionArn)
	if err != nil {
		return nil, err
	}

	for _, revs := range b.taskDefinitions {
		for _, r := range revs {
			if r.TaskDefinitionArn == td.TaskDefinitionArn {
				r.Status = statusInactive
				cp := *r

				return &cp, nil
			}
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrTaskDefinitionNotFound, taskDefinitionArn)
}

// ListTaskDefinitions returns ARNs of task definitions, optionally filtered by family prefix.
func (b *InMemoryBackend) ListTaskDefinitions(familyPrefix string) ([]string, error) {
	b.mu.RLock("ListTaskDefinitions")
	defer b.mu.RUnlock()

	var arns []string

	for family, revs := range b.taskDefinitions {
		if familyPrefix != "" && !strings.HasPrefix(family, familyPrefix) {
			continue
		}

		for _, td := range revs {
			if td.Status == statusActive {
				arns = append(arns, td.TaskDefinitionArn)
			}
		}
	}

	return arns, nil
}

// ensureClusterLocked returns the cluster maps, auto-creating the default cluster if needed.
// Must be called with write lock held.
func (b *InMemoryBackend) ensureClusterLocked(clusterName string) {
	if _, ok := b.clusters[clusterName]; !ok && clusterName == defaultCluster {
		b.clusters[clusterName] = &Cluster{
			CreatedAt:   time.Now(),
			ClusterArn:  fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterName),
			ClusterName: clusterName,
			Status:      statusActive,
		}
		b.services[clusterName] = make(map[string]*Service)
		b.tasks[clusterName] = make(map[string]*Task)
		b.containerInstances[clusterName] = make(map[string]*ContainerInstance)
	}
}

// CreateService creates a new ECS service.
func (b *InMemoryBackend) CreateService(input CreateServiceInput) (*Service, error) {
	if input.ServiceName == "" {
		return nil, fmt.Errorf("%w: serviceName is required", ErrInvalidParameter)
	}

	if input.TaskDefinition == "" {
		return nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	b.ensureClusterLocked(clusterName)

	if _, ok := b.services[clusterName][input.ServiceName]; ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceAlreadyExists, input.ServiceName)
	}

	td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
	if err != nil {
		return nil, err
	}

	launchType := input.LaunchType
	if launchType == "" {
		launchType = launchTypeFargate
	}

	schedulingStrategy := input.SchedulingStrategy
	if schedulingStrategy == "" {
		schedulingStrategy = "REPLICA"
	}

	svc := &Service{
		CreatedAt: time.Now(),
		ServiceArn: fmt.Sprintf(
			"arn:aws:ecs:%s:%s:service/%s/%s",
			b.region,
			b.accountID,
			clusterName,
			input.ServiceName,
		),
		ServiceName:        input.ServiceName,
		ClusterArn:         fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterName),
		TaskDefinition:     td.TaskDefinitionArn,
		Status:             statusActive,
		LaunchType:         launchType,
		SchedulingStrategy: schedulingStrategy,
		DesiredCount:       input.DesiredCount,
	}

	b.services[clusterName][input.ServiceName] = svc

	cp := *svc

	return &cp, nil
}

// DescribeServices returns services for the given cluster, optionally filtered by name.
func (b *InMemoryBackend) DescribeServices(cluster string, serviceNames []string) ([]Service, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeServices")
	defer b.mu.RUnlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(serviceNames) == 0 {
		out := make([]Service, 0, len(svcs))
		for _, s := range svcs {
			out = append(out, b.enrichService(s, clusterName))
		}

		return out, nil
	}

	out := make([]Service, 0, len(serviceNames))

	for _, name := range serviceNames {
		// Support ARN lookup by extracting the service name.
		key := serviceKey(name)

		s, found := svcs[key]
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, name)
		}

		out = append(out, b.enrichService(s, clusterName))
	}

	return out, nil
}

// serviceKey extracts service name from an ARN or returns name as-is.
func serviceKey(serviceRef string) string {
	for i := len(serviceRef) - 1; i >= 0; i-- {
		if serviceRef[i] == '/' {
			return serviceRef[i+1:]
		}
	}

	return serviceRef
}

// enrichService fills in runtime-computed counts for a service.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) enrichService(s *Service, clusterName string) Service {
	cp := *s

	running := 0
	pending := 0

	for _, t := range b.tasks[clusterName] {
		if t.Group == "service:"+s.ServiceName {
			switch t.LastStatus {
			case statusRunning:
				running++
			case statusPending, statusProvisioning:
				pending++
			}
		}
	}

	cp.RunningCount = running
	cp.PendingCount = pending

	return cp
}

// UpdateService updates an existing ECS service.
func (b *InMemoryBackend) UpdateService(input UpdateServiceInput) (*Service, error) {
	if input.Service == "" {
		return nil, fmt.Errorf("%w: service is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))
	serviceKey := serviceKey(input.Service)

	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	svc, ok := svcs[serviceKey]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, input.Service)
	}

	if input.DesiredCount != nil {
		svc.DesiredCount = *input.DesiredCount
	}

	if input.TaskDefinition != "" {
		td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
		if err != nil {
			return nil, err
		}

		svc.TaskDefinition = td.TaskDefinitionArn
	}

	cp := *svc

	return &cp, nil
}

// DeleteService removes a service from the cluster.
func (b *InMemoryBackend) DeleteService(cluster, serviceName string) (*Service, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))
	key := serviceKey(serviceName)

	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	svcs, ok := b.services[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svc, ok := svcs[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, serviceName)
	}

	delete(svcs, key)
	delete(b.taskSets, svc.ServiceArn)

	cp := *svc

	return &cp, nil
}

// RunTask starts one or more tasks on the given cluster.
func (b *InMemoryBackend) RunTask(input RunTaskInput) ([]Task, error) {
	if input.TaskDefinition == "" {
		return nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	count := input.Count
	if count <= 0 {
		count = 1
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("RunTask")
	defer b.mu.Unlock()

	b.ensureClusterLocked(clusterName)

	td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
	if err != nil {
		return nil, err
	}

	clusterArn := fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterName)

	launchType := input.LaunchType
	if launchType == "" {
		launchType = launchTypeFargate
	}

	tasks := make([]Task, 0, count)

	for range count {
		taskArn := fmt.Sprintf("arn:aws:ecs:%s:%s:task/%s/%s", b.region, b.accountID, clusterName, uuid.NewString())

		now := time.Now()
		task := &Task{
			TaskArn:           taskArn,
			ClusterArn:        clusterArn,
			TaskDefinitionArn: td.TaskDefinitionArn,
			LastStatus:        statusProvisioning,
			DesiredStatus:     statusRunning,
			Group:             input.Group,
			LaunchType:        launchType,
			StartedAt:         &now,
		}

		b.tasks[clusterName][taskArn] = task

		if b.runner != nil {
			// Transition task to RUNNING after successful container start.
			if runErr := b.runner.RunTask(task, td); runErr == nil {
				task.LastStatus = statusRunning
			}
		} else {
			// No runtime: immediately move to RUNNING (simulated).
			task.LastStatus = statusRunning
		}

		cp := *task
		tasks = append(tasks, cp)
	}

	return tasks, nil
}

// DescribeTasks returns tasks on a given cluster, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeTasks(cluster string, taskArns []string) ([]Task, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeTasks")
	defer b.mu.RUnlock()

	clusterTasks, ok := b.tasks[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(taskArns) == 0 {
		out := make([]Task, 0, len(clusterTasks))
		for _, t := range clusterTasks {
			out = append(out, *t)
		}

		return out, nil
	}

	out := make([]Task, 0, len(taskArns))

	for _, arn := range taskArns {
		t, found := clusterTasks[arn]
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, arn)
		}

		out = append(out, *t)
	}

	return out, nil
}

// StopTask stops a running task.
func (b *InMemoryBackend) StopTask(cluster, taskArn, reason string) (*Task, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("StopTask")
	defer b.mu.Unlock()

	clusterTasks, ok := b.tasks[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	task, ok := clusterTasks[taskArn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskArn)
	}

	if b.runner != nil {
		_ = b.runner.StopTask(task)
	}

	now := time.Now()
	task.LastStatus = statusStopped
	task.DesiredStatus = statusStopped
	task.StoppedAt = &now
	task.StoppedReason = reason

	cp := *task

	return &cp, nil
}

// ListTasks returns task ARNs for the given cluster.
func (b *InMemoryBackend) ListTasks(cluster string) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListTasks")
	defer b.mu.RUnlock()

	clusterTasks, ok := b.tasks[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	arns := make([]string, 0, len(clusterTasks))
	for arn := range clusterTasks {
		arns = append(arns, arn)
	}

	return arns, nil
}

// getServicesForReconciler returns a snapshot of all services for the reconciler.
func (b *InMemoryBackend) getServicesForReconciler() []serviceSnapshot {
	b.mu.RLock("GetServicesForReconciler")
	defer b.mu.RUnlock()

	var out []serviceSnapshot

	for clusterName, svcs := range b.services {
		for _, svc := range svcs {
			out = append(out, serviceSnapshot{
				clusterName: clusterName,
				service:     *svc,
			})
		}
	}

	return out
}

// serviceSnapshot is a point-in-time copy of a service for the reconciler.
type serviceSnapshot struct {
	clusterName string
	service     Service
}

// CountRunningTasksForService counts running tasks for a service on a cluster.
func (b *InMemoryBackend) CountRunningTasksForService(clusterName, serviceName string) int {
	b.mu.RLock("CountRunningTasksForService")
	defer b.mu.RUnlock()

	count := 0
	group := "service:" + serviceName

	for _, t := range b.tasks[clusterName] {
		if t.Group == group && t.LastStatus == statusRunning {
			count++
		}
	}

	return count
}

// StartTaskForService launches a task on behalf of a service.
func (b *InMemoryBackend) StartTaskForService(clusterName, serviceName, taskDefinitionArn string) error {
	_, err := b.RunTask(RunTaskInput{
		Cluster:        clusterName,
		TaskDefinition: taskDefinitionArn,
		Count:          1,
		Group:          "service:" + serviceName,
	})

	return err
}

// StopOldestServiceTask stops the oldest running task for a service.
func (b *InMemoryBackend) StopOldestServiceTask(clusterName, serviceName string) error {
	b.mu.Lock("StopOldestServiceTask")
	defer b.mu.Unlock()

	group := "service:" + serviceName

	var oldest *Task

	for _, t := range b.tasks[clusterName] {
		if t.Group == group && t.LastStatus == statusRunning {
			if oldest == nil ||
				(t.StartedAt != nil && oldest.StartedAt != nil && t.StartedAt.Before(*oldest.StartedAt)) {
				oldest = t
			}
		}
	}

	if oldest == nil {
		return nil
	}

	if b.runner != nil {
		_ = b.runner.StopTask(oldest)
	}

	now := time.Now()
	oldest.LastStatus = statusStopped
	oldest.DesiredStatus = statusStopped
	oldest.StoppedAt = &now
	oldest.StoppedReason = "service scale-in"

	return nil
}

// noopRunner is a TaskRunner that does nothing (used when no runtime is configured).
type noopRunner struct{}

func (noopRunner) RunTask(_ *Task, _ *TaskDefinition) error { return nil }
func (noopRunner) StopTask(_ *Task) error                   { return nil }

// NewNoopRunner returns a TaskRunner that does nothing.
func NewNoopRunner() TaskRunner { return noopRunner{} }
