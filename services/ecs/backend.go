package ecs

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusRunning           = "RUNNING"
	statusStopped           = "STOPPED"
	statusActive            = "ACTIVE"
	statusInactive          = "INACTIVE"
	statusProvisioning      = "PROVISIONING"
	statusPending           = "PENDING"
	launchTypeFargate       = "FARGATE"
	defaultCluster          = "default"
	deploymentStatusPrimary = "PRIMARY"

	// maxTaskDefinitionRevisions is the maximum number of revisions retained per
	// task definition family. Older INACTIVE revisions beyond this cap are
	// removed to prevent unbounded memory growth.
	maxTaskDefinitionRevisions = 100
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
	CreatedAt                         time.Time                      `json:"createdAt"`
	ClusterArn                        string                         `json:"clusterArn"`
	ClusterName                       string                         `json:"clusterName"`
	Status                            string                         `json:"status"`
	DefaultCapacityProviderStrategy   []CapacityProviderStrategyItem `json:"defaultCapacityProviderStrategy,omitempty"`
	Settings                          []ClusterSetting               `json:"settings,omitempty"`
	CapacityProviders                 []string                       `json:"capacityProviders,omitempty"`
	ActiveServicesCount               int                            `json:"activeServicesCount"`
	PendingTasksCount                 int                            `json:"pendingTasksCount"`
	RegisteredContainerInstancesCount int                            `json:"registeredContainerInstancesCount"`
	RunningTasksCount                 int                            `json:"runningTasksCount"`
}

// ContainerDefinition represents a container definition in a task definition.
type ContainerDefinition struct {
	LogConfiguration       *LogConfiguration      `json:"logConfiguration,omitempty"`
	FirelensConfiguration  *FirelensConfiguration `json:"firelensConfiguration,omitempty"`
	HealthCheck            *HealthCheck           `json:"healthCheck,omitempty"`
	RepositoryCredentials  *RepositoryCredentials `json:"repositoryCredentials,omitempty"`
	Image                  string                 `json:"image"`
	Name                   string                 `json:"name"`
	EntryPoint             []string               `json:"entryPoint,omitempty"`
	Command                []string               `json:"command,omitempty"`
	Environment            []KeyValuePair         `json:"environment,omitempty"`
	Secrets                []SecretReference      `json:"secrets,omitempty"`
	EnvironmentFiles       []EnvironmentFile      `json:"environmentFiles,omitempty"`
	ResourceRequirements   []ResourceRequirement  `json:"resourceRequirements,omitempty"`
	PortMappings           []PortMapping          `json:"portMappings,omitempty"`
	MountPoints            []MountPoint           `json:"mountPoints,omitempty"`
	VolumesFrom            []VolumeFrom           `json:"volumesFrom,omitempty"`
	DependsOn              []ContainerDependency  `json:"dependsOn,omitempty"`
	Memory                 int                    `json:"memory,omitempty"`
	MemoryReservation      int                    `json:"memoryReservation,omitempty"`
	CPU                    int                    `json:"cpu,omitempty"`
	Essential              bool                   `json:"essential"`
	DisableNetworking      bool                   `json:"disableNetworking,omitempty"`
	Privileged             bool                   `json:"privileged,omitempty"`
	ReadonlyRootFilesystem bool                   `json:"readonlyRootFilesystem,omitempty"`
}

// ContainerDependency specifies a start/stop dependency between containers.
type ContainerDependency struct {
	ContainerName string `json:"containerName"`
	Condition     string `json:"condition"`
}

// KeyValuePair is a name/value pair.
type KeyValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// PortMapping maps a container port to a host port.
type PortMapping struct {
	Name               string `json:"name,omitempty"`
	Protocol           string `json:"protocol,omitempty"`
	AppProtocol        string `json:"appProtocol,omitempty"`
	ContainerPortRange string `json:"containerPortRange,omitempty"`
	ContainerPort      int    `json:"containerPort,omitempty"`
	HostPort           int    `json:"hostPort,omitempty"`
}

// TaskDefinition represents an ECS task definition.
type TaskDefinition struct {
	RuntimePlatform         *RuntimePlatform      `json:"runtimePlatform,omitempty"`
	EphemeralStorage        *EphemeralStorage     `json:"ephemeralStorage,omitempty"`
	RegisteredAt            time.Time             `json:"registeredAt"`
	TaskDefinitionArn       string                `json:"taskDefinitionArn"`
	Family                  string                `json:"family"`
	TaskRoleArn             string                `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn        string                `json:"executionRoleArn,omitempty"`
	NetworkMode             string                `json:"networkMode,omitempty"`
	Status                  string                `json:"status"`
	PlatformFamily          string                `json:"platformFamily,omitempty"`
	CPU                     string                `json:"cpu,omitempty"`
	Memory                  string                `json:"memory,omitempty"`
	ContainerDefinitions    []ContainerDefinition `json:"containerDefinitions"`
	Volumes                 []Volume              `json:"volumes,omitempty"`
	PlacementConstraints    []PlacementConstraint `json:"placementConstraints,omitempty"`
	RequiresCompatibilities []string              `json:"requiresCompatibilities,omitempty"`
	InferenceAccelerators   []InferenceAccelerator `json:"inferenceAccelerators,omitempty"`
	Revision                int                   `json:"revision"`
}

// Service represents an ECS service.
type Service struct {
	CreatedAt                   time.Time                      `json:"createdAt"`
	ServiceConnectConfiguration *ServiceConnectConfiguration   `json:"serviceConnectConfiguration,omitempty"`
	DeploymentConfiguration     *DeploymentConfiguration       `json:"deploymentConfiguration,omitempty"`
	DeploymentController        *DeploymentController          `json:"deploymentController,omitempty"`
	NetworkConfiguration        *NetworkConfiguration          `json:"networkConfiguration,omitempty"`
	ServiceArn                  string                         `json:"serviceArn"`
	ServiceName                 string                         `json:"serviceName"`
	ClusterArn                  string                         `json:"clusterArn"`
	TaskDefinition              string                         `json:"taskDefinition"`
	Status                      string                         `json:"status"`
	LaunchType                  string                         `json:"launchType,omitempty"`
	SchedulingStrategy          string                         `json:"schedulingStrategy,omitempty"`
	PropagateTags               string                         `json:"propagateTags,omitempty"`
	Tags                        []Tag                          `json:"tags,omitempty"`
	LoadBalancers               []LoadBalancer                 `json:"loadBalancers,omitempty"`
	ServiceRegistries           []ServiceRegistry              `json:"serviceRegistries,omitempty"`
	PlacementConstraints        []PlacementConstraint          `json:"placementConstraints,omitempty"`
	PlacementStrategy           []PlacementStrategy            `json:"placementStrategy,omitempty"`
	CapacityProviderStrategy    []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	Deployments                 []Deployment                   `json:"deployments,omitempty"`
	DesiredCount                int                            `json:"desiredCount"`
	PendingCount                int                            `json:"pendingCount"`
	RunningCount                int                            `json:"runningCount"`
	EnableExecuteCommand        bool                           `json:"enableExecuteCommand,omitempty"`
}

// Task represents an ECS task.
type Task struct {
	StartedAt            *time.Time            `json:"startedAt,omitempty"`
	StoppedAt            *time.Time            `json:"stoppedAt,omitempty"`
	ConnectivityAt       *time.Time            `json:"connectivityAt,omitempty"`
	Overrides            *TaskOverride         `json:"overrides,omitempty"`
	NetworkConfiguration *NetworkConfiguration `json:"networkConfiguration,omitempty"`
	TaskArn              string                `json:"taskArn"`
	ClusterArn           string                `json:"clusterArn"`
	TaskDefinitionArn    string                `json:"taskDefinitionArn"`
	LastStatus           string                `json:"lastStatus"`
	DesiredStatus        string                `json:"desiredStatus"`
	Connectivity         string                `json:"connectivity,omitempty"`
	StoppedReason        string                `json:"stoppedReason,omitempty"`
	Group                string                `json:"group,omitempty"`
	LaunchType           string                `json:"launchType,omitempty"`
	ContainerInstanceArn string                `json:"containerInstanceArn,omitempty"`
	StartedBy            string                `json:"startedBy,omitempty"`
	PlatformVersion      string                `json:"platformVersion,omitempty"`
	PlatformFamily       string                `json:"platformFamily,omitempty"`
	RuntimeID            string                `json:"runtimeId,omitempty"`
	PropagateTags        string                `json:"propagateTags,omitempty"`
	Tags                 []Tag                 `json:"tags,omitempty"`
	Attachments          []TaskAttachment      `json:"attachments,omitempty"`
	Containers           []Container           `json:"containers,omitempty"`
	EnableExecuteCommand bool                  `json:"enableExecuteCommand,omitempty"`
}

// CreateClusterInput holds input for CreateCluster.
type CreateClusterInput struct {
	ClusterName string
	Settings    []ClusterSetting
}

// RegisterTaskDefinitionInput holds input for RegisterTaskDefinition.
type RegisterTaskDefinitionInput struct {
	RuntimePlatform         *RuntimePlatform       `json:"runtimePlatform,omitempty"`
	EphemeralStorage        *EphemeralStorage      `json:"ephemeralStorage,omitempty"`
	Family                  string                 `json:"family"`
	TaskRoleArn             string                 `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn        string                 `json:"executionRoleArn,omitempty"`
	NetworkMode             string                 `json:"networkMode,omitempty"`
	CPU                     string                 `json:"cpu,omitempty"`
	Memory                  string                 `json:"memory,omitempty"`
	PlatformFamily          string                 `json:"platformFamily,omitempty"`
	ContainerDefinitions    []ContainerDefinition  `json:"containerDefinitions"`
	Volumes                 []Volume               `json:"volumes,omitempty"`
	PlacementConstraints    []PlacementConstraint  `json:"placementConstraints,omitempty"`
	RequiresCompatibilities []string               `json:"requiresCompatibilities,omitempty"`
	InferenceAccelerators   []InferenceAccelerator `json:"inferenceAccelerators,omitempty"`
	Tags                    []Tag                  `json:"tags,omitempty"`
}

// CreateServiceInput holds input for CreateService.
type CreateServiceInput struct {
	DeploymentConfiguration     *DeploymentConfiguration       `json:"deploymentConfiguration,omitempty"`
	DeploymentController        *DeploymentController          `json:"deploymentController,omitempty"`
	NetworkConfiguration        *NetworkConfiguration          `json:"networkConfiguration,omitempty"`
	ServiceConnectConfiguration *ServiceConnectConfiguration   `json:"serviceConnectConfiguration,omitempty"`
	ServiceName                 string                         `json:"serviceName"`
	Cluster                     string                         `json:"cluster,omitempty"`
	TaskDefinition              string                         `json:"taskDefinition"`
	LaunchType                  string                         `json:"launchType,omitempty"`
	SchedulingStrategy          string                         `json:"schedulingStrategy,omitempty"`
	PropagateTags               string                         `json:"propagateTags,omitempty"`
	Tags                        []Tag                          `json:"tags,omitempty"`
	LoadBalancers               []LoadBalancer                 `json:"loadBalancers,omitempty"`
	ServiceRegistries           []ServiceRegistry              `json:"serviceRegistries,omitempty"`
	CapacityProviderStrategy    []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	PlacementConstraints        []PlacementConstraint          `json:"placementConstraints,omitempty"`
	PlacementStrategy           []PlacementStrategy            `json:"placementStrategy,omitempty"`
	DesiredCount                int                            `json:"desiredCount"`
	EnableExecuteCommand        bool                           `json:"enableExecuteCommand,omitempty"`
}

// UpdateServiceInput holds input for UpdateService.
type UpdateServiceInput struct {
	EnableExecuteCommand        *bool                          `json:"enableExecuteCommand,omitempty"`
	DesiredCount                *int                           `json:"desiredCount,omitempty"`
	DeploymentConfiguration     *DeploymentConfiguration       `json:"deploymentConfiguration,omitempty"`
	NetworkConfiguration        *NetworkConfiguration          `json:"networkConfiguration,omitempty"`
	ServiceConnectConfiguration *ServiceConnectConfiguration   `json:"serviceConnectConfiguration,omitempty"`
	Cluster                     string                         `json:"cluster,omitempty"`
	Service                     string                         `json:"service"`
	TaskDefinition              string                         `json:"taskDefinition,omitempty"`
	PropagateTags               string                         `json:"propagateTags,omitempty"`
	LoadBalancers               []LoadBalancer                 `json:"loadBalancers,omitempty"`
	CapacityProviderStrategy    []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	PlacementConstraints        []PlacementConstraint          `json:"placementConstraints,omitempty"`
	PlacementStrategy           []PlacementStrategy            `json:"placementStrategy,omitempty"`
}

// RunTaskInput holds input for RunTask.
type RunTaskInput struct {
	Overrides               *TaskOverride         `json:"overrides,omitempty"`
	NetworkConfiguration    *NetworkConfiguration `json:"networkConfiguration,omitempty"`
	Cluster                 string                `json:"cluster,omitempty"`
	TaskDefinition          string                `json:"taskDefinition"`
	LaunchType              string                `json:"launchType,omitempty"`
	Group                   string                `json:"group,omitempty"`
	StartedBy               string                `json:"startedBy,omitempty"`
	PlatformVersion         string                `json:"platformVersion,omitempty"`
	PropagateTags           string                `json:"propagateTags,omitempty"`
	serviceNameForTags      string
	Tags                    []Tag `json:"tags,omitempty"`
	serviceTagsForPropagate []Tag
	Count                   int  `json:"count,omitempty"`
	EnableECSManagedTags    bool `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand    bool `json:"enableExecuteCommand,omitempty"`
}

// compile-time assertion.
var _ Backend = (*InMemoryBackend)(nil)

// InMemoryBackend stores ECS state in memory.
type InMemoryBackend struct {
	runner                 TaskRunner
	clusters               map[string]*Cluster
	taskDefinitions        map[string][]*TaskDefinition
	taskDefByArn           map[string]*TaskDefinition // ARN → TaskDefinition cache
	services               map[string]map[string]*Service
	tasks                  map[string]map[string]*Task
	containerInstances     map[string]map[string]*ContainerInstance
	taskSets               map[string]map[string]*TaskSet
	taskProtections        map[string]*TaskProtection // taskArn → TaskProtection
	capacityProviders      map[string]*CapacityProvider
	accountSettings        map[string]*AccountSetting
	attributes             map[string]map[string]*Attribute // clusterName → attributeKey → Attribute
	serviceDeployments     map[string]*ServiceDeployment
	expressGatewayServices map[string]*ExpressGatewayService
	resourceTags           map[string][]Tag // resourceArn → tags
	// tasksByInstance is a reverse index: clusterName → containerInstanceArn → set of taskArns.
	// It allows enrichContainerInstance to look up tasks in O(k) instead of O(n).
	tasksByInstance map[string]map[string]map[string]bool
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
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
		clusters:               make(map[string]*Cluster),
		taskDefinitions:        make(map[string][]*TaskDefinition),
		taskDefByArn:           make(map[string]*TaskDefinition),
		services:               make(map[string]map[string]*Service),
		tasks:                  make(map[string]map[string]*Task),
		containerInstances:     make(map[string]map[string]*ContainerInstance),
		taskSets:               make(map[string]map[string]*TaskSet),
		taskProtections:        make(map[string]*TaskProtection),
		capacityProviders:      make(map[string]*CapacityProvider),
		accountSettings:        make(map[string]*AccountSetting),
		attributes:             make(map[string]map[string]*Attribute),
		serviceDeployments:     make(map[string]*ServiceDeployment),
		expressGatewayServices: make(map[string]*ExpressGatewayService),
		resourceTags:           make(map[string][]Tag),
		tasksByInstance:        make(map[string]map[string]map[string]bool),
		mu:                     lockmetrics.New("ecs"),
		accountID:              accountID,
		region:                 region,
		runner:                 runner,
	}
}

// Reset zeroes all backend state for test isolation.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*Cluster)
	b.taskDefinitions = make(map[string][]*TaskDefinition)
	b.taskDefByArn = make(map[string]*TaskDefinition)
	b.services = make(map[string]map[string]*Service)
	b.tasks = make(map[string]map[string]*Task)
	b.containerInstances = make(map[string]map[string]*ContainerInstance)
	b.taskSets = make(map[string]map[string]*TaskSet)
	b.taskProtections = make(map[string]*TaskProtection)
	b.capacityProviders = make(map[string]*CapacityProvider)
	b.accountSettings = make(map[string]*AccountSetting)
	b.attributes = make(map[string]map[string]*Attribute)
	b.serviceDeployments = make(map[string]*ServiceDeployment)
	b.expressGatewayServices = make(map[string]*ExpressGatewayService)
	b.resourceTags = make(map[string][]Tag)
	b.tasksByInstance = make(map[string]map[string]map[string]bool)
}

// Purge removes all ECS resources created before the given cutoff time.
func (b *InMemoryBackend) Purge(_ context.Context, cutoff time.Time) {
	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	for name, c := range b.clusters {
		if c.CreatedAt.Before(cutoff) {
			delete(b.clusters, name)
			delete(b.services, name)
			delete(b.tasks, name)
			delete(b.containerInstances, name)
			delete(b.taskSets, name)
			delete(b.attributes, name)
		}
	}

	for family, revs := range b.taskDefinitions {
		kept := make([]*TaskDefinition, 0, len(revs))

		for _, td := range revs {
			if td.RegisteredAt.Before(cutoff) {
				delete(b.taskDefByArn, td.TaskDefinitionArn)
			} else {
				kept = append(kept, td)
			}
		}

		if len(kept) == 0 {
			delete(b.taskDefinitions, family)
		} else {
			b.taskDefinitions[family] = kept
		}
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
		Settings:    input.Settings,
	}
	b.clusters[name] = cluster
	b.services[name] = make(map[string]*Service)
	b.tasks[name] = make(map[string]*Task)
	b.containerInstances[name] = make(map[string]*ContainerInstance)

	cp := *cluster

	return &cp, nil
}

// ListClusters returns all clusters.
func (b *InMemoryBackend) ListClusters() ([]Cluster, error) {
	return b.DescribeClusters(nil)
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
	cp.RegisteredContainerInstancesCount = len(b.containerInstances[c.ClusterName])

	running := 0
	pending := 0

	for _, t := range b.tasks[c.ClusterName] {
		switch t.LastStatus {
		case statusRunning:
			running++
		case statusProvisioning, statusPending:
			pending++
		}
	}

	cp.RunningTasksCount = running
	cp.PendingTasksCount = pending

	return cp
}

// DeleteCluster removes a cluster.
func (b *InMemoryBackend) DeleteCluster(clusterName string) (*Cluster, error) {
	key := clusterKey(clusterName)

	b.mu.Lock("DeleteCluster")

	c, ok := b.clusters[key]
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	// Snapshot task pointers while still holding the lock so we can stop their
	// Docker containers after releasing it.  Performing Docker API calls under
	// the backend lock would unnecessarily serialize all other operations.
	tasksToStop := make([]*Task, 0, len(b.tasks[key]))

	if b.runner != nil {
		for _, task := range b.tasks[key] {
			tasksToStop = append(tasksToStop, task)
		}
	}

	// Delete task sets and service deployments for all services in this cluster
	// before removing the services map, preventing stale entries on cluster recreation.
	if svcs, exists := b.services[key]; exists {
		for _, svc := range svcs {
			delete(b.taskSets, svc.ServiceArn)
			delete(b.serviceDeployments, svc.ServiceArn)
		}
	}

	// Clean up task protections for all tasks in this cluster to avoid memory leaks.
	for taskArn := range b.tasks[key] {
		delete(b.taskProtections, taskArn)
	}

	delete(b.clusters, key)
	delete(b.services, key)
	delete(b.tasks, key)
	delete(b.containerInstances, key)

	cp := *c

	// Release the lock before issuing Docker API calls so other backend
	// operations are not serialized behind potentially slow container stops.
	b.mu.Unlock()

	for _, task := range tasksToStop {
		_ = b.runner.StopTask(task)
	}

	return &cp, nil
}

// RegisterTaskDefinition registers a new task definition revision.
func (b *InMemoryBackend) RegisterTaskDefinition(input RegisterTaskDefinitionInput) (*TaskDefinition, error) {
	if input.Family == "" {
		return nil, fmt.Errorf("%w: family is required", ErrInvalidParameter)
	}

	isFargate := false

	for _, rc := range input.RequiresCompatibilities {
		if strings.EqualFold(rc, launchTypeFargate) {
			isFargate = true

			break
		}
	}

	if isFargate {
		if err := validateFargateCPUMemory(input.CPU, input.Memory); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("RegisterTaskDefinition")
	defer b.mu.Unlock()

	revisions := b.taskDefinitions[input.Family]

	// Determine the next revision number based on the last stored revision
	// so trimming the cap does not desync the counter.
	revision := 1
	if len(revisions) > 0 {
		revision = revisions[len(revisions)-1].Revision + 1
	}

	td := &TaskDefinition{
		RegisteredAt: time.Now(),
		TaskDefinitionArn: fmt.Sprintf(
			"arn:aws:ecs:%s:%s:task-definition/%s:%d",
			b.region,
			b.accountID,
			input.Family,
			revision,
		),
		Family:                  input.Family,
		TaskRoleArn:             input.TaskRoleArn,
		ExecutionRoleArn:        input.ExecutionRoleArn,
		NetworkMode:             input.NetworkMode,
		CPU:                     input.CPU,
		Memory:                  input.Memory,
		PlatformFamily:          input.PlatformFamily,
		Status:                  statusActive,
		ContainerDefinitions:    input.ContainerDefinitions,
		Volumes:                 input.Volumes,
		PlacementConstraints:    input.PlacementConstraints,
		RequiresCompatibilities: input.RequiresCompatibilities,
		RuntimePlatform:         input.RuntimePlatform,
		EphemeralStorage:        input.EphemeralStorage,
		InferenceAccelerators:   input.InferenceAccelerators,
		Revision:                revision,
	}

	revisions = append(revisions, td)

	// Enforce the revision cap: if we exceed maxTaskDefinitionRevisions, trim
	// the oldest entries to prevent unbounded memory growth.
	if len(revisions) > maxTaskDefinitionRevisions {
		excess := len(revisions) - maxTaskDefinitionRevisions

		for _, evicted := range revisions[:excess] {
			delete(b.taskDefByArn, evicted.TaskDefinitionArn)
		}

		revisions = revisions[excess:]
	}

	b.taskDefinitions[input.Family] = revisions
	b.taskDefByArn[td.TaskDefinitionArn] = td

	// Persist registration tags via the same resourceTags map used by
	// TagResource so that DescribeTaskDefinition can surface them when
	// the include=TAGS option is provided.
	if len(input.Tags) > 0 {
		if b.resourceTags == nil {
			b.resourceTags = make(map[string][]Tag)
		}

		copied := make([]Tag, len(input.Tags))
		copy(copied, input.Tags)
		b.resourceTags[resourceTagKey(td.TaskDefinitionArn)] = copied
	}

	cp := *td

	return &cp, nil
}

// DescribeTaskDefinition returns the latest revision of a task definition by family or ARN.
func (b *InMemoryBackend) DescribeTaskDefinition(familyOrArn string) (*TaskDefinition, error) {
	b.mu.RLock("DescribeTaskDefinition")
	defer b.mu.RUnlock()

	return b.findTaskDefinitionLocked(familyOrArn)
}

// findFamilyRevisionLocked looks up a task definition by "family:revision" shorthand.
// Must be called with lock held.
func (b *InMemoryBackend) findFamilyRevisionLocked(familyOrArn string) (*TaskDefinition, bool) {
	idx := strings.LastIndex(familyOrArn, ":")
	if idx <= 0 {
		return nil, false
	}

	family := familyOrArn[:idx]

	revNum, err := strconv.Atoi(familyOrArn[idx+1:])
	if err != nil {
		return nil, false
	}

	for _, td := range b.taskDefinitions[family] {
		if td.Revision == revNum {
			cp := *td

			return &cp, true
		}
	}

	return nil, false
}

// findTaskDefinitionLocked finds a task definition. Must be called with lock held.
func (b *InMemoryBackend) findTaskDefinitionLocked(familyOrArn string) (*TaskDefinition, error) {
	// Try direct family lookup (latest revision).
	if revs, ok := b.taskDefinitions[familyOrArn]; ok && len(revs) > 0 {
		cp := *revs[len(revs)-1]

		return &cp, nil
	}

	// Fast path: ARN cache lookup.
	if td, ok := b.taskDefByArn[familyOrArn]; ok {
		cp := *td

		return &cp, nil
	}

	// Fallback scan when the ARN cache is empty or stale (e.g. after restore).
	for _, revs := range b.taskDefinitions {
		for _, td := range revs {
			if td.TaskDefinitionArn == familyOrArn {
				cp := *td

				return &cp, nil
			}
		}
	}

	// Support "family:revision" shorthand.
	if td, ok := b.findFamilyRevisionLocked(familyOrArn); ok {
		return td, nil
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

// ListTaskDefinitionsInput holds optional filters for ListTaskDefinitions.
type ListTaskDefinitionsInput struct {
	FamilyPrefix string
	// Status filters by task definition status: "ACTIVE", "INACTIVE", or
	// "DELETE_IN_PROGRESS". Empty string matches only ACTIVE (AWS default).
	Status string
}

// ListTaskDefinitions returns ARNs of task definitions, optionally filtered by family prefix.
// ARNs are returned sorted for deterministic output.
func (b *InMemoryBackend) ListTaskDefinitions(familyPrefix string) ([]string, error) {
	return b.ListTaskDefinitionsFiltered(ListTaskDefinitionsInput{FamilyPrefix: familyPrefix})
}

// ListTaskDefinitionsFiltered returns task definition ARNs with status filtering.
func (b *InMemoryBackend) ListTaskDefinitionsFiltered(input ListTaskDefinitionsInput) ([]string, error) {
	b.mu.RLock("ListTaskDefinitionsFiltered")
	defer b.mu.RUnlock()

	wantStatus := strings.ToUpper(input.Status)
	if wantStatus == "" {
		wantStatus = statusActive
	}

	var arns []string

	for family, revs := range b.taskDefinitions {
		if input.FamilyPrefix != "" && !strings.HasPrefix(family, input.FamilyPrefix) {
			continue
		}

		for _, td := range revs {
			if strings.EqualFold(td.Status, wantStatus) {
				arns = append(arns, td.TaskDefinitionArn)
			}
		}
	}

	sort.Strings(arns)

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

	if err := validateDeploymentController(input.DeploymentController); err != nil {
		return nil, err
	}

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

	propagateTags := input.PropagateTags
	if propagateTags == "" {
		propagateTags = propagateTagsNone
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
		ServiceName:                 input.ServiceName,
		ClusterArn:                  fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterName),
		TaskDefinition:              td.TaskDefinitionArn,
		Status:                      statusActive,
		LaunchType:                  launchType,
		SchedulingStrategy:          schedulingStrategy,
		PropagateTags:               propagateTags,
		Tags:                        input.Tags,
		LoadBalancers:               input.LoadBalancers,
		ServiceRegistries:           input.ServiceRegistries,
		DeploymentConfiguration:     input.DeploymentConfiguration.withAWSDefaults(),
		DeploymentController:        input.DeploymentController,
		NetworkConfiguration:        input.NetworkConfiguration,
		CapacityProviderStrategy:    input.CapacityProviderStrategy,
		PlacementConstraints:        input.PlacementConstraints,
		PlacementStrategy:           input.PlacementStrategy,
		ServiceConnectConfiguration: input.ServiceConnectConfiguration,
		DesiredCount:                input.DesiredCount,
		EnableExecuteCommand:        input.EnableExecuteCommand,
	}

	svc.Deployments = []Deployment{newPrimaryDeployment(svc)}

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
		// Create a new PRIMARY deployment and demote the old one to ACTIVE.
		svc.Deployments = rotatePrimaryDeployment(svc)
	}

	if input.DeploymentConfiguration != nil {
		svc.DeploymentConfiguration = input.DeploymentConfiguration
	}

	if len(input.CapacityProviderStrategy) > 0 {
		svc.CapacityProviderStrategy = input.CapacityProviderStrategy
	}

	if len(input.PlacementConstraints) > 0 {
		svc.PlacementConstraints = input.PlacementConstraints
	}

	if len(input.PlacementStrategy) > 0 {
		svc.PlacementStrategy = input.PlacementStrategy
	}

	if input.ServiceConnectConfiguration != nil {
		svc.ServiceConnectConfiguration = input.ServiceConnectConfiguration
	}

	if input.NetworkConfiguration != nil {
		svc.NetworkConfiguration = input.NetworkConfiguration
	}

	if input.PropagateTags != "" {
		svc.PropagateTags = input.PropagateTags
	}

	if len(input.LoadBalancers) > 0 {
		svc.LoadBalancers = input.LoadBalancers
	}

	if input.EnableExecuteCommand != nil {
		svc.EnableExecuteCommand = *input.EnableExecuteCommand
	}

	cp := *svc

	return &cp, nil
}

// rotatePrimaryDeployment demotes the existing PRIMARY deployment to ACTIVE and
// prepends a fresh PRIMARY for the new task definition.
func rotatePrimaryDeployment(svc *Service) []Deployment {
	deployments := make([]Deployment, 0, len(svc.Deployments)+1)
	newPrimary := newActiveDeployment(svc)
	deployments = append(deployments, newPrimary)

	for _, d := range svc.Deployments {
		if d.Status == deploymentStatusPrimary {
			d.Status = statusActive
		}

		deployments = append(deployments, d)
	}

	return deployments
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

// taskWork pairs a task with its definition for lock-free Docker API calls and
// for collecting the final task state when building the API response.
type taskWork struct {
	task *Task
	td   *TaskDefinition
}

// RunTask starts one or more tasks on the given cluster.
func (b *InMemoryBackend) RunTask(input RunTaskInput) ([]Task, error) {
	if input.TaskDefinition == "" {
		return nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	if err := validatePlatformVersion(input.PlatformVersion); err != nil {
		return nil, err
	}

	count := input.Count
	if count <= 0 {
		count = 1
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("RunTask")

	b.ensureClusterLocked(clusterName)

	td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	clusterArn := fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterName)

	launchType := input.LaunchType
	if launchType == "" {
		launchType = launchTypeFargate
	}

	// Resolve task tags respecting propagateTags + ECS-managed-tags semantics.
	// We read tdTags while holding the write lock — safe, no deadlock risk.
	tdTags := b.resourceTags[resourceTagKey(td.TaskDefinitionArn)]
	resolvedTags := resolveTaskTags(
		input.Tags,
		input.PropagateTags,
		input.EnableECSManagedTags,
		clusterName,
		input.serviceNameForTags,
		td,
		tdTags,
		input.serviceTagsForPropagate,
	)

	// Create all task entries in PROVISIONING state under the lock so they are
	// immediately visible, then release the lock before issuing Docker API calls.
	work := b.createTaskEntriesLocked(clusterName, clusterArn, launchType, resolvedTags, count, td, input)

	b.mu.Unlock()

	b.startTasksOutsideLock(work)

	// Snapshot final task states to build the API response.
	b.mu.RLock("RunTask-response")

	tasks := make([]Task, 0, len(work))
	for _, w := range work {
		cp := *w.task
		tasks = append(tasks, cp)
	}

	b.mu.RUnlock()

	return tasks, nil
}

// startTasksOutsideLock starts containers outside the lock to avoid serializing other
// operations behind potentially slow Docker API calls (image pull, network setup, etc.).
func (b *InMemoryBackend) startTasksOutsideLock(work []taskWork) {
	for _, w := range work {
		if b.runner == nil {
			// No runtime: immediately move to RUNNING (simulated).
			b.mu.Lock("RunTask-setRunning")

			if w.task.LastStatus == statusProvisioning {
				w.task.LastStatus = statusRunning
				syncContainerStatuses(w.task, nil)
			}

			b.mu.Unlock()

			continue
		}

		runErr := b.runner.RunTask(w.task, w.td)

		b.mu.Lock("RunTask-setRunning")

		// Only update status if no concurrent operation (e.g. StopTask) has
		// already changed the task away from PROVISIONING.
		if w.task.LastStatus == statusProvisioning {
			if runErr == nil {
				w.task.LastStatus = statusRunning
				syncContainerStatuses(w.task, nil)
			} else {
				// Container start failed — mark STOPPED so the task does not
				// remain in PROVISIONING permanently (resource leak + wrong semantics).
				now := time.Now()
				w.task.LastStatus = statusStopped
				w.task.DesiredStatus = statusStopped
				w.task.StoppedAt = &now
				w.task.StoppedReason = fmt.Sprintf("container start failed: %v", runErr)
				exitCode := 1
				syncContainerStatuses(w.task, &exitCode)
			}
		}

		b.mu.Unlock()
	}
}

// createTaskEntriesLocked creates task entries in PROVISIONING state.
// Must be called with write lock held; the lock is NOT released here.
func (b *InMemoryBackend) createTaskEntriesLocked(
	clusterName, clusterArn, launchType string,
	resolvedTags []Tag,
	count int,
	td *TaskDefinition,
	input RunTaskInput,
) []taskWork {
	work := make([]taskWork, 0, count)

	for range count {
		taskArn := fmt.Sprintf(
			"arn:aws:ecs:%s:%s:task/%s/%s",
			b.region, b.accountID, clusterName, uuid.NewString(),
		)

		now := time.Now()
		task := &Task{
			TaskArn:              taskArn,
			ClusterArn:           clusterArn,
			TaskDefinitionArn:    td.TaskDefinitionArn,
			LastStatus:           statusProvisioning,
			DesiredStatus:        statusRunning,
			Group:                input.Group,
			LaunchType:           launchType,
			StartedBy:            input.StartedBy,
			PlatformVersion:      input.PlatformVersion,
			PropagateTags:        input.PropagateTags,
			Tags:                 resolvedTags,
			StartedAt:            &now,
			Connectivity:         connectivityConnected,
			ConnectivityAt:       &now,
			Overrides:            input.Overrides,
			NetworkConfiguration: input.NetworkConfiguration,
			EnableExecuteCommand: input.EnableExecuteCommand,
		}

		if launchType == launchTypeFargate {
			task.Attachments = []TaskAttachment{newFargateTaskAttachment(taskArn)}
		} else {
			// EC2 launch type: select a container instance respecting placement
			// constraints and strategies, then record it in the reverse index.
			if instanceArn := selectContainerInstance(
				b.containerInstances[clusterName],
				b.tasks[clusterName],
				td.PlacementConstraints,
				nil,
				input.serviceNameForTags,
			); instanceArn != "" {
				task.ContainerInstanceArn = instanceArn
				b.indexTaskOnInstance(clusterName, instanceArn, taskArn)
			}
		}

		task.Containers = buildContainersForTask(task, td)

		b.tasks[clusterName][taskArn] = task
		work = append(work, taskWork{task: task, td: td})
	}

	return work
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

	clusterTasks, ok := b.tasks[clusterName]
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	task, ok := clusterTasks[taskArn]
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskArn)
	}

	now := time.Now()
	task.LastStatus = statusStopped
	task.DesiredStatus = statusStopped
	task.StoppedAt = &now
	task.StoppedReason = reason
	syncContainerStatuses(task, nil)

	instanceArn := task.ContainerInstanceArn
	cp := *task

	// Release the lock before issuing Docker API calls so other backend
	// operations are not serialized behind potentially slow container stops.
	b.mu.Unlock()

	if b.runner != nil {
		_ = b.runner.StopTask(task)
	}

	// Clean up task protection entry and reverse index to avoid stale entries.
	b.mu.Lock("StopTask-cleanup")
	delete(b.taskProtections, taskArn)
	b.unindexTaskFromInstance(clusterName, instanceArn, taskArn)
	b.mu.Unlock()

	return &cp, nil
}

// ListTasksInput holds optional filters for ListTasks.
type ListTasksInput struct {
	Cluster           string
	ContainerInstance string
	Family            string
	ServiceName       string
	DesiredStatus     string
	LaunchType        string
	StartedBy         string
}

func (b *InMemoryBackend) ListTasks(cluster string) ([]string, error) {
	return b.ListTasksFiltered(ListTasksInput{Cluster: cluster})
}

func (b *InMemoryBackend) ListTasksFiltered(input ListTasksInput) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.RLock("ListTasksFiltered")
	defer b.mu.RUnlock()

	clusterTasks, ok := b.tasks[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	arns := make([]string, 0, len(clusterTasks))
	for arn, task := range clusterTasks {
		if input.ContainerInstance != "" && task.ContainerInstanceArn != input.ContainerInstance {
			continue
		}
		if input.DesiredStatus != "" && !strings.EqualFold(task.DesiredStatus, input.DesiredStatus) {
			continue
		}
		if input.LaunchType != "" && !strings.EqualFold(task.LaunchType, input.LaunchType) {
			continue
		}
		if input.StartedBy != "" && task.StartedBy != input.StartedBy {
			continue
		}
		if input.Family != "" && !strings.Contains(task.TaskDefinitionArn, "/"+input.Family+":") {
			continue
		}
		if input.ServiceName != "" && task.Group != "service:"+input.ServiceName {
			continue
		}
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
// It reads the service's PropagateTags, Tags, and EnableECSManagedTags settings so
// that tasks spawned by the reconciler honour the same tag propagation as tasks
// started directly via RunTask.
func (b *InMemoryBackend) StartTaskForService(clusterName, serviceName, taskDefinitionArn string) error {
	// Snapshot service tag config without holding the lock during RunTask.
	b.mu.RLock("StartTaskForService-svcSnap")

	var svcPropagateTags string
	var svcTags []Tag
	var svcEnableExec bool

	if svcs, ok := b.services[clusterName]; ok {
		if svc, found := svcs[serviceName]; found {
			svcPropagateTags = svc.PropagateTags
			svcTags = copyTags(svc.Tags)
			svcEnableExec = svc.EnableExecuteCommand
		}
	}

	b.mu.RUnlock()

	_, err := b.RunTask(RunTaskInput{
		Cluster:                 clusterName,
		TaskDefinition:          taskDefinitionArn,
		Count:                   1,
		Group:                   "service:" + serviceName,
		PropagateTags:           svcPropagateTags,
		serviceNameForTags:      serviceName,
		serviceTagsForPropagate: svcTags,
		EnableExecuteCommand:    svcEnableExec,
	})

	return err
}

// StopOldestServiceTask stops the oldest running task for a service.
func (b *InMemoryBackend) StopOldestServiceTask(clusterName, serviceName string) error {
	b.mu.Lock("StopOldestServiceTask")

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
		b.mu.Unlock()

		return nil
	}

	now := time.Now()
	oldest.LastStatus = statusStopped
	oldest.DesiredStatus = statusStopped
	oldest.StoppedAt = &now
	oldest.StoppedReason = "service scale-in"
	syncContainerStatuses(oldest, nil)

	instanceArn := oldest.ContainerInstanceArn
	taskArn := oldest.TaskArn

	// Release the lock before issuing Docker API calls so other backend
	// operations are not serialized behind potentially slow container stops.
	b.mu.Unlock()

	if b.runner != nil {
		_ = b.runner.StopTask(oldest)
	}

	b.mu.Lock("StopOldestServiceTask-unindex")
	b.unindexTaskFromInstance(clusterName, instanceArn, taskArn)
	b.mu.Unlock()

	return nil
}

// noopRunner is a TaskRunner that does nothing (used when no runtime is configured).
type noopRunner struct{}

func (noopRunner) RunTask(_ *Task, _ *TaskDefinition) error { return nil }
func (noopRunner) StopTask(_ *Task) error                   { return nil }

// NewNoopRunner returns a TaskRunner that does nothing.
func NewNoopRunner() TaskRunner { return noopRunner{} }
