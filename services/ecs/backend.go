package ecs

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusRunning           = "RUNNING"
	statusStopped           = "STOPPED"
	statusActive            = "ACTIVE"
	statusInactive          = "INACTIVE"
	statusProvisioning      = "PROVISIONING"
	statusPending           = "PENDING"
	statusDeactivating      = "DEACTIVATING"
	statusStopping          = "STOPPING"
	statusDeprovisioning    = "DEPROVISIONING"
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
	// ErrClient is returned when a request is structurally invalid in a way that
	// AWS ECS reports as a ClientException (for example, malformed container
	// definitions or an unsupported network mode / launch-type combination).
	ErrClient = awserr.New("ClientException", awserr.ErrInvalidParameter)
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
	RuntimePlatform         *RuntimePlatform       `json:"runtimePlatform,omitempty"`
	EphemeralStorage        *EphemeralStorage      `json:"ephemeralStorage,omitempty"`
	RegisteredAt            time.Time              `json:"registeredAt"`
	TaskDefinitionArn       string                 `json:"taskDefinitionArn"`
	Family                  string                 `json:"family"`
	TaskRoleArn             string                 `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn        string                 `json:"executionRoleArn,omitempty"`
	NetworkMode             string                 `json:"networkMode,omitempty"`
	Status                  string                 `json:"status"`
	PlatformFamily          string                 `json:"platformFamily,omitempty"`
	CPU                     string                 `json:"cpu,omitempty"`
	Memory                  string                 `json:"memory,omitempty"`
	ContainerDefinitions    []ContainerDefinition  `json:"containerDefinitions"`
	Volumes                 []Volume               `json:"volumes,omitempty"`
	PlacementConstraints    []PlacementConstraint  `json:"placementConstraints,omitempty"`
	RequiresCompatibilities []string               `json:"requiresCompatibilities,omitempty"`
	InferenceAccelerators   []InferenceAccelerator `json:"inferenceAccelerators,omitempty"`
	Revision                int                    `json:"revision"`
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
	// TaskRoleArn is the effective IAM role ARN for task containers.
	// Resolved from Overrides.TaskRoleArn if set, else from the task definition.
	TaskRoleArn          string           `json:"taskRoleArn,omitempty"`
	Tags                 []Tag            `json:"tags,omitempty"`
	Attachments          []TaskAttachment `json:"attachments,omitempty"`
	Containers           []Container      `json:"containers,omitempty"`
	EnableExecuteCommand bool             `json:"enableExecuteCommand,omitempty"`
}

// CreateClusterInput holds input for CreateCluster.
type CreateClusterInput struct {
	ClusterName                     string
	Settings                        []ClusterSetting
	CapacityProviders               []string
	DefaultCapacityProviderStrategy []CapacityProviderStrategyItem
	Tags                            []Tag
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
	PlacementConstraints    []PlacementConstraint `json:"placementConstraints,omitempty"`
	PlacementStrategy       []PlacementStrategy   `json:"placementStrategy,omitempty"`
	Count                   int                   `json:"count,omitempty"`
	EnableECSManagedTags    bool                  `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand    bool                  `json:"enableExecuteCommand,omitempty"`
}

// compile-time assertion.
var _ Backend = (*InMemoryBackend)(nil)

// svcRef is a composite key identifying a service by its cluster key and service name.
type svcRef struct {
	cluster string
	name    string
}

// InMemoryBackend stores ECS state in memory.
//
// Field ordering is optimised for pointer-byte alignment (govet fieldalignment);
// keep new fields grouped with their kind rather than by logical concern.
type InMemoryBackend struct {
	runner TaskRunner
	// elbv2Registrar, when set (see SetELBv2Registrar), registers/deregisters
	// real ELBv2 targets as tasks belonging to a service with LoadBalancers
	// reach/leave RUNNING. Nil preserves the historical behavior of
	// Service.LoadBalancers being stored and echoed with no effect on ELBv2.
	elbv2Registrar ELBv2TargetRegistrar
	// registry is the Phase 3.3 datalayer lifecycle registry: every *store.Table
	// below (except taskDefByArn/daemonTaskDefByArn, which are derived caches --
	// see store_setup.go) is registered on it exactly once at construction, so
	// Reset/Snapshot/Restore collapse to one registry call each instead of one
	// hand-written block per map. See pkgs/store's package doc.
	registry                    *store.Registry
	serviceDeployments          *store.Table[ServiceDeployment]
	taskSets                    *store.Table[TaskSet]
	taskSetsByService           *store.Index[TaskSet]
	taskDefByArn                *store.Table[TaskDefinition]
	services                    *store.Table[Service]
	servicesByCluster           *store.Index[Service]
	tasks                       *store.Table[Task]
	tasksByCluster              *store.Index[Task]
	containerInstances          *store.Table[ContainerInstance]
	containerInstancesByCluster *store.Index[ContainerInstance]
	clusters                    *store.Table[Cluster]
	taskProtections             *store.Table[TaskProtection]
	capacityProviders           *store.Table[CapacityProvider]
	accountSettings             *store.Table[AccountSetting]
	taskDefinitions             map[string][]*TaskDefinition
	attributes                  map[string]map[string]*Attribute
	mu                          *lockmetrics.RWMutex
	resourceTags                map[string][]Tag
	tasksByInstance             map[string]map[string]map[string]bool
	serviceIndex                map[svcRef]bool
	expressGatewayServices      *store.Table[ExpressGatewayService]
	daemonRevisions             *store.Table[DaemonRevision]
	daemonDeployments           *store.Table[DaemonDeployment]
	daemons                     *store.Table[Daemon] // composite "clusterName/daemonName" key; see daemonsByCluster
	daemonsByCluster            *store.Index[Daemon]
	daemonTaskDefinitions       map[string][]*DaemonTaskDefinition
	daemonTaskDefByArn          *store.Table[DaemonTaskDefinition]
	// daemonTaskDefs exists only for structural compatibility with purge.go's
	// per-cluster daemon cleanup (purgeDaemonsLocked). Real daemon task
	// definitions are registered independently by family, like ordinary task
	// definitions — not owned by a single daemon — matching the real
	// RegisterDaemonTaskDefinition/DescribeDaemonTaskDefinition API (see
	// daemonTaskDefinitions/daemonTaskDefByArn and backend_daemon.go), so this
	// map is intentionally never populated.
	daemonTaskDefs map[string][]*DaemonTaskDefinition
	// lifecycle tracks tasks transitioning through observable intermediate states
	// (RUNNING→DEACTIVATING→STOPPING→DEPROVISIONING→STOPPED on stop,
	// PROVISIONING→PENDING→RUNNING on start), keyed by task ARN. Entries exist
	// only while stopDelay/startDelay > 0; the default fast path finalizes inline.
	lifecycle map[string]*taskLifecycle
	// serviceRevisions and serviceRevisionsByArn exist only for structural
	// compatibility with purge.go's per-service cleanup. This backend derives
	// ServiceRevision snapshots on demand from each Service's Deployments (see
	// DescribeServiceRevisions below and buildServiceRevision in
	// backend_parity2.go) instead of persisting them separately, so these maps
	// are intentionally never populated.
	serviceRevisions      map[string][]*ServiceRevision
	serviceRevisionsByArn map[string]*ServiceRevision
	region                string
	accountID             string
	// clusterDeleteHooks are invoked (outside the backend lock) with each cluster
	// key removed by DeleteCluster or Purge, so external components such as the
	// Reconciler can release per-cluster resources and avoid unbounded growth.
	clusterDeleteHooks []func(clusterName string)
	// stopDelay is the per-phase delay applied to the task stop lifecycle. Zero
	// (the default) finalizes to STOPPED immediately; positive makes the task pass
	// through DEACTIVATING/STOPPING/DEPROVISIONING so SDK waiters observe them.
	stopDelay time.Duration
	// startDelay is the per-phase delay applied to the no-runner task start
	// lifecycle (PROVISIONING→PENDING→RUNNING). Zero finalizes immediately.
	startDelay time.Duration
	hooksMu    sync.Mutex
}

// TaskRunner is the interface for launching container tasks.
// The no-op implementation is used when no runtime is configured.
type TaskRunner interface {
	RunTask(task *Task, td *TaskDefinition) error
	StopTask(task *Task) error
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string, runner TaskRunner) *InMemoryBackend {
	b := &InMemoryBackend{
		taskDefinitions:       make(map[string][]*TaskDefinition),
		attributes:            make(map[string]map[string]*Attribute),
		resourceTags:          make(map[string][]Tag),
		tasksByInstance:       make(map[string]map[string]map[string]bool),
		serviceIndex:          make(map[svcRef]bool),
		lifecycle:             make(map[string]*taskLifecycle),
		mu:                    lockmetrics.New("ecs"),
		accountID:             accountID,
		region:                region,
		runner:                runner,
		daemonTaskDefinitions: make(map[string][]*DaemonTaskDefinition),
		daemonTaskDefs:        make(map[string][]*DaemonTaskDefinition),
		serviceRevisions:      make(map[string][]*ServiceRevision),
		serviceRevisionsByArn: make(map[string]*ServiceRevision),
		registry:              store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Reset zeroes all backend state for test isolation.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.taskDefByArn.Reset()
	b.daemonTaskDefByArn.Reset()

	b.taskDefinitions = make(map[string][]*TaskDefinition)
	b.attributes = make(map[string]map[string]*Attribute)
	b.resourceTags = make(map[string][]Tag)
	b.tasksByInstance = make(map[string]map[string]map[string]bool)
	b.serviceIndex = make(map[svcRef]bool)
	b.daemonTaskDefinitions = make(map[string][]*DaemonTaskDefinition)
	b.daemonTaskDefs = make(map[string][]*DaemonTaskDefinition)
	b.serviceRevisions = make(map[string][]*ServiceRevision)
	b.serviceRevisionsByArn = make(map[string]*ServiceRevision)
	b.lifecycle = make(map[string]*taskLifecycle)
}

// RegisterClusterDeleteHook registers a callback invoked (outside the backend
// lock) with the key of each cluster removed by DeleteCluster or Purge. It lets
// external components — such as the Reconciler's per-cluster launch semaphores —
// release cluster-scoped resources and avoid unbounded growth.
func (b *InMemoryBackend) RegisterClusterDeleteHook(fn func(clusterName string)) {
	if fn == nil {
		return
	}

	b.hooksMu.Lock()
	defer b.hooksMu.Unlock()

	b.clusterDeleteHooks = append(b.clusterDeleteHooks, fn)
}

// fireClusterDeleteHooks invokes every registered cluster-delete hook for each
// removed cluster key. Must be called without the backend lock held (hooks may
// take their own locks).
func (b *InMemoryBackend) fireClusterDeleteHooks(clusterNames ...string) {
	if len(clusterNames) == 0 {
		return
	}

	b.hooksMu.Lock()
	hooks := make([]func(string), len(b.clusterDeleteHooks))
	copy(hooks, b.clusterDeleteHooks)
	b.hooksMu.Unlock()

	for _, name := range clusterNames {
		for _, h := range hooks {
			h(name)
		}
	}
}

// GetRegion returns the AWS region this backend is configured for.
func (b *InMemoryBackend) GetRegion() string {
	return b.region
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

	if b.clusters.Has(name) {
		return nil, fmt.Errorf("%w: %s", ErrClusterAlreadyExists, name)
	}

	cluster := &Cluster{
		CreatedAt:                       time.Now(),
		ClusterArn:                      arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("cluster/%s", name)),
		ClusterName:                     name,
		Status:                          statusActive,
		Settings:                        input.Settings,
		CapacityProviders:               input.CapacityProviders,
		DefaultCapacityProviderStrategy: input.DefaultCapacityProviderStrategy,
	}
	b.clusters.Put(cluster)

	if len(input.Tags) > 0 {
		b.setResourceTagsLocked(cluster.ClusterArn, input.Tags)
	}

	cp := *cluster

	return &cp, nil
}

// ListClusters returns all clusters.
func (b *InMemoryBackend) ListClusters() ([]Cluster, error) {
	clusters, _, err := b.DescribeClusters(nil)

	return clusters, err
}

// DescribeClusters returns cluster metadata.
// Unknown cluster names are returned as failures, not errors, matching AWS behaviour.
func (b *InMemoryBackend) DescribeClusters(clusterNames []string) ([]Cluster, []Failure, error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if len(clusterNames) == 0 {
		all := b.clusters.All()
		out := make([]Cluster, 0, len(all))
		for _, c := range all {
			out = append(out, b.enrichCluster(c))
		}

		return out, nil, nil
	}

	out := make([]Cluster, 0, len(clusterNames))
	failures := make([]Failure, 0, len(clusterNames))

	for _, name := range clusterNames {
		key := clusterKey(name)

		c, ok := b.clusters.Get(key)
		if !ok {
			failures = append(failures, Failure{
				Arn:    name,
				Reason: statusMissing,
				Detail: fmt.Sprintf("cluster %s not found", name),
			})

			continue
		}

		out = append(out, b.enrichCluster(c))
	}

	return out, failures, nil
}

// enrichCluster fills in runtime-computed counts for a cluster.
// Must be called with at least an RLock held.
// Running and pending task counts are cached on the cluster struct and updated
// incrementally at each state transition to avoid an O(n) task scan here.
func (b *InMemoryBackend) enrichCluster(c *Cluster) Cluster {
	cp := *c

	cp.ActiveServicesCount = len(b.servicesByCluster.Get(c.ClusterName))
	cp.RegisteredContainerInstancesCount = len(b.containerInstancesByCluster.Get(c.ClusterName))

	// RunningTasksCount and PendingTasksCount are maintained as cached counters
	// on the Cluster struct. No task iteration needed here.

	return cp
}

// DeleteCluster removes a cluster.
func (b *InMemoryBackend) DeleteCluster(clusterName string) (*Cluster, error) {
	key := clusterKey(clusterName)

	b.mu.Lock("DeleteCluster")

	c, ok := b.clusters.Get(key)
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	// Snapshot task pointers while still holding the lock so we can stop their
	// Docker containers after releasing it.  Performing Docker API calls under
	// the backend lock would unnecessarily serialize all other operations.
	clusterTasks := b.tasksInClusterLocked(key)
	tasksToStop := make([]*Task, 0, len(clusterTasks))

	if b.runner != nil {
		tasksToStop = append(tasksToStop, clusterTasks...)
	}

	// Delete task sets and service deployments for all services in this cluster
	// before removing the services map, preventing stale entries on cluster recreation.
	for _, svc := range b.servicesInClusterLocked(key) {
		b.deleteTaskSetsForServiceLocked(svc.ServiceArn)
		b.deleteServiceDeploymentsForServiceLocked(svc.ServiceArn)
		delete(b.serviceIndex, svcRef{cluster: key, name: svc.ServiceName})
	}

	// Clean up per-task state for all tasks in this cluster to avoid memory leaks.
	for _, task := range clusterTasks {
		b.taskProtections.Delete(task.TaskArn)
		delete(b.lifecycle, task.TaskArn)
	}

	b.clusters.Delete(key)
	b.deleteServicesForClusterLocked(key)
	b.deleteTasksForClusterLocked(key)
	b.deleteContainerInstancesForClusterLocked(key)
	delete(b.attributes, key)
	delete(b.tasksByInstance, key)

	cp := *c

	// Release the lock before issuing Docker API calls so other backend
	// operations are not serialized behind potentially slow container stops.
	b.mu.Unlock()

	for _, task := range tasksToStop {
		_ = b.runner.StopTask(task)
	}

	// Notify hooks (e.g. reconciler semaphore eviction) after releasing the lock.
	b.fireClusterDeleteHooks(key)

	return &cp, nil
}

// RegisterTaskDefinition registers a new task definition revision.
func (b *InMemoryBackend) RegisterTaskDefinition(
	input RegisterTaskDefinitionInput,
) (*TaskDefinition, error) {
	if input.Family == "" {
		return nil, fmt.Errorf("%w: family is required", ErrInvalidParameter)
	}

	if err := validateRegisterTaskDefinition(input); err != nil {
		return nil, err
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
			b.taskDefByArn.Delete(evicted.TaskDefinitionArn)
		}

		revisions = revisions[excess:]
	}

	b.taskDefinitions[input.Family] = revisions
	b.taskDefByArn.Put(td)

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
	if td, ok := b.taskDefByArn.Get(familyOrArn); ok {
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
func (b *InMemoryBackend) DeregisterTaskDefinition(
	taskDefinitionArn string,
) (*TaskDefinition, error) {
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
func (b *InMemoryBackend) ListTaskDefinitionsFiltered(
	input ListTaskDefinitionsInput,
) ([]string, error) {
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
	if !b.clusters.Has(clusterName) && clusterName == defaultCluster {
		b.clusters.Put(&Cluster{
			CreatedAt: time.Now(),
			ClusterArn: arn.Build(
				"ecs",
				b.region,
				b.accountID,
				fmt.Sprintf("cluster/%s", clusterName),
			),
			ClusterName: clusterName,
			Status:      statusActive,
		})
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

	if b.services.Has(scopedKey(clusterName, input.ServiceName)) {
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
		ServiceName: input.ServiceName,
		ClusterArn: arn.Build(
			"ecs",
			b.region,
			b.accountID,
			fmt.Sprintf("cluster/%s", clusterName),
		),
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

	b.services.Put(svc)
	b.serviceIndex[svcRef{cluster: clusterName, name: input.ServiceName}] = true

	b.addServiceRevisionLocked(svc)
	b.syncServiceDeploymentsLocked(svc)

	cp := *svc

	return &cp, nil
}

// DescribeServices returns services for the given cluster, optionally filtered by name.
// Unknown service names are returned as failures, not errors, matching AWS behaviour.
func (b *InMemoryBackend) DescribeServices(
	cluster string,
	serviceNames []string,
) ([]Service, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeServices")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(serviceNames) == 0 {
		svcs := b.servicesByCluster.Get(clusterName)
		out := make([]Service, 0, len(svcs))
		for _, s := range svcs {
			out = append(out, b.enrichService(s, clusterName))
		}

		return out, nil, nil
	}

	out := make([]Service, 0, len(serviceNames))
	failures := make([]Failure, 0, len(serviceNames))

	for _, name := range serviceNames {
		// Support ARN lookup by extracting the service name.
		key := serviceKey(name)

		s, found := b.services.Get(scopedKey(clusterName, key))
		if !found {
			failures = append(failures, Failure{
				Arn:    name,
				Reason: statusMissing,
				Detail: fmt.Sprintf("service %s not found", name),
			})

			continue
		}

		out = append(out, b.enrichService(s, clusterName))
	}

	return out, failures, nil
}

// DescribeServiceRevisions returns the service revisions captured by past and
// current deployments across all clusters that match the given ARNs.
// Unknown ARNs are reported as failures, matching AWS behaviour for batch describes.
func (b *InMemoryBackend) DescribeServiceRevisions(arns []string) ([]ServiceRevision, []Failure) {
	b.mu.RLock("DescribeServiceRevisions")
	defer b.mu.RUnlock()

	byArn := make(map[string]ServiceRevision)

	for _, svc := range b.services.All() {
		for _, d := range svc.Deployments {
			if d.ServiceRevisionArn == "" {
				continue
			}

			byArn[d.ServiceRevisionArn] = buildServiceRevision(svc, d)
		}
	}

	out := make([]ServiceRevision, 0, len(arns))
	failures := make([]Failure, 0)

	for _, arn := range arns {
		rev, ok := byArn[arn]
		if !ok {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("service revision %s not found", arn),
			})

			continue
		}

		out = append(out, rev)
	}

	return out, failures
}

// DiscoverPollEndpoint returns the ECS agent poll, Service Connect, and telemetry
// endpoints for the configured region. Real ECS agents use this to discover which
// regional endpoint to poll for task state updates.
func (b *InMemoryBackend) DiscoverPollEndpoint() (string, string, string) {
	b.mu.RLock("DiscoverPollEndpoint")
	defer b.mu.RUnlock()

	base := fmt.Sprintf("ecs-a-1.%s.amazonaws.com", b.region)

	return fmt.Sprintf("https://%s/", base),
		fmt.Sprintf("https://ecs-a-1.svc.%s.amazonaws.com/", b.region),
		fmt.Sprintf("https://ecs-t-1.%s.amazonaws.com/", b.region)
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

// enrichService fills in runtime-computed counts for a service and
// updates each deployment's RunningCount/PendingCount + RolloutState.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) enrichService(s *Service, clusterName string) Service {
	cp := *s

	running := 0
	pending := 0

	// Per-deployment counters keyed by task definition ARN.
	deplRunning := make(map[string]int, len(s.Deployments))
	deplPending := make(map[string]int, len(s.Deployments))

	for _, t := range b.tasksByCluster.Get(clusterName) {
		if t.Group == "service:"+s.ServiceName {
			switch t.LastStatus {
			case statusRunning:
				running++
				deplRunning[t.TaskDefinitionArn]++
			case statusPending, statusProvisioning:
				pending++
				deplPending[t.TaskDefinitionArn]++
			}
		}
	}

	cp.RunningCount = running
	cp.PendingCount = pending

	// Update per-deployment counts and advance RolloutState to COMPLETED
	// when the PRIMARY deployment has reached its desired count.
	deployments := make([]Deployment, len(s.Deployments))

	for i, d := range s.Deployments {
		d.RunningCount = deplRunning[d.TaskDefinition]
		d.PendingCount = deplPending[d.TaskDefinition]

		if d.Status == deploymentStatusPrimary &&
			d.RolloutState == deploymentRolloutStateInProgress &&
			d.RunningCount >= d.DesiredCount && d.DesiredCount > 0 {
			d.RolloutState = deploymentRolloutStateCompleted
			d.RolloutStateReason = fmt.Sprintf(
				"ECS deployment ecs-svc completed. %d out of %d tasks running.",
				d.RunningCount, d.DesiredCount,
			)
		}

		deployments[i] = d
	}

	cp.Deployments = deployments

	return cp
}

// UpdateService updates an existing ECS service.

func applyServiceConfigUpdates(svc *Service, input UpdateServiceInput) {
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
}

func (b *InMemoryBackend) UpdateService(input UpdateServiceInput) (*Service, error) {
	if input.Service == "" {
		return nil, fmt.Errorf("%w: service is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))
	serviceKey := serviceKey(input.Service)

	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	svc, ok := b.services.Get(scopedKey(clusterName, serviceKey))
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

	applyServiceConfigUpdates(svc, input)

	b.addServiceRevisionLocked(svc)
	b.syncServiceDeploymentsLocked(svc)

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

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	svc, ok := b.services.Get(scopedKey(clusterName, key))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, serviceName)
	}

	b.services.Delete(scopedKey(clusterName, key))
	b.deleteTaskSetsForServiceLocked(svc.ServiceArn)
	delete(b.serviceIndex, svcRef{cluster: clusterName, name: key})
	b.deleteServiceDeploymentsForServiceLocked(svc.ServiceArn)

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

	clusterArn := arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("cluster/%s", clusterName))

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
	work := b.createTaskEntriesLocked(
		clusterName,
		clusterArn,
		launchType,
		resolvedTags,
		count,
		td,
		input,
	)

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
		clusterName := clusterKey(clusterFromTaskARN(w.task.TaskArn))

		if b.runner == nil {
			if b.maybeRegisterStartLifecycle(w.task, clusterName) {
				continue
			}

			b.applyNoRunnerTransition(w.task, clusterName)

			continue
		}

		// Transition PROVISIONING → PENDING before the potentially-slow container
		// runtime call, then PENDING → RUNNING/STOPPED based on the result.
		b.applyPendingTransition(w.task)
		runErr := b.runner.RunTask(w.task, w.td)
		b.applyRunnerTransition(w.task, clusterName, runErr)
	}
}

// applyPendingTransition moves a PROVISIONING task to PENDING under the lock.
// Called for tasks with a real container runner, before the runner is invoked.
func (b *InMemoryBackend) applyPendingTransition(task *Task) {
	b.mu.Lock("RunTask-setPending")
	defer b.mu.Unlock()

	if task.LastStatus == statusProvisioning {
		task.LastStatus = statusPending
	}
}

// maybeRegisterStartLifecycle enrolls a no-runner task in the observable start
// pipeline when a start delay is configured, returning true if it did. When it
// returns false the caller applies the immediate transition instead. Must be
// called without any lock held.
func (b *InMemoryBackend) maybeRegisterStartLifecycle(task *Task, clusterName string) bool {
	b.mu.Lock("RunTask-registerStartLifecycle")
	defer b.mu.Unlock()

	if b.startDelay <= 0 || task.LastStatus != statusProvisioning {
		return false
	}

	b.registerStartLifecycleLocked(task, clusterName)

	return true
}

// applyNoRunnerTransition transitions a PROVISIONING task through PENDING to RUNNING
// when no container runtime is configured. Must be called without any lock held.
func (b *InMemoryBackend) applyNoRunnerTransition(task *Task, clusterName string) {
	b.mu.Lock("RunTask-setRunning")
	defer b.mu.Unlock()

	if task.LastStatus != statusProvisioning {
		return
	}

	// Pass through PENDING (resource provisioning complete, container starting).
	task.LastStatus = statusPending
	// Immediately advance to RUNNING since there is no real container to wait for.
	task.LastStatus = statusRunning
	syncContainerStatuses(task, nil)

	if c, _ := b.clusters.Get(clusterName); c != nil {
		c.PendingTasksCount--
		c.RunningTasksCount++
	}

	b.registerTaskWithELBv2Locked(task, clusterName)
}

// applyRunnerTransition transitions a PENDING task to RUNNING or STOPPED
// based on the container runtime result. Must be called without any lock held.
func (b *InMemoryBackend) applyRunnerTransition(task *Task, clusterName string, runErr error) {
	b.mu.Lock("RunTask-setRunning")
	defer b.mu.Unlock()

	// Only update status if no concurrent operation (e.g. StopTask) has
	// already changed the task away from PENDING. A task enters PENDING just
	// before the runner call via applyPendingTransition.
	if task.LastStatus != statusPending {
		return
	}

	if runErr == nil {
		task.LastStatus = statusRunning
		syncContainerStatuses(task, nil)

		if c, _ := b.clusters.Get(clusterName); c != nil {
			c.PendingTasksCount--
			c.RunningTasksCount++
		}

		b.registerTaskWithELBv2Locked(task, clusterName)

		return
	}

	// Container start failed — mark STOPPED so the task does not
	// remain in PROVISIONING permanently (resource leak + wrong semantics).
	now := time.Now()
	task.LastStatus = statusStopped
	task.DesiredStatus = statusStopped
	task.StoppedAt = &now
	task.StoppedReason = fmt.Sprintf("container start failed: %v", runErr)
	exitCode := 1
	syncContainerStatuses(task, &exitCode)

	if c, _ := b.clusters.Get(clusterName); c != nil {
		c.PendingTasksCount--
	}

	// A failed launch for a service task counts against the deployment circuit
	// breaker, which may trip the deployment to FAILED and (if enabled) roll the
	// service back to its last stable task definition.
	b.recordServiceTaskFailureLocked(clusterName, task)
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

		// Resolve the effective task IAM role: per-run override takes precedence.
		taskRoleArn := td.TaskRoleArn
		if input.Overrides != nil && input.Overrides.TaskRoleArn != "" {
			taskRoleArn = input.Overrides.TaskRoleArn
		}

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
			TaskRoleArn:          taskRoleArn,
		}

		if launchType == launchTypeFargate {
			task.Attachments = []TaskAttachment{newFargateTaskAttachment(taskArn)}
		} else {
			// EC2 launch type: select a container instance respecting placement
			// constraints and strategies, then record it in the reverse index.
			// Merge task-definition constraints with any run-time override constraints.
			constraints := mergeConstraints(td.PlacementConstraints, input.PlacementConstraints)
			if instanceArn := selectContainerInstance(
				b.containerInstancesByCluster.Get(clusterName),
				b.tasksByCluster.Get(clusterName),
				constraints,
				input.PlacementStrategy,
				input.serviceNameForTags,
			); instanceArn != "" {
				task.ContainerInstanceArn = instanceArn
				b.indexTaskOnInstance(clusterName, instanceArn, taskArn)
			}
		}

		task.Containers = buildContainersForTask(task, td)

		b.tasks.Put(task)
		work = append(work, taskWork{task: task, td: td})

		// Increment the cached pending counter on the cluster.
		if c, _ := b.clusters.Get(clusterName); c != nil {
			c.PendingTasksCount++
		}
	}

	return work
}

// DescribeTasks returns tasks on a given cluster, optionally filtered by ARN.
// Unknown task ARNs are returned as failures, not errors, matching AWS behaviour.
func (b *InMemoryBackend) DescribeTasks(
	cluster string,
	taskArns []string,
) ([]Task, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("DescribeTasks")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(taskArns) == 0 {
		clusterTasks := b.tasksByCluster.Get(clusterName)
		out := make([]Task, 0, len(clusterTasks))
		for _, t := range clusterTasks {
			out = append(out, *t)
		}

		return out, nil, nil
	}

	out := make([]Task, 0, len(taskArns))
	failures := make([]Failure, 0, len(taskArns))

	for _, arn := range taskArns {
		t, found := b.tasks.Get(arn)
		if !found || clusterKey(t.ClusterArn) != clusterName {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("task %s not found", arn),
			})

			continue
		}

		out = append(out, *t)
	}

	return out, failures, nil
}

// StopTask stops a running task.
func (b *InMemoryBackend) StopTask(cluster, taskArn, reason string) (*Task, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("StopTask")

	if !b.clusters.Has(clusterName) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	task, ok := b.tasks.Get(taskArn)
	if !ok || clusterKey(task.ClusterArn) != clusterName {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskArn)
	}

	now := time.Now()
	prevStatus := task.LastStatus
	task.DesiredStatus = statusStopped
	task.StoppedReason = reason

	// Decrement the cached cluster counters once, as the task leaves its active
	// state. This is done up front for both the fast and delayed paths so the
	// counters stay correct regardless of when the task finally reaches STOPPED.
	if c, _ := b.clusters.Get(clusterName); c != nil {
		switch prevStatus {
		case statusRunning:
			c.RunningTasksCount--
		case statusProvisioning, statusPending:
			c.PendingTasksCount--
		}
	}

	// Delayed path: leave the task in DEACTIVATING and let the lifecycle stepper
	// advance it through STOPPING/DEPROVISIONING/STOPPED so SDK waiters observe
	// the intermediate states. Only used when a stop delay is configured and the
	// task is actually in an active state.
	if b.stopDelay > 0 && isStoppableStatus(prevStatus) {
		task.LastStatus = statusDeactivating
		b.lifecycle[taskArn] = &taskLifecycle{
			clusterName: clusterName,
			kind:        lifecycleKindStop,
			phase:       statusDeactivating,
			nextAt:      now.Add(b.stopDelay),
			reason:      reason,
		}

		cp := *task
		b.mu.Unlock()

		return &cp, nil
	}

	// Fast path: transition straight to STOPPED.
	task.LastStatus = statusStopped
	task.StoppedAt = &now
	syncContainerStatuses(task, nil)
	b.deregisterTaskFromELBv2Locked(task, clusterName)

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
	b.taskProtections.Delete(taskArn)
	b.unindexTaskFromInstance(clusterName, instanceArn, taskArn)
	b.mu.Unlock()

	return &cp, nil
}

// isStoppableStatus reports whether a task in the given state has an active
// lifecycle that can be observably wound down (as opposed to one that is already
// stopped or mid-transition).
func isStoppableStatus(status string) bool {
	switch status {
	case statusRunning, statusPending, statusProvisioning:
		return true
	default:
		return false
	}
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

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	clusterTasks := b.tasksByCluster.Get(clusterName)
	arns := make([]string, 0, len(clusterTasks))
	for _, task := range clusterTasks {
		if input.ContainerInstance != "" && task.ContainerInstanceArn != input.ContainerInstance {
			continue
		}
		if input.DesiredStatus != "" &&
			!strings.EqualFold(task.DesiredStatus, input.DesiredStatus) {
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
		arns = append(arns, task.TaskArn)
	}

	return arns, nil
}

// getServicesForReconciler returns a snapshot of all services for the reconciler.
// Uses the flat serviceIndex for O(len) single-pass iteration without nested loops
// or a pre-counting pass.
func (b *InMemoryBackend) getServicesForReconciler() []serviceSnapshot {
	b.mu.RLock("GetServicesForReconciler")
	defer b.mu.RUnlock()

	out := make([]serviceSnapshot, 0, len(b.serviceIndex))

	for ref := range b.serviceIndex {
		svc, _ := b.services.Get(scopedKey(ref.cluster, ref.name))
		out = append(out, serviceSnapshot{
			clusterName: ref.cluster,
			service:     *svc,
		})
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

	for _, t := range b.tasksByCluster.Get(clusterName) {
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
func (b *InMemoryBackend) StartTaskForService(
	clusterName, serviceName, taskDefinitionArn string,
) error {
	// Snapshot service config without holding the lock during RunTask.
	b.mu.RLock("StartTaskForService-svcSnap")

	var svcPropagateTags string
	var svcTags []Tag
	var svcEnableExec bool
	var svcLaunchType string
	var svcPlacementConstraints []PlacementConstraint
	var svcPlacementStrategy []PlacementStrategy

	if svc, found := b.services.Get(scopedKey(clusterName, serviceName)); found {
		svcPropagateTags = svc.PropagateTags
		svcTags = copyTags(svc.Tags)
		svcEnableExec = svc.EnableExecuteCommand
		svcLaunchType = svc.LaunchType
		svcPlacementConstraints = svc.PlacementConstraints
		svcPlacementStrategy = svc.PlacementStrategy
	}

	b.mu.RUnlock()

	_, err := b.RunTask(RunTaskInput{
		Cluster:                 clusterName,
		TaskDefinition:          taskDefinitionArn,
		Count:                   1,
		Group:                   "service:" + serviceName,
		LaunchType:              svcLaunchType,
		PropagateTags:           svcPropagateTags,
		serviceNameForTags:      serviceName,
		serviceTagsForPropagate: svcTags,
		EnableExecuteCommand:    svcEnableExec,
		PlacementConstraints:    svcPlacementConstraints,
		PlacementStrategy:       svcPlacementStrategy,
	})

	return err
}

// StopOldestServiceTask stops the oldest running task for a service.
func (b *InMemoryBackend) StopOldestServiceTask(clusterName, serviceName string) error {
	b.mu.Lock("StopOldestServiceTask")

	group := "service:" + serviceName

	var oldest *Task

	for _, t := range b.tasksByCluster.Get(clusterName) {
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
	b.deregisterTaskFromELBv2Locked(oldest, clusterName)

	// Decrement the cached running counter (scale-in always stops a running task).
	if c, _ := b.clusters.Get(clusterName); c != nil {
		c.RunningTasksCount--
	}

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
