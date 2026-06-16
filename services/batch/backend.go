package batch

import (
	"encoding/base64"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusValid = "VALID"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ClientException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request contains invalid parameters.
	ErrValidation = awserr.New("ClientException", awserr.ErrInvalidParameter)
)

const (
	jobDefStatusActive   = "ACTIVE"
	jobDefStatusInactive = "INACTIVE"

	jobStatusSubmitted = "SUBMITTED"
	jobStatusPending   = "PENDING"
	jobStatusRunnable  = "RUNNABLE"
	jobStatusStarting  = "STARTING"
	jobStatusRunning   = "RUNNING"
	jobStatusSucceeded = "SUCCEEDED"
	jobStatusFailed    = "FAILED"

	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"

	resourceTypeReplenishable    = "REPLENISHABLE"
	resourceTypeNonReplenishable = "NON_REPLENISHABLE"
	fargateSpot                  = "FARGATE_SPOT"

	msPerSecond = 1000.0

	maxJobNameLength       = 128
	maxCENameLength        = 128
	maxJobQueueNameLength  = 128
	defaultPaginationLimit = 100

	maxTagCount    = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

// jobDefNameRegex validates AWS Batch job definition names.
var jobDefNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

// isValidCEType returns true if the given type is a valid compute environment type (MANAGED or UNMANAGED).
func isValidCEType(t string) bool {
	return t == "MANAGED" || t == "UNMANAGED"
}

// isValidComputeResourcesType returns true if the given type is a valid ComputeResources type.
func isValidComputeResourcesType(t string) bool {
	switch t {
	case "EC2", "SPOT", "FARGATE", fargateSpot:
		return true
	}

	return false
}

// isValidAllocationStrategy returns true if the given allocation strategy is valid.
func isValidAllocationStrategy(s string) bool {
	switch s {
	case "", "BEST_FIT", "BEST_FIT_PROGRESSIVE", "SPOT_CAPACITY_OPTIMIZED", "SPOT_PRICE_CAPACITY_OPTIMIZED":
		return true
	}

	return false
}

// tagsCloneOrEmpty clones a tag map, returning an empty map for nil input.
// Use when building response copies so that JSON serialises as {} not null/absent.
func tagsCloneOrEmpty(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return map[string]string{}
	}

	return maps.Clone(tags)
}

// validateTags checks tag count and key/value length constraints.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: too many tags: max %d, got %d", ErrValidation, maxTagCount, len(tags))
	}

	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrValidation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0-%d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

// decodeNextToken decodes a pagination token (base64 or plain integer) into an offset.
func decodeNextToken(token string) int {
	if token == "" {
		return 0
	}

	if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
		if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
			return n
		}
	}

	if n, err := strconv.Atoi(token); err == nil && n > 0 {
		return n
	}

	return 0
}

// encodeNextToken encodes an offset as a base64 pagination token.
func encodeNextToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// paginateSlice sorts and paginates a slice, returning the page and next token.
func paginateSlice[T any](all []T, nextToken string, maxResults int32, less func(i, j int) bool) ([]T, string) {
	sort.Slice(all, less)

	limit := maxResults
	if limit <= 0 {
		limit = defaultPaginationLimit
	}

	offset := decodeNextToken(nextToken)

	if offset >= len(all) {
		return []T{}, ""
	}

	end := min(offset+int(limit), len(all))
	page := all[offset:end]

	outToken := ""
	if end < len(all) {
		outToken = encodeNextToken(end)
	}

	return page, outToken
}

// --- ComputeResources sub-types ---

// LaunchTemplateOverride specifies a launch template override for specific instance types.
type LaunchTemplateOverride struct {
	LaunchTemplateName  string   `json:"launchTemplateName,omitempty"`
	LaunchTemplateID    string   `json:"launchTemplateId,omitempty"`
	Version             string   `json:"version,omitempty"`
	TargetInstanceTypes []string `json:"targetInstanceTypes,omitempty"`
}

// LaunchTemplate specifies an EC2 launch template.
type LaunchTemplate struct {
	LaunchTemplateName string                   `json:"launchTemplateName,omitempty"`
	LaunchTemplateID   string                   `json:"launchTemplateId,omitempty"`
	Version            string                   `json:"version,omitempty"`
	Overrides          []LaunchTemplateOverride `json:"overrides,omitempty"`
}

// Ec2Configuration specifies AMI matching configuration for EC2 compute environments.
type Ec2Configuration struct {
	ImageType              string `json:"imageType"`
	ImageIDOverride        string `json:"imageIdOverride,omitempty"`
	ImageKubernetesVersion string `json:"imageKubernetesVersion,omitempty"`
}

// ComputeResources holds compute resource configuration for a managed CE.
type ComputeResources struct {
	Type               string             `json:"type,omitempty"`
	AllocationStrategy string             `json:"allocationStrategy,omitempty"`
	InstanceRole       string             `json:"instanceRole,omitempty"`
	Ec2KeyPair         string             `json:"ec2KeyPair,omitempty"`
	ImageID            string             `json:"imageId,omitempty"`
	PlacementGroup     string             `json:"placementGroup,omitempty"`
	SpotIamFleetRole   string             `json:"spotIamFleetRole,omitempty"`
	InstanceTypes      []string           `json:"instanceTypes,omitempty"`
	Subnets            []string           `json:"subnets,omitempty"`
	SecurityGroupIDs   []string           `json:"securityGroupIds,omitempty"`
	Tags               map[string]string  `json:"tags,omitempty"`
	LaunchTemplate     *LaunchTemplate    `json:"launchTemplate,omitempty"`
	Ec2Configuration   []Ec2Configuration `json:"ec2Configuration,omitempty"`
	MinvCpus           int32              `json:"minvCpus,omitempty"`
	MaxvCpus           int32              `json:"maxvCpus,omitempty"`
	DesiredvCpus       int32              `json:"desiredvCpus,omitempty"`
	BidPercentage      int32              `json:"bidPercentage,omitempty"`
}

// EksConfiguration specifies EKS cluster configuration for a CE.
type EksConfiguration struct {
	EksClusterArn       string `json:"eksClusterArn"`
	KubernetesNamespace string `json:"kubernetesNamespace"`
}

// UpdatePolicy controls behaviour during in-place CE updates.
type UpdatePolicy struct {
	TerminateJobsOnUpdate      bool  `json:"terminateJobsOnUpdate,omitempty"`
	JobExecutionTimeoutMinutes int64 `json:"jobExecutionTimeoutMinutes,omitempty"`
}

// ComputeEnvironment represents a Batch compute environment.
type ComputeEnvironment struct {
	Tags                   map[string]string `json:"tags"`
	ComputeResources       *ComputeResources `json:"computeResources,omitempty"`
	EksConfiguration       *EksConfiguration `json:"eksConfiguration,omitempty"`
	UpdatePolicy           *UpdatePolicy     `json:"updatePolicy,omitempty"`
	ServiceRole            string            `json:"serviceRole,omitempty"`
	ComputeEnvironmentArn  string            `json:"computeEnvironmentArn"`
	Type                   string            `json:"type"`
	State                  string            `json:"state"`
	Status                 string            `json:"status"`
	StatusReason           string            `json:"statusReason,omitempty"`
	ComputeEnvironmentName string            `json:"computeEnvironmentName"`
}

// ComputeEnvironmentOrder pairs a compute environment with its ordering in a job queue.
type ComputeEnvironmentOrder struct {
	ComputeEnvironment string `json:"computeEnvironment"`
	Order              int32  `json:"order"`
}

// JobStateTimeLimitAction cancels jobs stuck in a given state beyond a time limit.
type JobStateTimeLimitAction struct {
	Reason         string `json:"reason"`
	State          string `json:"state"`
	Action         string `json:"action"`
	MaxTimeSeconds int32  `json:"maxTimeSeconds"`
}

// JobQueue represents a Batch job queue.
type JobQueue struct {
	Tags                     map[string]string         `json:"tags"`
	JobQueueName             string                    `json:"jobQueueName"`
	JobQueueArn              string                    `json:"jobQueueArn"`
	State                    string                    `json:"state"`
	Status                   string                    `json:"status"`
	StatusReason             string                    `json:"statusReason,omitempty"`
	SchedulingPolicyArn      string                    `json:"schedulingPolicyArn,omitempty"`
	ComputeEnvironmentOrder  []ComputeEnvironmentOrder `json:"computeEnvironmentOrder,omitempty"`
	JobStateTimeLimitActions []JobStateTimeLimitAction `json:"jobStateTimeLimitActions,omitempty"`
	Priority                 int32                     `json:"priority"`
}

// --- ContainerProperties sub-types ---

// KeyValuePair is a name/value environment variable pair.
type KeyValuePair struct {
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

// ResourceRequirement specifies a compute resource requirement (VCPU, MEMORY, or GPU).
type ResourceRequirement struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// HostVolume specifies a host-path volume binding.
type HostVolume struct {
	SourcePath string `json:"sourcePath,omitempty"`
}

// Volume specifies a volume available to containers.
type Volume struct {
	Host *HostVolume `json:"host,omitempty"`
	Name string      `json:"name"`
}

// MountPoint maps a volume into a container.
type MountPoint struct {
	ContainerPath string `json:"containerPath,omitempty"`
	SourceVolume  string `json:"sourceVolume,omitempty"`
	ReadOnly      bool   `json:"readOnly,omitempty"`
}

// Ulimit specifies a ulimit for a container.
type Ulimit struct {
	Name      string `json:"name"`
	SoftLimit int32  `json:"softLimit"`
	HardLimit int32  `json:"hardLimit"`
}

// LogConfiguration specifies the log driver configuration for a container.
type LogConfiguration struct {
	Options   map[string]string `json:"options,omitempty"`
	LogDriver string            `json:"logDriver"`
}

// NetworkConfiguration specifies network settings for Fargate containers.
type NetworkConfiguration struct {
	AssignPublicIP string `json:"assignPublicIp,omitempty"`
}

// FargatePlatformConfiguration specifies the Fargate platform version.
type FargatePlatformConfiguration struct {
	PlatformVersion string `json:"platformVersion,omitempty"`
}

// EphemeralStorage specifies ephemeral storage capacity for Fargate tasks.
type EphemeralStorage struct {
	SizeInGiB int32 `json:"sizeInGiB"`
}

// RuntimePlatform specifies the OS family and CPU architecture for a job.
type RuntimePlatform struct {
	OperatingSystemFamily string `json:"operatingSystemFamily,omitempty"`
	CPUArchitecture       string `json:"cpuArchitecture,omitempty"`
}

// RepositoryCredentials specifies credentials for a private container registry.
type RepositoryCredentials struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

// Secret specifies a secret to expose to a container via environment variable.
type Secret struct {
	Name      string `json:"name"`
	ValueFrom string `json:"valueFrom"`
}

// Device specifies a device to expose to a container.
type Device struct {
	HostPath      string   `json:"hostPath"`
	ContainerPath string   `json:"containerPath,omitempty"`
	Permissions   []string `json:"permissions,omitempty"`
}

// Tmpfs specifies a tmpfs mount for a container.
type Tmpfs struct {
	ContainerPath string   `json:"containerPath"`
	MountOptions  []string `json:"mountOptions,omitempty"`
	Size          int32    `json:"size"`
}

// LinuxParameters configures Linux-specific container settings.
type LinuxParameters struct {
	Devices            []Device `json:"devices,omitempty"`
	Tmpfs              []Tmpfs  `json:"tmpfs,omitempty"`
	InitProcessEnabled bool     `json:"initProcessEnabled,omitempty"`
	SharedMemorySize   int32    `json:"sharedMemorySize,omitempty"`
	MaxSwap            int32    `json:"maxSwap,omitempty"`
	Swappiness         int32    `json:"swappiness,omitempty"`
}

// ContainerProperties stores container configuration for a job definition.
type ContainerProperties struct {
	LinuxParameters              *LinuxParameters              `json:"linuxParameters,omitempty"`
	RepositoryCredentials        *RepositoryCredentials        `json:"repositoryCredentials,omitempty"`
	RuntimePlatform              *RuntimePlatform              `json:"runtimePlatform,omitempty"`
	EphemeralStorage             *EphemeralStorage             `json:"ephemeralStorage,omitempty"`
	FargatePlatformConfiguration *FargatePlatformConfiguration `json:"fargatePlatformConfiguration,omitempty"`
	NetworkConfiguration         *NetworkConfiguration         `json:"networkConfiguration,omitempty"`
	LogConfiguration             *LogConfiguration             `json:"logConfiguration,omitempty"`
	JobRoleArn                   string                        `json:"jobRoleArn,omitempty"`
	ExecutionRoleArn             string                        `json:"executionRoleArn,omitempty"`
	User                         string                        `json:"user,omitempty"`
	InstanceType                 string                        `json:"instanceType,omitempty"`
	Image                        string                        `json:"image,omitempty"`
	Command                      []string                      `json:"command,omitempty"`
	Secrets                      []Secret                      `json:"secrets,omitempty"`
	ResourceRequirements         []ResourceRequirement         `json:"resourceRequirements,omitempty"`
	Ulimits                      []Ulimit                      `json:"ulimits,omitempty"`
	MountPoints                  []MountPoint                  `json:"mountPoints,omitempty"`
	Volumes                      []Volume                      `json:"volumes,omitempty"`
	Environment                  []KeyValuePair                `json:"environment,omitempty"`
	Vcpus                        int32                         `json:"vcpus,omitempty"`
	Memory                       int32                         `json:"memory,omitempty"`
	ReadonlyRootFilesystem       bool                          `json:"readonlyRootFilesystem,omitempty"`
	Privileged                   bool                          `json:"privileged,omitempty"`
}

// --- JobDefinition sub-types ---

// NodeRangeProperty specifies container properties for a range of multi-node job nodes.
type NodeRangeProperty struct {
	ContainerProperties *ContainerProperties `json:"containerProperties,omitempty"`
	TargetNodes         string               `json:"targetNodes"`
}

// NodeProperties specifies multi-node parallel job configuration.
type NodeProperties struct {
	NodeRangeProperties []NodeRangeProperty `json:"nodeRangeProperties"`
	NumNodes            int32               `json:"numNodes"`
	MainNode            int32               `json:"mainNode"`
}

// EksContainerEnv is a name/value env var for EKS containers.
type EksContainerEnv struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

// EksContainerResources specifies resource limits and requests for EKS containers.
type EksContainerResources struct {
	Limits   map[string]string `json:"limits,omitempty"`
	Requests map[string]string `json:"requests,omitempty"`
}

// EksVolumeMount mounts a volume into an EKS container.
type EksVolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// EksSecurityContext specifies security settings for an EKS container.
type EksSecurityContext struct {
	RunAsUser                *int64 `json:"runAsUser,omitempty"`
	RunAsGroup               *int64 `json:"runAsGroup,omitempty"`
	Privileged               bool   `json:"privileged,omitempty"`
	ReadOnlyRootFilesystem   bool   `json:"readOnlyRootFilesystem,omitempty"`
	RunAsNonRoot             bool   `json:"runAsNonRoot,omitempty"`
	AllowPrivilegeEscalation bool   `json:"allowPrivilegeEscalation,omitempty"`
}

// EksContainer specifies an EKS pod container.
type EksContainer struct {
	Resources       *EksContainerResources `json:"resources,omitempty"`
	SecurityContext *EksSecurityContext    `json:"securityContext,omitempty"`
	Name            string                 `json:"name"`
	Image           string                 `json:"image"`
	Command         []string               `json:"command,omitempty"`
	Args            []string               `json:"args,omitempty"`
	Env             []EksContainerEnv      `json:"env,omitempty"`
	VolumeMounts    []EksVolumeMount       `json:"volumeMounts,omitempty"`
}

// EksHostPath specifies a host path volume for EKS.
type EksHostPath struct {
	Path string `json:"path,omitempty"`
}

// EksEmptyDir specifies an emptyDir volume for EKS.
type EksEmptyDir struct {
	Medium    string `json:"medium,omitempty"`
	SizeLimit string `json:"sizeLimit,omitempty"`
}

// EksSecret specifies a Kubernetes secret volume for EKS.
type EksSecret struct {
	SecretName string `json:"secretName"`
	Optional   bool   `json:"optional,omitempty"`
}

// EksVolume specifies a volume available to EKS pod containers.
type EksVolume struct {
	HostPath *EksHostPath `json:"hostPath,omitempty"`
	EmptyDir *EksEmptyDir `json:"emptyDir,omitempty"`
	Secret   *EksSecret   `json:"secret,omitempty"`
	Name     string       `json:"name"`
}

// EksMetadata holds labels and annotations for an EKS pod.
type EksMetadata struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// EksPodProperties specifies the Kubernetes pod spec for an EKS job.
type EksPodProperties struct {
	Metadata           *EksMetadata   `json:"metadata,omitempty"`
	ServiceAccountName string         `json:"serviceAccountName,omitempty"`
	DNSPolicy          string         `json:"dnsPolicy,omitempty"`
	Containers         []EksContainer `json:"containers,omitempty"`
	InitContainers     []EksContainer `json:"initContainers,omitempty"`
	Volumes            []EksVolume    `json:"volumes,omitempty"`
	HostNetwork        bool           `json:"hostNetwork,omitempty"`
}

// EksProperties specifies EKS-specific job definition properties.
type EksProperties struct {
	PodProperties *EksPodProperties `json:"podProperties,omitempty"`
}

// ConsumableResourceProperty specifies a consumable resource requirement for a job.
type ConsumableResourceProperty struct {
	ConsumableResource string  `json:"consumableResource"`
	Quantity           float64 `json:"quantity"`
}

// JobDefinition represents a Batch job definition.
type JobDefinition struct {
	DeregisteredAt               *time.Time                   `json:"deregisteredAt,omitempty"`
	Tags                         map[string]string            `json:"tags"`
	Parameters                   map[string]string            `json:"parameters,omitempty"`
	ContainerProperties          *ContainerProperties         `json:"containerProperties,omitempty"`
	NodeProperties               *NodeProperties              `json:"nodeProperties,omitempty"`
	EksProperties                *EksProperties               `json:"eksProperties,omitempty"`
	RuntimePlatform              *RuntimePlatform             `json:"runtimePlatform,omitempty"`
	ConsumableResourceProperties []ConsumableResourceProperty `json:"consumableResourceProperties,omitempty"`
	JobDefinitionName            string                       `json:"jobDefinitionName"`
	JobDefinitionArn             string                       `json:"jobDefinitionArn"`
	Type                         string                       `json:"type"`
	Status                       string                       `json:"status"`
	PlatformCapabilities         []string                     `json:"platformCapabilities,omitempty"`
	Revision                     int32                        `json:"revision"`
	TimeoutSeconds               int32                        `json:"timeoutSeconds,omitempty"`
	SchedulingPriority           int32                        `json:"schedulingPriority,omitempty"`
	PropagateTags                bool                         `json:"propagateTags,omitempty"`
}

// --- RetryStrategy ---

// EvaluateOnExit specifies a conditional retry rule evaluated against exit information.
type EvaluateOnExit struct {
	Action         string `json:"action"`
	OnStatusReason string `json:"onStatusReason,omitempty"`
	OnReason       string `json:"onReason,omitempty"`
	OnExitCode     string `json:"onExitCode,omitempty"`
}

// RetryStrategy configures automatic retry behavior for a job.
type RetryStrategy struct {
	EvaluateOnExit []EvaluateOnExit `json:"evaluateOnExit,omitempty"`
	Attempts       int32            `json:"attempts,omitempty"`
}

// JobTimeout configures the maximum duration for a job attempt.
type JobTimeout struct {
	AttemptDurationSeconds int32 `json:"attemptDurationSeconds,omitempty"`
}

// JobDependency represents a dependency between jobs.
type JobDependency struct {
	JobID string `json:"jobId,omitempty"`
	Type  string `json:"type,omitempty"`
}

// ArrayProperties specifies array job fan-out configuration.
type ArrayProperties struct {
	StatusSummary map[string]int32 `json:"statusSummary,omitempty"`
	Size          int32            `json:"size,omitempty"`
	Index         int32            `json:"index,omitempty"`
}

// ContainerOverrides overrides container properties at job submission time.
type ContainerOverrides struct {
	InstanceType         string                `json:"instanceType,omitempty"`
	Command              []string              `json:"command,omitempty"`
	Environment          []KeyValuePair        `json:"environment,omitempty"`
	ResourceRequirements []ResourceRequirement `json:"resourceRequirements,omitempty"`
}

// JobAttemptContainer holds per-attempt container execution details.
type JobAttemptContainer struct {
	LogStreamName string `json:"logStreamName,omitempty"`
	Reason        string `json:"reason,omitempty"`
	ExitCode      int32  `json:"exitCode,omitempty"`
}

// JobAttempt holds per-attempt lifecycle and result information.
type JobAttempt struct {
	Container    *JobAttemptContainer `json:"container,omitempty"`
	StartedAt    *int64               `json:"startedAt,omitempty"`
	StoppedAt    *int64               `json:"stoppedAt,omitempty"`
	StatusReason string               `json:"statusReason,omitempty"`
}

// Job represents a submitted Batch job.
type Job struct {
	ContainerOverrides           *ContainerOverrides          `json:"containerOverrides,omitempty"`
	Tags                         map[string]string            `json:"tags"`
	Parameters                   map[string]string            `json:"parameters,omitempty"`
	StartedAt                    *int64                       `json:"startedAt,omitempty"`
	StoppedAt                    *int64                       `json:"stoppedAt,omitempty"`
	RetryStrategy                *RetryStrategy               `json:"retryStrategy,omitempty"`
	Timeout                      *JobTimeout                  `json:"timeout,omitempty"`
	ArrayProperties              *ArrayProperties             `json:"arrayProperties,omitempty"`
	JobDefinition                string                       `json:"jobDefinition"`
	ShareIdentifier              string                       `json:"shareIdentifier,omitempty"`
	StatusReason                 string                       `json:"statusReason,omitempty"`
	JobID                        string                       `json:"jobId"`
	JobARN                       string                       `json:"jobArn"`
	JobName                      string                       `json:"jobName"`
	JobQueue                     string                       `json:"jobQueue"`
	Status                       string                       `json:"status"`
	DependsOn                    []JobDependency              `json:"dependsOn,omitempty"`
	ConsumableResourceProperties []ConsumableResourceProperty `json:"consumableResourceProperties,omitempty"`
	Attempts                     []JobAttempt                 `json:"attempts,omitempty"`
	CreatedAt                    int64                        `json:"createdAt"`
	SchedulingPriorityOverride   int32                        `json:"schedulingPriorityOverride,omitempty"`
	PropagateTags                bool                         `json:"propagateTags,omitempty"`
}

// ConsumableResource represents a Batch consumable resource.
type ConsumableResource struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

// ShareDistribution specifies a fair-share weight for a share identifier.
type ShareDistribution struct {
	ShareIdentifier string  `json:"shareIdentifier"`
	WeightFactor    float32 `json:"weightFactor,omitempty"`
}

// FairsharePolicy configures fair-share scheduling for a scheduling policy.
type FairsharePolicy struct {
	ShareDistribution  []ShareDistribution `json:"shareDistribution,omitempty"`
	ShareDecaySeconds  int32               `json:"shareDecaySeconds,omitempty"`
	ComputeReservation int32               `json:"computeReservation,omitempty"`
}

// SchedulingPolicy represents a Batch scheduling policy.
type SchedulingPolicy struct {
	Tags            map[string]string `json:"tags"`
	FairsharePolicy *FairsharePolicy  `json:"fairsharePolicy,omitempty"`
	Arn             string            `json:"arn"`
	Name            string            `json:"name"`
}

// ServiceEnvironment represents a Batch service environment.
type ServiceEnvironment struct {
	Tags                   map[string]string `json:"tags"`
	ServiceEnvironmentName string            `json:"serviceEnvironmentName"`
	ServiceEnvironmentArn  string            `json:"serviceEnvironmentArn"`
	ServiceEnvironmentType string            `json:"serviceEnvironmentType"`
	State                  string            `json:"state"`
	Status                 string            `json:"status"`
}

// ServiceJob represents a Batch service job.
type ServiceJob struct {
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

// JobQueueSnapshot represents the front-of-queue state for a job queue.
type JobQueueSnapshot struct {
	FrontOfQueue *FrontOfQueue `json:"frontOfQueue,omitempty"`
}

// FrontOfQueue holds jobs at the front of a job queue.
type FrontOfQueue struct {
	Jobs      []FrontOfQueueJob `json:"jobs,omitempty"`
	Timestamp float64           `json:"timestamp"`
}

// FrontOfQueueJob represents a single job at the front of a queue.
type FrontOfQueueJob struct {
	JobArn                 string  `json:"jobArn"`
	EarliestTimeAtPosition float64 `json:"earliestTimeAtPosition"`
}

// InMemoryBackend stores AWS Batch state in memory.
type InMemoryBackend struct {
	computeEnvironments    map[string]*ComputeEnvironment
	jobQueues              map[string]*JobQueue
	jobDefinitions         map[string]*JobDefinition
	jobs                   map[string]*Job     // job ID → Job
	jobsByQueue            map[string][]string // queue name → []jobID
	jobDefRevisions        map[string]int32
	consumableResources    map[string]*ConsumableResource
	schedulingPolicies     map[string]*SchedulingPolicy // ARN → SchedulingPolicy
	serviceEnvironments    map[string]*ServiceEnvironment
	serviceJobs            map[string]*ServiceJob // serviceJobID → ServiceJob
	schedulingPolicyByName map[string]string      // name → ARN
	jobsByARN              map[string]string      // job ARN → job ID
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		computeEnvironments:    make(map[string]*ComputeEnvironment),
		jobQueues:              make(map[string]*JobQueue),
		jobDefinitions:         make(map[string]*JobDefinition),
		jobs:                   make(map[string]*Job),
		jobsByQueue:            make(map[string][]string),
		jobDefRevisions:        make(map[string]int32),
		consumableResources:    make(map[string]*ConsumableResource),
		schedulingPolicies:     make(map[string]*SchedulingPolicy),
		serviceEnvironments:    make(map[string]*ServiceEnvironment),
		serviceJobs:            make(map[string]*ServiceJob),
		schedulingPolicyByName: make(map[string]string),
		jobsByARN:              make(map[string]string),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("batch"),
	}
}

// Reset clears all state from the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.computeEnvironments = make(map[string]*ComputeEnvironment)
	b.jobQueues = make(map[string]*JobQueue)
	b.jobDefinitions = make(map[string]*JobDefinition)
	b.jobs = make(map[string]*Job)
	b.jobsByQueue = make(map[string][]string)
	b.jobDefRevisions = make(map[string]int32)
	b.consumableResources = make(map[string]*ConsumableResource)
	b.schedulingPolicies = make(map[string]*SchedulingPolicy)
	b.serviceEnvironments = make(map[string]*ServiceEnvironment)
	b.serviceJobs = make(map[string]*ServiceJob)
	b.schedulingPolicyByName = make(map[string]string)
	b.jobsByARN = make(map[string]string)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// lookupCEByNameOrARN returns a compute environment by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupCEByNameOrARN(nameOrARN string) (*ComputeEnvironment, bool) {
	if ce, ok := b.computeEnvironments[nameOrARN]; ok {
		return ce, true
	}

	for _, ce := range b.computeEnvironments {
		if ce.ComputeEnvironmentArn == nameOrARN {
			return ce, true
		}
	}

	return nil, false
}

// lookupJQByNameOrARN returns a job queue by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupJQByNameOrARN(nameOrARN string) (*JobQueue, bool) {
	if jq, ok := b.jobQueues[nameOrARN]; ok {
		return jq, true
	}

	for _, jq := range b.jobQueues {
		if jq.JobQueueArn == nameOrARN {
			return jq, true
		}
	}

	return nil, false
}

// lookupJobByIDOrARN returns a job by ID or ARN using the jobsByARN index for O(1) ARN lookup.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupJobByIDOrARN(idOrARN string) (*Job, bool) {
	if j, ok := b.jobs[idOrARN]; ok {
		return j, true
	}

	if jobID, ok := b.jobsByARN[idOrARN]; ok {
		if j, found := b.jobs[jobID]; found {
			return j, true
		}
	}

	return nil, false
}

// CreateComputeEnvironment creates a new compute environment.
//
//nolint:gocognit,cyclop,funlen // Too complex to refactor given time constraints
func (b *InMemoryBackend) CreateComputeEnvironment(
	name, ceType, state string,
	tags map[string]string,
	serviceRole string,
	computeResources *ComputeResources,
	eksConfig *EksConfiguration,
	updatePolicy *UpdatePolicy,
) (*ComputeEnvironment, error) {
	b.mu.Lock("CreateComputeEnvironment")
	defer b.mu.Unlock()

	if len(name) == 0 || len(name) > maxCENameLength {
		return nil, fmt.Errorf(
			"%w: computeEnvironmentName must be between 1 and %d characters",
			ErrValidation, maxCENameLength,
		)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return nil, fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if !isValidCEType(ceType) {
		return nil, fmt.Errorf(
			"%w: invalid compute environment type %q (must be MANAGED or UNMANAGED)",
			ErrValidation,
			ceType,
		)
	}

	if computeResources != nil {
		if computeResources.Type != "" && !isValidComputeResourcesType(computeResources.Type) {
			return nil, fmt.Errorf(
				"%w: invalid computeResources.type %q (must be EC2, SPOT, FARGATE, or FARGATE_SPOT)",
				ErrValidation, computeResources.Type,
			)
		}

		if !isValidAllocationStrategy(computeResources.AllocationStrategy) {
			return nil, fmt.Errorf(
				"%w: invalid allocationStrategy %q",
				ErrValidation, computeResources.AllocationStrategy,
			)
		}

		if (computeResources.Type == "SPOT" || computeResources.Type == "FARGATE_SPOT") &&
			computeResources.AllocationStrategy == "" {
			return nil, fmt.Errorf(
				"%w: allocationStrategy is required for SPOT and FARGATE_SPOT compute resources",
				ErrValidation,
			)
		}

		if ceType == "UNMANAGED" && (computeResources.Type == "FARGATE" || computeResources.Type == "FARGATE_SPOT") {
			return nil, fmt.Errorf(
				"%w: FARGATE and FARGATE_SPOT are not valid for UNMANAGED compute environments",
				ErrValidation,
			)
		}
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if _, ok := b.computeEnvironments[name]; ok {
		return nil, fmt.Errorf("%w: compute environment %s already exists", ErrAlreadyExists, name)
	}

	ceARN := arn.Build("batch", b.region, b.accountID, "compute-environment/"+name)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	if state == "" {
		state = stateEnabled
	}

	crCopy := cloneComputeResources(computeResources)

	var eksCopy *EksConfiguration
	if eksConfig != nil {
		cp := *eksConfig
		eksCopy = &cp
	}

	var upCopy *UpdatePolicy
	if updatePolicy != nil {
		cp := *updatePolicy
		upCopy = &cp
	}

	ce := &ComputeEnvironment{
		ComputeEnvironmentName: name,
		ComputeEnvironmentArn:  ceARN,
		Type:                   ceType,
		State:                  state,
		Status:                 statusValid,
		Tags:                   tagsCopy,
		ServiceRole:            serviceRole,
		ComputeResources:       crCopy,
		EksConfiguration:       eksCopy,
		UpdatePolicy:           upCopy,
	}
	b.computeEnvironments[name] = ce
	cp := *ce

	return &cp, nil
}

// cloneComputeResources performs a shallow copy of a ComputeResources struct.
func cloneComputeResources(cr *ComputeResources) *ComputeResources {
	if cr == nil {
		return nil
	}

	clone := *cr
	if cr.Tags != nil {
		clone.Tags = maps.Clone(cr.Tags)
	}

	if cr.Ec2Configuration != nil {
		ec2Config := make([]Ec2Configuration, len(cr.Ec2Configuration))
		copy(ec2Config, cr.Ec2Configuration)
		clone.Ec2Configuration = ec2Config
	}

	return &clone
}

// DescribeComputeEnvironments returns compute environments, optionally filtered by names/ARNs.
// When names is empty, results are paginated via maxResults/nextToken.
//
//nolint:dupl // Boilerplate pagination logic is similar to DescribeJobQueues
func (b *InMemoryBackend) DescribeComputeEnvironments(
	names []string,
	maxResults int32,
	nextToken string,
) ([]*ComputeEnvironment, string) {
	b.mu.RLock("DescribeComputeEnvironments")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		list := make([]*ComputeEnvironment, 0, len(names))

		for _, nameOrARN := range names {
			if ce, ok := b.lookupCEByNameOrARN(nameOrARN); ok {
				cp := *ce
				cp.Tags = tagsCloneOrEmpty(ce.Tags)
				list = append(list, &cp)
			}
		}

		return list, ""
	}

	all := make([]*ComputeEnvironment, 0, len(b.computeEnvironments))
	for _, ce := range b.computeEnvironments {
		cp := *ce
		cp.Tags = tagsCloneOrEmpty(ce.Tags)
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, func(i, j int) bool {
		return all[i].ComputeEnvironmentName < all[j].ComputeEnvironmentName
	})
}

// UpdateComputeEnvironment updates the state, service role, compute resources, and/or update policy.
func (b *InMemoryBackend) UpdateComputeEnvironment(
	nameOrARN, state, serviceRole string,
	computeResources *ComputeResources,
	updatePolicy *UpdatePolicy,
) (*ComputeEnvironment, error) {
	b.mu.Lock("UpdateComputeEnvironment")
	defer b.mu.Unlock()

	ce, ok := b.lookupCEByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: compute environment %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return nil, fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if state != "" {
		ce.State = state
	}

	if serviceRole != "" {
		ce.ServiceRole = serviceRole
	}

	if computeResources != nil {
		ce.ComputeResources = cloneComputeResources(computeResources)
	}

	if updatePolicy != nil {
		up := *updatePolicy
		ce.UpdatePolicy = &up
	}

	cp := *ce

	return &cp, nil
}

// DeleteComputeEnvironment removes a compute environment.
func (b *InMemoryBackend) DeleteComputeEnvironment(nameOrARN string) error {
	b.mu.Lock("DeleteComputeEnvironment")
	defer b.mu.Unlock()

	ce, ok := b.lookupCEByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: compute environment %s not found", ErrNotFound, nameOrARN)
	}

	if ce.State != "DISABLED" {
		return fmt.Errorf(
			"%w: compute environment %s must be DISABLED before it can be deleted",
			ErrValidation,
			nameOrARN,
		)
	}

	// Check if referenced by any job queue.
	for _, jq := range b.jobQueues {
		for _, ceOrder := range jq.ComputeEnvironmentOrder {
			if ceOrder.ComputeEnvironment == ce.ComputeEnvironmentName ||
				ceOrder.ComputeEnvironment == ce.ComputeEnvironmentArn {
				return fmt.Errorf(
					"%w: compute environment %s is referenced by one or more job queues",
					ErrValidation,
					nameOrARN,
				)
			}
		}
	}

	delete(b.computeEnvironments, ce.ComputeEnvironmentName)

	return nil
}

// CreateJobQueue creates a new job queue.
func (b *InMemoryBackend) CreateJobQueue(
	name string,
	priority int32,
	state string,
	ceOrder []ComputeEnvironmentOrder,
	tags map[string]string,
	schedulingPolicyArn string,
	jobStateTimeLimitActions []JobStateTimeLimitAction,
) (*JobQueue, error) {
	b.mu.Lock("CreateJobQueue")
	defer b.mu.Unlock()

	if len(name) == 0 || len(name) > maxJobQueueNameLength {
		return nil, fmt.Errorf(
			"%w: jobQueueName must be between 1 and %d characters",
			ErrValidation, maxJobQueueNameLength,
		)
	}

	if _, ok := b.jobQueues[name]; ok {
		return nil, fmt.Errorf("%w: job queue %s already exists", ErrAlreadyExists, name)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	jqARN := arn.Build("batch", b.region, b.accountID, "job-queue/"+name)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	orderCopy := make([]ComputeEnvironmentOrder, len(ceOrder))
	copy(orderCopy, ceOrder)

	var actionsCopy []JobStateTimeLimitAction
	if len(jobStateTimeLimitActions) > 0 {
		actionsCopy = make([]JobStateTimeLimitAction, len(jobStateTimeLimitActions))
		copy(actionsCopy, jobStateTimeLimitActions)
	}

	jq := &JobQueue{
		JobQueueName:             name,
		JobQueueArn:              jqARN,
		State:                    state,
		Status:                   statusValid,
		Priority:                 priority,
		ComputeEnvironmentOrder:  orderCopy,
		Tags:                     tagsCopy,
		SchedulingPolicyArn:      schedulingPolicyArn,
		JobStateTimeLimitActions: actionsCopy,
	}
	b.jobQueues[name] = jq
	cp := *jq

	return &cp, nil
}

// DescribeJobQueues returns job queues, optionally filtered by names/ARNs.
// When names are provided, all matching queues are returned without pagination.
// When names is empty, results are paginated using maxResults and nextToken.
//
//nolint:dupl // Boilerplate pagination logic is similar to DescribeComputeEnvironments
func (b *InMemoryBackend) DescribeJobQueues(names []string, maxResults int32, nextToken string) ([]*JobQueue, string) {
	b.mu.RLock("DescribeJobQueues")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		list := make([]*JobQueue, 0, len(names))

		for _, nameOrARN := range names {
			if jq, ok := b.lookupJQByNameOrARN(nameOrARN); ok {
				cp := *jq
				cp.Tags = tagsCloneOrEmpty(jq.Tags)
				list = append(list, &cp)
			}
		}

		return list, ""
	}

	all := make([]*JobQueue, 0, len(b.jobQueues))
	for _, jq := range b.jobQueues {
		cp := *jq
		cp.Tags = tagsCloneOrEmpty(jq.Tags)
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, func(i, j int) bool {
		return all[i].JobQueueName < all[j].JobQueueName
	})
}

// UpdateJobQueue updates a job queue's state, priority, CE order, and/or time-limit actions.
func (b *InMemoryBackend) UpdateJobQueue(
	nameOrARN string,
	priority *int32,
	state string,
	ceOrder []ComputeEnvironmentOrder,
	jobStateTimeLimitActions []JobStateTimeLimitAction,
) (*JobQueue, error) {
	b.mu.Lock("UpdateJobQueue")
	defer b.mu.Unlock()

	jq, ok := b.lookupJQByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" && state != stateEnabled && state != stateDisabled {
		return nil, fmt.Errorf("%w: state must be %s or %s", ErrValidation, stateEnabled, stateDisabled)
	}

	if state != "" {
		jq.State = state
	}

	if priority != nil {
		jq.Priority = *priority
	}

	if ceOrder != nil {
		orderCopy := make([]ComputeEnvironmentOrder, len(ceOrder))
		copy(orderCopy, ceOrder)
		jq.ComputeEnvironmentOrder = orderCopy
	}

	if jobStateTimeLimitActions != nil {
		actionsCopy := make([]JobStateTimeLimitAction, len(jobStateTimeLimitActions))
		copy(actionsCopy, jobStateTimeLimitActions)
		jq.JobStateTimeLimitActions = actionsCopy
	}

	cp := *jq

	return &cp, nil
}

// DeleteJobQueue removes a job queue and all associated jobs.
// The queue must be in DISABLED state before deletion.
func (b *InMemoryBackend) DeleteJobQueue(nameOrARN string) error {
	b.mu.Lock("DeleteJobQueue")
	defer b.mu.Unlock()

	jq, ok := b.lookupJQByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: job queue %s not found", ErrNotFound, nameOrARN)
	}

	if jq.State != "DISABLED" {
		return fmt.Errorf("%w: job queue %s must be DISABLED before it can be deleted", ErrValidation, nameOrARN)
	}

	queueName := jq.JobQueueName

	for _, jobID := range b.jobsByQueue[queueName] {
		if j, ok2 := b.jobs[jobID]; ok2 {
			delete(b.jobsByARN, j.JobARN)
		}

		delete(b.jobs, jobID)
	}

	delete(b.jobsByQueue, queueName)
	delete(b.jobQueues, queueName)

	return nil
}

// RegisterJobDefinition registers a new job definition (or a new revision).
func (b *InMemoryBackend) RegisterJobDefinition(
	name, defType string,
	tags map[string]string,
	platformCapabilities []string,
	timeoutSeconds int32,
	schedulingPriority int32,
	containerProps *ContainerProperties,
	nodeProps *NodeProperties,
	eksProps *EksProperties,
	runtimePlatform *RuntimePlatform,
	consumableResourceProperties []ConsumableResourceProperty,
	parameters map[string]string,
	propagateTags bool,
) (*JobDefinition, error) {
	b.mu.Lock("RegisterJobDefinition")
	defer b.mu.Unlock()

	if !jobDefNameRegex.MatchString(name) {
		return nil, fmt.Errorf(
			"%w: jobDefinitionName must match [a-zA-Z0-9_-]{1,128}",
			ErrValidation,
		)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.jobDefRevisions[name]++
	revision := b.jobDefRevisions[name]

	jdARN := arn.Build("batch", b.region, b.accountID, fmt.Sprintf("job-definition/%s:%d", name, revision))

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	var crpCopy []ConsumableResourceProperty
	if len(consumableResourceProperties) > 0 {
		crpCopy = make([]ConsumableResourceProperty, len(consumableResourceProperties))
		copy(crpCopy, consumableResourceProperties)
	}

	jd := &JobDefinition{
		JobDefinitionName:            name,
		JobDefinitionArn:             jdARN,
		Type:                         defType,
		Status:                       jobDefStatusActive,
		Revision:                     revision,
		Tags:                         tagsCopy,
		PlatformCapabilities:         platformCapabilities,
		TimeoutSeconds:               timeoutSeconds,
		SchedulingPriority:           schedulingPriority,
		ContainerProperties:          containerProps,
		NodeProperties:               nodeProps,
		EksProperties:                eksProps,
		RuntimePlatform:              runtimePlatform,
		ConsumableResourceProperties: crpCopy,
		Parameters:                   maps.Clone(parameters),
		PropagateTags:                propagateTags,
	}
	b.jobDefinitions[jdARN] = jd
	cp := *jd

	return &cp, nil
}

// DescribeJobDefinitions returns job definitions, optionally filtered by names/ARNs.
// When names is empty, results are paginated via maxResults/nextToken.
func (b *InMemoryBackend) DescribeJobDefinitions(
	names []string,
	status, jobDefinitionName string,
	maxResults int32,
	nextToken string,
) ([]*JobDefinition, string) {
	b.mu.RLock("DescribeJobDefinitions")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		return b.describeAllJobDefinitions(status, jobDefinitionName, maxResults, nextToken)
	}

	list := b.describeJobDefinitionsByNames(names, status)

	return list, ""
}

func (b *InMemoryBackend) describeAllJobDefinitions(
	status, jobDefinitionName string,
	maxResults int32,
	nextToken string,
) ([]*JobDefinition, string) {
	all := make([]*JobDefinition, 0, len(b.jobDefinitions))

	for _, jd := range b.jobDefinitions {
		if status != "" && jd.Status != status {
			continue
		}

		if jobDefinitionName != "" && jd.JobDefinitionName != jobDefinitionName {
			continue
		}

		cp := *jd
		cp.Tags = tagsCloneOrEmpty(jd.Tags)
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Revision > all[j].Revision
	})

	limit := maxResults
	if limit <= 0 {
		limit = defaultPaginationLimit
	}

	offset := decodeNextToken(nextToken)

	if offset >= len(all) {
		return []*JobDefinition{}, ""
	}

	end := min(offset+int(limit), len(all))
	page := all[offset:end]

	outToken := ""
	if end < len(all) {
		outToken = encodeNextToken(end)
	}

	return page, outToken
}

func (b *InMemoryBackend) describeJobDefinitionsByNames(names []string, status string) []*JobDefinition {
	seen := make(map[string]bool)
	list := make([]*JobDefinition, 0, len(names))

	for _, nameOrARN := range names {
		if jd, ok := b.jobDefinitions[nameOrARN]; ok {
			if !seen[jd.JobDefinitionArn] && (status == "" || jd.Status == status) {
				seen[jd.JobDefinitionArn] = true
				cp := *jd
				cp.Tags = tagsCloneOrEmpty(jd.Tags)
				list = append(list, &cp)
			}

			continue
		}

		baseName, _, _ := strings.Cut(nameOrARN, ":")

		for _, jd := range b.jobDefinitions {
			if jd.JobDefinitionName == baseName && !seen[jd.JobDefinitionArn] && (status == "" || jd.Status == status) {
				seen[jd.JobDefinitionArn] = true
				cp := *jd
				cp.Tags = tagsCloneOrEmpty(jd.Tags)
				list = append(list, &cp)
			}
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Revision > list[j].Revision
	})

	return list
}

// DeregisterJobDefinition marks a job definition as INACTIVE by ARN or name:revision.
// INACTIVE definitions remain visible in DescribeJobDefinitions (matching AWS behavior)
// and are swept by the janitor after the configured TTL.
func (b *InMemoryBackend) DeregisterJobDefinition(arnOrNameRev string) error {
	b.mu.Lock("DeregisterJobDefinition")
	defer b.mu.Unlock()

	now := time.Now()

	// Try direct ARN lookup first.
	if jd, ok := b.jobDefinitions[arnOrNameRev]; ok {
		jd.Status = jobDefStatusInactive
		jd.DeregisteredAt = &now

		return nil
	}

	// Fall back to name:revision lookup (e.g. "my-job:3").
	for _, jd := range b.jobDefinitions {
		nameRev := fmt.Sprintf("%s:%d", jd.JobDefinitionName, jd.Revision)
		if nameRev == arnOrNameRev {
			jd.Status = jobDefStatusInactive
			jd.DeregisteredAt = &now

			return nil
		}
	}

	return fmt.Errorf("%w: job definition %s not found", ErrNotFound, arnOrNameRev)
}

// ListTagsForResource returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if tags, ok := b.findTagsByARN(resourceARN); ok {
		out := make(map[string]string, len(tags))
		maps.Copy(out, tags)

		return out, nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	if existing == nil {
		b.initTagsByARN(resourceARN)
		existing, _ = b.findTagsByARN(resourceARN)
	}

	// Validate combined tag count (new keys only).
	newKeys := 0
	for k := range tags {
		if _, alreadyPresent := existing[k]; !alreadyPresent {
			newKeys++
		}
	}

	if len(existing)+newKeys > maxTagCount {
		return fmt.Errorf("%w: resource would exceed max tag count of %d", ErrValidation, maxTagCount)
	}

	if err := validateTags(tags); err != nil {
		return err
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// findTagsByARN looks up the tags map for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (map[string]string, bool) {
	if tags, ok := b.findTagsInCoreResources(resourceARN); ok {
		return tags, true
	}

	return b.findTagsInPolicyResources(resourceARN)
}

func (b *InMemoryBackend) findTagsInCoreResources(resourceARN string) (map[string]string, bool) {
	for _, ce := range b.computeEnvironments {
		if ce.ComputeEnvironmentArn == resourceARN {
			return ce.Tags, true
		}
	}

	for _, jq := range b.jobQueues {
		if jq.JobQueueArn == resourceARN {
			return jq.Tags, true
		}
	}

	if jd, ok := b.jobDefinitions[resourceARN]; ok {
		return jd.Tags, true
	}

	for _, j := range b.jobs {
		if j.JobARN == resourceARN {
			return j.Tags, true
		}
	}

	for _, cr := range b.consumableResources {
		if cr.ConsumableResourceArn == resourceARN {
			return cr.Tags, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) findTagsInPolicyResources(resourceARN string) (map[string]string, bool) {
	for _, sp := range b.schedulingPolicies {
		if sp.Arn == resourceARN {
			return sp.Tags, true
		}
	}

	for _, se := range b.serviceEnvironments {
		if se.ServiceEnvironmentArn == resourceARN {
			return se.Tags, true
		}
	}

	for _, sj := range b.serviceJobs {
		if sj.ServiceJobArn == resourceARN {
			return sj.Tags, true
		}
	}

	return nil, false
}

// initTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) initTagsByARN(resourceARN string) {
	if b.initTagsInCoreResources(resourceARN) {
		return
	}

	b.initTagsInPolicyResources(resourceARN)
}

func (b *InMemoryBackend) initTagsInCoreResources(resourceARN string) bool {
	for _, ce := range b.computeEnvironments {
		if ce.ComputeEnvironmentArn == resourceARN {
			ce.Tags = make(map[string]string)

			return true
		}
	}

	for _, jq := range b.jobQueues {
		if jq.JobQueueArn == resourceARN {
			jq.Tags = make(map[string]string)

			return true
		}
	}

	if jd, ok := b.jobDefinitions[resourceARN]; ok {
		jd.Tags = make(map[string]string)

		return true
	}

	for _, j := range b.jobs {
		if j.JobARN == resourceARN {
			j.Tags = make(map[string]string)

			return true
		}
	}

	for _, cr := range b.consumableResources {
		if cr.ConsumableResourceArn == resourceARN {
			cr.Tags = make(map[string]string)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) initTagsInPolicyResources(resourceARN string) {
	for _, sp := range b.schedulingPolicies {
		if sp.Arn == resourceARN {
			sp.Tags = make(map[string]string)

			return
		}
	}

	for _, se := range b.serviceEnvironments {
		if se.ServiceEnvironmentArn == resourceARN {
			se.Tags = make(map[string]string)

			return
		}
	}

	for _, sj := range b.serviceJobs {
		if sj.ServiceJobArn == resourceARN {
			sj.Tags = make(map[string]string)

			return
		}
	}
}

// SubmitJob submits a new Batch job for execution.
//
//nolint:funlen // Too complex to refactor given time constraints
func (b *InMemoryBackend) SubmitJob(
	name, queue, jobDefinition string,
	tags map[string]string,
	parameters map[string]string,
	dependsOn []JobDependency,
	retryStrategy *RetryStrategy,
	timeout *JobTimeout,
	arrayProperties *ArrayProperties,
	containerOverrides *ContainerOverrides,
	consumableResourceProperties []ConsumableResourceProperty,
	shareIdentifier string,
	schedulingPriorityOverride int32,
	propagateTags bool,
) (*Job, error) {
	b.mu.Lock("SubmitJob")
	defer b.mu.Unlock()

	if len(name) == 0 || len(name) > maxJobNameLength {
		return nil, fmt.Errorf("%w: jobName must be between 1 and %d characters", ErrValidation, maxJobNameLength)
	}

	jq, ok := b.lookupJQByNameOrARN(queue)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, queue)
	}

	if jq.State == stateDisabled {
		return nil, fmt.Errorf("%w: job queue %s is %s", ErrValidation, queue, stateDisabled)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	paramsCopy := maps.Clone(parameters)

	var dependsOnCopy []JobDependency
	if len(dependsOn) > 0 {
		dependsOnCopy = make([]JobDependency, len(dependsOn))
		copy(dependsOnCopy, dependsOn)
	}

	var retryCopy *RetryStrategy
	if retryStrategy != nil {
		rs := *retryStrategy
		if len(rs.EvaluateOnExit) > 0 {
			exitRules := make([]EvaluateOnExit, len(rs.EvaluateOnExit))
			copy(exitRules, rs.EvaluateOnExit)
			rs.EvaluateOnExit = exitRules
		}

		retryCopy = &rs
	}

	var timeoutCopy *JobTimeout
	if timeout != nil {
		tc := *timeout
		timeoutCopy = &tc
	}

	var arrayPropsCopy *ArrayProperties
	if arrayProperties != nil {
		ap := *arrayProperties
		arrayPropsCopy = &ap
	}

	var overridesCopy *ContainerOverrides
	if containerOverrides != nil {
		co := *containerOverrides
		overridesCopy = &co
	}

	var crpCopy []ConsumableResourceProperty
	if len(consumableResourceProperties) > 0 {
		crpCopy = make([]ConsumableResourceProperty, len(consumableResourceProperties))
		copy(crpCopy, consumableResourceProperties)
	}

	now := time.Now().UnixMilli()
	jobID := uuid.NewString()
	jobARN := arn.Build("batch", b.region, b.accountID, "job/"+jobID)

	j := &Job{
		JobID:                        jobID,
		JobARN:                       jobARN,
		JobName:                      name,
		JobQueue:                     jq.JobQueueName,
		JobDefinition:                jobDefinition,
		Status:                       jobStatusSubmitted,
		CreatedAt:                    now,
		Tags:                         tagsCopy,
		Parameters:                   paramsCopy,
		DependsOn:                    dependsOnCopy,
		RetryStrategy:                retryCopy,
		Timeout:                      timeoutCopy,
		ArrayProperties:              arrayPropsCopy,
		ContainerOverrides:           overridesCopy,
		ConsumableResourceProperties: crpCopy,
		ShareIdentifier:              shareIdentifier,
		SchedulingPriorityOverride:   schedulingPriorityOverride,
		PropagateTags:                propagateTags,
	}
	b.jobs[jobID] = j
	b.jobsByARN[jobARN] = jobID
	b.jobsByQueue[jq.JobQueueName] = append(b.jobsByQueue[jq.JobQueueName], jobID)

	cp := *j
	cp.Tags = tagsCloneOrEmpty(j.Tags)

	return &cp, nil
}

// listAllJobs returns all jobs across all queues filtered by status.
func (b *InMemoryBackend) listAllJobs(status string) []*Job {
	all := make([]*Job, 0, len(b.jobs))

	for _, j := range b.jobs {
		if status != "" && j.Status != status {
			continue
		}

		cp := *j
		cp.Tags = tagsCloneOrEmpty(j.Tags)
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt < all[j].CreatedAt })

	return all
}

// listQueueJobs returns jobs in the given queue filtered by status.
func (b *InMemoryBackend) listQueueJobs(queue, status string) ([]*Job, error) {
	jq, ok := b.lookupJQByNameOrARN(queue)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, queue)
	}

	ids := b.jobsByQueue[jq.JobQueueName]
	all := make([]*Job, 0, len(ids))

	for _, id := range ids {
		j, exists := b.jobs[id]
		if !exists {
			continue
		}

		if status != "" && j.Status != status {
			continue
		}

		cp := *j
		cp.Tags = tagsCloneOrEmpty(j.Tags)
		all = append(all, &cp)
	}

	return all, nil
}

// ListJobs returns job summaries for a queue, optionally filtered by status.
// Pagination is controlled via maxResults and nextToken (token encodes an integer offset).
func (b *InMemoryBackend) ListJobs(queue, status, nextToken string, maxResults int32) ([]*Job, string, error) {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	limit := maxResults
	if limit <= 0 {
		limit = defaultPaginationLimit
	}

	offset := decodeNextToken(nextToken)

	var (
		all []*Job
		err error
	)

	if queue == "" {
		all = b.listAllJobs(status)
	} else {
		all, err = b.listQueueJobs(queue, status)
		if err != nil {
			return nil, "", err
		}
	}

	if offset >= len(all) {
		return []*Job{}, "", nil
	}

	end := min(offset+int(limit), len(all))
	page := all[offset:end]

	outToken := ""
	if end < len(all) {
		outToken = encodeNextToken(end)
	}

	return page, outToken, nil
}

// DescribeJobs returns full job details for the given job IDs or ARNs.
func (b *InMemoryBackend) DescribeJobs(jobIDs []string) []*Job {
	b.mu.RLock("DescribeJobs")
	defer b.mu.RUnlock()

	out := make([]*Job, 0, len(jobIDs))

	for _, id := range jobIDs {
		j, ok := b.lookupJobByIDOrARN(id)
		if !ok {
			continue
		}

		cp := *j
		cp.Tags = tagsCloneOrEmpty(j.Tags)
		out = append(out, &cp)
	}

	return out
}

// TerminateJob marks a job as FAILED with the given reason.
// Valid for any non-terminal state. Accepts job ID or ARN.
func (b *InMemoryBackend) TerminateJob(idOrARN, reason string) error {
	b.mu.Lock("TerminateJob")
	defer b.mu.Unlock()

	j, ok := b.lookupJobByIDOrARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, idOrARN)
	}

	if j.Status == jobStatusSucceeded || j.Status == jobStatusFailed {
		return fmt.Errorf("%w: job %s is already in terminal state %s", ErrValidation, idOrARN, j.Status)
	}

	now := time.Now().UnixMilli()
	j.Status = jobStatusFailed
	j.StatusReason = reason
	j.StoppedAt = &now

	return nil
}

// CancelJob cancels a job in SUBMITTED, PENDING, or RUNNABLE state.
// Accepts job ID or ARN.
func (b *InMemoryBackend) CancelJob(idOrARN, reason string) error {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	j, ok := b.lookupJobByIDOrARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, idOrARN)
	}

	switch j.Status {
	case jobStatusSubmitted, jobStatusPending, jobStatusRunnable:
		now := time.Now().UnixMilli()
		j.Status = jobStatusFailed
		j.StatusReason = reason
		j.StoppedAt = &now

		return nil
	default:
		return fmt.Errorf("%w: cannot cancel job %s in %s state", ErrValidation, idOrARN, j.Status)
	}
}

// CreateConsumableResource creates a new consumable resource.
func (b *InMemoryBackend) CreateConsumableResource(
	name, resourceType string,
	totalQuantity int64,
	tags map[string]string,
) (*ConsumableResource, error) {
	b.mu.Lock("CreateConsumableResource")
	defer b.mu.Unlock()

	if _, ok := b.consumableResources[name]; ok {
		return nil, fmt.Errorf("%w: consumable resource %s already exists", ErrAlreadyExists, name)
	}

	if resourceType == "" {
		resourceType = resourceTypeReplenishable
	}

	if resourceType != resourceTypeReplenishable && resourceType != resourceTypeNonReplenishable {
		return nil, fmt.Errorf("%w: invalid resource type %s", ErrValidation, resourceType)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	crARN := arn.Build("batch", b.region, b.accountID, "consumable-resource/"+name)

	cr := &ConsumableResource{
		ConsumableResourceName: name,
		ConsumableResourceArn:  crARN,
		ResourceType:           resourceType,
		TotalQuantity:          totalQuantity,
		AvailableQuantity:      totalQuantity,
		InUseQuantity:          0,
		CreatedAt:              time.Now().UnixMilli(),
		Tags:                   tagsCloneOrEmpty(tags),
	}
	b.consumableResources[name] = cr
	cp := *cr

	return &cp, nil
}

// DeleteConsumableResource removes a consumable resource by name or ARN.
func (b *InMemoryBackend) DeleteConsumableResource(nameOrARN string) error {
	b.mu.Lock("DeleteConsumableResource")
	defer b.mu.Unlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	delete(b.consumableResources, cr.ConsumableResourceName)

	return nil
}

// DescribeConsumableResource returns details for a consumable resource identified by name or ARN.
func (b *InMemoryBackend) DescribeConsumableResource(nameOrARN string) (*ConsumableResource, error) {
	b.mu.RLock("DescribeConsumableResource")
	defer b.mu.RUnlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	cp := *cr
	cp.Tags = tagsCloneOrEmpty(cr.Tags)

	return &cp, nil
}

// lookupConsumableResourceByNameOrARN returns a consumable resource by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupConsumableResourceByNameOrARN(nameOrARN string) (*ConsumableResource, bool) {
	if cr, ok := b.consumableResources[nameOrARN]; ok {
		return cr, true
	}

	for _, cr := range b.consumableResources {
		if cr.ConsumableResourceArn == nameOrARN {
			return cr, true
		}
	}

	return nil, false
}

// CreateSchedulingPolicy creates a new scheduling policy.
func (b *InMemoryBackend) CreateSchedulingPolicy(
	name string,
	tags map[string]string,
	fairsharePolicy *FairsharePolicy,
) (*SchedulingPolicy, error) {
	b.mu.Lock("CreateSchedulingPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if _, ok := b.schedulingPolicyByName[name]; ok {
		return nil, fmt.Errorf("%w: scheduling policy %s already exists", ErrAlreadyExists, name)
	}

	policyARN := arn.Build("batch", b.region, b.accountID, "scheduling-policy/"+name)

	sp := &SchedulingPolicy{
		Arn:             policyARN,
		Name:            name,
		Tags:            tagsCloneOrEmpty(tags),
		FairsharePolicy: cloneFairsharePolicy(fairsharePolicy),
	}
	b.schedulingPolicies[policyARN] = sp
	b.schedulingPolicyByName[name] = policyARN
	cp := *sp

	return &cp, nil
}

// cloneFairsharePolicy deep-copies a FairsharePolicy.
func cloneFairsharePolicy(fp *FairsharePolicy) *FairsharePolicy {
	if fp == nil {
		return nil
	}

	clone := *fp
	if len(fp.ShareDistribution) > 0 {
		sd := make([]ShareDistribution, len(fp.ShareDistribution))
		copy(sd, fp.ShareDistribution)
		clone.ShareDistribution = sd
	}

	return &clone
}

// DeleteSchedulingPolicy removes a scheduling policy by ARN.
func (b *InMemoryBackend) DeleteSchedulingPolicy(policyARN string) error {
	b.mu.Lock("DeleteSchedulingPolicy")
	defer b.mu.Unlock()

	sp, ok := b.schedulingPolicies[policyARN]
	if !ok {
		return fmt.Errorf("%w: scheduling policy %s not found", ErrNotFound, policyARN)
	}

	delete(b.schedulingPolicyByName, sp.Name)
	delete(b.schedulingPolicies, policyARN)

	return nil
}

// CreateServiceEnvironment creates a new service environment.
func (b *InMemoryBackend) CreateServiceEnvironment(
	name, envType, state string,
	tags map[string]string,
) (*ServiceEnvironment, error) {
	b.mu.Lock("CreateServiceEnvironment")
	defer b.mu.Unlock()

	if _, ok := b.serviceEnvironments[name]; ok {
		return nil, fmt.Errorf("%w: service environment %s already exists", ErrAlreadyExists, name)
	}

	seARN := arn.Build("batch", b.region, b.accountID, "service-environment/"+name)

	if state == "" {
		state = stateEnabled
	}

	se := &ServiceEnvironment{
		ServiceEnvironmentName: name,
		ServiceEnvironmentArn:  seARN,
		ServiceEnvironmentType: envType,
		State:                  state,
		Status:                 statusValid,
		Tags:                   tagsCloneOrEmpty(tags),
	}
	b.serviceEnvironments[name] = se
	cp := *se

	return &cp, nil
}

// DeleteServiceEnvironment removes a service environment by name or ARN.
func (b *InMemoryBackend) DeleteServiceEnvironment(nameOrARN string) error {
	b.mu.Lock("DeleteServiceEnvironment")
	defer b.mu.Unlock()

	se, ok := b.lookupServiceEnvironmentByNameOrARN(nameOrARN)
	if !ok {
		return fmt.Errorf("%w: service environment %s not found", ErrNotFound, nameOrARN)
	}

	delete(b.serviceEnvironments, se.ServiceEnvironmentName)

	return nil
}

// lookupServiceEnvironmentByNameOrARN returns a service environment by name or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupServiceEnvironmentByNameOrARN(nameOrARN string) (*ServiceEnvironment, bool) {
	if se, ok := b.serviceEnvironments[nameOrARN]; ok {
		return se, true
	}

	for _, se := range b.serviceEnvironments {
		if se.ServiceEnvironmentArn == nameOrARN {
			return se, true
		}
	}

	return nil, false
}

// UpdateConsumableResource updates the quantity of a consumable resource.
func (b *InMemoryBackend) UpdateConsumableResource(
	nameOrARN, operation string,
	quantity int64,
) (*ConsumableResource, error) {
	b.mu.Lock("UpdateConsumableResource")
	defer b.mu.Unlock()

	cr, ok := b.lookupConsumableResourceByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: consumable resource %s not found", ErrNotFound, nameOrARN)
	}

	if quantity < 0 {
		return nil, fmt.Errorf("%w: quantity must be non-negative", ErrValidation)
	}

	if operation == "" {
		operation = "SET"
	}

	switch operation {
	case "SET":
		cr.TotalQuantity = quantity
		cr.AvailableQuantity = quantity
	case "ADD":
		cr.TotalQuantity += quantity
		cr.AvailableQuantity += quantity
	case "REMOVE":
		if quantity > cr.TotalQuantity {
			return nil, fmt.Errorf(
				"%w: cannot remove %d from total quantity %d",
				ErrValidation,
				quantity,
				cr.TotalQuantity,
			)
		}

		if quantity > cr.AvailableQuantity {
			return nil, fmt.Errorf(
				"%w: cannot remove %d from available quantity %d (in-use reservations block removal)",
				ErrValidation,
				quantity,
				cr.AvailableQuantity,
			)
		}

		cr.TotalQuantity -= quantity
		cr.AvailableQuantity -= quantity
	default:
		return nil, fmt.Errorf("%w: unsupported operation %s", ErrValidation, operation)
	}

	cp := *cr
	cp.Tags = tagsCloneOrEmpty(cr.Tags)

	return &cp, nil
}

// ListConsumableResources returns all consumable resources sorted by name.
func (b *InMemoryBackend) ListConsumableResources() []*ConsumableResource {
	b.mu.RLock("ListConsumableResources")
	defer b.mu.RUnlock()

	list := make([]*ConsumableResource, 0, len(b.consumableResources))

	for _, cr := range b.consumableResources {
		cp := *cr
		cp.Tags = tagsCloneOrEmpty(cr.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ConsumableResourceName < list[j].ConsumableResourceName
	})

	return list
}

// ListSchedulingPolicies returns all scheduling policies sorted by ARN.
func (b *InMemoryBackend) ListSchedulingPolicies() []*SchedulingPolicy {
	b.mu.RLock("ListSchedulingPolicies")
	defer b.mu.RUnlock()

	list := make([]*SchedulingPolicy, 0, len(b.schedulingPolicies))

	for _, sp := range b.schedulingPolicies {
		cp := *sp
		cp.Tags = tagsCloneOrEmpty(sp.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Arn < list[j].Arn })

	return list
}

// DescribeSchedulingPolicies returns scheduling policies, optionally filtered by ARNs.
func (b *InMemoryBackend) DescribeSchedulingPolicies(arns []string) []*SchedulingPolicy {
	b.mu.RLock("DescribeSchedulingPolicies")
	defer b.mu.RUnlock()

	if len(arns) == 0 {
		list := make([]*SchedulingPolicy, 0, len(b.schedulingPolicies))
		for _, sp := range b.schedulingPolicies {
			cp := *sp
			cp.Tags = tagsCloneOrEmpty(sp.Tags)
			list = append(list, &cp)
		}

		sort.Slice(list, func(i, j int) bool { return list[i].Arn < list[j].Arn })

		return list
	}

	list := make([]*SchedulingPolicy, 0, len(arns))

	for _, a := range arns {
		if sp, ok := b.schedulingPolicies[a]; ok {
			cp := *sp
			cp.Tags = tagsCloneOrEmpty(sp.Tags)
			list = append(list, &cp)
		}
	}

	return list
}

// DescribeServiceEnvironments returns service environments, optionally filtered by names/ARNs.
func (b *InMemoryBackend) DescribeServiceEnvironments(names []string) []*ServiceEnvironment {
	b.mu.RLock("DescribeServiceEnvironments")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		list := make([]*ServiceEnvironment, 0, len(b.serviceEnvironments))
		for _, se := range b.serviceEnvironments {
			cp := *se
			cp.Tags = tagsCloneOrEmpty(se.Tags)
			list = append(list, &cp)
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].ServiceEnvironmentName < list[j].ServiceEnvironmentName
		})

		return list
	}

	list := make([]*ServiceEnvironment, 0, len(names))

	for _, nameOrARN := range names {
		if se, ok := b.lookupServiceEnvironmentByNameOrARN(nameOrARN); ok {
			cp := *se
			cp.Tags = tagsCloneOrEmpty(se.Tags)
			list = append(list, &cp)
		}
	}

	return list
}

// UpdateSchedulingPolicy updates a scheduling policy's fairshare configuration.
func (b *InMemoryBackend) UpdateSchedulingPolicy(policyARN string, fairsharePolicy *FairsharePolicy) error {
	b.mu.Lock("UpdateSchedulingPolicy")
	defer b.mu.Unlock()

	sp, ok := b.schedulingPolicies[policyARN]
	if !ok {
		return fmt.Errorf("%w: scheduling policy %s not found", ErrNotFound, policyARN)
	}

	if fairsharePolicy != nil {
		sp.FairsharePolicy = cloneFairsharePolicy(fairsharePolicy)
	}

	return nil
}

// UpdateServiceEnvironment updates the state of a service environment.
func (b *InMemoryBackend) UpdateServiceEnvironment(nameOrARN, state string) (*ServiceEnvironment, error) {
	b.mu.Lock("UpdateServiceEnvironment")
	defer b.mu.Unlock()

	se, ok := b.lookupServiceEnvironmentByNameOrARN(nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: service environment %s not found", ErrNotFound, nameOrARN)
	}

	if state != "" {
		se.State = state
	}

	cp := *se
	cp.Tags = tagsCloneOrEmpty(se.Tags)

	return &cp, nil
}

// SubmitServiceJob creates a new service job in SUBMITTED status.
func (b *InMemoryBackend) SubmitServiceJob(name, serviceEnv string, tags map[string]string) (*ServiceJob, error) {
	b.mu.Lock("SubmitServiceJob")
	defer b.mu.Unlock()

	tagsCopy := tagsCloneOrEmpty(tags)
	now := time.Now().UnixMilli()
	jobID := uuid.NewString()
	jobARN := arn.Build("batch", b.region, b.accountID, "service-job/"+jobID)

	sj := &ServiceJob{
		ServiceJobID:       jobID,
		ServiceJobArn:      jobARN,
		ServiceJobName:     name,
		ServiceEnvironment: serviceEnv,
		Status:             jobStatusSubmitted,
		CreatedAt:          now,
		Tags:               tagsCopy,
	}
	b.serviceJobs[jobID] = sj
	cp := *sj

	return &cp, nil
}

// DescribeServiceJob returns a single service job by ID.
func (b *InMemoryBackend) DescribeServiceJob(serviceJobID string) (*ServiceJob, error) {
	b.mu.RLock("DescribeServiceJob")
	defer b.mu.RUnlock()

	sj, ok := b.serviceJobs[serviceJobID]
	if !ok {
		return nil, fmt.Errorf("%w: service job %s not found", ErrNotFound, serviceJobID)
	}

	cp := *sj
	cp.Tags = tagsCloneOrEmpty(sj.Tags)

	return &cp, nil
}

// ListServiceJobs returns service jobs, optionally filtered by service environment.
func (b *InMemoryBackend) ListServiceJobs(serviceEnv string) ([]*ServiceJob, error) {
	b.mu.RLock("ListServiceJobs")
	defer b.mu.RUnlock()

	list := make([]*ServiceJob, 0, len(b.serviceJobs))

	for _, sj := range b.serviceJobs {
		if serviceEnv != "" && sj.ServiceEnvironment != serviceEnv {
			continue
		}
		cp := *sj
		cp.Tags = tagsCloneOrEmpty(sj.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })

	return list, nil
}

// TerminateServiceJob marks a service job as FAILED.
func (b *InMemoryBackend) TerminateServiceJob(serviceJobID, reason string) error {
	b.mu.Lock("TerminateServiceJob")
	defer b.mu.Unlock()

	sj, ok := b.serviceJobs[serviceJobID]
	if !ok {
		return fmt.Errorf("%w: service job %s not found", ErrNotFound, serviceJobID)
	}

	now := time.Now().UnixMilli()
	sj.Status = jobStatusFailed
	sj.StatusReason = reason
	sj.StoppedAt = &now

	return nil
}

// GetJobQueueSnapshot returns a snapshot of the front of a job queue.
func (b *InMemoryBackend) GetJobQueueSnapshot(jobQueue string) (*JobQueueSnapshot, error) {
	b.mu.RLock("GetJobQueueSnapshot")
	defer b.mu.RUnlock()

	jq, ok := b.lookupJQByNameOrARN(jobQueue)
	if !ok {
		return nil, fmt.Errorf("%w: job queue %s not found", ErrNotFound, jobQueue)
	}

	ids := b.jobsByQueue[jq.JobQueueName]
	runnableJobs := make([]*Job, 0, len(ids))

	for _, id := range ids {
		j, ok2 := b.jobs[id]
		if !ok2 {
			continue
		}
		if j.Status == jobStatusRunnable {
			runnableJobs = append(runnableJobs, j)
		}
	}

	sort.Slice(runnableJobs, func(i, j int) bool { return runnableJobs[i].CreatedAt < runnableJobs[j].CreatedAt })

	const maxFrontOfQueue = 100
	if len(runnableJobs) > maxFrontOfQueue {
		runnableJobs = runnableJobs[:maxFrontOfQueue]
	}

	foqJobs := make([]FrontOfQueueJob, 0, len(runnableJobs))
	now := float64(time.Now().UnixMilli()) / msPerSecond

	for _, j := range runnableJobs {
		foqJobs = append(foqJobs, FrontOfQueueJob{
			JobArn:                 j.JobARN,
			EarliestTimeAtPosition: float64(j.CreatedAt) / msPerSecond,
		})
	}

	return &JobQueueSnapshot{
		FrontOfQueue: &FrontOfQueue{
			Jobs:      foqJobs,
			Timestamp: now,
		},
	}, nil
}

// ListJobsByConsumableResource returns jobs that reference the named consumable resource
// via their ConsumableResourceProperties.
func (b *InMemoryBackend) ListJobsByConsumableResource(consumableResource string) ([]*Job, error) {
	b.mu.RLock("ListJobsByConsumableResource")
	defer b.mu.RUnlock()

	list := make([]*Job, 0)

	for _, j := range b.jobs {
		if jobReferencesConsumableResource(j, consumableResource) {
			cp := *j
			cp.Tags = tagsCloneOrEmpty(j.Tags)
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })

	return list, nil
}

// jobReferencesConsumableResource reports whether a job's ConsumableResourceProperties
// references the named consumable resource.
func jobReferencesConsumableResource(j *Job, consumableResource string) bool {
	for _, crp := range j.ConsumableResourceProperties {
		if crp.ConsumableResource == consumableResource {
			return true
		}
	}

	return false
}
