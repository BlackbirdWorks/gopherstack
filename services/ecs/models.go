package ecs

import "time"

// ---- Load balancer, logging, secrets, and volume models ----

// LoadBalancer wires ECS tasks to ALB/NLB target groups.
type LoadBalancer struct {
	TargetGroupArn   string `json:"targetGroupArn,omitempty"`
	LoadBalancerName string `json:"loadBalancerName,omitempty"`
	ContainerName    string `json:"containerName,omitempty"`
	ContainerPort    int    `json:"containerPort,omitempty"`
}

const (
	deploymentControllerRolling    = "ROLLING"
	deploymentControllerExternal   = "EXTERNAL"
	deploymentControllerCodeDeploy = "CODE_DEPLOY"
)

// DeploymentController specifies the deployment controller type for a service.
type DeploymentController struct {
	Type string `json:"type"`
}

// LogConfiguration specifies the logging driver and options for a container.
type LogConfiguration struct {
	LogDriver     string            `json:"logDriver"`
	Options       map[string]string `json:"options,omitempty"`
	SecretOptions []SecretOption    `json:"secretOptions,omitempty"`
}

// SecretOption is a secret key/value pair for log driver authentication.
type SecretOption struct {
	Name      string `json:"name"`
	ValueFrom string `json:"valueFrom"`
}

// FirelensConfiguration specifies FireLens routing for container logs.
type FirelensConfiguration struct {
	Options map[string]string `json:"options,omitempty"`
	Type    string            `json:"type"`
}

// SecretReference resolves a SecretsManager ARN or SSM path into an env var.
type SecretReference struct {
	Name      string `json:"name"`
	ValueFrom string `json:"valueFrom"`
}

// EFSVolumeConfiguration holds EFS-specific volume options.
type EFSVolumeConfiguration struct {
	AuthorizationConfig *EFSAuthorizationConfig `json:"authorizationConfig,omitempty"`
	FileSystemID        string                  `json:"fileSystemId"`
	RootDirectory       string                  `json:"rootDirectory,omitempty"`
	TransitEncryption   string                  `json:"transitEncryption,omitempty"`
}

// EFSAuthorizationConfig holds EFS authorization settings.
type EFSAuthorizationConfig struct {
	AccessPointID string `json:"accessPointId,omitempty"`
	IAM           string `json:"iam,omitempty"`
}

// HostVolumeProperties holds host-bind volume path.
type HostVolumeProperties struct {
	SourcePath string `json:"sourcePath,omitempty"`
}

// Volume defines a storage volume for a task definition.
type Volume struct {
	Host                   *HostVolumeProperties   `json:"host,omitempty"`
	EFSVolumeConfiguration *EFSVolumeConfiguration `json:"efsVolumeConfiguration,omitempty"`
	Name                   string                  `json:"name"`
	ConfiguredAtLaunch     bool                    `json:"configuredAtLaunch,omitempty"`
}

// MountPoint maps a volume into a container.
type MountPoint struct {
	SourceVolume  string `json:"sourceVolume,omitempty"`
	ContainerPath string `json:"containerPath,omitempty"`
	ReadOnly      bool   `json:"readOnly,omitempty"`
}

// VolumeFrom specifies a container whose volumes to mount.
type VolumeFrom struct {
	SourceContainer string `json:"sourceContainer,omitempty"`
	ReadOnly        bool   `json:"readOnly,omitempty"`
}

const (
	assignPublicIPEnabled  = "ENABLED"
	assignPublicIPDisabled = "DISABLED"
)

// AwsvpcConfiguration holds awsvpc mode network settings.
type AwsvpcConfiguration struct {
	AssignPublicIP string   `json:"assignPublicIp,omitempty"`
	Subnets        []string `json:"subnets"`
	SecurityGroups []string `json:"securityGroups,omitempty"`
}

// NetworkConfiguration holds the network settings for a task or service.
type NetworkConfiguration struct {
	AwsvpcConfiguration *AwsvpcConfiguration `json:"awsvpcConfiguration,omitempty"`
}

// HealthCheck configures the container health check.
type HealthCheck struct {
	Command     []string `json:"command"`
	Interval    int      `json:"interval,omitempty"`
	Timeout     int      `json:"timeout,omitempty"`
	Retries     int      `json:"retries,omitempty"`
	StartPeriod int      `json:"startPeriod,omitempty"`
}

// RepositoryCredentials references a SecretsManager secret for private registry pulls.
type RepositoryCredentials struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

// ServiceRegistry wires tasks into a CloudMap namespace for service discovery.
type ServiceRegistry struct {
	RegistryArn   string `json:"registryArn,omitempty"`
	ContainerName string `json:"containerName,omitempty"`
	Port          int    `json:"port,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
}

// ---- Container runtime and deployment models ----

// NetworkInterface holds the network interface details for a running container.
type NetworkInterface struct {
	AttachmentID       string `json:"attachmentId,omitempty"`
	PrivateIpv4Address string `json:"privateIpv4Address,omitempty"`
	Ipv6Address        string `json:"ipv6Address,omitempty"`
}

// NetworkBinding maps a container port to a host port.
type NetworkBinding struct {
	BindIP        string `json:"bindIP,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
	HostPort      int    `json:"hostPort,omitempty"`
}

// Container holds the runtime status of a single container within a task.
type Container struct {
	ContainerArn      string             `json:"containerArn,omitempty"`
	TaskArn           string             `json:"taskArn,omitempty"`
	Name              string             `json:"name"`
	Image             string             `json:"image,omitempty"`
	ImageDigest       string             `json:"imageDigest,omitempty"`
	RuntimeID         string             `json:"runtimeId,omitempty"`
	LastStatus        string             `json:"lastStatus"`
	HealthStatus      string             `json:"healthStatus,omitempty"`
	CPU               string             `json:"cpu,omitempty"`
	Memory            string             `json:"memory,omitempty"`
	MemoryReservation string             `json:"memoryReservation,omitempty"`
	Reason            string             `json:"reason,omitempty"`
	ExitCode          *int               `json:"exitCode,omitempty"`
	NetworkInterfaces []NetworkInterface `json:"networkInterfaces,omitempty"`
	NetworkBindings   []NetworkBinding   `json:"networkBindings,omitempty"`
}

// Deployment represents an active or completed deployment on an ECS service.
type Deployment struct {
	CreatedAt          *float64 `json:"createdAt,omitempty"`
	UpdatedAt          *float64 `json:"updatedAt,omitempty"`
	ID                 string   `json:"id"`
	Status             string   `json:"status"`
	TaskDefinition     string   `json:"taskDefinition"`
	LaunchType         string   `json:"launchType,omitempty"`
	PlatformVersion    string   `json:"platformVersion,omitempty"`
	RolloutState       string   `json:"rolloutState,omitempty"`
	RolloutStateReason string   `json:"rolloutStateReason,omitempty"`
	ServiceRevisionArn string   `json:"serviceRevisionArn,omitempty"`
	DesiredCount       int      `json:"desiredCount"`
	PendingCount       int      `json:"pendingCount"`
	RunningCount       int      `json:"runningCount"`
	FailedTasks        int      `json:"failedTasks"`
}

// ServiceRevision describes the workload configuration captured by a single
// deployment of an ECS service, as returned by DescribeServiceRevisions.
type ServiceRevision struct {
	CreatedAt                   *float64                       `json:"createdAt,omitempty"`
	NetworkConfiguration        *NetworkConfiguration          `json:"networkConfiguration,omitempty"`
	ServiceConnectConfiguration *ServiceConnectConfiguration   `json:"serviceConnectConfiguration,omitempty"`
	ClusterArn                  string                         `json:"clusterArn,omitempty"`
	LaunchType                  string                         `json:"launchType,omitempty"`
	PlatformVersion             string                         `json:"platformVersion,omitempty"`
	ServiceArn                  string                         `json:"serviceArn,omitempty"`
	ServiceRevisionArn          string                         `json:"serviceRevisionArn,omitempty"`
	TaskDefinition              string                         `json:"taskDefinition,omitempty"`
	CapacityProviderStrategy    []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	LoadBalancers               []LoadBalancer                 `json:"loadBalancers,omitempty"`
	ServiceRegistries           []ServiceRegistry              `json:"serviceRegistries,omitempty"`
}

// ---- Capacity provider strategy and cluster setting models ----

// CapacityProviderStrategyItem represents a single item in a default capacity provider strategy.
type CapacityProviderStrategyItem struct {
	CapacityProvider string `json:"capacityProvider"`
	Base             int    `json:"base,omitempty"`
	Weight           int    `json:"weight,omitempty"`
}

// ClusterSetting represents a single cluster setting name/value pair.
type ClusterSetting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// TaskProtection represents the protection state for a task.
type TaskProtection struct {
	ExpirationDate    *time.Time `json:"expirationDate,omitempty"`
	TaskArn           string     `json:"taskArn"`
	ProtectionEnabled bool       `json:"protectionEnabled"`
}

// UpdateExpressGatewayServiceInput holds input for UpdateExpressGatewayService.
// Mirrors the real UpdateExpressGatewayServiceInput shape: most fields describe
// the next service-revision configuration (CPU/Memory/HealthCheckPath/
// NetworkConfiguration/PrimaryContainer/ScalingTarget/TaskDefinitionArn/
// TaskRoleArn/ExecutionRoleArn), while InfrastructureRoleArn is the only field
// that mutates the service itself rather than spawning a new revision.
type UpdateExpressGatewayServiceInput struct {
	NetworkConfiguration  *ExpressGatewayServiceNetworkConfiguration
	PrimaryContainer      *ExpressGatewayContainer
	ScalingTarget         *ExpressGatewayScalingTarget
	ServiceArn            string
	CPU                   string
	Memory                string
	HealthCheckPath       string
	ExecutionRoleArn      string
	InfrastructureRoleArn string
	TaskDefinitionArn     string
	TaskRoleArn           string
}

// ---- Fargate/inference resource models ----

// RuntimePlatform specifies the CPU architecture and OS family for a task definition.
type RuntimePlatform struct {
	CPUArchitecture       string `json:"cpuArchitecture,omitempty"`
	OperatingSystemFamily string `json:"operatingSystemFamily,omitempty"`
}

// EphemeralStorage specifies the amount of ephemeral storage for a Fargate task.
type EphemeralStorage struct {
	SizeInGiB int `json:"sizeInGiB"`
}

// InferenceAccelerator specifies an Elastic Inference accelerator for a task definition.
type InferenceAccelerator struct {
	DeviceName string `json:"deviceName"`
	DeviceType string `json:"deviceType"`
}

// ResourceRequirement specifies a GPU or InferenceAccelerator requirement for a container.
type ResourceRequirement struct {
	Type  string `json:"type"`  // GPU or InferenceAccelerator
	Value string `json:"value"` // quantity (e.g. "1")
}

// EnvironmentFile specifies an S3 ARN containing environment variables for a container.
type EnvironmentFile struct {
	Type  string `json:"type"`  // always "s3"
	Value string `json:"value"` // S3 ARN
}

// DeploymentAlarms configures CloudWatch alarm-based rollback for a service deployment.
type DeploymentAlarms struct {
	AlarmNames []string `json:"alarmNames"`
	Enable     bool     `json:"enable"`
	Rollback   bool     `json:"rollback"`
}

// ---- Cluster/capacity-provider/task update input models ----

// UpdateClusterInput holds input for UpdateCluster.
type UpdateClusterInput struct {
	Cluster                         string
	CapacityProviders               []string
	DefaultCapacityProviderStrategy []CapacityProviderStrategyItem
	Settings                        []ClusterSetting
}

// UpdateCapacityProviderInput holds input for UpdateCapacityProvider.
type UpdateCapacityProviderInput struct {
	Name                     string
	Status                   string
	AutoScalingGroupProvider *AutoScalingGroupProvider
	Tags                     []Tag
}

// StartTaskInput holds input for StartTask (place a task on a specific container instance).
type StartTaskInput struct {
	Cluster            string
	TaskDefinition     string
	Group              string
	StartedBy          string
	ContainerInstances []string
}

// ---- Tag, capacity provider, account setting, and attribute models ----

// Tag is a key/value metadata pair.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ManagedScaling configures managed scaling for an ASG-backed capacity provider.
type ManagedScaling struct {
	Status                    string `json:"status,omitempty"`
	TargetCapacityPercent     int    `json:"targetCapacityPercent,omitempty"`
	MinimumScalingStepSize    int    `json:"minimumScalingStepSize,omitempty"`
	MaximumScalingStepSize    int    `json:"maximumScalingStepSize,omitempty"`
	InstanceWarmupPeriod      int    `json:"instanceWarmupPeriod,omitempty"`
	TargetCapacityUtilization int    `json:"targetCapacityUtilization,omitempty"`
}

// AutoScalingGroupProvider configures an ASG-backed capacity provider.
type AutoScalingGroupProvider struct {
	AutoScalingGroupArn          string          `json:"autoScalingGroupArn"`
	ManagedScaling               *ManagedScaling `json:"managedScaling,omitempty"`
	ManagedTerminationProtection string          `json:"managedTerminationProtection,omitempty"`
	ManagedDraining              string          `json:"managedDraining,omitempty"`
}

// CapacityProvider represents an ECS capacity provider.
type CapacityProvider struct {
	CreatedAt                time.Time                 `json:"createdAt"`
	CapacityProviderArn      string                    `json:"capacityProviderArn"`
	Name                     string                    `json:"name"`
	Status                   string                    `json:"status"`
	UpdateStatus             string                    `json:"updateStatus,omitempty"`
	UpdateStatusReason       string                    `json:"updateStatusReason,omitempty"`
	AutoScalingGroupProvider *AutoScalingGroupProvider `json:"autoScalingGroupProvider,omitempty"`
	Tags                     []Tag                     `json:"tags,omitempty"`
}

// AccountSetting represents an ECS account setting for a principal.
type AccountSetting struct {
	Name         string `json:"name"`
	Value        string `json:"value"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

// Attribute represents a custom ECS resource attribute.
type Attribute struct {
	Name       string `json:"name"`
	Value      string `json:"value,omitempty"`
	TargetID   string `json:"targetId,omitempty"`
	TargetType string `json:"targetType,omitempty"`
}

// ServiceDeployment represents an ECS service deployment.
type ServiceDeployment struct {
	CreatedAt            *time.Time `json:"createdAt,omitempty"`
	UpdatedAt            *time.Time `json:"updatedAt,omitempty"`
	ServiceDeploymentArn string     `json:"serviceDeploymentArn"`
	ClusterArn           string     `json:"clusterArn"`
	ServiceArn           string     `json:"serviceArn"`
	Status               string     `json:"status"`
	StatusReason         string     `json:"statusReason,omitempty"`
}

// ExpressGatewayServiceNetworkConfiguration is the VPC network configuration
// for an Express service's tasks. Mirrors
// types.ExpressGatewayServiceNetworkConfiguration.
type ExpressGatewayServiceNetworkConfiguration struct {
	SecurityGroups []string `json:"securityGroups,omitempty"`
	Subnets        []string `json:"subnets,omitempty"`
}

// ExpressGatewayServiceAwsLogsConfiguration is the CloudWatch Logs
// configuration for an Express service's primary container. Mirrors
// types.ExpressGatewayServiceAwsLogsConfiguration.
type ExpressGatewayServiceAwsLogsConfiguration struct {
	LogGroup        string `json:"logGroup"`
	LogStreamPrefix string `json:"logStreamPrefix"`
}

// ExpressGatewayRepositoryCredentials references the private-registry
// credentials for an Express service's primary container image. Mirrors
// types.ExpressGatewayRepositoryCredentials.
type ExpressGatewayRepositoryCredentials struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

// ExpressGatewayContainer is the primary container configuration for an
// Express service revision. Mirrors types.ExpressGatewayContainer.
type ExpressGatewayContainer struct {
	AwsLogsConfiguration  *ExpressGatewayServiceAwsLogsConfiguration `json:"awsLogsConfiguration,omitempty"`
	RepositoryCredentials *ExpressGatewayRepositoryCredentials       `json:"repositoryCredentials,omitempty"`
	ContainerPort         *int                                       `json:"containerPort,omitempty"`
	Image                 string                                     `json:"image"`
	Command               []string                                   `json:"command,omitempty"`
	Environment           []KeyValuePair                             `json:"environment,omitempty"`
	Secrets               []SecretReference                          `json:"secrets,omitempty"`
}

// ExpressGatewayScalingTarget is the auto-scaling configuration for an
// Express service revision. Mirrors types.ExpressGatewayScalingTarget.
type ExpressGatewayScalingTarget struct {
	AutoScalingTargetValue *int   `json:"autoScalingTargetValue,omitempty"`
	MaxTaskCount           *int   `json:"maxTaskCount,omitempty"`
	MinTaskCount           *int   `json:"minTaskCount,omitempty"`
	AutoScalingMetric      string `json:"autoScalingMetric,omitempty"`
}

// IngressPathSummary is an access endpoint for an Express service revision.
// This backend does not emulate the managed ALB/target-group infrastructure
// an Express service provisions, so IngressPaths is always empty -- see
// ECSExpressGatewayService.ActiveConfigurations in models.go and the
// managed-infrastructure note on ExpressGatewayServiceConfiguration. Mirrors
// types.IngressPathSummary.
type IngressPathSummary struct {
	AccessType string `json:"accessType,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
}

// ExpressGatewayServiceConfiguration is a single service-revision snapshot of
// an Express service's compute/network/scaling configuration. Every
// Create/UpdateExpressGatewayService call produces one of these, appended to
// (or replacing, per real AWS "one active revision" semantics -- see
// CreateExpressGatewayService/UpdateExpressGatewayService in
// express_gateway.go) ExpressGatewayService.ActiveConfigurations. Mirrors
// types.ExpressGatewayServiceConfiguration.
type ExpressGatewayServiceConfiguration struct {
	CreatedAt            time.Time                                  `json:"createdAt"`
	NetworkConfiguration *ExpressGatewayServiceNetworkConfiguration `json:"networkConfiguration,omitempty"`
	PrimaryContainer     *ExpressGatewayContainer                   `json:"primaryContainer,omitempty"`
	ScalingTarget        *ExpressGatewayScalingTarget               `json:"scalingTarget,omitempty"`
	ServiceRevisionArn   string                                     `json:"serviceRevisionArn,omitempty"`
	CPU                  string                                     `json:"cpu,omitempty"`
	Memory               string                                     `json:"memory,omitempty"`
	HealthCheckPath      string                                     `json:"healthCheckPath,omitempty"`
	ExecutionRoleArn     string                                     `json:"executionRoleArn,omitempty"`
	TaskRoleArn          string                                     `json:"taskRoleArn,omitempty"`
	TaskDefinitionArn    string                                     `json:"taskDefinitionArn,omitempty"`
	IngressPaths         []IngressPathSummary                       `json:"ingressPaths,omitempty"`
}

// ExpressGatewayServiceStatus is the current status of an Express service.
// Mirrors types.ExpressGatewayServiceStatus (StatusCode + StatusReason),
// which real AWS nests as an object rather than a bare status string.
type ExpressGatewayServiceStatus struct {
	StatusCode   string `json:"statusCode,omitempty"`
	StatusReason string `json:"statusReason,omitempty"`
}

// ExpressGatewayService represents an ECS express gateway service. Mirrors
// types.ECSExpressGatewayService. Note ExecutionRoleArn/TaskRoleArn/CPU/
// Memory/etc. are NOT top-level fields on the real shape -- they live on each
// entry of ActiveConfigurations (see ExpressGatewayServiceConfiguration);
// this struct previously invented a top-level ExecutionRoleArn field that
// does not exist on the wire.
type ExpressGatewayService struct {
	CreatedAt             time.Time                            `json:"createdAt"`
	UpdatedAt             time.Time                            `json:"updatedAt"`
	ServiceArn            string                               `json:"serviceArn"`
	ServiceName           string                               `json:"serviceName"`
	Cluster               string                               `json:"cluster"`
	Status                string                               `json:"status"`
	StatusReason          string                               `json:"statusReason,omitempty"`
	InfrastructureRoleArn string                               `json:"infrastructureRoleArn"`
	CurrentDeployment     string                               `json:"currentDeployment,omitempty"`
	ActiveConfigurations  []ExpressGatewayServiceConfiguration `json:"activeConfigurations,omitempty"`
	Tags                  []Tag                                `json:"tags,omitempty"`
}

// Failure represents a resource-level failure returned in batch operations.
type Failure struct {
	Arn    string `json:"arn,omitempty"`
	Detail string `json:"detail,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// CreateCapacityProviderInput holds input for CreateCapacityProvider.
type CreateCapacityProviderInput struct {
	Name                     string
	AutoScalingGroupProvider *AutoScalingGroupProvider
	Tags                     []Tag
}

// CreateExpressGatewayServiceInput holds input for CreateExpressGatewayService.
type CreateExpressGatewayServiceInput struct {
	NetworkConfiguration  *ExpressGatewayServiceNetworkConfiguration
	PrimaryContainer      *ExpressGatewayContainer
	ScalingTarget         *ExpressGatewayScalingTarget
	ExecutionRoleArn      string
	InfrastructureRoleArn string
	Cluster               string
	ServiceName           string
	CPU                   string
	Memory                string
	HealthCheckPath       string
	TaskDefinitionArn     string
	TaskRoleArn           string
	Tags                  []Tag
}

// ---- Service Connect and task override models ----

// ServiceConnectClientAlias represents an alias for a service connect client.
type ServiceConnectClientAlias struct {
	DNSName string `json:"dnsName,omitempty"`
	Port    int    `json:"port"`
}

// ServiceConnectService represents a service in a service connect configuration.
type ServiceConnectService struct {
	PortName      string                      `json:"portName"`
	DiscoveryName string                      `json:"discoveryName,omitempty"`
	ClientAliases []ServiceConnectClientAlias `json:"clientAliases,omitempty"`
}

// ServiceConnectConfiguration represents the service connect configuration for a service.
type ServiceConnectConfiguration struct {
	Namespace string                  `json:"namespace,omitempty"`
	Services  []ServiceConnectService `json:"services,omitempty"`
	Enabled   bool                    `json:"enabled"`
}

// ContainerOverride represents a container override for a task.
type ContainerOverride struct {
	CPU         *int              `json:"cpu,omitempty"`
	Memory      *int              `json:"memory,omitempty"`
	Name        string            `json:"name"`
	Command     []string          `json:"command,omitempty"`
	Environment []KeyValuePair    `json:"environment,omitempty"`
	Secrets     []SecretReference `json:"secrets,omitempty"`
}

// TaskOverride represents overrides for a task run.
type TaskOverride struct {
	TaskRoleArn        string              `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn   string              `json:"executionRoleArn,omitempty"`
	CPU                string              `json:"cpu,omitempty"`
	Memory             string              `json:"memory,omitempty"`
	ContainerOverrides []ContainerOverride `json:"containerOverrides,omitempty"`
}

// ---- Deployment configuration and placement models ----

// DeploymentCircuitBreaker configures the deployment circuit breaker for a service.
type DeploymentCircuitBreaker struct {
	Enable   bool `json:"enable"`
	Rollback bool `json:"rollback"`
}

// DeploymentConfiguration holds deployment settings for a service.
// MinimumHealthyPercent and MaximumPercent default to the AWS values of 100 and 200.
type DeploymentConfiguration struct {
	DeploymentCircuitBreaker *DeploymentCircuitBreaker `json:"deploymentCircuitBreaker,omitempty"`
	MinimumHealthyPercent    *int                      `json:"minimumHealthyPercent,omitempty"`
	MaximumPercent           *int                      `json:"maximumPercent,omitempty"`
}

const (
	defaultMinimumHealthyPercent = 100
	defaultMaximumPercent        = 200
)

// withAWSDefaults fills in AWS-mandated defaults for unset deployment configuration fields.
func (dc *DeploymentConfiguration) withAWSDefaults() *DeploymentConfiguration {
	if dc == nil {
		minPct := defaultMinimumHealthyPercent
		maxPct := defaultMaximumPercent

		return &DeploymentConfiguration{
			MinimumHealthyPercent: &minPct,
			MaximumPercent:        &maxPct,
		}
	}

	out := *dc

	if out.MinimumHealthyPercent == nil {
		minPct := defaultMinimumHealthyPercent
		out.MinimumHealthyPercent = &minPct
	}

	if out.MaximumPercent == nil {
		maxPct := defaultMaximumPercent
		out.MaximumPercent = &maxPct
	}

	return &out
}

// PlacementConstraint specifies a placement constraint for a task or service.
type PlacementConstraint struct {
	Type       string `json:"type,omitempty"`
	Expression string `json:"expression,omitempty"`
}

// PlacementStrategy specifies a placement strategy for tasks.
type PlacementStrategy struct {
	Type  string `json:"type,omitempty"`
	Field string `json:"field,omitempty"`
}

// TaskAttachment represents an ENI or other attachment on a task (e.g. Fargate ENI).
type TaskAttachment struct {
	ID      string         `json:"id"`
	Type    string         `json:"type"`
	Status  string         `json:"status"`
	Details []KeyValuePair `json:"details,omitempty"`
}

// ---- Core cluster/task-definition/service/task models ----

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
	TaskRoleArn string `json:"taskRoleArn,omitempty"`
	// CapacityProviderName is the capacity provider actually selected to run this
	// task. Set from RunTaskInput.CapacityProviderStrategy (the first entry, as a
	// documented simplification: this backend does not model weight/base-driven
	// distribution across multiple providers -- see RunTask in tasks.go).
	CapacityProviderName string           `json:"capacityProviderName,omitempty"`
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
	Overrides                *TaskOverride         `json:"overrides,omitempty"`
	NetworkConfiguration     *NetworkConfiguration `json:"networkConfiguration,omitempty"`
	Cluster                  string                `json:"cluster,omitempty"`
	TaskDefinition           string                `json:"taskDefinition"`
	LaunchType               string                `json:"launchType,omitempty"`
	Group                    string                `json:"group,omitempty"`
	StartedBy                string                `json:"startedBy,omitempty"`
	PlatformVersion          string                `json:"platformVersion,omitempty"`
	PropagateTags            string                `json:"propagateTags,omitempty"`
	serviceNameForTags       string
	Tags                     []Tag `json:"tags,omitempty"`
	serviceTagsForPropagate  []Tag
	PlacementConstraints     []PlacementConstraint          `json:"placementConstraints,omitempty"`
	PlacementStrategy        []PlacementStrategy            `json:"placementStrategy,omitempty"`
	CapacityProviderStrategy []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	Count                    int                            `json:"count,omitempty"`
	EnableECSManagedTags     bool                           `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                           `json:"enableExecuteCommand,omitempty"`
}

// ListTaskDefinitionsInput holds optional filters for ListTaskDefinitions.
type ListTaskDefinitionsInput struct {
	FamilyPrefix string
	// Status filters by task definition status: "ACTIVE", "INACTIVE", or
	// "DELETE_IN_PROGRESS". Empty string matches only ACTIVE (AWS default).
	Status string
}

// ---- Container instance and task set models ----

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
	NetworkConfiguration     *NetworkConfiguration          `json:"networkConfiguration,omitempty"`
	CreatedAt                time.Time                      `json:"createdAt"`
	UpdatedAt                time.Time                      `json:"updatedAt"`
	StabilityStatusAt        time.Time                      `json:"stabilityStatusAt"`
	Status                   string                         `json:"status"`
	TaskSetArn               string                         `json:"taskSetArn"`
	ID                       string                         `json:"id"`
	ServiceArn               string                         `json:"serviceArn"`
	ClusterArn               string                         `json:"clusterArn"`
	TaskDefinition           string                         `json:"taskDefinition"`
	ExternalID               string                         `json:"externalId,omitempty"`
	PlatformVersion          string                         `json:"platformVersion,omitempty"`
	LaunchType               string                         `json:"launchType,omitempty"`
	StabilityStatus          string                         `json:"stabilityStatus,omitempty"`
	Scale                    TaskSetScale                   `json:"scale"`
	LoadBalancers            []LoadBalancer                 `json:"loadBalancers,omitempty"`
	ServiceRegistries        []ServiceRegistry              `json:"serviceRegistries,omitempty"`
	CapacityProviderStrategy []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
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
	NetworkConfiguration     *NetworkConfiguration
	Scale                    *TaskSetScale
	Cluster                  string
	Service                  string
	TaskDefinition           string
	ExternalID               string
	PlatformVersion          string
	LaunchType               string
	LoadBalancers            []LoadBalancer
	ServiceRegistries        []ServiceRegistry
	CapacityProviderStrategy []CapacityProviderStrategyItem
	Tags                     []Tag
}
