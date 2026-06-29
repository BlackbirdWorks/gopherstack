package opsworks

import (
	"context"
	"time"
)

// StorageBackend is the interface for OpsWorks storage operations.
type StorageBackend interface {
	// Stack operations
	CreateStack(name, region, defaultInstanceProfileArn, serviceRoleArn string) (*Stack, error)
	CloneStack(sourceStackID, name, region string) (*Stack, error)
	DescribeStacks(stackIDs []string) ([]*Stack, error)
	UpdateStack(stackID, name string) error
	DeleteStack(stackID string) error
	StartStack(stackID string) error
	StopStack(stackID string) error
	GetHostnameSuggestion(stackID, layerID string) (string, error)
	DescribeStackSummary(stackID string) (*StackSummary, error)
	DescribeStackProvisioningParameters(stackID string) (map[string]string, string, error)

	// Layer operations
	CreateLayer(stackID, layerType, name, shortname string) (*Layer, error)
	DescribeLayers(stackID string, layerIDs []string) ([]*Layer, error)
	UpdateLayer(layerID, name string) error
	DeleteLayer(layerID string) error

	// Instance operations
	CreateInstance(stackID, layerID, instanceType string) (*Instance, error)
	RegisterInstance(stackID, hostname string) (string, error)
	DeregisterInstance(instanceID string) error
	AssignInstance(instanceID string, layerIDs []string) error
	UnassignInstance(instanceID string) error
	DescribeInstances(stackID, layerID string, instanceIDs []string) ([]*Instance, error)
	UpdateInstance(instanceID, hostname string) error
	DeleteInstance(instanceID string) error
	StartInstance(instanceID string) error
	StopInstance(instanceID string) error
	RebootInstance(instanceID string) error

	// App operations
	CreateApp(stackID, name, appType string) (*App, error)
	DescribeApps(stackID string, appIDs []string) ([]*App, error)
	UpdateApp(appID, name string) error
	DeleteApp(appID string) error

	// Deployment operations
	CreateDeployment(stackID, appID, command string) (*Deployment, error)
	DescribeDeployments(stackID, appID string, deploymentIDs []string) ([]*Deployment, error)

	// Command operations
	DescribeCommands(deploymentID, instanceID string, commandIDs []string) ([]*Command, error)

	// Tag operations
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTags(resourceARN string, maxResults int32, nextToken string) (map[string]string, string, error)

	// User profile operations
	CreateUserProfile(iamUserArn, sshUsername, sshPublicKey string, allowSelfManagement bool) (*UserProfile, error)
	DeleteUserProfile(iamUserArn string) error
	DescribeUserProfiles(iamUserArns []string) ([]*UserProfile, error)
	UpdateUserProfile(iamUserArn, sshUsername, sshPublicKey string) error
	DescribeMyUserProfile() (*UserProfile, error)
	UpdateMyUserProfile(sshPublicKey string) error

	// Elastic Load Balancer operations
	AttachElasticLoadBalancer(elbName, layerID string) error
	DetachElasticLoadBalancer(elbName, layerID string) error
	DescribeElasticLoadBalancers(stackID, layerID string) ([]*ElasticLoadBalancer, error)

	// Elastic IP operations
	AssociateElasticIP(elasticIP, instanceID string) error
	DisassociateElasticIP(elasticIP string) error
	RegisterElasticIP(elasticIP, region string) (*ElasticIP, error)
	DeregisterElasticIP(elasticIP string) error
	DescribeElasticIps(instanceID string, ips []string) ([]*ElasticIP, error)
	UpdateElasticIP(elasticIP, name string) error

	// Volume operations
	RegisterVolume(ec2VolumeID, stackID string) (string, error)
	DeregisterVolume(volumeID string) error
	AssignVolume(volumeID, instanceID string) error
	UnassignVolume(volumeID string) error
	DescribeVolumes(instanceID, raidArrayID string, volumeIDs []string) ([]*Volume, error)
	UpdateVolume(volumeID, name, mountPoint string) error

	// RDS DB Instance operations
	RegisterRdsDBInstance(stackID, rdsDBInstanceArn, dbUser, dbPassword string) error
	DeregisterRdsDBInstance(rdsDBInstanceArn string) error
	DescribeRdsDBInstances(stackID string, rdsDBInstanceArns []string) ([]*RdsDBInstance, error)
	UpdateRdsDBInstance(rdsDBInstanceArn, dbUser, dbPassword string) error

	// ECS Cluster operations
	RegisterEcsCluster(ecsClusterArn, stackID string) (string, error)
	DeregisterEcsCluster(ecsClusterArn string) error
	DescribeEcsClusters(stackID string, ecsClusterArns []string) ([]*EcsCluster, error)

	// Permission operations
	SetPermission(stackID, iamUserArn, level string, allowSSH, allowSudo bool) error
	DescribePermissions(stackID, iamUserArn string) ([]*Permission, error)

	// Auto-scaling operations
	SetTimeBasedAutoScaling(instanceID string, schedule *AutoScalingSchedule) error
	DescribeTimeBasedAutoScaling(instanceIDs []string) ([]*TimeBasedAutoScaling, error)
	SetLoadBasedAutoScaling(layerID string, enable bool, upScaling, downScaling *ScalingParameters) error
	DescribeLoadBasedAutoScaling(layerIDs []string) ([]*LoadBasedAutoScaling, error)

	// Misc operations
	GrantAccess(instanceID string, validForInMinutes int32) (*TemporaryCredential, error)
	DescribeServiceErrors(stackID, instanceID string, serviceErrorIDs []string) ([]map[string]any, error)
	DescribeRaidArrays(instanceID, stackID string, raidArrayIDs []string) ([]map[string]any, error)
	DescribeAgentVersions(stackID string) ([]*AgentVersion, error)
	DescribeOperatingSystems() ([]*OperatingSystem, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Stack represents an OpsWorks stack.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Stack struct {
	CreatedAt                 time.Time
	Tags                      map[string]string
	StackID                   string
	Arn                       string
	Name                      string
	Region                    string
	DefaultInstanceProfileArn string
	ServiceRoleArn            string
	Status                    string
}

// StackSummary represents summary information about a stack.
type StackSummary struct {
	InstancesCount   *InstancesCount
	StackID          string
	Arn              string
	Name             string
	LayersCount      int32
	AppsCount        int32
	DeploymentsCount int32
}

// InstancesCount holds counts of instances in various states.
type InstancesCount struct {
	Online   int32
	Stopped  int32
	Starting int32
	Stopping int32
	Total    int32
}

// Layer represents an OpsWorks layer.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Layer struct {
	CreatedAt time.Time
	StackID   string
	LayerID   string
	Arn       string
	Type      string
	Name      string
	Shortname string
}

// Instance represents an OpsWorks instance.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Instance struct {
	CreatedAt    time.Time
	StackID      string
	LayerID      string
	InstanceID   string
	Arn          string
	Hostname     string
	InstanceType string
	Status       string
	// Registered indicates this is an on-premises registered instance.
	Registered bool
}

// App represents an OpsWorks app.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type App struct {
	CreatedAt time.Time
	StackID   string
	AppID     string
	Arn       string
	Name      string
	Type      string
}

// Deployment represents an OpsWorks deployment.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Deployment struct {
	CreatedAt    time.Time
	CompletedAt  time.Time
	StackID      string
	AppID        string
	DeploymentID string
	Command      string
	Status       string
	Duration     int32
}

// Command represents an OpsWorks command.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Command struct {
	CreatedAt      time.Time
	AcknowledgedAt time.Time
	CompletedAt    time.Time
	DeploymentID   string
	InstanceID     string
	CommandID      string
	Type           string
	Status         string
	LogURL         string
	ExitCode       int32
}

// UserProfile represents an OpsWorks IAM user profile.
type UserProfile struct {
	IamUserArn          string
	Name                string
	SSHUsername         string
	SSHPublicKey        string
	AllowSelfManagement bool
}

// ElasticLoadBalancer represents an OpsWorks-attached elastic load balancer.
type ElasticLoadBalancer struct {
	ElasticLoadBalancerName string
	Region                  string
	DNSName                 string
	StackID                 string
	LayerID                 string
}

// ElasticIP represents an elastic IP registered with OpsWorks.
type ElasticIP struct {
	IP         string
	Domain     string
	Name       string
	Region     string
	InstanceID string
}

// Volume represents a registered volume.
type Volume struct {
	RegisteredAt time.Time
	VolumeID     string
	Ec2VolumeID  string
	StackID      string
	InstanceID   string
	Name         string
	MountPoint   string
	Region       string
	Status       string
	Size         int32
}

// RdsDBInstance represents a registered RDS DB instance.
type RdsDBInstance struct {
	RdsDBInstanceArn     string
	DBInstanceIdentifier string
	DBUser               string
	StackID              string
	Region               string
	Address              string
}

// EcsCluster represents a registered ECS cluster.
type EcsCluster struct {
	RegisteredAt   time.Time
	EcsClusterArn  string
	EcsClusterName string
	StackID        string
	Status         string
}

// Permission represents OpsWorks stack access permissions for an IAM user.
type Permission struct {
	StackID    string
	IamUserArn string
	Level      string
	AllowSSH   bool
	AllowSudo  bool
}

// AutoScalingSchedule specifies time-based auto-scaling configuration.
type AutoScalingSchedule struct {
	Monday    map[string]string `json:"Monday"`
	Tuesday   map[string]string `json:"Tuesday"`
	Wednesday map[string]string `json:"Wednesday"`
	Thursday  map[string]string `json:"Thursday"`
	Friday    map[string]string `json:"Friday"`
	Saturday  map[string]string `json:"Saturday"`
	Sunday    map[string]string `json:"Sunday"`
}

// TimeBasedAutoScaling associates an instance with its auto-scaling schedule.
type TimeBasedAutoScaling struct {
	AutoScalingSchedule *AutoScalingSchedule
	InstanceID          string
}

// ScalingParameters holds scaling trigger thresholds.
type ScalingParameters struct {
	CPUThreshold       float64 `json:"CpuThreshold"`
	LoadThreshold      float64 `json:"LoadThreshold"`
	MemoryThreshold    float64 `json:"MemoryThreshold"`
	IgnoreMetricsTime  int32   `json:"IgnoreMetricsTime"`
	InstanceCount      int32   `json:"InstanceCount"`
	ThresholdsWaitTime int32   `json:"ThresholdsWaitTime"`
}

// LoadBasedAutoScaling associates a layer with load-based auto-scaling settings.
type LoadBasedAutoScaling struct {
	UpScaling   *ScalingParameters
	DownScaling *ScalingParameters
	LayerID     string
	Enable      bool
}

// TemporaryCredential holds short-lived SSH credentials returned by GrantAccess.
type TemporaryCredential struct {
	InstanceID        string
	Username          string
	Password          string
	ValidForInMinutes int32
}

// AgentVersion describes an OpsWorks agent version.
type AgentVersion struct {
	ConfigurationManager *ConfigurationManager
	Version              string
}

// ConfigurationManager describes a configuration manager.
type ConfigurationManager struct {
	Name    string
	Version string
}

// OperatingSystem describes a supported OpsWorks operating system.
// Strings first: moves slice ptr to later offset, reducing GC pointer scan bytes (80→72).
type OperatingSystem struct {
	ID                    string
	Name                  string
	Type                  string
	ReportedVersion       string
	ConfigurationManagers []*ConfigurationManager
	Supported             bool
}

var _ StorageBackend = (*InMemoryBackend)(nil)
