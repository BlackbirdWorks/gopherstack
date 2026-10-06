package opsworks

import (
	"context"
	"encoding/json"
	"time"
)

// StorageBackend is the interface for OpsWorks storage operations.
type StorageBackend interface {
	// Stack operations
	CreateStack(name, region, defaultInstanceProfileArn, serviceRoleArn string, opts CreateStackOptions) (*Stack, error)
	CloneStack(sourceStackID, name, region, serviceRoleArn string, opts CloneStackOptions) (*Stack, error)
	DescribeStacks(stackIDs []string) ([]*Stack, error)
	UpdateStack(stackID, name string, opts UpdateStackOptions) error
	DeleteStack(stackID string) error
	StartStack(stackID string) error
	StopStack(stackID string) error
	GetHostnameSuggestion(layerID string) (string, error)
	DescribeStackSummary(stackID string) (*StackSummary, error)
	DescribeStackProvisioningParameters(stackID string) (agentInstallerURL string, params map[string]string, err error)

	// Layer operations
	CreateLayer(
		stackID, layerType, name, shortname string, settings LayerSettings,
	) (*Layer, error)
	DescribeLayers(stackID string, layerIDs []string) ([]*Layer, error)
	UpdateLayer(layerID, name, shortname string, settings LayerSettings) error
	DeleteLayer(layerID string) error

	// Instance operations
	CreateInstance(
		stackID string, layerIDs []string, instanceType string, opts CreateInstanceOptions,
	) (*Instance, error)
	RegisterInstance(stackID, hostname string, extras InstanceExtras) (string, error)
	DeregisterInstance(instanceID string) error
	AssignInstance(instanceID string, layerIDs []string) error
	UnassignInstance(instanceID string) error
	DescribeInstances(stackID, layerID string, instanceIDs []string) ([]*Instance, error)
	UpdateInstance(instanceID, hostname string, opts UpdateInstanceOptions) error
	DeleteInstance(instanceID string, deleteElasticIP, deleteVolumes bool) error
	StartInstance(instanceID string) error
	StopInstance(instanceID string) error
	RebootInstance(instanceID string) error

	// App operations
	CreateApp(stackID, name, appType string, opts AppOptions) (*App, error)
	DescribeApps(stackID string, appIDs []string) ([]*App, error)
	UpdateApp(appID, name, appType string, opts AppOptions) error
	DeleteApp(appID string) error

	// Deployment operations
	CreateDeployment(stackID, appID, command, customJSON string, opts DeploymentOptions) (*Deployment, error)
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
	DescribeElasticLoadBalancers(stackID string, layerIDs []string) ([]*ElasticLoadBalancer, error)

	// Elastic IP operations
	AssociateElasticIP(elasticIP, instanceID string) error
	DisassociateElasticIP(elasticIP string) error
	RegisterElasticIP(elasticIP, stackID string) (*ElasticIP, error)
	DeregisterElasticIP(elasticIP string) error
	DescribeElasticIps(stackID, instanceID string, ips []string) ([]*ElasticIP, error)
	UpdateElasticIP(elasticIP, name string) error

	// Volume operations
	RegisterVolume(ec2VolumeID, stackID string) (string, error)
	DeregisterVolume(volumeID string) error
	AssignVolume(volumeID, instanceID string) error
	UnassignVolume(volumeID string) error
	DescribeVolumes(stackID, instanceID, raidArrayID string, volumeIDs []string) ([]*Volume, error)
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
	DescribeAgentVersions(stackID string, configManagerName, configManagerVersion string) ([]*AgentVersion, error)
	DescribeOperatingSystems() ([]*OperatingSystem, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Stack represents an OpsWorks stack.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
//
// No Status field: the real AWS types.Stack has no such member (confirmed
// against aws-sdk-go-v2/service/opsworks@v1.31.0's types.go) -- a previous
// pass invented one and serialized it on the wire, which this pass removed.
type Stack struct {
	CreatedAt                 time.Time
	ConfigurationManager      *StackConfigurationManager
	ChefConfiguration         *ChefConfiguration
	UseOpsworksSecurityGroups *bool
	Tags                      map[string]string
	Attributes                map[string]string
	StackID                   string
	Arn                       string
	Name                      string
	Region                    string
	DefaultInstanceProfileArn string
	ServiceRoleArn            string
	VpcID                     string
	AgentVersion              string
	CustomJSON                string
	DefaultAvailabilityZone   string
	DefaultOs                 string
	DefaultRootDeviceType     string
	DefaultSSHKeyName         string
	DefaultSubnetID           string
	HostnameTheme             string
}

// StackConfigurationManager mirrors the real types.StackConfigurationManager
// (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's types.go).
type StackConfigurationManager struct {
	Name    string
	Version string
}

// ChefConfiguration mirrors the real types.ChefConfiguration (confirmed
// against aws-sdk-go-v2/service/opsworks@v1.31.0's types.go).
type ChefConfiguration struct {
	BerkshelfVersion string
	ManageBerkshelf  bool
}

// CreateStackOptions carries CreateStack's optional parameters. AWS's
// CreateStackInput also has CustomCookbooksSource/UseCustomCookbooks, which
// remain unmodeled -- honoring a cookbook source would mean fetching from a
// git/svn/s3/http repository, which this backend has no model for (see
// PARITY.md's items_still_open).
type CreateStackOptions struct {
	ConfigurationManager      *StackConfigurationManager
	ChefConfiguration         *ChefConfiguration
	UseOpsworksSecurityGroups *bool
	Attributes                map[string]string
	VpcID                     string
	AgentVersion              string
	CustomJSON                string
	DefaultAvailabilityZone   string
	DefaultOs                 string
	DefaultRootDeviceType     string
	DefaultSSHKeyName         string
	DefaultSubnetID           string
	HostnameTheme             string
}

// CloneStackOptions carries CloneStack's optional overrides; a zero value inherits from the source stack.
type CloneStackOptions struct {
	ClonePermissions          *bool
	ConfigurationManager      *StackConfigurationManager
	ChefConfiguration         *ChefConfiguration
	UseOpsworksSecurityGroups *bool
	Attributes                map[string]string
	VpcID                     string
	AgentVersion              string
	CustomJSON                string
	DefaultAvailabilityZone   string
	DefaultInstanceProfileArn string
	DefaultOs                 string
	DefaultRootDeviceType     string
	DefaultSSHKeyName         string
	DefaultSubnetID           string
	HostnameTheme             string
	CloneAppIDs               []string
}

// UpdateStackOptions carries UpdateStack's optional stack-attribute
// overrides. An empty string / nil pointer leaves the current value
// unchanged. No VpcId: the real UpdateStackInput has no such member
// (confirmed against api_op_UpdateStack.go) -- a stack's VPC cannot change
// after creation.
type UpdateStackOptions struct {
	ConfigurationManager      *StackConfigurationManager
	ChefConfiguration         *ChefConfiguration
	UseOpsworksSecurityGroups *bool
	Attributes                map[string]string
	AgentVersion              string
	CustomJSON                string
	DefaultAvailabilityZone   string
	DefaultInstanceProfileArn string
	DefaultOs                 string
	DefaultRootDeviceType     string
	DefaultSSHKeyName         string
	DefaultSubnetID           string
	HostnameTheme             string
	ServiceRoleArn            string
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

// InstancesCount holds counts of instances in various states. Field names
// and set match types.InstancesCount in aws-sdk-go-v2/service/opsworks
// exactly (19 states, no "Total" or "Starting" -- those were invented by a
// previous pass and did not exist in the real API; the real enum's
// transient boot/on-request states are "Requested", not "Starting"). This
// backend's instance state machine only ever produces "online"/"stopped"
// (see StartInstance/StopInstance doc comments), so only Online and Stopped
// are ever non-zero here; the rest exist for wire-shape completeness.
type InstancesCount struct {
	Assigning      int32
	Booting        int32
	ConnectionLost int32
	Deregistering  int32
	Online         int32
	Pending        int32
	Rebooting      int32
	Registered     int32
	Registering    int32
	Requested      int32
	RunningSetup   int32
	SetupFailed    int32
	ShuttingDown   int32
	StartFailed    int32
	StopFailed     int32
	Stopped        int32
	Stopping       int32
	Terminated     int32
	Terminating    int32
	Unassigning    int32
}

// Layer represents an OpsWorks layer.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Layer struct {
	CreatedAt            time.Time
	InstallUpdatesOnBoot *bool
	Attributes           map[string]string
	StackID              string
	LayerID              string
	Arn                  string
	Type                 string
	Name                 string
	Shortname            string
	Settings             LayerSettings
}

// LayerSettings carries the optional CreateLayer/UpdateLayer members; nil/empty means unset.
// Nested structures are kept as raw JSON so they round-trip verbatim.
type LayerSettings struct {
	InstallUpdatesOnBoot        *bool             `json:"InstallUpdatesOnBoot,omitempty"`
	AutoAssignElasticIps        *bool             `json:"AutoAssignElasticIps,omitempty"`
	AutoAssignPublicIps         *bool             `json:"AutoAssignPublicIps,omitempty"`
	EnableAutoHealing           *bool             `json:"EnableAutoHealing,omitempty"`
	UseEbsOptimizedInstances    *bool             `json:"UseEbsOptimizedInstances,omitempty"`
	Attributes                  map[string]string `json:"Attributes,omitempty"`
	CustomInstanceProfileArn    string            `json:"CustomInstanceProfileArn,omitempty"`
	CustomJSON                  string            `json:"CustomJson,omitempty"`
	CustomRecipes               json.RawMessage   `json:"CustomRecipes,omitempty"`
	CloudWatchLogsConfiguration json.RawMessage   `json:"CloudWatchLogsConfiguration,omitempty"`
	LifecycleEventConfiguration json.RawMessage   `json:"LifecycleEventConfiguration,omitempty"`
	VolumeConfigurations        json.RawMessage   `json:"VolumeConfigurations,omitempty"`
	Packages                    []string          `json:"Packages,omitempty"`
	CustomSecurityGroupIDs      []string          `json:"CustomSecurityGroupIds,omitempty"`
}

// Instance represents an OpsWorks instance.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Instance struct {
	CreatedAt            time.Time
	Extras               InstanceExtras
	InstallUpdatesOnBoot *bool
	AgentVersion         string
	InstanceID           string
	Arn                  string
	Hostname             string
	InstanceType         string
	Status               string
	StackID              string
	Architecture         string
	Os                   string
	SubnetID             string
	Tenancy              string
	LayerIDs             []string
	// Registered indicates this is an on-premises registered instance.
	Registered bool
}

// InstanceExtras holds the optional instance members echoed by DescribeInstances; unset means absent.
type InstanceExtras struct {
	EbsOptimized       *bool  `json:"EbsOptimized,omitempty"`
	AmiID              string `json:"AmiId,omitempty"`
	AutoScalingType    string `json:"AutoScalingType,omitempty"`
	AvailabilityZone   string `json:"AvailabilityZone,omitempty"`
	RootDeviceType     string `json:"RootDeviceType,omitempty"`
	SSHKeyName         string `json:"SshKeyName,omitempty"`
	VirtualizationType string `json:"VirtualizationType,omitempty"`
	PrivateIP          string `json:"PrivateIp,omitempty"`
	PublicIP           string `json:"PublicIp,omitempty"`
}

// CreateInstanceOptions carries CreateInstance's optional attribute
// overrides that this backend models (a subset of the real
// CreateInstanceInput's optional surface -- see PARITY.md's
// items_still_open for the unmodeled remainder).
type CreateInstanceOptions struct {
	Extras               InstanceExtras
	InstallUpdatesOnBoot *bool
	Hostname             string
	AgentVersion         string
	Architecture         string
	Os                   string
	SubnetID             string
	Tenancy              string
}

// UpdateInstanceOptions carries UpdateInstance's optional attribute
// overrides that this backend models.
type UpdateInstanceOptions struct {
	InstallUpdatesOnBoot *bool
	Extras               InstanceExtras
	InstanceType         string
	Architecture         string
	AgentVersion         string
	Os                   string
	LayerIDs             []string
}

// App represents an OpsWorks app.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
//
// Arn is kept as an internal bookkeeping field only (e.g. for future
// resourceExists-style lookups) -- it is deliberately NOT serialized on the
// wire in appsToJSON, because the real AWS types.App has no Arn member
// (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's types.go).
// Real OpsWorks apps are not independently ARN-addressable; TagResource /
// UntagResource / ListTags only accept a stack's or layer's ARN.
type App struct {
	CreatedAt time.Time
	StackID   string
	AppID     string
	Arn       string
	Name      string
	Type      string
	Options   AppOptions
}

// AppSource is types.Source; Password and SshKey are masked in responses.
type AppSource struct {
	Type     string `json:"Type,omitempty"`
	URL      string `json:"Url,omitempty"`
	Username string `json:"Username,omitempty"`
	Password string `json:"Password,omitempty"`
	SSHKey   string `json:"SshKey,omitempty"`
	Revision string `json:"Revision,omitempty"`
}

// AppDataSource is types.DataSource.
type AppDataSource struct {
	Type         string `json:"Type,omitempty"`
	Arn          string `json:"Arn,omitempty"`
	DatabaseName string `json:"DatabaseName,omitempty"`
}

// AppEnvVar is types.EnvironmentVariable; a Secure value is masked in responses.
type AppEnvVar struct {
	Secure *bool  `json:"Secure,omitempty"`
	Key    string `json:"Key"`
	Value  string `json:"Value"`
}

// AppSSL is types.SslConfiguration.
type AppSSL struct {
	Certificate string `json:"Certificate,omitempty"`
	PrivateKey  string `json:"PrivateKey,omitempty"`
	Chain       string `json:"Chain,omitempty"`
}

// AppOptions carries the optional CreateApp/UpdateApp members; nil/empty means unset.
type AppOptions struct {
	Attributes       map[string]string `json:"Attributes,omitempty"`
	AppSource        *AppSource        `json:"AppSource,omitempty"`
	SslConfiguration *AppSSL           `json:"SslConfiguration,omitempty"`
	EnableSsl        *bool             `json:"EnableSsl,omitempty"`
	Description      string            `json:"Description,omitempty"`
	Shortname        string            `json:"Shortname,omitempty"`
	Domains          []string          `json:"Domains,omitempty"`
	DataSources      []AppDataSource   `json:"DataSources,omitempty"`
	Environment      []AppEnvVar       `json:"Environment,omitempty"`
}

// Deployment represents an OpsWorks deployment.
// CreatedAt is first: time.Time non-pointer prefix reduces GC pointer bytes.
type Deployment struct {
	CreatedAt    time.Time
	CompletedAt  time.Time
	Comment      string
	StackID      string
	AppID        string
	DeploymentID string
	Command      string
	Status       string
	CustomJSON   string
	InstanceIDs  []string
	Duration     int32
}

// DeploymentOptions carries CreateDeployment's targeting and comment members.
type DeploymentOptions struct {
	Comment     string
	InstanceIDs []string
	LayerIDs    []string
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
//
// StackID is kept for the real, "This member is required"
// RegisterElasticIpInput field and the real DescribeElasticIpsInput's filter
// member, but is deliberately NOT serialized on the wire in
// elasticIpsToJSON: the real types.ElasticIp has no StackId member
// (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's types.go --
// only Domain/InstanceId/Ip/Name/Region).
type ElasticIP struct {
	IP         string
	Domain     string
	Name       string
	Region     string
	InstanceID string
	StackID    string
}

// Volume represents a registered volume.
//
// RegisteredAt is internal bookkeeping only (not serialized -- the real
// types.Volume has no such field). StackID is kept for stack-scoped lookups
// (deleteStackAssociations, DescribeVolumes' StackId filter) but likewise not
// serialized on the wire: the real AWS types.Volume has no StackId member
// (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's types.go) --
// only InstanceId and RaidArrayId associate a Volume with other resources on
// the wire.
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
