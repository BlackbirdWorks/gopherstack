package emr

import (
	"time"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

const (
	StateWaiting              = "WAITING"
	StateTerminated           = "TERMINATED"
	StateTerminatedWithErrors = "TERMINATED_WITH_ERRORS"

	// StateRunning is the real ClusterState value StartSession's doc allows
	// sessions to start against, alongside StateWaiting. This backend's own
	// cluster state machine never produces RUNNING -- clusters are created
	// directly in WAITING (see buildNewCluster) and go straight from WAITING
	// to TERMINATED (see terminateSingle), with no simulated
	// STARTING/BOOTSTRAPPING/RUNNING/TERMINATING in between -- so in
	// practice only StateWaiting ever passes the sessionCanStart check
	// below. StateRunning is included for correctness against the real API
	// and forward-compatibility (e.g. a hand-seeded test cluster).
	StateRunning = "RUNNING"

	StepStatePending   = "PENDING"
	StepStateCompleted = "COMPLETED"
	StepStateCancelled = "CANCELLED"

	// SessionStateSubmitted and its siblings below mirror the real
	// SessionState enum (aws-sdk-go-v2/service/emr/types/enums.go) verbatim:
	// SUBMITTED, STARTING, STARTED, IDLE, BUSY, TERMINATING, TERMINATED,
	// FAILED. This backend only ever drives two of them -- see sessions.go's
	// package doc comment for the full state-model rationale.
	SessionStateSubmitted   = "SUBMITTED"
	SessionStateStarting    = "STARTING"
	SessionStateStarted     = "STARTED"
	SessionStateIdle        = "IDLE"
	SessionStateBusy        = "BUSY"
	SessionStateTerminating = "TERMINATING"
	SessionStateTerminated  = "TERMINATED"
	SessionStateFailed      = "FAILED"

	// cancelStepsStatusSubmitted/Failed are the only two values of the real
	// CancelStepsRequestStatus enum (SUBMITTED | FAILED) -- not the ad hoc
	// "SUCCESS"/"QUEUED" strings this backend used to return.
	cancelStepsStatusSubmitted = "SUBMITTED"
	cancelStepsStatusFailed    = "FAILED"

	defaultReleaseLabel    = "emr-7.3.0"
	defaultStepConcurrency = 1

	minIdleTimeout = 60
	maxIdleTimeout = 604800

	minStepConcurrency = 1
	maxStepConcurrency = 256

	timelineKeyCreation = "CreationDateTime"
	timelineKeyReady    = "ReadyDateTime"
	timelineKeyEnd      = "EndDateTime"

	// stepCompletionDelay is how long a step stays PENDING before gopherstack
	// promotes it to COMPLETED on read. AWS steps run asynchronously and may
	// stay PENDING/RUNNING for as long as the underlying Hadoop job takes;
	// gopherstack has no real workload to run, so it simulates near-instant
	// completion instead of leaving steps parked in PENDING forever, which
	// would hang a real client's StepComplete waiter (min poll interval 30s).
	stepCompletionDelay = 3 * time.Second

	listClustersPageSize         = 50
	listSecConfigsPageSize       = 50
	listReleaseLabelsPage        = 50
	listInstanceTypesPage        = 50
	listStepsPageSize            = 50
	listInstancesPageSize        = 500
	listStudiosPageSize          = 50
	listNotebookExecPageSize     = 50
	listBootstrapActionsPageSize = 50
	listSessionsPageSize         = 50

	instanceGroupStateRunning = "RUNNING"

	defaultSSHPort = 22

	sessionCredentialExpiry = 12 * time.Hour

	archX86   = "X86_64"
	archARM64 = "ARM64"

	// EC2 instance size constants used in the supportedInstanceTypes catalog.
	vcpu4  = 4
	vcpu8  = 8
	vcpu16 = 16
	vcpu32 = 32

	gb8   = float64(8)
	gb16  = float64(16)
	gb30  = float64(30)
	gb32  = float64(32)
	gb61  = float64(61)
	gb64  = float64(64)
	gb128 = float64(128)

	ndisk1 = 1
	ndisk2 = 2

	appHadoop = "Hadoop"
	appHive   = "Hive"
	appHue    = "Hue"
	appLivy   = "Livy"
	appMXNet  = "MXNet"
	appOozie  = "Oozie"
	appPig    = "Pig"
	appPresto = "Presto"
	appSpark  = "Spark"
	appTez    = "Tez"
	appFlink  = "Flink"
	appHBase  = "HBase"
	appTF     = "TensorFlow"
	appTrino  = "Trino"
)

// --- Domain types ---

// Tag is an EMR resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Configuration is a recursive EMR classification configuration entry.
type Configuration struct {
	Classification string            `json:"Classification,omitempty"`
	Properties     map[string]string `json:"Properties,omitempty"`
	Configurations []Configuration   `json:"Configurations,omitempty"`
}

// Application represents an EMR application bundled in a cluster.
type Application struct {
	Name    string `json:"Name"`
	Version string `json:"Version,omitempty"`
}

// BootstrapActionScript holds the script path and arguments for a bootstrap action.
type BootstrapActionScript struct {
	Path string   `json:"Path"`
	Args []string `json:"Args,omitempty"`
}

// BootstrapActionConfig is the full bootstrap action specification used in RunJobFlow input.
type BootstrapActionConfig struct {
	Name                  string                `json:"Name"`
	ScriptBootstrapAction BootstrapActionScript `json:"ScriptBootstrapAction"`
}

// Command is the flattened representation of a bootstrap action returned by ListBootstrapActions.
type Command struct {
	Name       string   `json:"Name"`
	ScriptPath string   `json:"ScriptPath"`
	Args       []string `json:"Args,omitempty"`
}

// StepHadoopJarStep defines the JAR execution for a step.
type StepHadoopJarStep struct {
	Jar       string   `json:"Jar"`
	MainClass string   `json:"MainClass,omitempty"`
	Args      []string `json:"Args,omitempty"`
}

// StepTimeline tracks creation and completion times of a step.
type StepTimeline struct {
	CreationDateTime float64 `json:"CreationDateTime"`
	StartDateTime    float64 `json:"StartDateTime,omitempty"`
	EndDateTime      float64 `json:"EndDateTime,omitempty"`
}

// StepStatus holds the lifecycle state of an EMR step.
type StepStatus struct {
	State    string       `json:"State"`
	Timeline StepTimeline `json:"Timeline"`
}

// CancelStepsInfo represents the result of cancelling a single step.
type CancelStepsInfo struct {
	StepID string `json:"StepId"`
	Status string `json:"Status"`
	Reason string `json:"Reason,omitempty"`
}

// Step represents an EMR step attached to a cluster.
type Step struct {
	ID              string            `json:"Id"`
	Name            string            `json:"Name"`
	HadoopJarStep   StepHadoopJarStep `json:"HadoopJarStep"`
	ActionOnFailure string            `json:"ActionOnFailure"`
	Status          StepStatus        `json:"Status"`
}

// StepSpec is the input for adding a new step.
type StepSpec struct {
	Name            string            `json:"Name"`
	ActionOnFailure string            `json:"ActionOnFailure"`
	HadoopJarStep   StepHadoopJarStep `json:"HadoopJarStep"`
}

// ComputeLimits defines compute bounds for managed scaling.
type ComputeLimits struct {
	UnitType                     string `json:"UnitType"`
	MinimumCapacityUnits         int    `json:"MinimumCapacityUnits"`
	MaximumCapacityUnits         int    `json:"MaximumCapacityUnits"`
	MaximumOnDemandCapacityUnits int    `json:"MaximumOnDemandCapacityUnits,omitempty"`
	MaximumCoreCapacityUnits     int    `json:"MaximumCoreCapacityUnits,omitempty"`
}

// ManagedScalingPolicy defines managed scaling for a cluster.
type ManagedScalingPolicy struct {
	ComputeLimits ComputeLimits `json:"ComputeLimits"`
}

// AutoTerminationPolicy defines the auto-termination idle timeout for a cluster.
type AutoTerminationPolicy struct {
	IdleTimeout int64 `json:"IdleTimeout"`
}

// PortRange defines an inclusive range of port numbers.
type PortRange struct {
	MinRange int `json:"MinRange"`
	MaxRange int `json:"MaxRange"`
}

// BlockPublicAccessConfiguration is the account-level block-public-access config.
type BlockPublicAccessConfiguration struct {
	region                                 string
	PermittedPublicSecurityGroupRuleRanges []PortRange `json:"PermittedPublicSecurityGroupRuleRanges,omitempty"`
	BlockPublicSecurityGroupRules          bool        `json:"BlockPublicSecurityGroupRules"`
}

// blockPublicAccessMeta holds metadata for the block-public-access configuration.
// CreationDateTime is epoch seconds (float64); see SecurityConfiguration for
// why (EMR's awsjson1.1 unixTimestamp wire format).
type blockPublicAccessMeta struct {
	CreatedByArn string `json:"CreatedByArn,omitempty"`
	// region is the store.Table primary key (one meta record per region).
	region           string
	CreationDateTime float64 `json:"CreationDateTime"`
}

// AutoScalingConstraints defines capacity bounds for an auto-scaling policy.
type AutoScalingConstraints struct {
	MinCapacity int `json:"MinCapacity"`
	MaxCapacity int `json:"MaxCapacity"`
}

// SimpleScalingPolicyConfiguration defines scaling adjustment details.
type SimpleScalingPolicyConfiguration struct {
	AdjustmentType    string `json:"AdjustmentType"`
	ScalingAdjustment int    `json:"ScalingAdjustment"`
	CoolDown          int    `json:"CoolDown,omitempty"`
}

// ScalingAction defines what to do when a scaling rule fires.
type ScalingAction struct {
	SimpleScalingPolicyConfiguration SimpleScalingPolicyConfiguration `json:"SimpleScalingPolicyConfiguration"`
}

// CloudWatchAlarmDefinition is the CloudWatch alarm that triggers scaling.
type CloudWatchAlarmDefinition struct {
	ComparisonOperator string  `json:"ComparisonOperator"`
	MetricName         string  `json:"MetricName"`
	Namespace          string  `json:"Namespace,omitempty"`
	Statistic          string  `json:"Statistic,omitempty"`
	Unit               string  `json:"Unit,omitempty"`
	EvaluationPeriods  int     `json:"EvaluationPeriods"`
	Period             int     `json:"Period"`
	Threshold          float64 `json:"Threshold"`
}

// ScalingTrigger wraps a CloudWatch alarm definition.
type ScalingTrigger struct {
	CloudWatchAlarmDefinition CloudWatchAlarmDefinition `json:"CloudWatchAlarmDefinition"`
}

// ScalingRule is a named auto-scaling rule combining action and trigger.
type ScalingRule struct {
	Name        string         `json:"Name"`
	Description string         `json:"Description,omitempty"`
	Action      ScalingAction  `json:"Action"`
	Trigger     ScalingTrigger `json:"Trigger"`
}

// AutoScalingPolicySpec is used as input to PutAutoScalingPolicy.
type AutoScalingPolicySpec struct {
	Rules       []ScalingRule          `json:"Rules,omitempty"`
	Constraints AutoScalingConstraints `json:"Constraints"`
}

// AutoScalingPolicyDetail is stored on an instance group after PutAutoScalingPolicy.
type AutoScalingPolicyDetail struct {
	Status      map[string]string      `json:"Status"`
	Rules       []ScalingRule          `json:"Rules,omitempty"`
	Constraints AutoScalingConstraints `json:"Constraints"`
}

// ClusterInstance represents a single EC2 instance in a cluster.
type ClusterInstance struct {
	ID               string                `json:"Id"`
	Ec2InstanceID    string                `json:"Ec2InstanceId"`
	PrivateDNSName   string                `json:"PrivateDnsName"`
	PublicDNSName    string                `json:"PublicDnsName,omitempty"`
	PrivateIPAddress string                `json:"PrivateIpAddress,omitempty"`
	InstanceGroupID  string                `json:"InstanceGroupId,omitempty"`
	InstanceFleetID  string                `json:"InstanceFleetId,omitempty"`
	Market           string                `json:"Market"`
	InstanceType     string                `json:"InstanceType"`
	Status           ClusterInstanceStatus `json:"Status"`
}

// ClusterInstanceStatus holds the state of a ClusterInstance.
type ClusterInstanceStatus struct {
	State string `json:"State"`
}

// SupportedInstanceType describes an EC2 instance type supported by EMR.
type SupportedInstanceType struct {
	Type          string  `json:"Type"`
	Architecture  string  `json:"Architecture"`
	MemoryGB      float64 `json:"MemoryGB"`
	VCPU          int     `json:"VCPU"`
	NumberOfDisks int     `json:"NumberOfDisks,omitempty"`
	Is64BitsOnly  bool    `json:"Is64BitsOnly"`
}

// ReleaseLabelApplication is an application listed for a release label.
type ReleaseLabelApplication struct {
	Name    string `json:"Name"`
	Version string `json:"Version"`
}

// ReleaseLabel holds details about an EMR release label.
type ReleaseLabel struct {
	ReleaseLabel string                    `json:"ReleaseLabel"`
	Applications []ReleaseLabelApplication `json:"Applications,omitempty"`
}

// InstanceFleetStatus tracks the provisioning state of an EMR instance fleet.
type InstanceFleetStatus struct {
	State string `json:"State"`
}

// NotebookExecutionStatus values for a notebook execution.
const (
	NotebookStatusRunning  = "RUNNING"
	NotebookStatusStopping = "STOPPING"
	NotebookStatusStopped  = "STOPPED"
	NotebookStatusFinished = "FINISHED"
)

// NotebookExecution represents an EMR Studio notebook execution.
//
// StartTime/EndTime are epoch seconds (float64), matching the EMR awsjson1.1
// wire format -- the real SDK deserializer parses these with
// smithytime.ParseEpochSeconds and rejects RFC3339 strings. A zero value
// (unset) is omitted via omitempty, matching the "not yet ended" case where
// AWS omits EndTime entirely.
type NotebookExecution struct {
	NotebookExecutionID   string `json:"NotebookExecutionId"`
	EditorID              string `json:"EditorId,omitempty"`
	NotebookExecutionName string `json:"NotebookExecutionName,omitempty"`
	NotebookParams        string `json:"NotebookParams,omitempty"`
	ExecutionEngineID     string `json:"ExecutionEngineId,omitempty"`
	Status                string `json:"Status"`
	region                string
	Tags                  []Tag   `json:"Tags"`
	StartTime             float64 `json:"StartTime,omitempty"`
	EndTime               float64 `json:"EndTime,omitempty"`
}

// InstanceGroupStatus is the status of an EMR instance group.
type InstanceGroupStatus struct {
	State string `json:"State"`
}

// InstanceGroupSpec is the input specification for an instance group from RunJobFlow.
type InstanceGroupSpec struct {
	Name           string          `json:"Name"`
	Market         string          `json:"Market"`
	InstanceRole   string          `json:"InstanceRole"`
	InstanceType   string          `json:"InstanceType"`
	BidPrice       string          `json:"BidPrice,omitempty"`
	Configurations []Configuration `json:"Configurations,omitempty"`
	InstanceCount  int             `json:"InstanceCount"`
}

// InstanceGroup represents an EMR instance group returned by ListInstanceGroups.
type InstanceGroup struct {
	AutoScalingPolicy      *AutoScalingPolicyDetail `json:"AutoScalingPolicy,omitempty"`
	Status                 InstanceGroupStatus      `json:"Status"`
	ID                     string                   `json:"Id"`
	Name                   string                   `json:"Name"`
	Market                 string                   `json:"Market"`
	BidPrice               string                   `json:"BidPrice,omitempty"`
	InstanceGroupType      string                   `json:"InstanceGroupType"`
	InstanceType           string                   `json:"InstanceType"`
	Configurations         []Configuration          `json:"Configurations,omitempty"`
	RequestedInstanceCount int                      `json:"RequestedInstanceCount"`
	RunningInstanceCount   int                      `json:"RunningInstanceCount"`
}

// EC2InstanceAttributes represents EC2 instance attributes for an EMR cluster.
type EC2InstanceAttributes struct {
	Ec2KeyName                     string   `json:"Ec2KeyName,omitempty"`
	Ec2SubnetID                    string   `json:"Ec2SubnetId,omitempty"`
	Ec2AvailabilityZone            string   `json:"Ec2AvailabilityZone,omitempty"`
	EmrManagedMasterSecurityGroup  string   `json:"EmrManagedMasterSecurityGroup,omitempty"`
	EmrManagedSlaveSecurityGroup   string   `json:"EmrManagedSlaveSecurityGroup,omitempty"`
	ServiceAccessSecurityGroup     string   `json:"ServiceAccessSecurityGroup,omitempty"`
	IamInstanceProfile             string   `json:"IamInstanceProfile,omitempty"`
	AdditionalMasterSecurityGroups []string `json:"AdditionalMasterSecurityGroups,omitempty"`
	AdditionalSlaveSecurityGroups  []string `json:"AdditionalSlaveSecurityGroups,omitempty"`
	RequestedEc2SubnetIDs          []string `json:"RequestedEc2SubnetIds,omitempty"`
}

// KerberosAttributes holds Kerberos configuration for a cluster, set via
// RunJobFlow and echoed back on Cluster when Kerberos authentication is
// enabled using a security configuration.
type KerberosAttributes struct {
	Realm                            string `json:"Realm"`
	KdcAdminPassword                 string `json:"KdcAdminPassword"`
	ADDomainJoinUser                 string `json:"ADDomainJoinUser,omitempty"`
	ADDomainJoinPassword             string `json:"ADDomainJoinPassword,omitempty"`
	CrossRealmTrustPrincipalPassword string `json:"CrossRealmTrustPrincipalPassword,omitempty"`
}

// PlacementGroupConfig is the placement group configuration for a single
// instance role, part of RunJobFlow's Instances.Placement and echoed back on
// Cluster.PlacementGroups.
type PlacementGroupConfig struct {
	InstanceRole      string `json:"InstanceRole"`
	PlacementStrategy string `json:"PlacementStrategy,omitempty"`
}

// Cluster represents an EMR cluster.
type Cluster struct {
	TerminatedAt          time.Time              `json:"TerminatedAt,omitzero"`
	Ec2InstanceAttributes *EC2InstanceAttributes `json:"Ec2InstanceAttributes"`
	KerberosAttributes    *KerberosAttributes    `json:"KerberosAttributes,omitempty"`
	autoTerminationPolicy *AutoTerminationPolicy
	managedScalingPolicy  *ManagedScalingPolicy
	// region is the store.Table composite-key qualifier (see regionKey in
	// backend.go); it is unexported so it is never marshaled by a plain
	// json.Marshal(Cluster) and is instead carried through persistence via
	// clusterDTO (see persistence.go).
	region                 string
	Status                 ClusterStatus `json:"Status"`
	ScaleDownBehavior      string        `json:"ScaleDownBehavior,omitempty"`
	ID                     string        `json:"Id"`
	ARN                    string        `json:"ClusterArn"`
	ReleaseLabel           string        `json:"ReleaseLabel"`
	OSReleaseLabel         string        `json:"OSReleaseLabel,omitempty"`
	LogURI                 string        `json:"LogUri,omitempty"`
	ServiceRole            string        `json:"ServiceRole,omitempty"`
	AutoScalingRole        string        `json:"AutoScalingRole,omitempty"`
	Name                   string        `json:"Name"`
	SecurityConfiguration  string        `json:"SecurityConfiguration,omitempty"`
	CustomAmiID            string        `json:"CustomAmiId,omitempty"`
	InstanceCollectionType string        `json:"InstanceCollectionType,omitempty"`
	instanceGroups         []InstanceGroup
	bootstrapActions       []BootstrapActionConfig
	Tags                   []Tag                  `json:"Tags"`
	Applications           []Application          `json:"Applications,omitempty"`
	Configurations         []Configuration        `json:"Configurations,omitempty"`
	PlacementGroups        []PlacementGroupConfig `json:"PlacementGroups,omitempty"`
	steps                  []Step
	instanceFleets         []InstanceFleet
	// sessions holds the interactive (Spark Connect) sessions started on
	// this cluster via StartSession -- like steps/instanceGroups/
	// instanceFleets above, real EMR has no ListSessions-across-clusters
	// operation, only ListSessions(ClusterId), so sessions are modeled as a
	// child collection embedded directly on the owning Cluster rather than a
	// separate store.Table. This also gives cascade-delete for free: when
	// the janitor sweeps a TERMINATED cluster's row (see janitor.go), every
	// session on it is removed in the same operation -- no separate sweep
	// needed to avoid orphaned sessions.
	sessions                    []Session
	StepConcurrencyLevel        int  `json:"StepConcurrencyLevel,omitempty"`
	EbsRootVolumeSize           int  `json:"EbsRootVolumeSize,omitempty"`
	EbsRootVolumeIops           int  `json:"EbsRootVolumeIops,omitempty"`
	EbsRootVolumeThroughput     int  `json:"EbsRootVolumeThroughput,omitempty"`
	UnhealthyNodeReplacement    bool `json:"UnhealthyNodeReplacement"`
	KeepJobFlowAliveWhenNoSteps bool `json:"KeepJobFlowAliveWhenNoSteps"`
	TerminationProtected        bool `json:"TerminationProtected"`
	VisibleToAllUsers           bool `json:"VisibleToAllUsers"`
	// AutoTerminate is the real API's inverse of KeepJobFlowAliveWhenNoSteps:
	// true means the cluster terminates after completing all steps.
	AutoTerminate bool `json:"AutoTerminate"`
}

// ClusterStatus holds the status fields for a Cluster.
type ClusterStatus struct {
	StateChangeReason map[string]any `json:"StateChangeReason,omitempty"`
	Timeline          map[string]any `json:"Timeline,omitempty"`
	State             string         `json:"State"`
}

// ClusterSummary is a trimmed-down view used for ListClusters.
//
// NormalizedInstanceHours is a real ClusterSummary member
// (aws-sdk-go-v2/service/emr/types.ClusterSummary) this backend never
// populates -- an honest omission (a real client sees it as nil/zero), not
// fabricated. OutpostArn is likewise real and omitted for the same reason.
// ReleaseLabel used to live here but was deleted: the real ClusterSummary
// has no such member at all (only Id, Name, Status, ClusterArn,
// NormalizedInstanceHours, OutpostArn) -- it was an invented field, not an
// omission.
type ClusterSummary struct {
	ID         string        `json:"Id"`
	Name       string        `json:"Name"`
	Status     ClusterStatus `json:"Status"`
	ClusterArn string        `json:"ClusterArn"`
}

// InstanceFleet represents an EMR instance fleet returned by AddInstanceFleet.
type InstanceFleet struct {
	Status                      InstanceFleetStatus `json:"Status"`
	ID                          string              `json:"Id"`
	Name                        string              `json:"Name"`
	InstanceFleetType           string              `json:"InstanceFleetType"`
	TargetOnDemandCapacity      int                 `json:"TargetOnDemandCapacity"`
	TargetSpotCapacity          int                 `json:"TargetSpotCapacity"`
	ProvisionedOnDemandCapacity int                 `json:"ProvisionedOnDemandCapacity"`
	ProvisionedSpotCapacity     int                 `json:"ProvisionedSpotCapacity"`
}

// InstanceFleetSpec is the input specification for an instance fleet.
type InstanceFleetSpec struct {
	Name                   string          `json:"Name"`
	InstanceFleetType      string          `json:"InstanceFleetType"`
	Configurations         []Configuration `json:"Configurations,omitempty"`
	TargetOnDemandCapacity int             `json:"TargetOnDemandCapacity"`
	TargetSpotCapacity     int             `json:"TargetSpotCapacity"`
}

// SecurityConfiguration stores an EMR security configuration.
//
// CreationDateTime is epoch seconds (float64), matching the EMR awsjson1.1
// wire format -- the real SDK deserializer parses CreationDateTime fields
// with smithytime.ParseEpochSeconds and rejects RFC3339 strings.
type SecurityConfiguration struct {
	Name           string `json:"Name"`
	SecurityConfig string `json:"SecurityConfiguration"`
	// region is the store.Table composite-key qualifier (see regionKey).
	region           string
	CreationDateTime float64 `json:"CreationDateTime"`
}

// Studio represents an EMR Studio.
//
// CreationTime is epoch seconds (float64), matching the EMR awsjson1.1 wire
// format -- the real SDK deserializer parses CreationTime with
// smithytime.ParseEpochSeconds and rejects RFC3339 strings.
type Studio struct {
	EngineSecurityGroupID             string `json:"EngineSecurityGroupId"`
	VpcID                             string `json:"VpcId"`
	StudioID                          string `json:"StudioId"`
	EncryptionKeyArn                  string `json:"EncryptionKeyArn,omitempty"`
	Name                              string `json:"Name"`
	Description                       string `json:"Description,omitempty"`
	AuthMode                          string `json:"AuthMode"`
	DefaultS3Location                 string `json:"DefaultS3Location"`
	ServiceRole                       string `json:"ServiceRole"`
	IdcInstanceArn                    string `json:"IdcInstanceArn,omitempty"`
	URL                               string `json:"Url"`
	WorkspaceSecurityGroupID          string `json:"WorkspaceSecurityGroupId"`
	StudioArn                         string `json:"StudioArn"`
	UserRole                          string `json:"UserRole,omitempty"`
	IdpAuthURL                        string `json:"IdpAuthUrl,omitempty"`
	IdpRelayStateParameterName        string `json:"IdpRelayStateParameterName,omitempty"`
	region                            string
	Tags                              []Tag    `json:"Tags"`
	SubnetIDs                         []string `json:"SubnetIds"`
	CreationTime                      float64  `json:"CreationTime,omitempty"`
	TrustedIdentityPropagationEnabled bool     `json:"TrustedIdentityPropagationEnabled"`
}

// StudioSummary is a trimmed view of Studio for ListStudios.
// CreationTime is epoch seconds (float64); see Studio for why.
type StudioSummary struct {
	StudioID          string  `json:"StudioId"`
	StudioArn         string  `json:"StudioArn"`
	Name              string  `json:"Name"`
	VpcID             string  `json:"VpcId"`
	DefaultS3Location string  `json:"DefaultS3Location"`
	AuthMode          string  `json:"AuthMode"`
	URL               string  `json:"Url"`
	Description       string  `json:"Description,omitempty"`
	CreationTime      float64 `json:"CreationTime,omitempty"`
}

// StudioSessionMapping maps a user or group to an EMR Studio.
// CreationTime/LastModifiedTime are epoch seconds (float64); see Studio for why.
type StudioSessionMapping struct {
	StudioID         string `json:"StudioId"`
	IdentityType     string `json:"IdentityType"`
	IdentityID       string `json:"IdentityId,omitempty"`
	IdentityName     string `json:"IdentityName,omitempty"`
	SessionPolicyArn string `json:"SessionPolicyArn"`
	// region is the store.Table composite-key qualifier (see regionKey).
	region           string
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
	CreationTime     float64 `json:"CreationTime,omitempty"`
}

// PersistentAppUI represents an EMR persistent application user interface.
type PersistentAppUI struct {
	ID                        string `json:"PersistentAppUIId"`
	TargetResourceArn         string `json:"TargetResourceArn"`
	region                    string
	RuntimeRoleEnabledCluster bool `json:"RuntimeRoleEnabledCluster"`
}

// RunJobFlowInstances holds the Instances block from a RunJobFlow call.
//
// NOTE: real EMR's JobFlowInstancesConfig has no IamInstanceProfile member --
// that attribute is set via the top-level RunJobFlowInput.JobFlowRole field
// instead (see RunJobFlowParams.JobFlowRole) and echoed back on
// Cluster.Ec2InstanceAttributes.IamInstanceProfile. An IamInstanceProfile
// field used to live here, but no real client ever populates it at this
// nesting level, so it was deleted.
type RunJobFlowInstances struct {
	Ec2KeyName                     string              `json:"Ec2KeyName,omitempty"`
	Ec2SubnetID                    string              `json:"Ec2SubnetId,omitempty"`
	EmrManagedMasterSecurityGroup  string              `json:"EmrManagedMasterSecurityGroup,omitempty"`
	EmrManagedSlaveSecurityGroup   string              `json:"EmrManagedSlaveSecurityGroup,omitempty"`
	ServiceAccessSecurityGroup     string              `json:"ServiceAccessSecurityGroup,omitempty"`
	InstanceGroups                 []InstanceGroupSpec `json:"InstanceGroups,omitempty"`
	InstanceFleets                 []InstanceFleetSpec `json:"InstanceFleets,omitempty"`
	Ec2SubnetIDs                   []string            `json:"Ec2SubnetIds,omitempty"`
	AdditionalMasterSecurityGroups []string            `json:"AdditionalMasterSecurityGroups,omitempty"`
	AdditionalSlaveSecurityGroups  []string            `json:"AdditionalSlaveSecurityGroups,omitempty"`
	KeepJobFlowAliveWhenNoSteps    bool                `json:"KeepJobFlowAliveWhenNoSteps"`
	TerminationProtected           bool                `json:"TerminationProtected"`
}

// RunJobFlowParams is the full input for creating a new cluster.
type RunJobFlowParams struct {
	SecurityConfiguration string `json:"SecurityConfiguration,omitempty"`
	ReleaseLabel          string `json:"ReleaseLabel"`
	OSReleaseLabel        string `json:"OSReleaseLabel,omitempty"`
	LogURI                string `json:"LogUri,omitempty"`
	ServiceRole           string `json:"ServiceRole,omitempty"`
	// JobFlowRole is the real RunJobFlowInput field (also called the EC2
	// instance profile); it becomes Cluster.Ec2InstanceAttributes.IamInstanceProfile.
	JobFlowRole             string                  `json:"JobFlowRole,omitempty"`
	AutoScalingRole         string                  `json:"AutoScalingRole,omitempty"`
	Name                    string                  `json:"Name"`
	ScaleDownBehavior       string                  `json:"ScaleDownBehavior,omitempty"`
	CustomAmiID             string                  `json:"CustomAmiId,omitempty"`
	Tags                    []Tag                   `json:"Tags,omitempty"`
	Applications            []Application           `json:"Applications,omitempty"`
	Configurations          []Configuration         `json:"Configurations,omitempty"`
	Steps                   []StepSpec              `json:"Steps,omitempty"`
	BootstrapActions        []BootstrapActionConfig `json:"BootstrapActions,omitempty"`
	KerberosAttributes      *KerberosAttributes     `json:"KerberosAttributes,omitempty"`
	PlacementGroupConfigs   []PlacementGroupConfig  `json:"PlacementGroupConfigs,omitempty"`
	ManagedScalingPolicy    *ManagedScalingPolicy   `json:"ManagedScalingPolicy,omitempty"`
	AutoTerminationPolicy   *AutoTerminationPolicy  `json:"AutoTerminationPolicy,omitempty"`
	Instances               RunJobFlowInstances     `json:"Instances"`
	StepConcurrencyLevel    int                     `json:"StepConcurrencyLevel,omitempty"`
	EbsRootVolumeSize       int                     `json:"EbsRootVolumeSize,omitempty"`
	EbsRootVolumeIops       int                     `json:"EbsRootVolumeIops,omitempty"`
	EbsRootVolumeThroughput int                     `json:"EbsRootVolumeThroughput,omitempty"`
	VisibleToAllUsers       bool                    `json:"VisibleToAllUsers"`
}

// ListClustersParams holds filter and pagination params for ListClusters.
type ListClustersParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Marker        string
	ClusterStates []string
}

// ListInstancesParams holds filter params for ListInstances.
type ListInstancesParams struct {
	InstanceGroupID    string
	InstanceFleetID    string
	Marker             string
	InstanceGroupTypes []string
	InstanceStates     []string
}

// InstanceGroupModification describes a single instance group count change.
type InstanceGroupModification struct {
	InstanceGroupID string `json:"InstanceGroupId"`
	InstanceCount   int    `json:"InstanceCount"`
}

// InstanceFleetModification describes a fleet target capacity change.
type InstanceFleetModification struct {
	InstanceFleetID        string `json:"InstanceFleetId"`
	TargetOnDemandCapacity int    `json:"TargetOnDemandCapacity,omitempty"`
	TargetSpotCapacity     int    `json:"TargetSpotCapacity,omitempty"`
}

// SecurityConfigSummary is returned by ListSecurityConfigurations.
// CreationDateTime is epoch seconds (float64); see SecurityConfiguration for why.
type SecurityConfigSummary struct {
	Name             string  `json:"Name"`
	CreationDateTime float64 `json:"CreationDateTime"`
}

// JobFlow is the legacy format returned by DescribeJobFlows.
type JobFlow struct {
	JobFlowID             string                       `json:"JobFlowId"`
	Name                  string                       `json:"Name"`
	ReleaseLabel          string                       `json:"ReleaseLabel,omitempty"`
	LogURI                string                       `json:"LogUri,omitempty"`
	ServiceRole           string                       `json:"ServiceRole,omitempty"`
	Instances             JobFlowInstancesDetail       `json:"Instances"`
	ExecutionStatusDetail JobFlowExecutionStatusDetail `json:"ExecutionStatusDetail"`
}

// JobFlowExecutionStatusDetail holds the legacy execution status.
type JobFlowExecutionStatusDetail struct {
	State             string  `json:"State"`
	StateChangeReason string  `json:"StateChangeReason,omitempty"`
	CreationDateTime  float64 `json:"CreationDateTime"`
	EndDateTime       float64 `json:"EndDateTime,omitempty"`
}

// JobFlowInstancesDetail holds the legacy instances detail.
type JobFlowInstancesDetail struct {
	MasterInstanceType string `json:"MasterInstanceType,omitempty"`
	SlaveInstanceType  string `json:"SlaveInstanceType,omitempty"`
	InstanceCount      int    `json:"InstanceCount"`
}

// ListNotebookExecutionsParams holds filters for ListNotebookExecutions.
type ListNotebookExecutionsParams struct {
	EditorID string
	Status   string
	Marker   string
}

// SessionCloudWatchLoggingConfiguration is the CloudWatch Logs configuration
// for a session (types.SessionCloudWatchLoggingConfiguration).
type SessionCloudWatchLoggingConfiguration struct {
	LogTypes            map[string][]string `json:"LogTypes,omitempty"`
	EncryptionKeyArn    string              `json:"EncryptionKeyArn,omitempty"`
	LogGroup            string              `json:"LogGroup,omitempty"`
	LogStreamNamePrefix string              `json:"LogStreamNamePrefix,omitempty"`
	Enabled             bool                `json:"Enabled,omitempty"`
}

// SessionManagedLoggingConfiguration is the Amazon EMR-managed logging
// configuration for a session (types.SessionManagedLoggingConfiguration).
type SessionManagedLoggingConfiguration struct {
	EncryptionKeyArn string `json:"EncryptionKeyArn,omitempty"`
	Enabled          bool   `json:"Enabled,omitempty"`
}

// SessionS3LoggingConfiguration is the Amazon S3 logging configuration for a
// session (types.SessionS3LoggingConfiguration).
type SessionS3LoggingConfiguration struct {
	LogTypes         map[string][]string `json:"LogTypes,omitempty"`
	EncryptionKeyArn string              `json:"EncryptionKeyArn,omitempty"`
	LogURI           string              `json:"LogUri,omitempty"`
	Enabled          bool                `json:"Enabled,omitempty"`
}

// SessionMonitoringConfiguration controls where a session's logs are
// published (types.SessionMonitoringConfiguration).
type SessionMonitoringConfiguration struct {
	CloudWatchLoggingConfiguration *SessionCloudWatchLoggingConfiguration `json:"CloudWatchLoggingConfiguration,omitempty"`
	ManagedLoggingConfiguration    *SessionManagedLoggingConfiguration    `json:"ManagedLoggingConfiguration,omitempty"`
	S3LoggingConfiguration         *SessionS3LoggingConfiguration         `json:"S3LoggingConfiguration,omitempty"`
}

// Session represents an interactive (Spark Connect) session running on an
// EMR cluster (types.Session). See sessions.go's package doc comment for
// the state-model rationale.
//
// CreatedAt/UpdatedAt are always populated once a session exists, so they
// carry no omitempty (matching Cluster/SecurityConfiguration's convention
// for "always set" epoch-seconds fields elsewhere in this package).
// EndedAt/IdleSince/StartedAt are genuine real Timestamp members this
// backend only sometimes populates (IdleSince/StartedAt are never
// populated at all -- see sessions.go), so they use omitempty, matching the
// real optional *time.Time members on types.Session. All are epoch seconds
// (float64), matching EMR's awsjson1.1 wire format -- see
// SecurityConfiguration for why.
type Session struct {
	MonitoringConfiguration     *SessionMonitoringConfiguration `json:"MonitoringConfiguration,omitempty"`
	ID                          string                          `json:"Id"`
	ClusterID                   string                          `json:"ClusterId"`
	ARN                         string                          `json:"Arn"`
	State                       string                          `json:"State"`
	AccountID                   string                          `json:"AccountId,omitempty"`
	Name                        string                          `json:"Name,omitempty"`
	ExecutionRoleArn            string                          `json:"ExecutionRoleArn,omitempty"`
	ReleaseLabel                string                          `json:"ReleaseLabel,omitempty"`
	ServerURL                   string                          `json:"ServerUrl,omitempty"`
	StateChangeReason           string                          `json:"StateChangeReason,omitempty"`
	EngineConfigurations        []Configuration                 `json:"EngineConfigurations,omitempty"`
	Tags                        []Tag                           `json:"Tags"`
	SessionIdleTimeoutInMinutes int64                           `json:"SessionIdleTimeoutInMinutes,omitempty"`
	CreatedAt                   float64                         `json:"CreatedAt"`
	UpdatedAt                   float64                         `json:"UpdatedAt"`
	EndedAt                     float64                         `json:"EndedAt,omitempty"`
	IdleSince                   float64                         `json:"IdleSince,omitempty"`
	StartedAt                   float64                         `json:"StartedAt,omitempty"`
}

// StartSessionParams is the input for creating a new session.
type StartSessionParams struct {
	MonitoringConfiguration     *SessionMonitoringConfiguration
	ClusterID                   string
	Name                        string
	ExecutionRoleArn            string
	EngineConfigurations        []Configuration
	Tags                        []Tag
	SessionIdleTimeoutInMinutes int64
}

// SessionEndpointResult is the output of GetSessionEndpoint.
type SessionEndpointResult struct {
	Expiry      time.Time
	Credentials map[string]any
	Endpoint    string
	AuthToken   string
}
