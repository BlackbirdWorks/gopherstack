package emr

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var ErrValidation = awserr.New(
	"ValidationException: required field is missing",
	awserr.ErrInvalidParameter,
)

var (
	ErrNotFound      = awserr.New("ClientException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
)

var errTerminationProtected = awserr.New(
	"ValidationException: cluster has termination protection enabled",
	awserr.ErrInvalidParameter,
)

const (
	StateWaiting              = "WAITING"
	StateTerminated           = "TERMINATED"
	StateTerminatedWithErrors = "TERMINATED_WITH_ERRORS"

	StepStatePending   = "PENDING"
	StepStateCompleted = "COMPLETED"
	StepStateCancelled = "CANCELLED"

	defaultReleaseLabel    = "emr-7.3.0"
	defaultStepConcurrency = 1

	minIdleTimeout = 60
	maxIdleTimeout = 604800

	minStepConcurrency = 1
	maxStepConcurrency = 256

	timelineKeyCreation = "CreationDateTime"
	timelineKeyEnd      = "EndDateTime"

	listClustersPageSize     = 50
	listSecConfigsPageSize   = 50
	listReleaseLabelsPage    = 50
	listInstanceTypesPage    = 50
	listStepsPageSize        = 50
	listInstancesPageSize    = 500
	listStudiosPageSize      = 50
	listNotebookExecPageSize = 50

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

// emr6xCoreApps is the base 6.x app set (no MXNet/TF).
var emr6xCoreApps = []string{ //nolint:gochecknoglobals // read-only lookup table
	appFlink, appHadoop, appHBase, appHive, appHue,
	appLivy, appOozie, appPig, appPresto, appSpark, appTez,
}

// emr6xApps is the emr-6.10+ app set (adds MXNet and TensorFlow).
var emr6xApps = []string{ //nolint:gochecknoglobals // read-only lookup table
	appFlink, appHadoop, appHBase, appHive, appHue,
	appLivy, appMXNet, appOozie, appPig, appPresto, appSpark, appTez, appTF,
}

// emr7xApps is the base 7.x app set (adds Trino, drops MXNet/TF).
var emr7xApps = []string{ //nolint:gochecknoglobals // read-only lookup table
	appFlink, appHadoop, appHBase, appHive, appHue,
	appLivy, appOozie, appPig, appPresto, appSpark, appTez, appTrino,
}

// releaseLabelApps maps a release label to its bundled application names.
var releaseLabelApps = map[string][]string{ //nolint:gochecknoglobals // read-only lookup table
	"emr-5.36.2": {
		appHadoop,
		appHive,
		appHue,
		appLivy,
		appMXNet,
		appOozie,
		appPig,
		appPresto,
		appSpark,
		appTez,
	},
	"emr-6.0.0":  emr6xCoreApps,
	"emr-6.1.0":  emr6xCoreApps,
	"emr-6.4.0":  emr6xCoreApps,
	"emr-6.8.0":  emr6xCoreApps,
	"emr-6.10.0": emr6xApps,
	"emr-6.11.0": emr6xApps,
	"emr-6.12.0": emr6xApps,
	"emr-6.13.0": emr6xApps,
	"emr-6.14.0": emr6xApps,
	"emr-6.15.0": emr6xApps,
	"emr-7.0.0":  emr7xApps,
	"emr-7.1.0":  emr7xApps,
	"emr-7.2.0":  emr7xApps,
	"emr-7.3.0":  emr7xApps,
}

// supportedInstanceTypes is a static catalog of EMR-supported EC2 instance types.
var supportedInstanceTypes = []SupportedInstanceType{ //nolint:gochecknoglobals // read-only hardware spec table
	{Type: "m5.xlarge", MemoryGB: gb16, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m5.2xlarge", MemoryGB: gb32, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m5.4xlarge", MemoryGB: gb64, VCPU: vcpu16, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m5.8xlarge", MemoryGB: gb128, VCPU: vcpu32, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m6g.xlarge", MemoryGB: gb16, VCPU: vcpu4, Architecture: archARM64, Is64BitsOnly: true},
	{Type: "m6g.2xlarge", MemoryGB: gb32, VCPU: vcpu8, Architecture: archARM64, Is64BitsOnly: true},
	{Type: "r5.xlarge", MemoryGB: gb32, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "r5.2xlarge", MemoryGB: gb64, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "r5.4xlarge", MemoryGB: gb128, VCPU: vcpu16, Architecture: archX86, Is64BitsOnly: true},
	{Type: "c5.xlarge", MemoryGB: gb8, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "c5.2xlarge", MemoryGB: gb16, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "c5.4xlarge", MemoryGB: gb32, VCPU: vcpu16, Architecture: archX86, Is64BitsOnly: true},
	{Type: "p3.2xlarge", MemoryGB: gb61, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "g4dn.xlarge", MemoryGB: gb16, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "i3.xlarge", MemoryGB: gb30, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true, NumberOfDisks: ndisk1},
	{Type: "i3.2xlarge", MemoryGB: gb61, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true, NumberOfDisks: ndisk2},
}

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
	PermittedPublicSecurityGroupRuleRanges []PortRange `json:"PermittedPublicSecurityGroupRuleRanges,omitempty"`
	BlockPublicSecurityGroupRules          bool        `json:"BlockPublicSecurityGroupRules"`
}

// blockPublicAccessMeta holds metadata for the block-public-access configuration.
type blockPublicAccessMeta struct {
	CreationDateTime time.Time `json:"CreationDateTime"`
	CreatedByArn     string    `json:"CreatedByArn,omitempty"`
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
type NotebookExecution struct {
	StartTime             time.Time `json:"StartTime,omitzero"`
	EndTime               time.Time `json:"EndTime,omitzero"`
	NotebookExecutionID   string    `json:"NotebookExecutionId"`
	EditorID              string    `json:"EditorId,omitempty"`
	NotebookExecutionName string    `json:"NotebookExecutionName,omitempty"`
	NotebookParams        string    `json:"NotebookParams,omitempty"`
	ExecutionEngineID     string    `json:"ExecutionEngineId,omitempty"`
	Status                string    `json:"Status"`
	Tags                  []Tag     `json:"Tags"`
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

// Cluster represents an EMR cluster.
type Cluster struct {
	TerminatedAt                time.Time              `json:"TerminatedAt,omitzero"`
	Ec2InstanceAttributes       *EC2InstanceAttributes `json:"Ec2InstanceAttributes"`
	autoTerminationPolicy       *AutoTerminationPolicy
	managedScalingPolicy        *ManagedScalingPolicy
	Status                      ClusterStatus `json:"Status"`
	ScaleDownBehavior           string        `json:"ScaleDownBehavior,omitempty"`
	ID                          string        `json:"Id"`
	ARN                         string        `json:"ClusterArn"`
	ReleaseLabel                string        `json:"ReleaseLabel"`
	OSReleaseLabel              string        `json:"OSReleaseLabel,omitempty"`
	LogURI                      string        `json:"LogUri,omitempty"`
	ServiceRole                 string        `json:"ServiceRole,omitempty"`
	AutoScalingRole             string        `json:"AutoScalingRole,omitempty"`
	Name                        string        `json:"Name"`
	SecurityConfiguration       string        `json:"SecurityConfiguration,omitempty"`
	CustomAmiID                 string        `json:"CustomAmiId,omitempty"`
	instanceGroups              []InstanceGroup
	Tags                        []Tag           `json:"Tags"`
	Applications                []Application   `json:"Applications,omitempty"`
	Configurations              []Configuration `json:"Configurations,omitempty"`
	steps                       []Step
	instanceFleets              []InstanceFleet
	StepConcurrencyLevel        int  `json:"StepConcurrencyLevel,omitempty"`
	EbsRootVolumeSize           int  `json:"EbsRootVolumeSize,omitempty"`
	EbsRootVolumeIops           int  `json:"EbsRootVolumeIops,omitempty"`
	EbsRootVolumeThroughput     int  `json:"EbsRootVolumeThroughput,omitempty"`
	UnhealthyNodeReplacement    bool `json:"UnhealthyNodeReplacement"`
	KeepJobFlowAliveWhenNoSteps bool `json:"KeepJobFlowAliveWhenNoSteps"`
	TerminationProtected        bool `json:"TerminationProtected"`
	VisibleToAllUsers           bool `json:"VisibleToAllUsers"`
}

// ClusterStatus holds the status fields for a Cluster.
type ClusterStatus struct {
	StateChangeReason map[string]any `json:"StateChangeReason,omitempty"`
	Timeline          map[string]any `json:"Timeline,omitempty"`
	State             string         `json:"State"`
}

// ClusterSummary is a trimmed-down view used for ListClusters.
type ClusterSummary struct {
	ID           string        `json:"Id"`
	Name         string        `json:"Name"`
	Status       ClusterStatus `json:"Status"`
	ClusterArn   string        `json:"ClusterArn"`
	ReleaseLabel string        `json:"ReleaseLabel"`
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
type SecurityConfiguration struct {
	CreationDateTime time.Time `json:"CreationDateTime"`
	Name             string    `json:"Name"`
	SecurityConfig   string    `json:"SecurityConfiguration"`
}

// Studio represents an EMR Studio.
type Studio struct {
	CreationTime                      time.Time `json:"CreationTime,omitzero"`
	ServiceRole                       string    `json:"ServiceRole"`
	VpcID                             string    `json:"VpcId"`
	StudioID                          string    `json:"StudioId"`
	EncryptionKeyArn                  string    `json:"EncryptionKeyArn,omitempty"`
	Name                              string    `json:"Name"`
	Description                       string    `json:"Description,omitempty"`
	AuthMode                          string    `json:"AuthMode"`
	DefaultS3Location                 string    `json:"DefaultS3Location"`
	IdcInstanceArn                    string    `json:"IdcInstanceArn,omitempty"`
	EngineSecurityGroupID             string    `json:"EngineSecurityGroupId"`
	StudioArn                         string    `json:"StudioArn"`
	WorkspaceSecurityGroupID          string    `json:"WorkspaceSecurityGroupId"`
	URL                               string    `json:"Url"`
	UserRole                          string    `json:"UserRole,omitempty"`
	IdpAuthURL                        string    `json:"IdpAuthUrl,omitempty"`
	IdpRelayStateParameterName        string    `json:"IdpRelayStateParameterName,omitempty"`
	SubnetIDs                         []string  `json:"SubnetIds"`
	Tags                              []Tag     `json:"Tags"`
	TrustedIdentityPropagationEnabled bool      `json:"TrustedIdentityPropagationEnabled"`
}

// StudioSummary is a trimmed view of Studio for ListStudios.
type StudioSummary struct {
	StudioID          string    `json:"StudioId"`
	StudioArn         string    `json:"StudioArn"`
	Name              string    `json:"Name"`
	VpcID             string    `json:"VpcId"`
	DefaultS3Location string    `json:"DefaultS3Location"`
	AuthMode          string    `json:"AuthMode"`
	URL               string    `json:"Url"`
	CreationTime      time.Time `json:"CreationTime,omitzero"`
	Description       string    `json:"Description,omitempty"`
}

// StudioSessionMapping maps a user or group to an EMR Studio.
type StudioSessionMapping struct {
	LastModifiedTime time.Time `json:"LastModifiedTime,omitzero"`
	CreationTime     time.Time `json:"CreationTime,omitzero"`
	StudioID         string    `json:"StudioId"`
	IdentityType     string    `json:"IdentityType"`
	IdentityID       string    `json:"IdentityId,omitempty"`
	IdentityName     string    `json:"IdentityName,omitempty"`
	SessionPolicyArn string    `json:"SessionPolicyArn"`
}

// PersistentAppUI represents an EMR persistent application user interface.
type PersistentAppUI struct {
	ID                        string `json:"PersistentAppUIId"`
	TargetResourceArn         string `json:"TargetResourceArn"`
	RuntimeRoleEnabledCluster bool   `json:"RuntimeRoleEnabledCluster"`
}

// RunJobFlowInstances holds the Instances block from a RunJobFlow call.
type RunJobFlowInstances struct {
	Ec2KeyName                     string              `json:"Ec2KeyName,omitempty"`
	Ec2SubnetID                    string              `json:"Ec2SubnetId,omitempty"`
	EmrManagedMasterSecurityGroup  string              `json:"EmrManagedMasterSecurityGroup,omitempty"`
	EmrManagedSlaveSecurityGroup   string              `json:"EmrManagedSlaveSecurityGroup,omitempty"`
	ServiceAccessSecurityGroup     string              `json:"ServiceAccessSecurityGroup,omitempty"`
	IamInstanceProfile             string              `json:"IamInstanceProfile,omitempty"`
	InstanceGroups                 []InstanceGroupSpec `json:"InstanceGroups,omitempty"`
	Ec2SubnetIDs                   []string            `json:"Ec2SubnetIds,omitempty"`
	AdditionalMasterSecurityGroups []string            `json:"AdditionalMasterSecurityGroups,omitempty"`
	AdditionalSlaveSecurityGroups  []string            `json:"AdditionalSlaveSecurityGroups,omitempty"`
	KeepJobFlowAliveWhenNoSteps    bool                `json:"KeepJobFlowAliveWhenNoSteps"`
	TerminationProtected           bool                `json:"TerminationProtected"`
}

// RunJobFlowParams is the full input for creating a new cluster.
type RunJobFlowParams struct {
	SecurityConfiguration   string              `json:"SecurityConfiguration,omitempty"`
	ReleaseLabel            string              `json:"ReleaseLabel"`
	OSReleaseLabel          string              `json:"OSReleaseLabel,omitempty"`
	LogURI                  string              `json:"LogUri,omitempty"`
	ServiceRole             string              `json:"ServiceRole,omitempty"`
	AutoScalingRole         string              `json:"AutoScalingRole,omitempty"`
	Name                    string              `json:"Name"`
	ScaleDownBehavior       string              `json:"ScaleDownBehavior,omitempty"`
	CustomAmiID             string              `json:"CustomAmiId,omitempty"`
	Tags                    []Tag               `json:"Tags,omitempty"`
	Applications            []Application       `json:"Applications,omitempty"`
	Configurations          []Configuration     `json:"Configurations,omitempty"`
	Steps                   []StepSpec          `json:"Steps,omitempty"`
	Instances               RunJobFlowInstances `json:"Instances"`
	StepConcurrencyLevel    int                 `json:"StepConcurrencyLevel,omitempty"`
	EbsRootVolumeSize       int                 `json:"EbsRootVolumeSize,omitempty"`
	EbsRootVolumeIops       int                 `json:"EbsRootVolumeIops,omitempty"`
	EbsRootVolumeThroughput int                 `json:"EbsRootVolumeThroughput,omitempty"`
	VisibleToAllUsers       bool                `json:"VisibleToAllUsers"`
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

// InMemoryBackend stores EMR state in memory.
type InMemoryBackend struct {
	clusters              map[string]*Cluster
	arnIndex              map[string]string
	securityConfigs       map[string]*SecurityConfiguration
	studios               map[string]*Studio
	studioSessionMappings map[string]*StudioSessionMapping
	persistentAppUIs      map[string]*PersistentAppUI
	notebookExecutions    map[string]*NotebookExecution
	blockPublicAccess     *BlockPublicAccessConfiguration
	blockPublicAccessMeta *blockPublicAccessMeta
	mu                    *lockmetrics.RWMutex
	accountID             string
	region                string
	counter               atomic.Int64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:              make(map[string]*Cluster),
		arnIndex:              make(map[string]string),
		securityConfigs:       make(map[string]*SecurityConfiguration),
		studios:               make(map[string]*Studio),
		studioSessionMappings: make(map[string]*StudioSessionMapping),
		persistentAppUIs:      make(map[string]*PersistentAppUI),
		notebookExecutions:    make(map[string]*NotebookExecution),
		accountID:             accountID,
		region:                region,
		mu:                    lockmetrics.New("emr"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) nextID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("j-%013d", n)
}

func (b *InMemoryBackend) nextFleetID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("if-%013d", n)
}

func (b *InMemoryBackend) nextStepID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("s-%013d", n)
}

func (b *InMemoryBackend) nextStudioID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("es-%013d", n)
}

func (b *InMemoryBackend) nextPersistentAppUIID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("pau-%013d", n)
}

// validateReleaseLabel returns an error if the label is not in the registry.
func validateReleaseLabel(label string) error {
	if _, ok := releaseLabelApps[label]; !ok {
		return fmt.Errorf("%w: invalid ReleaseLabel %q", ErrValidation, label)
	}

	return nil
}

// buildInstanceGroups converts input specs to InstanceGroup records.
func (b *InMemoryBackend) buildInstanceGroups(specs []InstanceGroupSpec) []InstanceGroup {
	groups := make([]InstanceGroup, 0, len(specs))

	for i, spec := range specs {
		market := spec.Market
		if market == "" {
			market = "ON_DEMAND"
		}

		groups = append(groups, InstanceGroup{
			ID:                     fmt.Sprintf("ig-%013d-%d", b.counter.Load(), i),
			Name:                   spec.Name,
			Market:                 market,
			BidPrice:               spec.BidPrice,
			InstanceGroupType:      spec.InstanceRole,
			InstanceType:           spec.InstanceType,
			Configurations:         cloneConfigurations(spec.Configurations),
			RequestedInstanceCount: spec.InstanceCount,
			RunningInstanceCount:   spec.InstanceCount,
			Status:                 InstanceGroupStatus{State: instanceGroupStateRunning},
		})
	}

	return groups
}

// cloneConfigurations deep-copies a Configuration slice.
func cloneConfigurations(cfgs []Configuration) []Configuration {
	if cfgs == nil {
		return nil
	}

	out := make([]Configuration, len(cfgs))
	for i, c := range cfgs {
		out[i] = cloneConfiguration(c)
	}

	return out
}

// cloneConfiguration deep-copies a single Configuration (recursive).
func cloneConfiguration(c Configuration) Configuration {
	cp := Configuration{
		Classification: c.Classification,
	}

	if c.Properties != nil {
		cp.Properties = maps.Clone(c.Properties)
	}

	if c.Configurations != nil {
		cp.Configurations = cloneConfigurations(c.Configurations)
	}

	return cp
}

// buildInitialSteps converts input StepSpec records into Step records.
func (b *InMemoryBackend) buildInitialSteps(specs []StepSpec) []Step {
	steps := make([]Step, 0, len(specs))
	now := float64(time.Now().UnixMilli())

	for _, spec := range specs {
		actionOnFailure := spec.ActionOnFailure
		if actionOnFailure == "" {
			actionOnFailure = "TERMINATE_CLUSTER"
		}

		steps = append(steps, Step{
			ID:              b.nextStepID(),
			Name:            spec.Name,
			HadoopJarStep:   spec.HadoopJarStep,
			ActionOnFailure: actionOnFailure,
			Status: StepStatus{
				State:    StepStatePending,
				Timeline: StepTimeline{CreationDateTime: now},
			},
		})
	}

	return steps
}

// buildDefaultApplications returns the default application list for a release label.
func buildDefaultApplications(releaseLabel string) []Application {
	apps, ok := releaseLabelApps[releaseLabel]
	if !ok {
		return nil
	}

	result := make([]Application, 0, len(apps))
	for _, name := range apps {
		result = append(result, Application{Name: name})
	}

	return result
}

// buildEC2Attrs populates EC2InstanceAttributes from the RunJobFlow instances block.
func buildEC2Attrs(inst RunJobFlowInstances) *EC2InstanceAttributes {
	return &EC2InstanceAttributes{
		Ec2KeyName:                     inst.Ec2KeyName,
		Ec2SubnetID:                    inst.Ec2SubnetID,
		EmrManagedMasterSecurityGroup:  inst.EmrManagedMasterSecurityGroup,
		EmrManagedSlaveSecurityGroup:   inst.EmrManagedSlaveSecurityGroup,
		ServiceAccessSecurityGroup:     inst.ServiceAccessSecurityGroup,
		IamInstanceProfile:             inst.IamInstanceProfile,
		AdditionalMasterSecurityGroups: inst.AdditionalMasterSecurityGroups,
		AdditionalSlaveSecurityGroups:  inst.AdditionalSlaveSecurityGroups,
		RequestedEc2SubnetIDs:          inst.Ec2SubnetIDs,
	}
}

// RunJobFlow creates a new EMR cluster.
func (b *InMemoryBackend) RunJobFlow(params RunJobFlowParams) (*Cluster, error) {
	releaseLabel := params.ReleaseLabel
	if releaseLabel == "" {
		releaseLabel = defaultReleaseLabel
	}

	if err := validateReleaseLabel(releaseLabel); err != nil {
		return nil, err
	}

	b.mu.Lock("RunJobFlow")
	defer b.mu.Unlock()

	id := b.nextID()
	clusterARN := arn.Build("elasticmapreduce", b.region, b.accountID, "cluster/"+id)

	tagsCopy := make([]Tag, len(params.Tags))
	copy(tagsCopy, params.Tags)

	apps := params.Applications
	if len(apps) == 0 {
		apps = buildDefaultApplications(releaseLabel)
	}

	stepConcurrency := params.StepConcurrencyLevel
	if stepConcurrency == 0 {
		stepConcurrency = defaultStepConcurrency
	}

	groups := b.buildInstanceGroups(params.Instances.InstanceGroups)
	steps := b.buildInitialSteps(params.Steps)

	cluster := &Cluster{
		ID:                    id,
		Name:                  params.Name,
		ReleaseLabel:          releaseLabel,
		OSReleaseLabel:        params.OSReleaseLabel,
		ARN:                   clusterARN,
		Ec2InstanceAttributes: buildEC2Attrs(params.Instances),
		Status: ClusterStatus{
			State:             StateWaiting,
			StateChangeReason: map[string]any{"Code": "USER_REQUEST", "Message": ""},
			Timeline:          map[string]any{timelineKeyCreation: time.Now().UnixMilli()},
		},
		Tags:                        tagsCopy,
		Applications:                apps,
		Configurations:              cloneConfigurations(params.Configurations),
		LogURI:                      params.LogURI,
		ServiceRole:                 params.ServiceRole,
		AutoScalingRole:             params.AutoScalingRole,
		ScaleDownBehavior:           params.ScaleDownBehavior,
		SecurityConfiguration:       params.SecurityConfiguration,
		CustomAmiID:                 params.CustomAmiID,
		StepConcurrencyLevel:        stepConcurrency,
		EbsRootVolumeSize:           params.EbsRootVolumeSize,
		EbsRootVolumeIops:           params.EbsRootVolumeIops,
		EbsRootVolumeThroughput:     params.EbsRootVolumeThroughput,
		VisibleToAllUsers:           params.VisibleToAllUsers,
		TerminationProtected:        params.Instances.TerminationProtected,
		KeepJobFlowAliveWhenNoSteps: params.Instances.KeepJobFlowAliveWhenNoSteps,
		instanceGroups:              groups,
		steps:                       steps,
	}
	b.clusters[id] = cluster
	b.arnIndex[clusterARN] = id
	cp := cluster.clone()

	return &cp, nil
}

// DescribeCluster returns a cluster by its ID.
func (b *InMemoryBackend) DescribeCluster(id string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[id]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
	}

	cp := cluster.clone()

	return &cp, nil
}

// clone returns a deep copy of the Cluster.
func (c Cluster) clone() Cluster {
	cp := c

	if c.Tags != nil {
		cp.Tags = make([]Tag, len(c.Tags))
		copy(cp.Tags, c.Tags)
	}

	if c.Applications != nil {
		cp.Applications = make([]Application, len(c.Applications))
		copy(cp.Applications, c.Applications)
	}

	cp.Configurations = cloneConfigurations(c.Configurations)

	if c.instanceGroups != nil {
		cp.instanceGroups = make([]InstanceGroup, len(c.instanceGroups))
		copy(cp.instanceGroups, c.instanceGroups)
	}

	if c.instanceFleets != nil {
		cp.instanceFleets = make([]InstanceFleet, len(c.instanceFleets))
		copy(cp.instanceFleets, c.instanceFleets)
	}

	if c.steps != nil {
		cp.steps = make([]Step, len(c.steps))
		copy(cp.steps, c.steps)
	}

	if c.managedScalingPolicy != nil {
		msp := *c.managedScalingPolicy
		cp.managedScalingPolicy = &msp
	}

	if c.autoTerminationPolicy != nil {
		atp := *c.autoTerminationPolicy
		cp.autoTerminationPolicy = &atp
	}

	cp.Status.StateChangeReason = maps.Clone(c.Status.StateChangeReason)
	cp.Status.Timeline = maps.Clone(c.Status.Timeline)

	return cp
}

// ListClusters returns cluster summaries matching the given filter, sorted by creation time descending.
func (b *InMemoryBackend) ListClusters(params ListClustersParams) ([]ClusterSummary, string) {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	stateSet := buildStateSet(params.ClusterStates)
	list := b.gatherClusterSummaries(stateSet, params)

	sort.Slice(list, func(i, j int) bool {
		ti := clusterCreationMillis(list[i])
		tj := clusterCreationMillis(list[j])
		if ti != tj {
			return ti > tj
		}

		return list[i].ID > list[j].ID
	})

	p := page.New(list, params.Marker, listClustersPageSize, listClustersPageSize)

	return p.Data, p.Next
}

// buildStateSet converts a slice of state strings to a set.
// An empty slice means "all non-terminal states".
func buildStateSet(states []string) map[string]bool {
	if len(states) == 0 {
		return nil
	}

	set := make(map[string]bool, len(states))
	for _, s := range states {
		set[s] = true
	}

	return set
}

// gatherClusterSummaries collects filtered cluster summaries. Caller holds read lock.
func (b *InMemoryBackend) gatherClusterSummaries(
	stateSet map[string]bool,
	params ListClustersParams,
) []ClusterSummary {
	list := make([]ClusterSummary, 0, len(b.clusters))

	for _, c := range b.clusters {
		if !clusterMatchesFilter(c, stateSet, params) {
			continue
		}

		status := ClusterStatus{
			State:             c.Status.State,
			StateChangeReason: maps.Clone(c.Status.StateChangeReason),
		}
		list = append(list, ClusterSummary{
			ID:           c.ID,
			Name:         c.Name,
			Status:       status,
			ClusterArn:   c.ARN,
			ReleaseLabel: c.ReleaseLabel,
		})
	}

	return list
}

// clusterMatchesFilter reports whether c satisfies the given filter.
func clusterMatchesFilter(c *Cluster, stateSet map[string]bool, params ListClustersParams) bool {
	if stateSet != nil {
		if !stateSet[c.Status.State] {
			return false
		}
	} else {
		if c.Status.State == StateTerminated || c.Status.State == StateTerminatedWithErrors {
			return false
		}
	}

	creationMillis := clusterCreationMillisFromCluster(c)
	if params.CreatedAfter != nil {
		if creationMillis < params.CreatedAfter.UnixMilli() {
			return false
		}
	}

	if params.CreatedBefore != nil {
		if creationMillis > params.CreatedBefore.UnixMilli() {
			return false
		}
	}

	return true
}

func clusterCreationMillis(cs ClusterSummary) int64 {
	return timelineMillis(cs.Status.Timeline, timelineKeyCreation)
}

func clusterCreationMillisFromCluster(c *Cluster) int64 {
	return timelineMillis(c.Status.Timeline, timelineKeyCreation)
}

func timelineMillis(timeline map[string]any, key string) int64 {
	switch v := timeline[key].(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

// TerminateJobFlows marks the specified clusters as TERMINATED.
// Returns ValidationException if any cluster has termination protection.
func (b *InMemoryBackend) TerminateJobFlows(ids []string) error {
	b.mu.Lock("TerminateJobFlows")
	defer b.mu.Unlock()

	for _, id := range ids {
		if err := b.terminateSingle(id); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) terminateSingle(id string) error {
	cluster, ok := b.clusters[id]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
	}

	if cluster.Status.State == StateTerminated ||
		cluster.Status.State == StateTerminatedWithErrors {
		return nil
	}

	if cluster.TerminationProtected {
		return fmt.Errorf("%w: cluster %s", errTerminationProtected, id)
	}

	now := time.Now()
	cluster.Status.State = StateTerminated
	cluster.Status.StateChangeReason = map[string]any{
		"Code":    "USER_REQUEST",
		"Message": "Terminated by user request",
	}
	cluster.Status.Timeline[timelineKeyEnd] = now.UnixMilli()
	cluster.TerminatedAt = now

	return nil
}

// ListInstanceGroups returns the instance groups for a cluster by its ID.
func (b *InMemoryBackend) ListInstanceGroups(clusterID string) ([]InstanceGroup, error) {
	b.mu.RLock("ListInstanceGroups")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	groups := make([]InstanceGroup, len(cluster.instanceGroups))
	copy(groups, cluster.instanceGroups)

	return groups, nil
}

// AddTags adds or updates tags on a cluster identified by ARN or ID.
func (b *InMemoryBackend) AddTags(resourceID string, tags []Tag) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	cluster := b.findClusterByIDOrARN(resourceID)
	if cluster == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(cluster.Tags)
	for _, t := range tags {
		existing[t.Key] = t.Value
	}

	cluster.Tags = mapToTags(existing)

	return nil
}

// RemoveTags removes tags from a cluster identified by ARN or ID.
func (b *InMemoryBackend) RemoveTags(resourceID string, tagKeys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	cluster := b.findClusterByIDOrARN(resourceID)
	if cluster == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	existing := tagsToMap(cluster.Tags)
	for _, k := range tagKeys {
		delete(existing, k)
	}

	cluster.Tags = mapToTags(existing)

	return nil
}

// ListTagsForResource returns tags for a cluster identified by ARN or ID, sorted by key.
func (b *InMemoryBackend) ListTagsForResource(resourceID string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	cluster := b.findClusterByIDOrARN(resourceID)
	if cluster == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}

	tags := make([]Tag, len(cluster.Tags))
	copy(tags, cluster.Tags)

	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})

	return tags, nil
}

// findClusterByIDOrARN looks up a cluster by either its ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findClusterByIDOrARN(idOrARN string) *Cluster {
	if c, ok := b.clusters[idOrARN]; ok {
		return c
	}

	if id, ok := b.arnIndex[idOrARN]; ok {
		return b.clusters[id]
	}

	return nil
}

func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func mapToTags(m map[string]string) []Tag {
	keys := sortedTagKeys(m)
	tags := make([]Tag, 0, len(keys))

	for _, k := range keys {
		tags = append(tags, Tag{Key: k, Value: m[k]})
	}

	return tags
}

func sortedTagKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// Reset clears all in-memory state from the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*Cluster)
	b.arnIndex = make(map[string]string)
	b.securityConfigs = make(map[string]*SecurityConfiguration)
	b.studios = make(map[string]*Studio)
	b.studioSessionMappings = make(map[string]*StudioSessionMapping)
	b.persistentAppUIs = make(map[string]*PersistentAppUI)
	b.notebookExecutions = make(map[string]*NotebookExecution)
	b.blockPublicAccess = nil
	b.blockPublicAccessMeta = nil
	b.counter.Store(0)
}

// AddInstanceFleet adds an instance fleet to an existing cluster.
func (b *InMemoryBackend) AddInstanceFleet(
	clusterID string,
	spec InstanceFleetSpec,
) (*InstanceFleet, string, error) {
	b.mu.Lock("AddInstanceFleet")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	fleet := InstanceFleet{
		ID:                          b.nextFleetID(),
		Name:                        spec.Name,
		InstanceFleetType:           spec.InstanceFleetType,
		TargetOnDemandCapacity:      spec.TargetOnDemandCapacity,
		TargetSpotCapacity:          spec.TargetSpotCapacity,
		ProvisionedOnDemandCapacity: spec.TargetOnDemandCapacity,
		ProvisionedSpotCapacity:     spec.TargetSpotCapacity,
		Status:                      InstanceFleetStatus{State: instanceGroupStateRunning},
	}

	cluster.instanceFleets = append(cluster.instanceFleets, fleet)

	return &fleet, cluster.ARN, nil
}

// AddInstanceGroups adds new instance groups to an existing cluster.
func (b *InMemoryBackend) AddInstanceGroups(
	clusterID string,
	specs []InstanceGroupSpec,
) ([]string, string, error) {
	b.mu.Lock("AddInstanceGroups")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	groupIDs := make([]string, 0, len(specs))

	for _, spec := range specs {
		market := spec.Market
		if market == "" {
			market = "ON_DEMAND"
		}

		grpID := fmt.Sprintf("ig-%013d", b.counter.Add(1))
		group := InstanceGroup{
			ID:                     grpID,
			Name:                   spec.Name,
			Market:                 market,
			InstanceGroupType:      spec.InstanceRole,
			InstanceType:           spec.InstanceType,
			RequestedInstanceCount: spec.InstanceCount,
			RunningInstanceCount:   spec.InstanceCount,
			Status:                 InstanceGroupStatus{State: instanceGroupStateRunning},
		}

		cluster.instanceGroups = append(cluster.instanceGroups, group)
		groupIDs = append(groupIDs, grpID)
	}

	return groupIDs, cluster.ARN, nil
}

// AddJobFlowSteps adds steps to a cluster and returns their IDs.
func (b *InMemoryBackend) AddJobFlowSteps(jobFlowID string, specs []StepSpec) ([]string, error) {
	b.mu.Lock("AddJobFlowSteps")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[jobFlowID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, jobFlowID)
	}

	now := float64(time.Now().UnixMilli())
	ids := make([]string, 0, len(specs))

	for _, spec := range specs {
		actionOnFailure := spec.ActionOnFailure
		if actionOnFailure == "" {
			actionOnFailure = "TERMINATE_CLUSTER"
		}

		step := Step{
			ID:              b.nextStepID(),
			Name:            spec.Name,
			HadoopJarStep:   spec.HadoopJarStep,
			ActionOnFailure: actionOnFailure,
			Status: StepStatus{
				State:    StepStatePending,
				Timeline: StepTimeline{CreationDateTime: now},
			},
		}

		cluster.steps = append(cluster.steps, step)
		ids = append(ids, step.ID)
	}

	return ids, nil
}

// ListSteps returns steps for a cluster, optionally filtered by state and/or ID.
func (b *InMemoryBackend) ListSteps(
	clusterID string,
	stepStates []string,
	stepIDs []string,
	marker string,
) ([]Step, string) {
	b.mu.RLock("ListSteps")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return []Step{}, ""
	}

	stateSet := buildStateSet(stepStates)
	idSet := buildStringSet(stepIDs)

	filtered := filterSteps(cluster.steps, stateSet, idSet)

	// AWS returns most recently added first.
	for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
		filtered[i], filtered[j] = filtered[j], filtered[i]
	}

	p := page.New(filtered, marker, listStepsPageSize, listStepsPageSize)

	return p.Data, p.Next
}

func filterSteps(steps []Step, stateSet, idSet map[string]bool) []Step {
	filtered := make([]Step, 0, len(steps))

	for _, s := range steps {
		if stateSet != nil && !stateSet[s.Status.State] {
			continue
		}

		if idSet != nil && !idSet[s.ID] {
			continue
		}

		filtered = append(filtered, s)
	}

	return filtered
}

func buildStringSet(items []string) map[string]bool {
	if len(items) == 0 {
		return nil
	}

	set := make(map[string]bool, len(items))
	for _, s := range items {
		set[s] = true
	}

	return set
}

// DescribeStep returns a single step by cluster ID and step ID.
func (b *InMemoryBackend) DescribeStep(clusterID, stepID string) (*Step, error) {
	b.mu.RLock("DescribeStep")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for _, s := range cluster.steps {
		if s.ID == stepID {
			cp := s

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: step %s not found", ErrNotFound, stepID)
}

// CancelSteps cancels pending steps on a cluster.
func (b *InMemoryBackend) CancelSteps(clusterID string, stepIDs []string) error {
	b.mu.Lock("CancelSteps")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	idSet := buildStringSet(stepIDs)

	for i := range cluster.steps {
		s := &cluster.steps[i]
		if (idSet == nil || idSet[s.ID]) && s.Status.State == StepStatePending {
			s.Status.State = StepStateCancelled
		}
	}

	return nil
}

// ModifyCluster updates StepConcurrencyLevel on a cluster.
func (b *InMemoryBackend) ModifyCluster(clusterID string, stepConcurrencyLevel int) (int, error) {
	if stepConcurrencyLevel < minStepConcurrency || stepConcurrencyLevel > maxStepConcurrency {
		return 0, fmt.Errorf(
			"%w: StepConcurrencyLevel must be between %d and %d",
			ErrValidation,
			minStepConcurrency,
			maxStepConcurrency,
		)
	}

	b.mu.Lock("ModifyCluster")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return 0, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cluster.StepConcurrencyLevel = stepConcurrencyLevel

	return stepConcurrencyLevel, nil
}

// ModifyInstanceGroups updates instance counts for the specified groups.
func (b *InMemoryBackend) ModifyInstanceGroups(
	clusterID string,
	mods []InstanceGroupModification,
) error {
	b.mu.Lock("ModifyInstanceGroups")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for _, mod := range mods {
		applyInstanceGroupMod(cluster, mod)
	}

	return nil
}

func applyInstanceGroupMod(cluster *Cluster, mod InstanceGroupModification) {
	for i := range cluster.instanceGroups {
		if cluster.instanceGroups[i].ID == mod.InstanceGroupID {
			cluster.instanceGroups[i].RequestedInstanceCount = mod.InstanceCount
			cluster.instanceGroups[i].RunningInstanceCount = mod.InstanceCount

			return
		}
	}
}

// InstanceGroupModification describes a single instance group count change.
type InstanceGroupModification struct {
	InstanceGroupID string `json:"InstanceGroupId"`
	InstanceCount   int    `json:"InstanceCount"`
}

// ModifyInstanceFleet updates target capacities on an instance fleet.
func (b *InMemoryBackend) ModifyInstanceFleet(
	clusterID string,
	mod InstanceFleetModification,
) error {
	b.mu.Lock("ModifyInstanceFleet")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for i := range cluster.instanceFleets {
		if cluster.instanceFleets[i].ID == mod.InstanceFleetID {
			return nil
		}
	}

	return fmt.Errorf("%w: instance fleet %s not found", ErrNotFound, mod.InstanceFleetID)
}

// InstanceFleetModification describes a fleet target capacity change.
type InstanceFleetModification struct {
	InstanceFleetID        string `json:"InstanceFleetId"`
	TargetOnDemandCapacity int    `json:"TargetOnDemandCapacity,omitempty"`
	TargetSpotCapacity     int    `json:"TargetSpotCapacity,omitempty"`
}

// SetTerminationProtection sets the TerminationProtected flag on clusters.
func (b *InMemoryBackend) SetTerminationProtection(jobFlowIDs []string, protect bool) error {
	b.mu.Lock("SetTerminationProtection")
	defer b.mu.Unlock()

	for _, id := range jobFlowIDs {
		cluster, ok := b.clusters[id]
		if !ok {
			return fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
		}

		cluster.TerminationProtected = protect
	}

	return nil
}

// SetKeepJobFlowAliveWhenNoSteps sets the KeepJobFlowAliveWhenNoSteps flag.
func (b *InMemoryBackend) SetKeepJobFlowAliveWhenNoSteps(jobFlowIDs []string, keep bool) error {
	b.mu.Lock("SetKeepJobFlowAliveWhenNoSteps")
	defer b.mu.Unlock()

	for _, id := range jobFlowIDs {
		cluster, ok := b.clusters[id]
		if !ok {
			return fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
		}

		cluster.KeepJobFlowAliveWhenNoSteps = keep
	}

	return nil
}

// SetVisibleToAllUsers sets the VisibleToAllUsers flag.
func (b *InMemoryBackend) SetVisibleToAllUsers(jobFlowIDs []string, visible bool) error {
	b.mu.Lock("SetVisibleToAllUsers")
	defer b.mu.Unlock()

	for _, id := range jobFlowIDs {
		cluster, ok := b.clusters[id]
		if !ok {
			return fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
		}

		cluster.VisibleToAllUsers = visible
	}

	return nil
}

// SetUnhealthyNodeReplacement sets the UnhealthyNodeReplacement flag.
func (b *InMemoryBackend) SetUnhealthyNodeReplacement(jobFlowIDs []string, replace bool) error {
	b.mu.Lock("SetUnhealthyNodeReplacement")
	defer b.mu.Unlock()

	for _, id := range jobFlowIDs {
		cluster, ok := b.clusters[id]
		if !ok {
			return fmt.Errorf("%w: cluster %s not found", ErrNotFound, id)
		}

		cluster.UnhealthyNodeReplacement = replace
	}

	return nil
}

// PutManagedScalingPolicy sets the managed scaling policy on a cluster.
func (b *InMemoryBackend) PutManagedScalingPolicy(
	clusterID string,
	policy ManagedScalingPolicy,
) error {
	if err := validateManagedScalingPolicy(policy); err != nil {
		return err
	}

	b.mu.Lock("PutManagedScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cp := policy
	cluster.managedScalingPolicy = &cp

	return nil
}

func validateManagedScalingPolicy(policy ManagedScalingPolicy) error {
	cl := policy.ComputeLimits
	if cl.MinimumCapacityUnits > cl.MaximumCapacityUnits {
		return fmt.Errorf("%w: MinimumCapacityUnits must be <= MaximumCapacityUnits", ErrValidation)
	}

	if cl.MaximumOnDemandCapacityUnits > 0 &&
		cl.MaximumOnDemandCapacityUnits > cl.MaximumCapacityUnits {
		return fmt.Errorf(
			"%w: MaximumOnDemandCapacityUnits must be <= MaximumCapacityUnits",
			ErrValidation,
		)
	}

	return nil
}

// GetManagedScalingPolicy returns the managed scaling policy for a cluster.
func (b *InMemoryBackend) GetManagedScalingPolicy(clusterID string) (*ManagedScalingPolicy, error) {
	b.mu.RLock("GetManagedScalingPolicy")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if cluster.managedScalingPolicy == nil {
		empty := ManagedScalingPolicy{}

		return &empty, nil
	}

	cp := *cluster.managedScalingPolicy

	return &cp, nil
}

// RemoveManagedScalingPolicy clears the managed scaling policy on a cluster.
func (b *InMemoryBackend) RemoveManagedScalingPolicy(clusterID string) error {
	b.mu.Lock("RemoveManagedScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cluster.managedScalingPolicy = nil

	return nil
}

// PutAutoTerminationPolicy sets the auto-termination policy on a cluster.
func (b *InMemoryBackend) PutAutoTerminationPolicy(
	clusterID string,
	policy AutoTerminationPolicy,
) error {
	if policy.IdleTimeout < minIdleTimeout || policy.IdleTimeout > maxIdleTimeout {
		return fmt.Errorf(
			"%w: IdleTimeout must be between %d and %d seconds",
			ErrValidation,
			minIdleTimeout,
			maxIdleTimeout,
		)
	}

	b.mu.Lock("PutAutoTerminationPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cp := policy
	cluster.autoTerminationPolicy = &cp

	return nil
}

// GetAutoTerminationPolicy returns the auto-termination policy for a cluster.
func (b *InMemoryBackend) GetAutoTerminationPolicy(
	clusterID string,
) (*AutoTerminationPolicy, error) {
	b.mu.RLock("GetAutoTerminationPolicy")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	if cluster.autoTerminationPolicy == nil {
		empty := AutoTerminationPolicy{}

		return &empty, nil
	}

	cp := *cluster.autoTerminationPolicy

	return &cp, nil
}

// RemoveAutoTerminationPolicy clears the auto-termination policy on a cluster.
func (b *InMemoryBackend) RemoveAutoTerminationPolicy(clusterID string) error {
	b.mu.Lock("RemoveAutoTerminationPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	cluster.autoTerminationPolicy = nil

	return nil
}

// PutAutoScalingPolicy persists an auto-scaling policy on an instance group.
func (b *InMemoryBackend) PutAutoScalingPolicy(
	clusterID, instanceGroupID string,
	policy AutoScalingPolicySpec,
) (*AutoScalingPolicyDetail, string, string, error) {
	b.mu.Lock("PutAutoScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, "", "", fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for i := range cluster.instanceGroups {
		if cluster.instanceGroups[i].ID == instanceGroupID {
			detail := &AutoScalingPolicyDetail{
				Status:      map[string]string{"State": "ATTACHED"},
				Constraints: policy.Constraints,
				Rules:       policy.Rules,
			}
			cluster.instanceGroups[i].AutoScalingPolicy = detail

			return detail, cluster.ARN, instanceGroupID, nil
		}
	}

	return nil, "", "", fmt.Errorf("%w: instance group %s not found", ErrNotFound, instanceGroupID)
}

// RemoveAutoScalingPolicy clears the auto-scaling policy on an instance group.
func (b *InMemoryBackend) RemoveAutoScalingPolicy(clusterID, instanceGroupID string) error {
	b.mu.Lock("RemoveAutoScalingPolicy")
	defer b.mu.Unlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	for i := range cluster.instanceGroups {
		if cluster.instanceGroups[i].ID == instanceGroupID {
			cluster.instanceGroups[i].AutoScalingPolicy = nil

			return nil
		}
	}

	return fmt.Errorf("%w: instance group %s not found", ErrNotFound, instanceGroupID)
}

// GetBlockPublicAccessConfiguration returns the account-level block-public-access config.
func (b *InMemoryBackend) GetBlockPublicAccessConfiguration() (BlockPublicAccessConfiguration, blockPublicAccessMeta) {
	b.mu.RLock("GetBlockPublicAccessConfiguration")
	defer b.mu.RUnlock()

	if b.blockPublicAccess == nil {
		return defaultBlockPublicAccess(), blockPublicAccessMeta{CreationDateTime: time.Now()}
	}

	return *b.blockPublicAccess, *b.blockPublicAccessMeta
}

func defaultBlockPublicAccess() BlockPublicAccessConfiguration {
	return BlockPublicAccessConfiguration{
		BlockPublicSecurityGroupRules: true,
		PermittedPublicSecurityGroupRuleRanges: []PortRange{
			{MinRange: defaultSSHPort, MaxRange: defaultSSHPort},
		},
	}
}

// PutBlockPublicAccessConfiguration sets the account-level block-public-access config.
func (b *InMemoryBackend) PutBlockPublicAccessConfiguration(
	config BlockPublicAccessConfiguration,
) error {
	if err := validatePortRanges(config.PermittedPublicSecurityGroupRuleRanges); err != nil {
		return err
	}

	b.mu.Lock("PutBlockPublicAccessConfiguration")
	defer b.mu.Unlock()

	cp := config
	b.blockPublicAccess = &cp
	b.blockPublicAccessMeta = &blockPublicAccessMeta{
		CreationDateTime: time.Now(),
		CreatedByArn:     arn.Build("iam", "", b.accountID, "root"),
	}

	return nil
}

func validatePortRanges(ranges []PortRange) error {
	for _, r := range ranges {
		if r.MinRange < 0 || r.MaxRange > 65535 || r.MinRange > r.MaxRange {
			return fmt.Errorf("%w: invalid port range %d-%d", ErrValidation, r.MinRange, r.MaxRange)
		}
	}

	return nil
}

// ListSecurityConfigurations returns all security configurations, sorted by name.
func (b *InMemoryBackend) ListSecurityConfigurations(
	marker string,
) ([]SecurityConfigSummary, string) {
	b.mu.RLock("ListSecurityConfigurations")
	defer b.mu.RUnlock()

	summaries := make([]SecurityConfigSummary, 0, len(b.securityConfigs))

	for _, sc := range b.securityConfigs {
		summaries = append(summaries, SecurityConfigSummary{
			Name:             sc.Name,
			CreationDateTime: sc.CreationDateTime,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	p := page.New(summaries, marker, listSecConfigsPageSize, listSecConfigsPageSize)

	return p.Data, p.Next
}

// SecurityConfigSummary is returned by ListSecurityConfigurations.
type SecurityConfigSummary struct {
	CreationDateTime time.Time `json:"CreationDateTime"`
	Name             string    `json:"Name"`
}

// ListReleaseLabels returns release labels optionally filtered by prefix and application.
func (b *InMemoryBackend) ListReleaseLabels(prefix, application, marker string) ([]string, string) {
	var labels []string

	for label := range releaseLabelApps {
		if prefix != "" && !stringHasPrefix(label, prefix) {
			continue
		}

		if application != "" && !labelHasApp(label, application) {
			continue
		}

		labels = append(labels, label)
	}

	sort.Strings(labels)

	p := page.New(labels, marker, listReleaseLabelsPage, listReleaseLabelsPage)

	return p.Data, p.Next
}

func stringHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func labelHasApp(label, application string) bool {
	apps, ok := releaseLabelApps[label]
	if !ok {
		return false
	}

	return slices.Contains(apps, application)
}

// DescribeReleaseLabel returns details about a specific release label.
func (b *InMemoryBackend) DescribeReleaseLabel(releaseLabel string) (*ReleaseLabel, error) {
	apps, ok := releaseLabelApps[releaseLabel]
	if !ok {
		return nil, fmt.Errorf("%w: release label %s not found", ErrNotFound, releaseLabel)
	}

	rla := make([]ReleaseLabelApplication, 0, len(apps))
	for _, name := range apps {
		rla = append(rla, ReleaseLabelApplication{Name: name, Version: "latest"})
	}

	return &ReleaseLabel{ReleaseLabel: releaseLabel, Applications: rla}, nil
}

// ListSupportedInstanceTypes returns the static catalog of EMR-supported instance types.
func (b *InMemoryBackend) ListSupportedInstanceTypes(
	releaseLabel, marker string,
) ([]SupportedInstanceType, string) {
	// Validate release label exists (unknown labels → empty list matches AWS behavior).
	if _, ok := releaseLabelApps[releaseLabel]; !ok {
		return []SupportedInstanceType{}, ""
	}

	p := page.New(supportedInstanceTypes, marker, listInstanceTypesPage, listInstanceTypesPage)

	return p.Data, p.Next
}

// ListInstances synthesizes per-group instances for a cluster.
func (b *InMemoryBackend) ListInstances(
	clusterID string,
	params ListInstancesParams,
) ([]ClusterInstance, string) {
	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return []ClusterInstance{}, ""
	}

	instances := buildInstanceList(cluster, params)

	p := page.New(instances, params.Marker, listInstancesPageSize, listInstancesPageSize)

	return p.Data, p.Next
}

func buildInstanceList(cluster *Cluster, params ListInstancesParams) []ClusterInstance {
	var instances []ClusterInstance
	idx := 0

	for _, grp := range cluster.instanceGroups {
		if !instanceGroupMatchesParams(grp, params) {
			continue
		}

		for range grp.RunningInstanceCount {
			instances = append(instances, synthesizeInstance(cluster.ID, grp, idx))
			idx++
		}
	}

	return instances
}

func instanceGroupMatchesParams(grp InstanceGroup, params ListInstancesParams) bool {
	if params.InstanceGroupID != "" && grp.ID != params.InstanceGroupID {
		return false
	}

	if len(params.InstanceGroupTypes) > 0 &&
		!slices.Contains(params.InstanceGroupTypes, grp.InstanceGroupType) {
		return false
	}

	return true
}

func synthesizeInstance(clusterID string, grp InstanceGroup, idx int) ClusterInstance {
	id := fmt.Sprintf("ci-%s-%d", clusterID, idx)
	ec2ID := fmt.Sprintf("i-%016x", idx+1)
	privateDNS := fmt.Sprintf("ip-10-0-0-%d.ec2.internal", idx+1)

	return ClusterInstance{
		ID:              id,
		Ec2InstanceID:   ec2ID,
		PrivateDNSName:  privateDNS,
		Market:          grp.Market,
		InstanceType:    grp.InstanceType,
		InstanceGroupID: grp.ID,
		Status:          ClusterInstanceStatus{State: grp.Status.State},
	}
}

// DescribeStudio returns an EMR Studio by ID.
func (b *InMemoryBackend) DescribeStudio(studioID string) (*Studio, error) {
	b.mu.RLock("DescribeStudio")
	defer b.mu.RUnlock()

	studio, ok := b.studios[studioID]
	if !ok {
		return nil, fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	cp := *studio

	return &cp, nil
}

// ListStudios returns all studios as summaries, sorted by name.
func (b *InMemoryBackend) ListStudios(marker string) ([]StudioSummary, string) {
	b.mu.RLock("ListStudios")
	defer b.mu.RUnlock()

	summaries := make([]StudioSummary, 0, len(b.studios))

	for _, s := range b.studios {
		summaries = append(summaries, StudioSummary{
			StudioID:          s.StudioID,
			StudioArn:         s.StudioArn,
			Name:              s.Name,
			VpcID:             s.VpcID,
			DefaultS3Location: s.DefaultS3Location,
			AuthMode:          s.AuthMode,
			URL:               s.URL,
			CreationTime:      s.CreationTime,
			Description:       s.Description,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	p := page.New(summaries, marker, listStudiosPageSize, listStudiosPageSize)

	return p.Data, p.Next
}

// UpdateStudio updates mutable fields on an EMR Studio.
func (b *InMemoryBackend) UpdateStudio(
	studioID, name, description, defaultS3Location, subnetIDsJSON string,
) error {
	b.mu.Lock("UpdateStudio")
	defer b.mu.Unlock()

	studio, ok := b.studios[studioID]
	if !ok {
		return fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	if name != "" {
		studio.Name = name
	}

	if description != "" {
		studio.Description = description
	}

	if defaultS3Location != "" {
		studio.DefaultS3Location = defaultS3Location
	}

	_ = subnetIDsJSON

	return nil
}

// GetStudioSessionMapping returns a session mapping for a studio.
func (b *InMemoryBackend) GetStudioSessionMapping(
	studioID, identityType, identityID, identityName string,
) (*StudioSessionMapping, error) {
	b.mu.RLock("GetStudioSessionMapping")
	defer b.mu.RUnlock()

	key := studioSessionKey(studioID, identityType, identityID, identityName)

	mapping, ok := b.studioSessionMappings[key]
	if !ok {
		return nil, fmt.Errorf("%w: session mapping not found for studio %s", ErrNotFound, studioID)
	}

	cp := *mapping

	return &cp, nil
}

// ListStudioSessionMappings returns session mappings for a studio, optionally filtered by identity type.
func (b *InMemoryBackend) ListStudioSessionMappings(
	studioID, identityType string,
) []StudioSessionMapping {
	b.mu.RLock("ListStudioSessionMappings")
	defer b.mu.RUnlock()

	result := make([]StudioSessionMapping, 0)

	for _, m := range b.studioSessionMappings {
		if m.StudioID != studioID {
			continue
		}

		if identityType != "" && m.IdentityType != identityType {
			continue
		}

		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].IdentityID < result[j].IdentityID
	})

	return result
}

// UpdateStudioSessionMapping changes the SessionPolicyArn on a mapping.
func (b *InMemoryBackend) UpdateStudioSessionMapping(
	studioID, identityType, identityID, identityName, sessionPolicyArn string,
) error {
	b.mu.Lock("UpdateStudioSessionMapping")
	defer b.mu.Unlock()

	key := studioSessionKey(studioID, identityType, identityID, identityName)

	mapping, ok := b.studioSessionMappings[key]
	if !ok {
		return fmt.Errorf("%w: session mapping not found for studio %s", ErrNotFound, studioID)
	}

	mapping.SessionPolicyArn = sessionPolicyArn
	mapping.LastModifiedTime = time.Now()

	return nil
}

// DescribeJobFlows translates clusters into the legacy JobFlow format.
func (b *InMemoryBackend) DescribeJobFlows(
	ids, states []string,
	createdAfter, createdBefore *time.Time,
) []JobFlow {
	b.mu.RLock("DescribeJobFlows")
	defer b.mu.RUnlock()

	idSet := buildStringSet(ids)
	stateSet := buildStateSet(states)

	flows := make([]JobFlow, 0)

	for _, c := range b.clusters {
		if !jobFlowMatchesFilter(c, idSet, stateSet, createdAfter, createdBefore) {
			continue
		}

		flows = append(flows, clusterToJobFlow(c))
	}

	sort.Slice(flows, func(i, j int) bool {
		return flows[i].JobFlowID < flows[j].JobFlowID
	})

	return flows
}

func jobFlowMatchesFilter(
	c *Cluster,
	idSet, stateSet map[string]bool,
	createdAfter, createdBefore *time.Time,
) bool {
	if idSet != nil && !idSet[c.ID] {
		return false
	}

	if stateSet != nil && !stateSet[c.Status.State] {
		return false
	}

	creationMillis := clusterCreationMillisFromCluster(c)
	if createdAfter != nil && creationMillis < createdAfter.UnixMilli() {
		return false
	}

	if createdBefore != nil && creationMillis > createdBefore.UnixMilli() {
		return false
	}

	return true
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

func clusterToJobFlow(c *Cluster) JobFlow {
	creationMillis, _ := c.Status.Timeline[timelineKeyCreation].(float64)
	endMillis, _ := c.Status.Timeline[timelineKeyEnd].(float64)

	stateChangeMsg := ""
	if m, ok := c.Status.StateChangeReason["Message"]; ok {
		stateChangeMsg, _ = m.(string)
	}

	totalInstances := 0
	masterType := ""
	slaveType := ""

	for _, grp := range c.instanceGroups {
		totalInstances += grp.RunningInstanceCount
		switch grp.InstanceGroupType {
		case "MASTER":
			masterType = grp.InstanceType
		case "CORE", "TASK":
			if slaveType == "" {
				slaveType = grp.InstanceType
			}
		}
	}

	return JobFlow{
		JobFlowID:    c.ID,
		Name:         c.Name,
		ReleaseLabel: c.ReleaseLabel,
		LogURI:       c.LogURI,
		ServiceRole:  c.ServiceRole,
		ExecutionStatusDetail: JobFlowExecutionStatusDetail{
			State:             c.Status.State,
			CreationDateTime:  creationMillis,
			EndDateTime:       endMillis,
			StateChangeReason: stateChangeMsg,
		},
		Instances: JobFlowInstancesDetail{
			MasterInstanceType: masterType,
			SlaveInstanceType:  slaveType,
			InstanceCount:      totalInstances,
		},
	}
}

// DescribePersistentAppUI returns a persistent app UI by ID.
func (b *InMemoryBackend) DescribePersistentAppUI(id string) (*PersistentAppUI, error) {
	b.mu.RLock("DescribePersistentAppUI")
	defer b.mu.RUnlock()

	ui, ok := b.persistentAppUIs[id]
	if !ok {
		return nil, fmt.Errorf("%w: persistent app UI %s not found", ErrNotFound, id)
	}

	cp := *ui

	return &cp, nil
}

// GetPresignedURL returns a synthetic presigned URL for a persistent app UI.
func (b *InMemoryBackend) GetPresignedURL(id, region string) string {
	return fmt.Sprintf(
		"https://%s.%s.persistent-emr.amazonaws.com?X-Amz-Signature=fakesig-%s",
		id,
		region,
		id,
	)
}

// GetClusterSessionCredentials returns synthesized credentials for cluster session access.
func (b *InMemoryBackend) GetClusterSessionCredentials(
	clusterID, executionRoleArn string,
) (map[string]any, time.Time, error) {
	if executionRoleArn == "" {
		return nil, time.Time{}, fmt.Errorf("%w: ExecutionRoleArn is required", ErrValidation)
	}

	b.mu.RLock("GetClusterSessionCredentials")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterID]; !ok {
		return nil, time.Time{}, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	expiry := time.Now().Add(sessionCredentialExpiry)
	creds := map[string]any{
		"UsernamePassword": map[string]string{
			"Username": "admin-" + clusterID,
			"Password": "fake-password-" + clusterID,
		},
	}

	return creds, expiry, nil
}

// CreatePersistentAppUI creates a new persistent application user interface.
func (b *InMemoryBackend) CreatePersistentAppUI(
	targetResourceArn string,
) (*PersistentAppUI, error) {
	if targetResourceArn == "" {
		return nil, fmt.Errorf("%w: TargetResourceArn is required", ErrValidation)
	}

	b.mu.Lock("CreatePersistentAppUI")
	defer b.mu.Unlock()

	id := b.nextPersistentAppUIID()
	ui := &PersistentAppUI{
		ID:                        id,
		TargetResourceArn:         targetResourceArn,
		RuntimeRoleEnabledCluster: false,
	}

	b.persistentAppUIs[id] = ui
	cp := *ui

	return &cp, nil
}

// CreateSecurityConfiguration creates a new security configuration.
func (b *InMemoryBackend) CreateSecurityConfiguration(
	name, securityConfig string,
) (*SecurityConfiguration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if !json.Valid([]byte(securityConfig)) {
		return nil, fmt.Errorf("%w: SecurityConfiguration must be valid JSON", ErrValidation)
	}

	b.mu.Lock("CreateSecurityConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.securityConfigs[name]; exists {
		return nil, fmt.Errorf(
			"%w: security configuration %s already exists",
			ErrAlreadyExists,
			name,
		)
	}

	sc := &SecurityConfiguration{
		Name:             name,
		SecurityConfig:   securityConfig,
		CreationDateTime: time.Now(),
	}

	b.securityConfigs[name] = sc

	cp := *sc

	return &cp, nil
}

// DeleteSecurityConfiguration deletes a security configuration by name.
func (b *InMemoryBackend) DeleteSecurityConfiguration(name string) error {
	b.mu.Lock("DeleteSecurityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.securityConfigs[name]; !ok {
		return fmt.Errorf("%w: security configuration %s not found", ErrNotFound, name)
	}

	delete(b.securityConfigs, name)

	return nil
}

// DescribeSecurityConfiguration returns the details of a security configuration.
func (b *InMemoryBackend) DescribeSecurityConfiguration(
	name string,
) (*SecurityConfiguration, error) {
	b.mu.RLock("DescribeSecurityConfiguration")
	defer b.mu.RUnlock()

	sc, ok := b.securityConfigs[name]
	if !ok {
		return nil, fmt.Errorf("%w: security configuration %s not found", ErrNotFound, name)
	}

	cp := *sc

	return &cp, nil
}

// CreateStudio creates a new EMR Studio.
func (b *InMemoryBackend) CreateStudio(
	name, authMode, defaultS3Location, engineSGID, serviceRole, vpcID, workspaceSGID string,
	subnetIDs []string, tags []Tag,
) (*Studio, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("CreateStudio")
	defer b.mu.Unlock()

	for _, s := range b.studios {
		if s.Name == name {
			return nil, fmt.Errorf("%w: studio with name %s already exists", ErrAlreadyExists, name)
		}
	}

	id := b.nextStudioID()
	studioARN := arn.Build("elasticmapreduce", b.region, b.accountID, "studio/"+id)

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	subnetCopy := make([]string, len(subnetIDs))
	copy(subnetCopy, subnetIDs)

	studio := &Studio{
		StudioID:                 id,
		StudioArn:                studioARN,
		Name:                     name,
		AuthMode:                 authMode,
		DefaultS3Location:        defaultS3Location,
		EngineSecurityGroupID:    engineSGID,
		ServiceRole:              serviceRole,
		VpcID:                    vpcID,
		WorkspaceSecurityGroupID: workspaceSGID,
		SubnetIDs:                subnetCopy,
		Tags:                     tagsCopy,
		CreationTime:             time.Now(),
		URL:                      "https://studio." + id + ".emrstudio-prod." + b.region + ".amazonaws.com",
	}

	b.studios[id] = studio

	cp := *studio

	return &cp, nil
}

// DeleteStudio deletes an EMR Studio by ID.
func (b *InMemoryBackend) DeleteStudio(studioID string) error {
	b.mu.Lock("DeleteStudio")
	defer b.mu.Unlock()

	if _, ok := b.studios[studioID]; !ok {
		return fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	delete(b.studios, studioID)

	for k, m := range b.studioSessionMappings {
		if m.StudioID == studioID {
			delete(b.studioSessionMappings, k)
		}
	}

	return nil
}

// studioSessionKey returns the composite key for a session mapping.
func studioSessionKey(studioID, identityType, identityID, identityName string) string {
	if identityID != "" {
		return studioID + "|" + identityType + "|id:" + identityID
	}

	return studioID + "|" + identityType + "|name:" + identityName
}

// CreateStudioSessionMapping maps a user or group to an EMR Studio.
func (b *InMemoryBackend) CreateStudioSessionMapping(
	studioID, identityType, identityID, identityName, sessionPolicyArn string,
) error {
	if studioID == "" {
		return fmt.Errorf("%w: StudioId is required", ErrValidation)
	}

	b.mu.Lock("CreateStudioSessionMapping")
	defer b.mu.Unlock()

	if _, ok := b.studios[studioID]; !ok {
		return fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	key := studioSessionKey(studioID, identityType, identityID, identityName)
	b.studioSessionMappings[key] = &StudioSessionMapping{
		StudioID:         studioID,
		IdentityType:     identityType,
		IdentityID:       identityID,
		IdentityName:     identityName,
		SessionPolicyArn: sessionPolicyArn,
		CreationTime:     time.Now(),
		LastModifiedTime: time.Now(),
	}

	return nil
}

// DeleteStudioSessionMapping removes a user or group from an EMR Studio.
func (b *InMemoryBackend) DeleteStudioSessionMapping(
	studioID, identityType, identityID, identityName string,
) error {
	b.mu.Lock("DeleteStudioSessionMapping")
	defer b.mu.Unlock()

	key := studioSessionKey(studioID, identityType, identityID, identityName)
	if _, ok := b.studioSessionMappings[key]; !ok {
		return fmt.Errorf("%w: session mapping not found for studio %s", ErrNotFound, studioID)
	}

	delete(b.studioSessionMappings, key)

	return nil
}

// ListInstanceFleets returns the instance fleets for a cluster by its ID.
func (b *InMemoryBackend) ListInstanceFleets(clusterID string) ([]InstanceFleet, error) {
	b.mu.RLock("ListInstanceFleets")
	defer b.mu.RUnlock()

	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterID)
	}

	fleets := make([]InstanceFleet, len(cluster.instanceFleets))
	copy(fleets, cluster.instanceFleets)

	return fleets, nil
}

// AddClusterInternal seeds a cluster directly into the backend for testing.
func (b *InMemoryBackend) AddClusterInternal(cluster *Cluster) {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	cp := cluster.clone()
	b.clusters[cluster.ID] = &cp
	b.arnIndex[cluster.ARN] = cluster.ID
}

// AddSecurityConfigInternal seeds a security configuration directly into the backend for testing.
func (b *InMemoryBackend) AddSecurityConfigInternal(sc SecurityConfiguration) {
	b.mu.Lock("AddSecurityConfigInternal")
	defer b.mu.Unlock()

	cp := sc
	b.securityConfigs[sc.Name] = &cp
}

// AddStudioInternal seeds a studio directly into the backend for testing.
func (b *InMemoryBackend) AddStudioInternal(studio Studio) {
	b.mu.Lock("AddStudioInternal")
	defer b.mu.Unlock()

	cp := studio
	b.studios[studio.StudioID] = &cp
}

// AddPersistentAppUIInternal seeds a persistent app UI directly into the backend for testing.
func (b *InMemoryBackend) AddPersistentAppUIInternal(ui PersistentAppUI) {
	b.mu.Lock("AddPersistentAppUIInternal")
	defer b.mu.Unlock()

	cp := ui
	b.persistentAppUIs[ui.ID] = &cp
}

// nextNotebookExecID generates a unique notebook execution ID.
func (b *InMemoryBackend) nextNotebookExecID() string {
	n := b.counter.Add(1)

	return fmt.Sprintf("ex-%013d", n)
}

// StartNotebookExecution creates a new notebook execution in RUNNING state.
func (b *InMemoryBackend) StartNotebookExecution(
	editorID, name, params, engineID string,
	tags []Tag,
) (*NotebookExecution, error) {
	b.mu.Lock("StartNotebookExecution")
	defer b.mu.Unlock()

	id := b.nextNotebookExecID()

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	ne := &NotebookExecution{
		NotebookExecutionID:   id,
		EditorID:              editorID,
		NotebookExecutionName: name,
		NotebookParams:        params,
		ExecutionEngineID:     engineID,
		Status:                NotebookStatusRunning,
		StartTime:             time.Now(),
		Tags:                  tagsCopy,
	}

	b.notebookExecutions[id] = ne

	cp := *ne

	return &cp, nil
}

// StopNotebookExecution transitions a RUNNING execution to STOPPED.
func (b *InMemoryBackend) StopNotebookExecution(id string) error {
	b.mu.Lock("StopNotebookExecution")
	defer b.mu.Unlock()

	ne, ok := b.notebookExecutions[id]
	if !ok {
		return fmt.Errorf("%w: notebook execution %s not found", ErrNotFound, id)
	}

	if ne.Status == NotebookStatusRunning || ne.Status == NotebookStatusStopping {
		ne.Status = NotebookStatusStopped
		ne.EndTime = time.Now()
	}

	return nil
}

// DescribeNotebookExecution returns a notebook execution by ID.
func (b *InMemoryBackend) DescribeNotebookExecution(id string) (*NotebookExecution, error) {
	b.mu.RLock("DescribeNotebookExecution")
	defer b.mu.RUnlock()

	ne, ok := b.notebookExecutions[id]
	if !ok {
		return nil, fmt.Errorf("%w: notebook execution %s not found", ErrNotFound, id)
	}

	cp := *ne

	return &cp, nil
}

// ListNotebookExecutionsParams holds filters for ListNotebookExecutions.
type ListNotebookExecutionsParams struct {
	EditorID string
	Status   string
	Marker   string
}

// ListNotebookExecutions returns paginated notebook executions matching the filter.
func (b *InMemoryBackend) ListNotebookExecutions(params ListNotebookExecutionsParams) ([]NotebookExecution, string) {
	b.mu.RLock("ListNotebookExecutions")
	defer b.mu.RUnlock()

	list := make([]NotebookExecution, 0, len(b.notebookExecutions))

	for _, ne := range b.notebookExecutions {
		if params.EditorID != "" && ne.EditorID != params.EditorID {
			continue
		}

		if params.Status != "" && ne.Status != params.Status {
			continue
		}

		list = append(list, *ne)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].NotebookExecutionID < list[j].NotebookExecutionID
	})

	p := page.New(list, params.Marker, listNotebookExecPageSize, listNotebookExecPageSize)

	return p.Data, p.Next
}
