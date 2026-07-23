package autoscaling

import "time"

// ScalingPolicy represents a scaling policy for an Auto Scaling group.
type ScalingPolicy struct {
	CustomizedMetricSpecification string           `json:"CustomizedMetricSpecification,omitempty"`
	PolicyName                    string           `json:"PolicyName"`
	AutoScalingGroupName          string           `json:"AutoScalingGroupName"`
	PolicyType                    string           `json:"PolicyType,omitempty"`
	AdjustmentType                string           `json:"AdjustmentType,omitempty"`
	MetricType                    string           `json:"MetricType,omitempty"`
	CustomMetricSpec              string           `json:"CustomMetricSpec,omitempty"`
	MetricAggregationType         string           `json:"MetricAggregationType,omitempty"`
	PolicyARN                     string           `json:"PolicyARN"`
	StepAdjustments               []StepAdjustment `json:"StepAdjustments,omitempty"`
	TargetValue                   float64          `json:"TargetValue,omitempty"`
	MinAdjustmentStep             int32            `json:"MinAdjustmentStep,omitempty"`
	MinAdjustmentMagnitude        int32            `json:"MinAdjustmentMagnitude,omitempty"`
	Cooldown                      int32            `json:"Cooldown,omitempty"`
	EstimatedWarmup               int32            `json:"EstimatedWarmup,omitempty"`
	ScalingAdjustment             int32            `json:"ScalingAdjustment,omitempty"`
	DisableScaleIn                bool             `json:"DisableScaleIn,omitempty"`
}

// StepAdjustment defines a scaling adjustment with a metric interval bound.
type StepAdjustment struct {
	MetricIntervalLowerBound *float64 `json:"MetricIntervalLowerBound,omitempty"`
	MetricIntervalUpperBound *float64 `json:"MetricIntervalUpperBound,omitempty"`
	ScalingAdjustment        int32    `json:"ScalingAdjustment"`
}

// ScalingPolicyInput holds the input for PutScalingPolicy.
type ScalingPolicyInput struct {
	CustomizedMetricSpecification string
	PolicyName                    string
	PolicyType                    string
	AdjustmentType                string
	MetricType                    string
	MetricAggregationType         string
	AutoScalingGroupName          string
	StepAdjustments               []StepAdjustment
	TargetValue                   float64
	MinAdjustmentStep             int32
	ScalingAdjustment             int32
	Cooldown                      int32
	EstimatedWarmup               int32
	MinAdjustmentMagnitude        int32
	DisableScaleIn                bool
}

// NotificationConfiguration represents a notification configuration for an Auto Scaling group.
type NotificationConfiguration struct {
	AutoScalingGroupName string `json:"AutoScalingGroupName"`
	TopicARN             string `json:"TopicARN"`
	NotificationType     string `json:"NotificationType"`
}

// InstanceReusePolicy controls whether warm-pool instances are reused on scale-in.
type InstanceReusePolicy struct {
	ReuseOnScaleIn bool `json:"ReuseOnScaleIn,omitempty"`
}

// WarmPool represents a warm pool configuration for an Auto Scaling group.
type WarmPool struct {
	AutoScalingGroupName     string              `json:"AutoScalingGroupName"`
	PoolState                string              `json:"PoolState,omitempty"`
	Status                   string              `json:"Status,omitempty"`
	MinSize                  int32               `json:"MinSize,omitempty"`
	MaxGroupPreparedCapacity int32               `json:"MaxGroupPreparedCapacity,omitempty"`
	InstanceReusePolicy      InstanceReusePolicy `json:"InstanceReusePolicy"`
}

// WarmPoolInput holds the input for PutWarmPool.
type WarmPoolInput struct {
	AutoScalingGroupName     string
	PoolState                string
	MinSize                  int32
	MaxGroupPreparedCapacity int32
	InstanceReusePolicy      InstanceReusePolicy
}

// AccountLimits represents AWS Auto Scaling account limits.
type AccountLimits struct {
	MaxNumberOfAutoScalingGroups    int32 `json:"MaxNumberOfAutoScalingGroups"`
	MaxNumberOfLaunchConfigurations int32 `json:"MaxNumberOfLaunchConfigurations"`
	NumberOfAutoScalingGroups       int32 `json:"NumberOfAutoScalingGroups"`
	NumberOfLaunchConfigurations    int32 `json:"NumberOfLaunchConfigurations"`
}

// MetricCollectionType represents a type of metric that can be collected.
type MetricCollectionType struct {
	Metric      string `json:"Metric"`
	Granularity string `json:"Granularity,omitempty"`
}

// LoadBalancerState represents the state of a load balancer attached to an ASG.
type LoadBalancerState struct {
	LoadBalancerName string `json:"LoadBalancerName"`
	State            string `json:"State"`
}

// LoadBalancerTargetGroupState represents the state of a target group attached to an ASG.
type LoadBalancerTargetGroupState struct {
	LoadBalancerTargetGroupARN string `json:"LoadBalancerTargetGroupARN"`
	State                      string `json:"State"`
}

// TrafficSourceState represents the state of a traffic source attached to an ASG.
type TrafficSourceState struct {
	Identifier string `json:"Identifier"`
	Type       string `json:"Type"`
	State      string `json:"State"`
}

// ExecutePolicyInput holds the input for ExecutePolicy.
// MetricValue and BreachThreshold are required (and only meaningful) when the
// target policy's PolicyType is StepScaling: AWS uses (MetricValue-BreachThreshold)
// to select which StepAdjustment interval applies.
type ExecutePolicyInput struct {
	MetricValue          *float64
	BreachThreshold      *float64
	AutoScalingGroupName string
	PolicyName           string
	HonorCooldown        bool
}

// RecordLifecycleActionHeartbeatInput holds the input for RecordLifecycleActionHeartbeat.
type RecordLifecycleActionHeartbeatInput struct {
	AutoScalingGroupName string
	LifecycleHookName    string
	LifecycleActionToken string
	InstanceID           string
}

// terminationDisposition controls what finishTermination does to the group's
// capacity bookkeeping once a Terminating:Wait lifecycle wait resolves. It is only
// meaningful for terminating actions (ignored for transitionLaunching waits).
type terminationDisposition int

const (
	// terminationReplace removes the instance and launches a replacement to
	// maintain DesiredCapacity. Used by TerminateInstanceInAutoScalingGroup when
	// ShouldDecrementDesiredCapacity=false. This is also the zero value, so waits
	// re-armed by Restore() (whose original disposition is never persisted) fall
	// back to this behavior, matching pre-existing behavior.
	terminationReplace terminationDisposition = iota
	// terminationDecrement removes the instance and decrements DesiredCapacity/
	// MinSize by one, without a replacement. Used by
	// TerminateInstanceInAutoScalingGroup when ShouldDecrementDesiredCapacity=true.
	terminationDecrement
	// terminationCapacityPreset removes the instance only: DesiredCapacity/MinSize
	// were already adjusted by the caller before the wait was armed, so no further
	// capacity bookkeeping is needed once it resolves. Used by the desired-capacity
	// -driven scale-in path (SetDesiredCapacity, UpdateAutoScalingGroup, and
	// ExecutePolicy scale-in), which sets DesiredCapacity to its new target up front.
	terminationCapacityPreset
)

// pendingHookAction tracks an in-flight lifecycle action with its timer.
// Transition is one of the transitionLaunching/transitionTerminating constants
// (store.go) and determines what resolving the action does to the instance.
// Disposition is only meaningful for terminating actions: it records what
// finishTermination should do to the group's capacity once the wait completes.
type pendingHookAction struct {
	timer         *time.Timer
	Token         string
	GroupName     string
	HookName      string
	InstanceID    string
	Transition    string
	DefaultResult string
	timeout       time.Duration
	Disposition   terminationDisposition
}

// LaunchTemplateSpecification identifies an EC2 launch template.
type LaunchTemplateSpecification struct {
	LaunchTemplateID   string `json:"LaunchTemplateId,omitempty"`
	LaunchTemplateName string `json:"LaunchTemplateName,omitempty"`
	Version            string `json:"Version,omitempty"`
}

// LaunchTemplateOverride allows specifying per-instance-type overrides in a MixedInstancesPolicy.
type LaunchTemplateOverride struct {
	InstanceType                string                       `json:"InstanceType,omitempty"`
	LaunchTemplateSpecification *LaunchTemplateSpecification `json:"LaunchTemplateSpecification,omitempty"`
	WeightedCapacity            string                       `json:"WeightedCapacity,omitempty"`
}

// MixedInstancesLaunchTemplate is the launch template component of MixedInstancesPolicy.
type MixedInstancesLaunchTemplate struct {
	LaunchTemplateSpecification LaunchTemplateSpecification `json:"LaunchTemplateSpecification"`
	Overrides                   []LaunchTemplateOverride    `json:"Overrides,omitempty"`
}

// InstancesDistribution controls on-demand vs spot allocation in MixedInstancesPolicy.
type InstancesDistribution struct {
	OnDemandAllocationStrategy          string `json:"OnDemandAllocationStrategy,omitempty"`
	SpotAllocationStrategy              string `json:"SpotAllocationStrategy,omitempty"`
	SpotMaxPrice                        string `json:"SpotMaxPrice,omitempty"`
	OnDemandBaseCapacity                int32  `json:"OnDemandBaseCapacity,omitempty"`
	OnDemandPercentageAboveBaseCapacity int32  `json:"OnDemandPercentageAboveBaseCapacity,omitempty"`
	SpotInstancePools                   int32  `json:"SpotInstancePools,omitempty"`
}

// MixedInstancesPolicy allows combining on-demand and spot instances with multiple instance types.
type MixedInstancesPolicy struct {
	LaunchTemplate        MixedInstancesLaunchTemplate `json:"LaunchTemplate"`
	InstancesDistribution InstancesDistribution        `json:"InstancesDistribution"`
}

// AvailabilityZoneDistribution controls how launch failures are handled across AZs.
type AvailabilityZoneDistribution struct {
	// CapacityDistributionStrategy is "balanced-only" or "balanced-best-effort".
	CapacityDistributionStrategy string `json:"CapacityDistributionStrategy,omitempty"`
}

// AvailabilityZoneImpairmentPolicy controls instance replacement behavior during a zonal shift.
type AvailabilityZoneImpairmentPolicy struct {
	// ImpairedZoneHealthCheckBehavior is "ReplaceUnhealthy" or "IgnoreUnhealthy".
	ImpairedZoneHealthCheckBehavior string `json:"ImpairedZoneHealthCheckBehavior,omitempty"`
	ZonalShiftEnabled               bool   `json:"ZonalShiftEnabled,omitempty"`
}

// CapacityReservationTarget identifies specific Capacity Reservations or resource groups to target.
type CapacityReservationTarget struct {
	CapacityReservationIDs               []string `json:"CapacityReservationIds,omitempty"`
	CapacityReservationResourceGroupARNs []string `json:"CapacityReservationResourceGroupArns,omitempty"`
}

// CapacityReservationSpecification controls how the group uses EC2 Capacity Reservations.
type CapacityReservationSpecification struct {
	CapacityReservationTarget *CapacityReservationTarget `json:"CapacityReservationTarget,omitempty"`
	// CapacityReservationPreference is one of "capacity-reservations-only",
	// "capacity-reservations-first", "none", or "default".
	CapacityReservationPreference string `json:"CapacityReservationPreference,omitempty"`
}

// RetentionTriggers defines the failure conditions that move an instance to a
// Retained state instead of terminating it.
type RetentionTriggers struct {
	// TerminateHookAbandon is "retain" or "terminate".
	TerminateHookAbandon string `json:"TerminateHookAbandon,omitempty"`
}

// InstanceLifecyclePolicy controls instance retention behavior on lifecycle transitions.
type InstanceLifecyclePolicy struct {
	RetentionTriggers *RetentionTriggers `json:"RetentionTriggers,omitempty"`
}

// InstanceMaintenancePolicy bounds the in-service/healthy percentage window
// used when replacing instances. Pointers distinguish "unset" from the valid
// value 0 (and the AWS "-1 clears a previously set value" sentinel).
type InstanceMaintenancePolicy struct {
	MinHealthyPercentage *int32 `json:"MinHealthyPercentage,omitempty"`
	MaxHealthyPercentage *int32 `json:"MaxHealthyPercentage,omitempty"`
}

// AutoScalingGroup represents an EC2 Auto Scaling group.
//
//nolint:revive // AutoScalingGroup is the canonical AWS type name; renaming to Group would break convention.
type AutoScalingGroup struct {
	LastScalingActivity              time.Time                         `json:"LastScalingActivity,omitzero"`
	CreatedTime                      time.Time                         `json:"CreatedTime"`
	LaunchTemplate                   *LaunchTemplateSpecification      `json:"LaunchTemplate,omitempty"`
	InstanceMaintenancePolicy        *InstanceMaintenancePolicy        `json:"InstanceMaintenancePolicy,omitempty"`
	InstanceLifecyclePolicy          *InstanceLifecyclePolicy          `json:"InstanceLifecyclePolicy,omitempty"`
	CapacityReservationSpecification *CapacityReservationSpecification `json:"CapacityReservationSpecification,omitempty"`
	AvailabilityZoneImpairmentPolicy *AvailabilityZoneImpairmentPolicy `json:"AvailabilityZoneImpairmentPolicy,omitempty"`
	AvailabilityZoneDistribution     *AvailabilityZoneDistribution     `json:"AvailabilityZoneDistribution,omitempty"`
	MixedInstancesPolicy             *MixedInstancesPolicy             `json:"MixedInstancesPolicy,omitempty"`
	DeletionProtection               string                            `json:"DeletionProtection,omitempty"`
	VPCZoneIdentifier                string                            `json:"VPCZoneIdentifier,omitempty"`
	Context                          string                            `json:"Context,omitempty"`
	PlacementGroup                   string                            `json:"PlacementGroup,omitempty"`
	AutoScalingGroupName             string                            `json:"AutoScalingGroupName"`
	Status                           string                            `json:"Status,omitempty"`
	HealthCheckType                  string                            `json:"HealthCheckType"`
	LaunchConfigurationName          string                            `json:"LaunchConfigurationName,omitempty"`
	DesiredCapacityType              string                            `json:"DesiredCapacityType,omitempty"`
	AutoScalingGroupARN              string                            `json:"AutoScalingGroupARN"`
	Instances                        []Instance                        `json:"Instances,omitempty"`
	EnabledMetrics                   []string                          `json:"EnabledMetrics,omitempty"`
	TerminationPolicies              []string                          `json:"TerminationPolicies,omitempty"`
	SuspendedProcesses               []string                          `json:"SuspendedProcesses,omitempty"`
	Tags                             []Tag                             `json:"Tags,omitempty"`
	AvailabilityZones                []string                          `json:"AvailabilityZones,omitempty"`
	TrafficSources                   []TrafficSource                   `json:"TrafficSources,omitempty"`
	TargetGroupARNs                  []string                          `json:"TargetGroupARNs,omitempty"`
	LoadBalancerNames                []string                          `json:"LoadBalancerNames,omitempty"`
	MinSize                          int32                             `json:"MinSize"`
	MaxSize                          int32                             `json:"MaxSize"`
	DesiredCapacity                  int32                             `json:"DesiredCapacity"`
	DefaultCooldown                  int32                             `json:"DefaultCooldown"`
	HealthCheckGracePeriod           int32                             `json:"HealthCheckGracePeriod"`
	MaxInstanceLifetime              int32                             `json:"MaxInstanceLifetime,omitempty"`
	DefaultInstanceWarmup            int32                             `json:"DefaultInstanceWarmup,omitempty"`
	NewInstancesProtectedFromScaleIn bool                              `json:"NewInstancesProtectedFromScaleIn,omitempty"`
	CapacityRebalance                bool                              `json:"CapacityRebalance,omitempty"`
	SkipZonalShiftValidation         bool                              `json:"SkipZonalShiftValidation,omitempty"`
}

// EbsBlockDevice describes an EBS volume configuration in a block device mapping.
type EbsBlockDevice struct {
	SnapshotID          string `json:"SnapshotId,omitempty"`
	VolumeType          string `json:"VolumeType,omitempty"`
	KmsKeyID            string `json:"KmsKeyId,omitempty"`
	VolumeSize          int32  `json:"VolumeSize,omitempty"`
	Iops                int32  `json:"Iops,omitempty"`
	Throughput          int32  `json:"Throughput,omitempty"`
	DeleteOnTermination bool   `json:"DeleteOnTermination,omitempty"`
	Encrypted           bool   `json:"Encrypted,omitempty"`
}

// LaunchConfiguration represents an Auto Scaling launch configuration.
type LaunchConfiguration struct {
	CreatedTime                  time.Time            `json:"CreatedTime"`
	LaunchConfigurationName      string               `json:"LaunchConfigurationName"`
	LaunchConfigurationARN       string               `json:"LaunchConfigurationARN"`
	ImageID                      string               `json:"ImageID"`
	InstanceType                 string               `json:"InstanceType"`
	KeyName                      string               `json:"KeyName,omitempty"`
	IAMInstanceProfile           string               `json:"IAMInstanceProfile,omitempty"`
	UserData                     string               `json:"UserData,omitempty"`
	KernelID                     string               `json:"KernelID,omitempty"`
	RamdiskID                    string               `json:"RamdiskID,omitempty"`
	SpotPrice                    string               `json:"SpotPrice,omitempty"`
	PlacementTenancy             string               `json:"PlacementTenancy,omitempty"`
	ClassicLinkVPCID             string               `json:"ClassicLinkVpcId,omitempty"`
	BlockDeviceMappings          []BlockDeviceMapping `json:"BlockDeviceMappings,omitempty"`
	ClassicLinkVPCSecurityGroups []string             `json:"ClassicLinkVpcSecurityGroups,omitempty"`
	SecurityGroups               []string             `json:"SecurityGroups,omitempty"`
	AssociatePublicIPAddress     bool                 `json:"AssociatePublicIpAddress,omitempty"`
	EbsOptimized                 bool                 `json:"EbsOptimized,omitempty"`
	InstanceMonitoring           bool                 `json:"InstanceMonitoring,omitempty"`
}

// BlockDeviceMapping represents an EBS or ephemeral block device mapping.
type BlockDeviceMapping struct {
	Ebs         *EbsBlockDevice `json:"Ebs,omitempty"`
	VirtualName string          `json:"VirtualName,omitempty"`
	DeviceName  string          `json:"DeviceName"`
	NoDevice    string          `json:"NoDevice,omitempty"`
}

// Instance represents an EC2 instance in an Auto Scaling group.
type Instance struct {
	LaunchTime              time.Time `json:"LaunchTime,omitzero"`
	InstanceID              string    `json:"InstanceID"`
	AvailabilityZone        string    `json:"AvailabilityZone"`
	LifecycleState          string    `json:"LifecycleState"`
	HealthStatus            string    `json:"HealthStatus"`
	LaunchConfigurationName string    `json:"LaunchConfigurationName,omitempty"`
	InstanceType            string    `json:"InstanceType,omitempty"`
	ProtectedFromScaleIn    bool      `json:"ProtectedFromScaleIn,omitempty"`
}

// Tag is a key/value pair attached to a resource, optionally propagated to new instances.
type Tag struct {
	Key               string `json:"Key"`
	Value             string `json:"Value"`
	PropagateAtLaunch bool   `json:"PropagateAtLaunch,omitempty"`
}

// TrafficSource identifies a traffic source attached to an Auto Scaling group.
type TrafficSource struct {
	Identifier string `json:"Identifier"`
	Type       string `json:"Type"`
}

// ResourceTag is a tag scoped to a specific resource (used by CreateOrUpdateTags).
type ResourceTag struct {
	ResourceID        string `json:"ResourceId"`
	ResourceType      string `json:"ResourceType"`
	Key               string `json:"Key"`
	Value             string `json:"Value"`
	PropagateAtLaunch bool   `json:"PropagateAtLaunch,omitempty"`
}

// ScheduledAction represents a scheduled scaling action for an Auto Scaling group.
type ScheduledAction struct {
	StartTime time.Time `json:"StartTime,omitzero"`
	EndTime   time.Time `json:"EndTime,omitzero"`
	// LastExecutedTime records when the background scheduler last applied this
	// action's capacity change. For a one-time action (empty Recurrence) it
	// gates against re-firing; for a recurring action it is the baseline
	// NextAfter is computed from. Internal bookkeeping only -- AWS's real
	// ScheduledUpdateGroupAction wire shape has no equivalent field, so this is
	// deliberately not projected onto the DescribeScheduledActions XML response.
	LastExecutedTime     time.Time `json:"LastExecutedTime,omitzero"`
	DesiredCapacity      *int32    `json:"DesiredCapacity,omitempty"`
	MinSize              *int32    `json:"MinSize,omitempty"`
	MaxSize              *int32    `json:"MaxSize,omitempty"`
	ScheduledActionName  string    `json:"ScheduledActionName"`
	ScheduledActionARN   string    `json:"ScheduledActionARN,omitempty"`
	AutoScalingGroupName string    `json:"AutoScalingGroupName"`
	Recurrence           string    `json:"Recurrence,omitempty"`
	TimeZone             string    `json:"TimeZone,omitempty"`
}

// ScheduledUpdateGroupAction is the input for a single action in BatchPutScheduledUpdateGroupAction.
type ScheduledUpdateGroupAction struct {
	StartTime           time.Time `json:"StartTime,omitzero"`
	EndTime             time.Time `json:"EndTime,omitzero"`
	DesiredCapacity     *int32    `json:"DesiredCapacity,omitempty"`
	MinSize             *int32    `json:"MinSize,omitempty"`
	MaxSize             *int32    `json:"MaxSize,omitempty"`
	ScheduledActionName string    `json:"ScheduledActionName"`
	Recurrence          string    `json:"Recurrence,omitempty"`
	TimeZone            string    `json:"TimeZone,omitempty"`
}

// FailedScheduledAction represents a scheduled action that failed during a batch operation.
type FailedScheduledAction struct {
	ScheduledActionName string `json:"ScheduledActionName"`
	ErrorCode           string `json:"ErrorCode"`
	ErrorMessage        string `json:"ErrorMessage"`
}

// InstanceRefreshPreferences holds the preferences for an instance refresh operation.
type InstanceRefreshPreferences struct {
	ScaleInProtectedInstances string  `json:"ScaleInProtectedInstances,omitempty"`
	StandbyInstances          string  `json:"StandbyInstances,omitempty"`
	CheckpointPercentages     []int32 `json:"CheckpointPercentages,omitempty"`
	MinHealthyPercentage      int32   `json:"MinHealthyPercentage,omitempty"`
	MaxHealthyPercentage      int32   `json:"MaxHealthyPercentage,omitempty"`
	InstanceWarmup            int32   `json:"InstanceWarmup,omitempty"`
	CheckpointDelay           int32   `json:"CheckpointDelay,omitempty"`
	SkipMatching              bool    `json:"SkipMatching,omitempty"`
	AutoRollback              bool    `json:"AutoRollback,omitempty"`
}

// InstanceRefresh represents an instance refresh operation for an Auto Scaling group.
type InstanceRefresh struct {
	StartTime            time.Time `json:"StartTime"`
	EndTime              time.Time `json:"EndTime,omitzero"`
	InstanceRefreshID    string    `json:"InstanceRefreshId"`
	AutoScalingGroupName string    `json:"AutoScalingGroupName"`
	Status               string    `json:"Status"`
	StatusReason         string    `json:"StatusReason,omitempty"`
	// Strategy is the instance refresh strategy; defaults to "Rolling".
	Strategy           string                     `json:"Strategy,omitempty"`
	Preferences        InstanceRefreshPreferences `json:"Preferences"`
	PercentageComplete int32                      `json:"PercentageComplete,omitempty"`
	InstancesToUpdate  int32                      `json:"InstancesToUpdate,omitempty"`
}

// LifecycleHook represents a lifecycle hook attached to an Auto Scaling group.
type LifecycleHook struct {
	LifecycleHookName     string `json:"LifecycleHookName"`
	AutoScalingGroupName  string `json:"AutoScalingGroupName"`
	LifecycleTransition   string `json:"LifecycleTransition,omitempty"`
	DefaultResult         string `json:"DefaultResult,omitempty"`
	NotificationTargetARN string `json:"NotificationTargetARN,omitempty"`
	NotificationMetadata  string `json:"NotificationMetadata,omitempty"`
	RoleARN               string `json:"RoleARN,omitempty"`
	HeartbeatTimeout      int32  `json:"HeartbeatTimeout,omitempty"`
	GlobalTimeout         int32  `json:"GlobalTimeout,omitempty"`
}

// CompleteLifecycleActionInput holds the input for CompleteLifecycleAction.
type CompleteLifecycleActionInput struct {
	AutoScalingGroupName  string
	LifecycleHookName     string
	LifecycleActionToken  string
	InstanceID            string
	LifecycleActionResult string
}

// ScalingActivity represents an Auto Scaling activity.
type ScalingActivity struct {
	StartTime            time.Time `json:"StartTime"`
	EndTime              time.Time `json:"EndTime"`
	ActivityID           string    `json:"ActivityID"`
	AutoScalingGroupName string    `json:"AutoScalingGroupName"`
	Description          string    `json:"Description,omitempty"`
	StatusCode           string    `json:"StatusCode"`
	StatusMessage        string    `json:"StatusMessage,omitempty"`
	Progress             int32     `json:"Progress"`
}

// TagFilter filters tags by resource type, resource ID, or key/value pairs.
type TagFilter struct {
	Name   string
	Values []string
}

// InstanceDetails holds detailed information about an EC2 instance in an ASG.
type InstanceDetails struct {
	AutoScalingGroupName    string `json:"AutoScalingGroupName"`
	InstanceID              string `json:"InstanceId"`
	AvailabilityZone        string `json:"AvailabilityZone"`
	LifecycleState          string `json:"LifecycleState"`
	HealthStatus            string `json:"HealthStatus"`
	LaunchConfigurationName string `json:"LaunchConfigurationName,omitempty"`
	InstanceType            string `json:"InstanceType,omitempty"`
	ProtectedFromScaleIn    bool   `json:"ProtectedFromScaleIn"`
}

// CreateAutoScalingGroupInput holds the input for CreateAutoScalingGroup.
type CreateAutoScalingGroupInput struct {
	MixedInstancesPolicy             *MixedInstancesPolicy
	LaunchTemplate                   *LaunchTemplateSpecification
	AvailabilityZoneDistribution     *AvailabilityZoneDistribution
	AvailabilityZoneImpairmentPolicy *AvailabilityZoneImpairmentPolicy
	CapacityReservationSpecification *CapacityReservationSpecification
	InstanceLifecyclePolicy          *InstanceLifecyclePolicy
	InstanceMaintenancePolicy        *InstanceMaintenancePolicy
	AutoScalingGroupName             string
	LaunchConfigurationName          string
	HealthCheckType                  string
	VPCZoneIdentifier                string
	PlacementGroup                   string
	Context                          string
	DesiredCapacityType              string
	DeletionProtection               string
	LoadBalancerNames                []string
	TargetGroupARNs                  []string
	TerminationPolicies              []string
	Tags                             []Tag
	AvailabilityZones                []string
	TrafficSources                   []TrafficSource
	LifecycleHookSpecificationList   []LifecycleHook
	MinSize                          int32
	DefaultCooldown                  int32
	HealthCheckGracePeriod           int32
	MaxInstanceLifetime              int32
	DefaultInstanceWarmup            int32
	MaxSize                          int32
	DesiredCapacity                  int32
	NewInstancesProtectedFromScaleIn bool
	CapacityRebalance                bool
	SkipZonalShiftValidation         bool
}

// UpdateAutoScalingGroupInput holds the input for UpdateAutoScalingGroup.
type UpdateAutoScalingGroupInput struct {
	LaunchTemplate                   *LaunchTemplateSpecification
	MaxSize                          *int32
	DesiredCapacity                  *int32
	DefaultCooldown                  *int32
	HealthCheckGracePeriod           *int32
	MaxInstanceLifetime              *int32
	DefaultInstanceWarmup            *int32
	NewInstancesProtectedFromScaleIn *bool
	CapacityRebalance                *bool
	SkipZonalShiftValidation         *bool
	MixedInstancesPolicy             *MixedInstancesPolicy
	AvailabilityZoneDistribution     *AvailabilityZoneDistribution
	AvailabilityZoneImpairmentPolicy *AvailabilityZoneImpairmentPolicy
	CapacityReservationSpecification *CapacityReservationSpecification
	InstanceLifecyclePolicy          *InstanceLifecyclePolicy
	InstanceMaintenancePolicy        *InstanceMaintenancePolicy
	MinSize                          *int32
	LaunchConfigurationName          string
	VPCZoneIdentifier                string
	PlacementGroup                   string
	Context                          string
	DesiredCapacityType              string
	HealthCheckType                  string
	AutoScalingGroupName             string
	DeletionProtection               string
	AvailabilityZones                []string
	TerminationPolicies              []string
}

// CreateLaunchConfigurationInput holds the input for CreateLaunchConfiguration.
type CreateLaunchConfigurationInput struct {
	LaunchConfigurationName      string
	ImageID                      string
	InstanceType                 string
	KeyName                      string
	IAMInstanceProfile           string
	UserData                     string
	KernelID                     string
	RamdiskID                    string
	SpotPrice                    string
	PlacementTenancy             string
	ClassicLinkVPCID             string
	SecurityGroups               []string
	ClassicLinkVPCSecurityGroups []string
	BlockDeviceMappings          []BlockDeviceMapping
	AssociatePublicIPAddress     bool
	EbsOptimized                 bool
	InstanceMonitoring           bool
}

// StartInstanceRefreshInput holds the input for StartInstanceRefresh.
type StartInstanceRefreshInput struct {
	AutoScalingGroupName string
	Strategy             string
	Preferences          InstanceRefreshPreferences
}
