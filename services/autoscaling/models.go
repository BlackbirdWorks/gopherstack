package autoscaling

import "time"

// ScalingPolicy represents a scaling policy for an Auto Scaling group.
type ScalingPolicy struct {
	PolicyName           string  `json:"PolicyName"`
	PolicyARN            string  `json:"PolicyARN"`
	AutoScalingGroupName string  `json:"AutoScalingGroupName"`
	PolicyType           string  `json:"PolicyType,omitempty"`
	AdjustmentType       string  `json:"AdjustmentType,omitempty"`
	MetricType           string  `json:"MetricType,omitempty"` // predefined metric type
	CustomMetricSpec     string  `json:"CustomMetricSpec,omitempty"`
	TargetValue          float64 `json:"TargetValue,omitempty"`
	ScalingAdjustment    int32   `json:"ScalingAdjustment,omitempty"`
	MinAdjustmentStep    int32   `json:"MinAdjustmentStep,omitempty"`
	Cooldown             int32   `json:"Cooldown,omitempty"`
	EstimatedWarmup      int32   `json:"EstimatedWarmup,omitempty"`
	DisableScaleIn       bool    `json:"DisableScaleIn,omitempty"`
}

// ScalingPolicyInput holds the input for PutScalingPolicy.
type ScalingPolicyInput struct {
	PolicyName           string
	AutoScalingGroupName string
	PolicyType           string
	AdjustmentType       string
	MetricType           string
	TargetValue          float64
	ScalingAdjustment    int32
	MinAdjustmentStep    int32
	Cooldown             int32
	EstimatedWarmup      int32
	DisableScaleIn       bool
}

// NotificationConfiguration represents a notification configuration for an Auto Scaling group.
type NotificationConfiguration struct {
	AutoScalingGroupName string `json:"AutoScalingGroupName"`
	TopicARN             string `json:"TopicARN"`
	NotificationType     string `json:"NotificationType"`
}

// WarmPool represents a warm pool configuration for an Auto Scaling group.
type WarmPool struct {
	AutoScalingGroupName     string `json:"AutoScalingGroupName"`
	PoolState                string `json:"PoolState,omitempty"`
	Status                   string `json:"Status,omitempty"`
	MinSize                  int32  `json:"MinSize,omitempty"`
	MaxGroupPreparedCapacity int32  `json:"MaxGroupPreparedCapacity,omitempty"`
}

// WarmPoolInput holds the input for PutWarmPool.
type WarmPoolInput struct {
	AutoScalingGroupName     string
	PoolState                string
	MinSize                  int32
	MaxGroupPreparedCapacity int32
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
type ExecutePolicyInput struct {
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

// pendingHookAction tracks an in-flight lifecycle action with its timer.
//
//nolint:govet // fieldalignment: logical grouping prioritized over size optimization
type pendingHookAction struct {
	Token         string
	GroupName     string
	HookName      string
	InstanceID    string
	DefaultResult string
	timeout       time.Duration
	timer         *time.Timer
}

// AutoScalingGroup represents an EC2 Auto Scaling group.
//
//nolint:revive // AutoScalingGroup is the canonical AWS type name; renaming to Group would break convention.
type AutoScalingGroup struct {
	CreatedTime             time.Time       `json:"CreatedTime"`
	LastScalingActivity     time.Time       `json:"LastScalingActivity,omitzero"`
	AutoScalingGroupName    string          `json:"AutoScalingGroupName"`
	Status                  string          `json:"Status,omitempty"`
	HealthCheckType         string          `json:"HealthCheckType"`
	LaunchConfigurationName string          `json:"LaunchConfigurationName,omitempty"`
	AutoScalingGroupARN     string          `json:"AutoScalingGroupARN"`
	LoadBalancerNames       []string        `json:"LoadBalancerNames,omitempty"`
	TargetGroupARNs         []string        `json:"TargetGroupARNs,omitempty"`
	TrafficSources          []TrafficSource `json:"TrafficSources,omitempty"`
	AvailabilityZones       []string        `json:"AvailabilityZones,omitempty"`
	Instances               []Instance      `json:"Instances,omitempty"`
	Tags                    []Tag           `json:"Tags,omitempty"`
	SuspendedProcesses      []string        `json:"SuspendedProcesses,omitempty"`
	EnabledMetrics          []string        `json:"EnabledMetrics,omitempty"`
	MinSize                 int32           `json:"MinSize"`
	MaxSize                 int32           `json:"MaxSize"`
	DesiredCapacity         int32           `json:"DesiredCapacity"`
	DefaultCooldown         int32           `json:"DefaultCooldown"`
	HealthCheckGracePeriod  int32           `json:"HealthCheckGracePeriod"`
}

// LaunchConfiguration represents an Auto Scaling launch configuration.
type LaunchConfiguration struct {
	CreatedTime             time.Time            `json:"CreatedTime"`
	LaunchConfigurationName string               `json:"LaunchConfigurationName"`
	LaunchConfigurationARN  string               `json:"LaunchConfigurationARN"`
	ImageID                 string               `json:"ImageID"`
	InstanceType            string               `json:"InstanceType"`
	KeyName                 string               `json:"KeyName,omitempty"`
	IAMInstanceProfile      string               `json:"IAMInstanceProfile,omitempty"`
	UserData                string               `json:"UserData,omitempty"`
	KernelID                string               `json:"KernelID,omitempty"`
	RamdiskID               string               `json:"RamdiskID,omitempty"`
	BlockDeviceMappings     []BlockDeviceMapping `json:"BlockDeviceMappings,omitempty"`
	SecurityGroups          []string             `json:"SecurityGroups,omitempty"`
}

// BlockDeviceMapping represents an EBS or ephemeral block device mapping.
type BlockDeviceMapping struct {
	VirtualName string `json:"VirtualName,omitempty"`
	DeviceName  string `json:"DeviceName"`
}

// Instance represents an EC2 instance in an Auto Scaling group.
type Instance struct {
	InstanceID              string `json:"InstanceID"`
	AvailabilityZone        string `json:"AvailabilityZone"`
	LifecycleState          string `json:"LifecycleState"`
	HealthStatus            string `json:"HealthStatus"`
	LaunchConfigurationName string `json:"LaunchConfigurationName,omitempty"`
	InstanceType            string `json:"InstanceType,omitempty"`
	ProtectedFromScaleIn    bool   `json:"ProtectedFromScaleIn,omitempty"`
}

// Tag is a key/value pair attached to a resource.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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
	StartTime            time.Time `json:"StartTime,omitzero"`
	EndTime              time.Time `json:"EndTime,omitzero"`
	DesiredCapacity      *int32    `json:"DesiredCapacity,omitempty"`
	MinSize              *int32    `json:"MinSize,omitempty"`
	MaxSize              *int32    `json:"MaxSize,omitempty"`
	ScheduledActionName  string    `json:"ScheduledActionName"`
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

// InstanceRefresh represents an instance refresh operation for an Auto Scaling group.
type InstanceRefresh struct {
	StartTime            time.Time `json:"StartTime"`
	EndTime              time.Time `json:"EndTime,omitzero"`
	InstanceRefreshID    string    `json:"InstanceRefreshId"`
	AutoScalingGroupName string    `json:"AutoScalingGroupName"`
	Status               string    `json:"Status"`
	// Strategy is the instance refresh strategy; defaults to "Rolling".
	Strategy string `json:"Strategy,omitempty"`
	// MinHealthyPercentage is the minimum healthy percentage during refresh; defaults to 90.
	MinHealthyPercentage int32 `json:"MinHealthyPercentage,omitempty"`
}

// LifecycleHook represents a lifecycle hook attached to an Auto Scaling group.
type LifecycleHook struct {
	LifecycleHookName     string `json:"LifecycleHookName"`
	AutoScalingGroupName  string `json:"AutoScalingGroupName"`
	LifecycleTransition   string `json:"LifecycleTransition,omitempty"`
	DefaultResult         string `json:"DefaultResult,omitempty"`
	NotificationTargetARN string `json:"NotificationTargetARN,omitempty"`
	RoleARN               string `json:"RoleARN,omitempty"`
	HeartbeatTimeout      int32  `json:"HeartbeatTimeout,omitempty"`
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
