package autoscaling

import "time"

// AutoScalingGroup represents an EC2 Auto Scaling group.
//
//nolint:revive // AutoScalingGroup is the canonical AWS type name; renaming to Group would break convention.
type AutoScalingGroup struct {
	CreatedTime             time.Time       `json:"CreatedTime"`
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
