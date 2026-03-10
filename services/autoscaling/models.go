package autoscaling

import "time"

// AutoScalingGroup represents an EC2 Auto Scaling group.
//
//nolint:revive // AutoScalingGroup is the canonical AWS type name; renaming to Group would break convention.
type AutoScalingGroup struct {
	CreatedTime             time.Time  `json:"CreatedTime"`
	AutoScalingGroupName    string     `json:"AutoScalingGroupName"`
	Status                  string     `json:"Status,omitempty"`
	HealthCheckType         string     `json:"HealthCheckType"`
	LaunchConfigurationName string     `json:"LaunchConfigurationName,omitempty"`
	AutoScalingGroupARN     string     `json:"AutoScalingGroupARN"`
	LoadBalancerNames       []string   `json:"LoadBalancerNames,omitempty"`
	TargetGroupARNs         []string   `json:"TargetGroupARNs,omitempty"`
	AvailabilityZones       []string   `json:"AvailabilityZones,omitempty"`
	Instances               []Instance `json:"Instances,omitempty"`
	Tags                    []Tag      `json:"Tags,omitempty"`
	MinSize                 int32      `json:"MinSize"`
	MaxSize                 int32      `json:"MaxSize"`
	DesiredCapacity         int32      `json:"DesiredCapacity"`
	DefaultCooldown         int32      `json:"DefaultCooldown"`
	HealthCheckGracePeriod  int32      `json:"HealthCheckGracePeriod"`
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
