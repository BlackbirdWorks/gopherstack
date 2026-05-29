package ecs

// RuntimePlatform specifies the CPU architecture and OS family for a task definition.
type RuntimePlatform struct {
	CpuArchitecture       string `json:"cpuArchitecture,omitempty"`
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

// validAccountSettingNames is the set of account setting names AWS ECS recognizes.
var validAccountSettingNames = map[string]bool{
	"containerInstanceLongArnFormat":   true,
	"serviceLongArnFormat":             true,
	"taskLongArnFormat":                true,
	"awsvpcTrunking":                   true,
	"containerInsights":                true,
	"dualStackIPv6":                    true,
	"fargateTaskRetirementWaitPeriod":  true,
	"tagResourceAuthorization":         true,
	"guardDutyActivate":                true,
	"fargateFIPSMode":                  true,
	"fargateEphemeralStorageKMSKey":    true,
	"defaultLogDriverMode":             true,
}

// builtinCapacityProviders returns a synthesized CapacityProvider for FARGATE or
// FARGATE_SPOT, which are managed by AWS and do not require explicit creation.
func builtinCapacityProvider(name string) *CapacityProvider {
	switch name {
	case "FARGATE", "FARGATE_SPOT":
		return &CapacityProvider{
			Name:   name,
			Status: statusActive,
			CapacityProviderArn: "arn:aws:ecs:::capacity-provider/" + name,
		}
	default:
		return nil
	}
}
