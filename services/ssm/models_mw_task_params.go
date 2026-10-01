package ssm

import (
	"fmt"
	"maps"
)

// MaintenanceWindowLoggingInfo mirrors types.LoggingInfo (ssm@v1.77.0 types.go:3521).
type MaintenanceWindowLoggingInfo struct {
	S3BucketName string `json:"S3BucketName"`
	S3Region     string `json:"S3Region"`
	S3KeyPrefix  string `json:"S3KeyPrefix,omitempty"`
}

// MaintenanceWindowTaskParameterValue mirrors types.MaintenanceWindowTaskParameterValueExpression (types.go:4068).
type MaintenanceWindowTaskParameterValue struct {
	Values []string `json:"Values,omitempty"`
}

// MaintenanceWindowTaskInvocationParameters mirrors types.MaintenanceWindowTaskInvocationParameters (types.go:4050).
type MaintenanceWindowTaskInvocationParameters struct {
	Automation    *MaintenanceWindowAutomationParameters    `json:"Automation,omitempty"`
	Lambda        *MaintenanceWindowLambdaParameters        `json:"Lambda,omitempty"`
	RunCommand    *MaintenanceWindowRunCommandParameters    `json:"RunCommand,omitempty"`
	StepFunctions *MaintenanceWindowStepFunctionsParameters `json:"StepFunctions,omitempty"`
}

// MaintenanceWindowAutomationParameters mirrors types.MaintenanceWindowAutomationParameters (types.go:3540).
type MaintenanceWindowAutomationParameters struct {
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
}

// MaintenanceWindowLambdaParameters mirrors types.MaintenanceWindowLambdaParameters (types.go:3780).
type MaintenanceWindowLambdaParameters struct {
	ClientContext string `json:"ClientContext,omitempty"`
	Qualifier     string `json:"Qualifier,omitempty"`
	Payload       []byte `json:"Payload,omitempty"`
}

// MaintenanceWindowStepFunctionsParameters mirrors types.MaintenanceWindowStepFunctionsParameters (types.go:3897).
type MaintenanceWindowStepFunctionsParameters struct {
	Input string `json:"Input,omitempty"`
	Name  string `json:"Name,omitempty"`
}

// MaintenanceWindowRunCommandParameters mirrors types.MaintenanceWindowRunCommandParameters (types.go:3817).
type MaintenanceWindowRunCommandParameters struct {
	CloudWatchOutputConfig *MaintenanceWindowCloudWatchOutputConfig `json:"CloudWatchOutputConfig,omitempty"`
	NotificationConfig     *MaintenanceWindowNotificationConfig     `json:"NotificationConfig,omitempty"`
	Parameters             map[string][]string                      `json:"Parameters,omitempty"`
	TimeoutSeconds         *int32                                   `json:"TimeoutSeconds,omitempty"`
	Comment                string                                   `json:"Comment,omitempty"`
	DocumentHash           string                                   `json:"DocumentHash,omitempty"`
	DocumentHashType       string                                   `json:"DocumentHashType,omitempty"`
	DocumentVersion        string                                   `json:"DocumentVersion,omitempty"`
	OutputS3BucketName     string                                   `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix      string                                   `json:"OutputS3KeyPrefix,omitempty"`
	ServiceRoleArn         string                                   `json:"ServiceRoleArn,omitempty"`
}

// MaintenanceWindowCloudWatchOutputConfig mirrors types.CloudWatchOutputConfig (types.go:1171).
type MaintenanceWindowCloudWatchOutputConfig struct {
	CloudWatchLogGroupName  string `json:"CloudWatchLogGroupName,omitempty"`
	CloudWatchOutputEnabled bool   `json:"CloudWatchOutputEnabled,omitempty"`
}

// MaintenanceWindowNotificationConfig mirrors types.NotificationConfig (types.go:4205).
type MaintenanceWindowNotificationConfig struct {
	NotificationArn    string   `json:"NotificationArn,omitempty"`
	NotificationType   string   `json:"NotificationType,omitempty"`
	NotificationEvents []string `json:"NotificationEvents,omitempty"`
}

func cloneTaskParameters(
	in map[string]MaintenanceWindowTaskParameterValue,
) map[string]MaintenanceWindowTaskParameterValue {
	if in == nil {
		return nil
	}

	out := make(map[string]MaintenanceWindowTaskParameterValue, len(in))
	for k, v := range in {
		out[k] = MaintenanceWindowTaskParameterValue{Values: append([]string(nil), v.Values...)}
	}

	return out
}

func cloneLoggingInfo(in *MaintenanceWindowLoggingInfo) *MaintenanceWindowLoggingInfo {
	if in == nil {
		return nil
	}

	c := *in

	return &c
}

func cloneTaskInvocationParameters(
	in *MaintenanceWindowTaskInvocationParameters,
) *MaintenanceWindowTaskInvocationParameters {
	if in == nil {
		return nil
	}

	out := &MaintenanceWindowTaskInvocationParameters{}

	if a := in.Automation; a != nil {
		c := *a
		c.Parameters = maps.Clone(a.Parameters)
		out.Automation = &c
	}

	if l := in.Lambda; l != nil {
		c := *l
		c.Payload = append([]byte(nil), l.Payload...)
		out.Lambda = &c
	}

	if s := in.StepFunctions; s != nil {
		c := *s
		out.StepFunctions = &c
	}

	if r := in.RunCommand; r != nil {
		c := *r
		c.Parameters = maps.Clone(r.Parameters)

		if r.TimeoutSeconds != nil {
			t := *r.TimeoutSeconds
			c.TimeoutSeconds = &t
		}

		if r.CloudWatchOutputConfig != nil {
			w := *r.CloudWatchOutputConfig
			c.CloudWatchOutputConfig = &w
		}

		if r.NotificationConfig != nil {
			n := *r.NotificationConfig
			n.NotificationEvents = append([]string(nil), r.NotificationConfig.NotificationEvents...)
			c.NotificationConfig = &n
		}

		out.RunCommand = &c
	}

	return out
}

func validateLoggingInfo(l *MaintenanceWindowLoggingInfo) error {
	if l != nil && (l.S3BucketName == "" || l.S3Region == "") {
		return fmt.Errorf("%w: LoggingInfo.S3BucketName and S3Region are required", ErrValidationException)
	}

	return nil
}
