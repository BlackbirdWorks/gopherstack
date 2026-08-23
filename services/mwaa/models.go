package mwaa

import "time"

// epochSecondsNow returns the current time as Unix epoch seconds (float64).
// The AWS SDK v2 deserializes MWAA timestamps as JSON numbers (__timestampUnix).
func epochSecondsNow() float64 {
	return float64(time.Now().Unix())
}

// Environment represents an MWAA environment.
type Environment struct {
	// region is the AWS region this environment belongs to. It is the outer
	// half of the composite key ("region|Name") used by the backend's flat
	// store.Table[Environment] (see regionKey in backend.go), which replaces
	// the old map[string]map[string]*Environment nesting (outer key =
	// region). Unexported so it never appears in MWAA wire responses (those
	// are built by marshaling Environment directly, but this field carries
	// no json tag and so is skipped by encoding/json regardless), but
	// persistence.go must carry it through a DTO explicitly since
	// json.Marshal never sees unexported fields.
	region                       string
	Tags                         map[string]string     `json:"Tags,omitempty"`
	NetworkConfiguration         *NetworkConfig        `json:"NetworkConfiguration,omitempty"`
	AirflowConfigurationOptions  map[string]string     `json:"AirflowConfigurationOptions,omitempty"`
	LoggingConfiguration         *LoggingConfiguration `json:"LoggingConfiguration,omitempty"`
	LastUpdate                   *LastUpdate           `json:"LastUpdate,omitempty"`
	Name                         string                `json:"Name"`
	AirflowVersion               string                `json:"AirflowVersion"`
	ExecutionRoleArn             string                `json:"ExecutionRoleArn"`
	SourceBucketArn              string                `json:"SourceBucketArn"`
	EnvironmentClass             string                `json:"EnvironmentClass"`
	WebserverURL                 string                `json:"WebserverUrl"`
	WebserverAccessMode          string                `json:"WebserverAccessMode"`
	DagS3Path                    string                `json:"DagS3Path"`
	Status                       string                `json:"Status"`
	ARN                          string                `json:"Arn"`
	KmsKey                       string                `json:"KmsKey,omitempty"`
	PluginsS3Path                string                `json:"PluginsS3Path,omitempty"`
	PluginsS3ObjectVersion       string                `json:"PluginsS3ObjectVersion,omitempty"`
	RequirementsS3Path           string                `json:"RequirementsS3Path,omitempty"`
	RequirementsS3ObjectVersion  string                `json:"RequirementsS3ObjectVersion,omitempty"`
	StartupScriptS3Path          string                `json:"StartupScriptS3Path,omitempty"`
	StartupScriptS3ObjectVersion string                `json:"StartupScriptS3ObjectVersion,omitempty"`
	EndpointManagement           string                `json:"EndpointManagement,omitempty"`
	ServiceRoleArn               string                `json:"ServiceRoleArn,omitempty"`
	CeleryExecutorQueue          string                `json:"CeleryExecutorQueue,omitempty"`
	DatabaseVpcEndpointService   string                `json:"DatabaseVpcEndpointService,omitempty"`
	WebserverVpcEndpointService  string                `json:"WebserverVpcEndpointService,omitempty"`
	WeeklyMaintenanceWindowStart string                `json:"WeeklyMaintenanceWindowStart,omitempty"`
	CreatedAt                    float64               `json:"CreatedAt"`
	MaxWorkers                   int32                 `json:"MaxWorkers"`
	MinWorkers                   int32                 `json:"MinWorkers"`
	MaxWebservers                int32                 `json:"MaxWebservers,omitempty"`
	MinWebservers                int32                 `json:"MinWebservers,omitempty"`
	Schedulers                   int32                 `json:"Schedulers,omitempty"`
}

// LoggingConfiguration aggregates the five Airflow module logging configs.
type LoggingConfiguration struct {
	DagProcessingLogs *ModuleLoggingConfiguration `json:"DagProcessingLogs,omitempty"`
	SchedulerLogs     *ModuleLoggingConfiguration `json:"SchedulerLogs,omitempty"`
	TaskLogs          *ModuleLoggingConfiguration `json:"TaskLogs,omitempty"`
	WebserverLogs     *ModuleLoggingConfiguration `json:"WebserverLogs,omitempty"`
	WorkerLogs        *ModuleLoggingConfiguration `json:"WorkerLogs,omitempty"`
}

// LoggingConfigurationInput is the wire shape CreateEnvironment/UpdateEnvironment
// requests use for LoggingConfiguration. Unlike the response's
// LoggingConfiguration, AWS's LoggingConfigurationInput/ModuleLoggingConfigurationInput
// has no CloudWatchLogGroupArn member -- that ARN is server-computed once a
// module's logs are enabled, never client-supplied. A prior version of this
// file reused the response type for request decode too, so a request body
// setting CloudWatchLogGroupArn was accepted and echoed straight back as if
// AWS-generated, even though no conformant SDK client (built from
// LoggingConfigurationInput) can ever send that field.
type LoggingConfigurationInput struct {
	DagProcessingLogs *ModuleLoggingConfigurationInput `json:"DagProcessingLogs,omitempty"`
	SchedulerLogs     *ModuleLoggingConfigurationInput `json:"SchedulerLogs,omitempty"`
	TaskLogs          *ModuleLoggingConfigurationInput `json:"TaskLogs,omitempty"`
	WebserverLogs     *ModuleLoggingConfigurationInput `json:"WebserverLogs,omitempty"`
	WorkerLogs        *ModuleLoggingConfigurationInput `json:"WorkerLogs,omitempty"`
}

// ModuleLoggingConfigurationInput is a single Airflow module logging request;
// no CloudWatchLogGroupArn (see LoggingConfigurationInput).
type ModuleLoggingConfigurationInput struct {
	Enabled  *bool  `json:"Enabled,omitempty"`
	LogLevel string `json:"LogLevel,omitempty"`
}

// ModuleLoggingConfiguration is a single Airflow module logging configuration.
type ModuleLoggingConfiguration struct {
	Enabled               *bool  `json:"Enabled,omitempty"`
	LogLevel              string `json:"LogLevel,omitempty"`
	CloudWatchLogGroupArn string `json:"CloudWatchLogGroupArn,omitempty"`
}

// LastUpdate captures the result of the most recent environment update.
type LastUpdate struct {
	Error                     *UpdateError `json:"Error,omitempty"`
	Status                    string       `json:"Status,omitempty"`
	Source                    string       `json:"Source,omitempty"`
	WorkerReplacementStrategy string       `json:"WorkerReplacementStrategy,omitempty"`
	CreatedAt                 float64      `json:"CreatedAt,omitempty"`
}

// UpdateError describes a failed update attempt.
type UpdateError struct {
	ErrorCode    string `json:"ErrorCode,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

// NetworkConfig holds the VPC networking configuration.
type NetworkConfig struct {
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
	SubnetIDs        []string `json:"SubnetIds"`
}

// UpdateNetworkConfig is the network configuration shape accepted by
// UpdateEnvironment. Unlike NetworkConfig (used by CreateEnvironment and
// returned by GetEnvironment), AWS's UpdateNetworkConfigurationInput shape
// has NO SubnetIds member -- subnets cannot be changed after an environment
// is created, only SecurityGroupIds can.
type UpdateNetworkConfig struct {
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

// createEnvironmentRequest is the request body for creating an MWAA environment.
// Deliberately has no WorkerReplacementStrategy field: AWS's
// CreateEnvironmentInput has no such member at all (confirmed against
// aws-sdk-go-v2/service/mwaa's validateOpCreateEnvironmentInput / struct
// definition) -- it exists only on UpdateEnvironmentInput, where it controls
// how already-running workers are replaced during an update. A prior version
// of this file fabricated the field on both Create and the Environment
// response shape; see updateEnvironmentRequest and LastUpdate for the real
// (Update-only) member.
type createEnvironmentRequest struct {
	NetworkConfiguration         *NetworkConfig             `json:"NetworkConfiguration"`
	Tags                         map[string]string          `json:"Tags"`
	AirflowConfigurationOptions  map[string]string          `json:"AirflowConfigurationOptions"`
	LoggingConfiguration         *LoggingConfigurationInput `json:"LoggingConfiguration"`
	DagS3Path                    string                     `json:"DagS3Path"`
	ExecutionRoleArn             string                     `json:"ExecutionRoleArn"`
	SourceBucketArn              string                     `json:"SourceBucketArn"`
	AirflowVersion               string                     `json:"AirflowVersion"`
	EnvironmentClass             string                     `json:"EnvironmentClass"`
	WebserverAccessMode          string                     `json:"WebserverAccessMode"`
	KmsKey                       string                     `json:"KmsKey"`
	PluginsS3Path                string                     `json:"PluginsS3Path"`
	PluginsS3ObjectVersion       string                     `json:"PluginsS3ObjectVersion"`
	RequirementsS3Path           string                     `json:"RequirementsS3Path"`
	RequirementsS3ObjectVersion  string                     `json:"RequirementsS3ObjectVersion"`
	StartupScriptS3Path          string                     `json:"StartupScriptS3Path"`
	StartupScriptS3ObjectVersion string                     `json:"StartupScriptS3ObjectVersion"`
	EndpointManagement           string                     `json:"EndpointManagement"`
	WeeklyMaintenanceWindowStart string                     `json:"WeeklyMaintenanceWindowStart"`
	MaxWorkers                   int32                      `json:"MaxWorkers"`
	MinWorkers                   int32                      `json:"MinWorkers"`
	MaxWebservers                int32                      `json:"MaxWebservers"`
	MinWebservers                int32                      `json:"MinWebservers"`
	Schedulers                   int32                      `json:"Schedulers"`
}

// updateEnvironmentRequest is the request body for updating an MWAA environment.
type updateEnvironmentRequest struct {
	NetworkConfiguration         *UpdateNetworkConfig       `json:"NetworkConfiguration"`
	AirflowConfigurationOptions  map[string]string          `json:"AirflowConfigurationOptions"`
	LoggingConfiguration         *LoggingConfigurationInput `json:"LoggingConfiguration"`
	DagS3Path                    string                     `json:"DagS3Path"`
	ExecutionRoleArn             string                     `json:"ExecutionRoleArn"`
	SourceBucketArn              string                     `json:"SourceBucketArn"`
	AirflowVersion               string                     `json:"AirflowVersion"`
	EnvironmentClass             string                     `json:"EnvironmentClass"`
	WebserverAccessMode          string                     `json:"WebserverAccessMode"`
	PluginsS3Path                string                     `json:"PluginsS3Path"`
	PluginsS3ObjectVersion       string                     `json:"PluginsS3ObjectVersion"`
	RequirementsS3Path           string                     `json:"RequirementsS3Path"`
	RequirementsS3ObjectVersion  string                     `json:"RequirementsS3ObjectVersion"`
	StartupScriptS3Path          string                     `json:"StartupScriptS3Path"`
	StartupScriptS3ObjectVersion string                     `json:"StartupScriptS3ObjectVersion"`
	WeeklyMaintenanceWindowStart string                     `json:"WeeklyMaintenanceWindowStart"`
	WorkerReplacementStrategy    string                     `json:"WorkerReplacementStrategy"`
	MaxWorkers                   int32                      `json:"MaxWorkers"`
	MinWorkers                   int32                      `json:"MinWorkers"`
	MaxWebservers                int32                      `json:"MaxWebservers"`
	MinWebservers                int32                      `json:"MinWebservers"`
	Schedulers                   int32                      `json:"Schedulers"`
}

// invokeRestAPIRequest is the request body for InvokeRestAPI.
type invokeRestAPIRequest struct {
	Body            any    `json:"Body"`
	QueryParameters any    `json:"QueryParameters"`
	Method          string `json:"Method"`
	Path            string `json:"Path"`
}

// InvokeRestAPIResponse is the response body for InvokeRestAPI.
type InvokeRestAPIResponse struct {
	RestAPIResponse   any   `json:"RestApiResponse"`
	RestAPIStatusCode int32 `json:"RestApiStatusCode"`
}

// Dimension represents an internal MWAA metric dimension.
type Dimension struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// StatisticSet represents the statistical values for a metric.
type StatisticSet struct {
	Maximum     *float64 `json:"Maximum,omitempty"`
	Minimum     *float64 `json:"Minimum,omitempty"`
	SampleCount *int32   `json:"SampleCount,omitempty"`
	Sum         *float64 `json:"Sum,omitempty"`
}

// MetricDatum represents a single metric data point for PublishMetrics.
type MetricDatum struct {
	StatisticValues *StatisticSet `json:"StatisticValues,omitempty"`
	Timestamp       *float64      `json:"Timestamp,omitempty"`
	Value           *float64      `json:"Value,omitempty"`
	MetricName      string        `json:"MetricName"`
	Unit            string        `json:"Unit,omitempty"`
	Dimensions      []Dimension   `json:"Dimensions,omitempty"`
}

// publishMetricsRequest is the request body for PublishMetrics.
type publishMetricsRequest struct {
	MetricData []MetricDatum `json:"MetricData"`
}
