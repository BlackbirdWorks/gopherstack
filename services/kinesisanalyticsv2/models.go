package kinesisanalyticsv2

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// CloudWatchLoggingOptionDesc describes a CloudWatch logging option.
type CloudWatchLoggingOptionDesc struct {
	CloudWatchLoggingOptionID string `json:"CloudWatchLoggingOptionId"`
	LogStreamARN              string `json:"LogStreamARN"`
	RoleARN                   string `json:"RoleARN,omitempty"`
}

// LambdaProcessorDesc describes a Lambda input processor.
type LambdaProcessorDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// InputProcessingConfigurationDesc describes an input processing configuration.
type InputProcessingConfigurationDesc struct {
	InputLambdaProcessor *LambdaProcessorDesc `json:"InputLambdaProcessor,omitempty"`
}

// KinesisStreamsInputDesc describes a Kinesis Streams input.
type KinesisStreamsInputDesc struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN,omitempty"`
}

// KinesisFirehoseInputDesc describes a Kinesis Firehose input.
type KinesisFirehoseInputDesc struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN,omitempty"`
}

// InputDescription describes an application input configuration.
type InputDescription struct {
	InputProcessingConfigurationDescription *InputProcessingConfigurationDesc `json:"InputProcessingConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	KinesisStreamsInputDescription          *KinesisStreamsInputDesc          `json:"KinesisStreamsInputDescription,omitempty"`          //nolint:lll // AWS API name
	KinesisFirehoseInputDescription         *KinesisFirehoseInputDesc         `json:"KinesisFirehoseInputDescription,omitempty"`         //nolint:lll // AWS API name
	InputID                                 string                            `json:"InputId"`
	NamePrefix                              string                            `json:"NamePrefix,omitempty"`
}

// KinesisStreamsOutputDesc describes a Kinesis Streams output.
type KinesisStreamsOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// KinesisFirehoseOutputDesc describes a Kinesis Firehose output.
type KinesisFirehoseOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// LambdaOutputDesc describes a Lambda output.
type LambdaOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// DestinationSchemaDesc describes the destination record format.
type DestinationSchemaDesc struct {
	RecordFormatType string `json:"RecordFormatType"`
}

// OutputDescription describes an application output configuration.
type OutputDescription struct {
	KinesisStreamsOutputDescription  *KinesisStreamsOutputDesc  `json:"KinesisStreamsOutputDescription,omitempty"`
	KinesisFirehoseOutputDescription *KinesisFirehoseOutputDesc `json:"KinesisFirehoseOutputDescription,omitempty"`
	LambdaOutputDescription          *LambdaOutputDesc          `json:"LambdaOutputDescription,omitempty"`
	DestinationSchema                *DestinationSchemaDesc     `json:"DestinationSchema,omitempty"`
	OutputID                         string                     `json:"OutputId"`
	Name                             string                     `json:"Name,omitempty"`
}

// S3ReferenceDataSourceDesc describes the S3 source for reference data.
type S3ReferenceDataSourceDesc struct {
	BucketARN string `json:"BucketARN"`
	FileKey   string `json:"FileKey"`
}

// ReferenceDataSourceDescription describes a reference data source.
type ReferenceDataSourceDescription struct {
	S3ReferenceDataSourceDescription *S3ReferenceDataSourceDesc `json:"S3ReferenceDataSourceDescription,omitempty"`
	ReferenceID                      string                     `json:"ReferenceId"`
	TableName                        string                     `json:"TableName,omitempty"`
}

// VpcConfigurationDescription describes a VPC configuration.
type VpcConfigurationDescription struct {
	VpcConfigurationID string   `json:"VpcConfigurationId"`
	VpcID              string   `json:"VpcId,omitempty"`
	SubnetIDs          []string `json:"SubnetIds"`
	SecurityGroupIDs   []string `json:"SecurityGroupIds"`
}

// S3CodeLocationDesc describes the S3 location of application code.
type S3CodeLocationDesc struct {
	BucketARN     string `json:"BucketARN"`
	FileKey       string `json:"FileKey"`
	ObjectVersion string `json:"ObjectVersion,omitempty"`
}

// CodeContentDescription describes the location and content of application code.
type CodeContentDescription struct {
	S3ApplicationCodeLocationDescription *S3CodeLocationDesc `json:"S3ApplicationCodeLocationDescription,omitempty"` //nolint:lll // AWS API name
	TextContent                          string              `json:"TextContent,omitempty"`
	CodeMD5                              string              `json:"CodeMD5,omitempty"`
	CodeSize                             int64               `json:"CodeSize,omitempty"`
}

// ApplicationCodeConfigDesc describes an application's code configuration.
type ApplicationCodeConfigDesc struct {
	CodeContentDescription *CodeContentDescription `json:"CodeContentDescription,omitempty"`
	CodeContentType        string                  `json:"CodeContentType"`
}

// CheckpointConfigDesc describes a Flink application's checkpointing configuration.
type CheckpointConfigDesc struct {
	CheckpointingEnabled       *bool  `json:"CheckpointingEnabled,omitempty"`
	CheckpointInterval         *int64 `json:"CheckpointInterval,omitempty"`
	MinPauseBetweenCheckpoints *int64 `json:"MinPauseBetweenCheckpoints,omitempty"`
	ConfigurationType          string `json:"ConfigurationType"`
}

// MonitoringConfigDesc describes a Flink application's CloudWatch logging configuration.
type MonitoringConfigDesc struct {
	ConfigurationType string `json:"ConfigurationType"`
	LogLevel          string `json:"LogLevel,omitempty"`
	MetricsLevel      string `json:"MetricsLevel,omitempty"`
}

// ParallelismConfigDesc describes a Flink application's parallelism configuration.
type ParallelismConfigDesc struct {
	AutoScalingEnabled *bool  `json:"AutoScalingEnabled,omitempty"`
	Parallelism        *int32 `json:"Parallelism,omitempty"`
	ParallelismPerKPU  *int32 `json:"ParallelismPerKPU,omitempty"`
	CurrentParallelism *int32 `json:"CurrentParallelism,omitempty"`
	ConfigurationType  string `json:"ConfigurationType"`
}

// FlinkApplicationConfigDesc describes a Flink application's runtime configuration.
type FlinkApplicationConfigDesc struct {
	CheckpointConfigurationDescription  *CheckpointConfigDesc  `json:"CheckpointConfigurationDescription,omitempty"`  //nolint:lll // AWS API name
	MonitoringConfigurationDescription  *MonitoringConfigDesc  `json:"MonitoringConfigurationDescription,omitempty"`  //nolint:lll // AWS API name
	ParallelismConfigurationDescription *ParallelismConfigDesc `json:"ParallelismConfigurationDescription,omitempty"` //nolint:lll // AWS API name
}

// PropertyGroup is a key-value execution property group (shared shape for
// both the request PropertyGroups and the response
// PropertyGroupDescriptions -- real AWS uses the identical PropertyGroup
// type on both sides).
type PropertyGroup struct {
	PropertyMap     map[string]string `json:"PropertyMap"`
	PropertyGroupID string            `json:"PropertyGroupId"`
}

// ApplicationSnapshotConfigDesc describes whether snapshots are enabled.
type ApplicationSnapshotConfigDesc struct {
	SnapshotsEnabled bool `json:"SnapshotsEnabled"`
}

// ApplicationSystemRollbackConfigDesc describes whether system rollback is enabled.
type ApplicationSystemRollbackConfigDesc struct {
	RollbackEnabled bool `json:"RollbackEnabled"`
}

// ApplicationEncryptionConfigDesc describes the encryption-at-rest configuration.
type ApplicationEncryptionConfigDesc struct {
	KeyType string `json:"KeyType"`
	KeyID   string `json:"KeyId,omitempty"`
}

// ApplicationRestoreConfig describes how a restarting application restores
// state (shared shape for RunConfiguration's request field and
// RunConfigurationDescription's response field -- real AWS uses the
// identical ApplicationRestoreConfiguration type on both sides).
type ApplicationRestoreConfig struct {
	ApplicationRestoreType string `json:"ApplicationRestoreType"`
	SnapshotName           string `json:"SnapshotName,omitempty"`
}

// FlinkRunConfig describes Flink-specific starting parameters (shared shape,
// same rationale as ApplicationRestoreConfig).
type FlinkRunConfig struct {
	AllowNonRestoredState *bool `json:"AllowNonRestoredState,omitempty"`
}

// RunConfigDesc describes an application's starting parameters.
type RunConfigDesc struct {
	ApplicationRestoreConfigurationDescription *ApplicationRestoreConfig `json:"ApplicationRestoreConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	FlinkRunConfigurationDescription           *FlinkRunConfig           `json:"FlinkRunConfigurationDescription,omitempty"`           //nolint:lll // AWS API name
}

const (
	// ApplicationStatusReady indicates a running application that is ready.
	ApplicationStatusReady = "READY"
	// ApplicationStatusRunning indicates a running application.
	ApplicationStatusRunning = "RUNNING"
	// ApplicationStatusDeleting indicates an application being deleted.
	ApplicationStatusDeleting = "DELETING"
)

// OperationStatusSuccessful is the real Kinesis Analytics v2 OperationStatus
// enum value ("SUCCESSFUL", not "SUCCESS") for a completed operation.
// gopherstack applies application-lifecycle operations
// (Start/Stop/UpdateApplication/RollbackApplication) synchronously, so every
// recorded operation goes straight to SUCCESSFUL -- there is no IN_PROGRESS
// window to observe via DescribeApplicationOperation/ListApplicationOperations.
const OperationStatusSuccessful = "SUCCESSFUL"

// ApplicationOperation represents a single KDA v2 application operation record.
type ApplicationOperation struct {
	StartTimestamp  time.Time `json:"-"`
	EndTimestamp    time.Time `json:"-"`
	OperationID     string    `json:"OperationId"`
	ApplicationName string    `json:"ApplicationName"`
	Operation       string    `json:"Operation"`
	OperationStatus string    `json:"OperationStatus"`
}

// ApplicationVersionSummary is a compact view of an application version.
type ApplicationVersionSummary struct {
	ApplicationStatus    string `json:"ApplicationStatus"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

// DiscoveredSchema holds the inferred schema from DiscoverInputSchema.
type DiscoveredSchema struct {
	RecordFormat       string     `json:"RecordFormat"`
	RecordEncoding     string     `json:"RecordEncoding,omitempty"`
	ParsedInputRecords [][]string `json:"ParsedInputRecords,omitempty"`
}

// Tag represents a key-value tag pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Application represents a Kinesis Data Analytics v2 application.
type Application struct {
	LastUpdateTimestamp               time.Time `json:"-"`
	CreatedAt                         time.Time `json:"-"`
	ApplicationVersionCreateTimestamp time.Time `json:"-"`
	RunConfig                         *RunConfigDesc
	EncryptionConfig                  *ApplicationEncryptionConfigDesc
	RollbackEnabled                   *bool
	SnapshotsEnabled                  *bool
	FlinkConfig                       *FlinkApplicationConfigDesc
	CodeConfig                        *ApplicationCodeConfigDesc
	ApplicationVersionRolledBackTo    *int64
	ApplicationVersionRolledBackFrom  *int64
	ApplicationVersionUpdatedFrom     *int64
	ApplicationMode                   string                        `json:"ApplicationMode,omitempty"`
	ApplicationStatus                 string                        `json:"ApplicationStatus"`
	ApplicationARN                    string                        `json:"ApplicationARN"`
	ApplicationName                   string                        `json:"ApplicationName"`
	RuntimeEnvironment                string                        `json:"RuntimeEnvironment"`
	ServiceExecutionRole              string                        `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription            string                        `json:"ApplicationDescription,omitempty"`
	Region                            string                        `json:"-"`
	MaintenanceWindowStartTime        string                        `json:"MaintenanceWindowStartTime,omitempty"`
	Tags                              []Tag                         `json:"-"`
	CloudWatchLoggingOptionDescs      []CloudWatchLoggingOptionDesc `json:"-"`
	EnvironmentPropertyGroups         []PropertyGroup
	InputDescriptions                 []InputDescription               `json:"-"`
	OutputDescriptions                []OutputDescription              `json:"-"`
	VpcConfigurationDescriptions      []VpcConfigurationDescription    `json:"-"`
	ReferenceDataSourceDescriptions   []ReferenceDataSourceDescription `json:"-"`
	ApplicationVersionID              int64                            `json:"ApplicationVersionId"`
}

// Snapshot represents an application snapshot.
type Snapshot struct {
	SnapshotCreation time.Time `json:"-"`
	ApplicationARN   string    `json:"ApplicationARN"`
	SnapshotName     string    `json:"SnapshotName"`
	SnapshotStatus   string    `json:"SnapshotStatus"`
	// Region and AppName are the owning region and application name, used
	// only to derive the store.Table composite key (region#appName#name) and
	// the byApp index -- SnapshotName alone is only unique within an
	// application. Never serialized on the wire: handler.go always builds a
	// dedicated snapshotDetail response DTO.
	Region             string `json:"-"`
	AppName            string `json:"-"`
	ApplicationVersion int64  `json:"ApplicationVersionId"`
}

// applicationSummary is a compact view of an application used in listings.
type applicationSummary struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationName      string `json:"ApplicationName"`
	ApplicationStatus    string `json:"ApplicationStatus"`
	RuntimeEnvironment   string `json:"RuntimeEnvironment"`
	ApplicationMode      string `json:"ApplicationMode,omitempty"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

// toSummary converts an Application to a summary.
func toSummary(app *Application) applicationSummary {
	return applicationSummary{
		ApplicationARN:       app.ApplicationARN,
		ApplicationName:      app.ApplicationName,
		ApplicationStatus:    app.ApplicationStatus,
		RuntimeEnvironment:   app.RuntimeEnvironment,
		ApplicationMode:      app.ApplicationMode,
		ApplicationVersionID: app.ApplicationVersionID,
	}
}

// snapshotDetail is the full snapshot view.
type snapshotDetail struct {
	ApplicationARN            string  `json:"ApplicationARN"`
	SnapshotName              string  `json:"SnapshotName"`
	SnapshotStatus            string  `json:"SnapshotStatus"`
	ApplicationVersion        int64   `json:"ApplicationVersionId"`
	SnapshotCreationTimestamp float64 `json:"SnapshotCreationTimestamp"`
}

// toSnapshotDetail converts a Snapshot to a snapshotDetail.
func toSnapshotDetail(s *Snapshot) snapshotDetail {
	return snapshotDetail{
		ApplicationARN:            s.ApplicationARN,
		SnapshotName:              s.SnapshotName,
		SnapshotStatus:            s.SnapshotStatus,
		ApplicationVersion:        s.ApplicationVersion,
		SnapshotCreationTimestamp: awstime.Epoch(s.SnapshotCreation),
	}
}

// cloneTags returns a copy of a tag slice. Always returns a non-nil slice.
func cloneTags(tags []Tag) []Tag {
	result := make([]Tag, len(tags))
	copy(result, tags)

	return result
}

// copyCWLOptions returns a deep copy of the CloudWatch logging option slice.
func copyCWLOptions(src []CloudWatchLoggingOptionDesc) []CloudWatchLoggingOptionDesc {
	out := make([]CloudWatchLoggingOptionDesc, len(src))
	copy(out, src)

	return out
}

// copyPropertyGroups returns a shallow copy of the property group slice
// (matching the copy* siblings' convention of not deep-cloning nested
// pointers/maps within each element -- see copyInputDescs).
func copyPropertyGroups(src []PropertyGroup) []PropertyGroup {
	out := make([]PropertyGroup, len(src))
	copy(out, src)

	return out
}

// copyInputDescs returns a deep copy of the input description slice.
func copyInputDescs(src []InputDescription) []InputDescription {
	out := make([]InputDescription, len(src))
	copy(out, src)

	return out
}

// copyOutputDescs returns a deep copy of the output description slice.
func copyOutputDescs(src []OutputDescription) []OutputDescription {
	out := make([]OutputDescription, len(src))
	copy(out, src)

	return out
}

// copyRefDataSources returns a deep copy of the reference data source description slice.
func copyRefDataSources(src []ReferenceDataSourceDescription) []ReferenceDataSourceDescription {
	out := make([]ReferenceDataSourceDescription, len(src))
	copy(out, src)

	return out
}

// copyVpcConfigs returns a deep copy of the VPC configuration description slice.
// Nil SubnetIDs/SecurityGroupIDs are normalized to empty slices.
func copyVpcConfigs(src []VpcConfigurationDescription) []VpcConfigurationDescription {
	out := make([]VpcConfigurationDescription, len(src))

	for i, v := range src {
		entry := v

		if v.SubnetIDs == nil {
			entry.SubnetIDs = []string{}
		} else {
			entry.SubnetIDs = make([]string, len(v.SubnetIDs))
			copy(entry.SubnetIDs, v.SubnetIDs)
		}

		if v.SecurityGroupIDs == nil {
			entry.SecurityGroupIDs = []string{}
		} else {
			entry.SecurityGroupIDs = make([]string, len(v.SecurityGroupIDs))
			copy(entry.SecurityGroupIDs, v.SecurityGroupIDs)
		}

		out[i] = entry
	}

	return out
}

// appCopy returns a deep copy of an Application, safe to return to callers.
func appCopy(src *Application) *Application {
	cp := *src
	cp.Tags = cloneTags(src.Tags)
	cp.CloudWatchLoggingOptionDescs = copyCWLOptions(src.CloudWatchLoggingOptionDescs)
	cp.InputDescriptions = copyInputDescs(src.InputDescriptions)
	cp.OutputDescriptions = copyOutputDescs(src.OutputDescriptions)
	cp.ReferenceDataSourceDescriptions = copyRefDataSources(src.ReferenceDataSourceDescriptions)
	cp.VpcConfigurationDescriptions = copyVpcConfigs(src.VpcConfigurationDescriptions)
	cp.EnvironmentPropertyGroups = copyPropertyGroups(src.EnvironmentPropertyGroups)

	return &cp
}

// tagsToMap converts a tag slice to a map for display.
func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToTags converts a map to a tag slice.
func mapToTags(m map[string]string) []Tag {
	tags := make([]Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, Tag{Key: k, Value: v})
	}

	return tags
}
