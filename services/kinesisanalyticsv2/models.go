package kinesisanalyticsv2

import (
	"fmt"
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
	InputStartingPositionConfiguration      *InputStartingPositionConfig      `json:"InputStartingPositionConfiguration,omitempty"`      //nolint:lll // AWS API name
	InputSchema                             *SourceSchemaDesc                 `json:"InputSchema,omitempty"`
	InputParallelism                        *InputParallelismDesc             `json:"InputParallelism,omitempty"`
	InputID                                 string                            `json:"InputId"`
	NamePrefix                              string                            `json:"NamePrefix,omitempty"`
	InAppStreamNames                        []string                          `json:"InAppStreamNames,omitempty"`
}

// inAppStreamNames generates the in-application stream names Kinesis Data
// Analytics assigns for a given NamePrefix/parallelism count -- documented
// directly on real AWS's Input.NamePrefix field: "Suppose that you specify a
// prefix 'MyInApplicationStream.' Kinesis Data Analytics then creates one or
// more (as per the InputParallelism count you specified) in-application
// streams with the names 'MyInApplicationStream_001,'
// 'MyInApplicationStream_002,' and so on."
// (aws-sdk-go-v2/service/kinesisanalyticsv2@v1.41.4 types/types.go, Input.NamePrefix doc).
func inAppStreamNames(namePrefix string, parallelism *InputParallelismDesc) []string {
	count := int32(1)
	if parallelism != nil && parallelism.Count > 0 {
		count = parallelism.Count
	}

	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("%s_%03d", namePrefix, i+1)
	}

	return names
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
	ReferenceSchema                  *SourceSchemaDesc          `json:"ReferenceSchema,omitempty"`
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

// InputStartingPositionConfig describes the point at which a SQL-based
// application starts reading from an input's streaming source
// (StartApplication's RunConfiguration.SqlRunConfigurations and the
// response-side InputDescription.InputStartingPositionConfiguration use the
// identical shape -- botocore kinesisanalyticsv2/2018-05-23/service-2.json.gz
// shape "InputStartingPositionConfiguration").
type InputStartingPositionConfig struct {
	InputStartingPosition string `json:"InputStartingPosition,omitempty"`
}

// SQLRunConfigInput carries one entry of StartApplication's
// RunConfiguration.SqlRunConfigurations -- the per-input starting position
// for a SQL-based application. Real AWS's RunConfigurationUpdate (used by
// UpdateApplication) has no such field: verified against botocore
// kinesisanalyticsv2/2018-05-23/service-2.json.gz, shape
// "RunConfigurationUpdate" only has FlinkRunConfiguration/
// ApplicationRestoreConfiguration.
type SQLRunConfigInput struct {
	InputStartingPositionConfiguration *InputStartingPositionConfig `json:"InputStartingPositionConfiguration,omitempty"` //nolint:lll // AWS API name
	InputID                            string                       `json:"InputId"`
}

// ZeppelinMonitoringConfigDesc describes CloudWatch logging verbosity for a
// Managed Service for Apache Flink Studio notebook (shared shape for both
// the create request and the describe response -- real AWS's
// ZeppelinMonitoringConfiguration and ZeppelinMonitoringConfigurationDescription
// both use the field name "LogLevel").
type ZeppelinMonitoringConfigDesc struct {
	LogLevel string `json:"LogLevel"`
}

// GlueDataCatalogConfigDesc identifies the Glue Data Catalog database used by
// a Studio notebook (shared shape, same rationale as ZeppelinMonitoringConfigDesc).
type GlueDataCatalogConfigDesc struct {
	DatabaseARN string `json:"DatabaseARN"`
}

// CatalogConfigDesc wraps GlueDataCatalogConfigDesc for CreateApplication's
// inline ApplicationConfiguration.ZeppelinApplicationConfiguration.CatalogConfiguration.
type CatalogConfigDesc struct {
	GlueDataCatalogConfiguration *GlueDataCatalogConfigDesc `json:"GlueDataCatalogConfiguration,omitempty"`
}

// CatalogConfigDescription wraps GlueDataCatalogConfigDesc for
// DescribeApplication's ...ZeppelinApplicationConfigurationDescription.CatalogConfigurationDescription.
type CatalogConfigDescription struct {
	GlueDataCatalogConfigurationDescription *GlueDataCatalogConfigDesc `json:"GlueDataCatalogConfigurationDescription,omitempty"` //nolint:lll // AWS API name
}

// S3ContentBaseLocationDesc describes an S3 base location (bucket + optional
// path prefix), shared by DeployAsApplicationConfiguration's request and
// response fields -- real AWS's S3ContentBaseLocation and
// S3ContentBaseLocationDescription both use BucketARN/BasePath.
type S3ContentBaseLocationDesc struct {
	BucketARN string `json:"BucketARN"`
	BasePath  string `json:"BasePath,omitempty"`
}

// DeployAsApplicationConfigDesc wraps the S3 location a Studio notebook
// deploys as a durable-state application, for CreateApplication's inline
// ZeppelinApplicationConfiguration.DeployAsApplicationConfiguration.
type DeployAsApplicationConfigDesc struct {
	S3ContentLocation *S3ContentBaseLocationDesc `json:"S3ContentLocation,omitempty"`
}

// DeployAsApplicationConfigDescription is the DescribeApplication-side
// counterpart to DeployAsApplicationConfigDesc.
type DeployAsApplicationConfigDescription struct {
	S3ContentLocationDescription *S3ContentBaseLocationDesc `json:"S3ContentLocationDescription,omitempty"` //nolint:lll // AWS API name
}

// MavenReferenceDesc identifies a Maven dependency JAR (shared shape for
// request and response -- real AWS's MavenReference uses GroupId/ArtifactId/
// Version on both CustomArtifactConfiguration and
// CustomArtifactConfigurationDescription).
type MavenReferenceDesc struct {
	GroupID    string `json:"GroupId"`
	ArtifactID string `json:"ArtifactId"`
	Version    string `json:"Version"`
}

// CustomArtifactConfigDescription describes one dependency JAR or UDF JAR
// (S3-hosted or Maven-hosted, per ArtifactType) attached to a Studio
// notebook. Real AWS reuses this same item shape (renamed
// CustomArtifactConfiguration on create) wholesale for
// UpdateApplication's CustomArtifactsConfigurationUpdate -- there is no
// separate per-item update shape (verified: botocore's
// "CustomArtifactConfigurationUpdate" shape does not exist).
type CustomArtifactConfigDescription struct {
	S3ContentLocationDescription *S3CodeLocationDesc `json:"S3ContentLocationDescription,omitempty"` //nolint:lll // AWS API name
	MavenReferenceDescription    *MavenReferenceDesc `json:"MavenReferenceDescription,omitempty"`    //nolint:lll // AWS API name
	ArtifactType                 string              `json:"ArtifactType"`
}

// ZeppelinApplicationConfigDescription mirrors real AWS's
// ZeppelinApplicationConfigurationDescription -- the Managed Service for
// Apache Flink Studio notebook configuration (Glue Data Catalog, Maven/S3
// custom artifacts, deploy-as-application) echoed by DescribeApplication.
// Stored directly on Application and reused as the response wire shape
// (matches the FlinkApplicationConfigDesc/ApplicationCodeConfigDesc
// convention elsewhere in this file).
type ZeppelinApplicationConfigDescription struct {
	MonitoringConfigurationDescription          *ZeppelinMonitoringConfigDesc         `json:"MonitoringConfigurationDescription"`                    //nolint:lll // AWS API name
	CatalogConfigurationDescription             *CatalogConfigDescription             `json:"CatalogConfigurationDescription,omitempty"`             //nolint:lll // AWS API name
	DeployAsApplicationConfigurationDescription *DeployAsApplicationConfigDescription `json:"DeployAsApplicationConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	CustomArtifactsConfigurationDescription     []CustomArtifactConfigDescription     `json:"CustomArtifactsConfigurationDescription,omitempty"`     //nolint:lll // AWS API name
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

// RecordColumnDesc describes one column mapped from a streaming source's
// sampled records (SourceSchema.RecordColumns is a required member of the
// real DiscoverInputSchemaResponse.InputSchema -- botocore
// kinesisanalyticsv2/2018-05-23/service-2.json.gz shape "RecordColumn").
type RecordColumnDesc struct {
	Name    string `json:"Name"`
	Mapping string `json:"Mapping,omitempty"`
	SQLType string `json:"SqlType"`
}

// CSVMappingParametersDesc describes delimiter mapping for CSV-formatted records.
type CSVMappingParametersDesc struct {
	RecordRowDelimiter    string `json:"RecordRowDelimiter"`
	RecordColumnDelimiter string `json:"RecordColumnDelimiter"`
}

// JSONMappingParametersDesc describes the root path for JSON-formatted records.
type JSONMappingParametersDesc struct {
	RecordRowPath string `json:"RecordRowPath"`
}

// MappingParametersDesc carries the format-specific mapping info for a RecordFormatDesc.
type MappingParametersDesc struct {
	CSVMappingParameters  *CSVMappingParametersDesc  `json:"CSVMappingParameters,omitempty"`
	JSONMappingParameters *JSONMappingParametersDesc `json:"JSONMappingParameters,omitempty"`
}

// RecordFormatDesc describes the format of records on a streaming or
// reference source (shared shape for SourceSchemaDesc.RecordFormat and
// InputSchemaUpdateDesc.RecordFormatUpdate -- real AWS's RecordFormat is
// used unrenamed in both places).
type RecordFormatDesc struct {
	MappingParameters *MappingParametersDesc `json:"MappingParameters,omitempty"`
	RecordFormatType  string                 `json:"RecordFormatType"`
}

// SourceSchemaDesc describes the format of records on a streaming source and
// how they map to in-application columns (real AWS's SourceSchema shape,
// reused unrenamed for Input.InputSchema, InputDescription.InputSchema,
// ReferenceDataSource.ReferenceSchema, ReferenceDataSourceDescription.ReferenceSchema,
// and ReferenceDataSourceUpdate.ReferenceSchemaUpdate -- verified against
// botocore kinesisanalyticsv2/2018-05-23/service-2.json.gz, all five of
// those members are typed "SourceSchema" with no per-direction renaming).
type SourceSchemaDesc struct {
	RecordFormat   *RecordFormatDesc  `json:"RecordFormat"`
	RecordEncoding string             `json:"RecordEncoding,omitempty"`
	RecordColumns  []RecordColumnDesc `json:"RecordColumns"`
}

// InputSchemaUpdateDesc mirrors real AWS's InputSchemaUpdate -- unlike
// ReferenceDataSourceUpdate.ReferenceSchemaUpdate (which reuses SourceSchema
// verbatim), InputUpdate.InputSchemaUpdate is its own shape with
// Update-suffixed field names (verified against botocore's
// "InputSchemaUpdate" shape).
type InputSchemaUpdateDesc struct {
	RecordFormatUpdate   *RecordFormatDesc  `json:"RecordFormatUpdate,omitempty"`
	RecordEncodingUpdate string             `json:"RecordEncodingUpdate,omitempty"`
	RecordColumnUpdates  []RecordColumnDesc `json:"RecordColumnUpdates,omitempty"`
}

// InputParallelismDesc describes the number of in-application streams
// created for an input's streaming source.
type InputParallelismDesc struct {
	Count int32 `json:"Count,omitempty"`
}

// InputParallelismUpdateDesc mirrors real AWS's InputParallelismUpdate.
type InputParallelismUpdateDesc struct {
	CountUpdate int32 `json:"CountUpdate,omitempty"`
}

// DiscoveredSchema holds the inferred schema from DiscoverInputSchema.
type DiscoveredSchema struct {
	RecordFormat       string             `json:"RecordFormat"`
	RecordEncoding     string             `json:"RecordEncoding,omitempty"`
	RecordColumns      []RecordColumnDesc `json:"RecordColumns"`
	ParsedInputRecords [][]string         `json:"ParsedInputRecords,omitempty"`
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
	ZeppelinConfig                    *ZeppelinApplicationConfigDescription
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
