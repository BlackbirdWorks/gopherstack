package kinesisanalyticsv2

// This file defines the backend-facing "delta" types accepted by
// UpdateApplication's ApplicationConfigurationUpdate/RunConfigurationUpdate/
// CloudWatchLoggingOptionUpdates request fields, StartApplication's/
// UpdateApplication's RunConfiguration(Update) field, and
// CreateApplication's inline ApplicationConfiguration extras -- all
// previously accepted on the wire (to avoid rejecting well-formed requests)
// but silently discarded (see PARITY.md gaps). Handler-side wire structs
// (handler_applications.go) convert into these before calling the backend;
// applications.go applies them to *Application field-by-field.

// CodeContentUpdate describes an update to application code content. Only
// one of TextContentUpdate/ZipFileContentUpdate/S3*Update is expected to be
// set per real AWS's CodeContentUpdate.
type CodeContentUpdate struct {
	TextContentUpdate     *string
	S3BucketARNUpdate     *string
	S3FileKeyUpdate       *string
	S3ObjectVersionUpdate *string
	ZipFileContentUpdate  []byte
}

// ApplicationCodeConfigUpdate describes updates to an application's code configuration.
type ApplicationCodeConfigUpdate struct {
	CodeContentUpdate     *CodeContentUpdate
	CodeContentTypeUpdate string
}

// CheckpointConfigUpdate describes updates to a Flink application's checkpointing configuration.
type CheckpointConfigUpdate struct {
	CheckpointingEnabledUpdate       *bool
	CheckpointIntervalUpdate         *int64
	MinPauseBetweenCheckpointsUpdate *int64
	ConfigurationTypeUpdate          string
}

// MonitoringConfigUpdate describes updates to a Flink application's CloudWatch logging configuration.
type MonitoringConfigUpdate struct {
	ConfigurationTypeUpdate string
	LogLevelUpdate          string
	MetricsLevelUpdate      string
}

// ParallelismConfigUpdate describes updates to a Flink application's parallelism configuration.
type ParallelismConfigUpdate struct {
	AutoScalingEnabledUpdate *bool
	ParallelismUpdate        *int32
	ParallelismPerKPUUpdate  *int32
	ConfigurationTypeUpdate  string
}

// FlinkApplicationConfigUpdate bundles the three FlinkApplicationConfigurationUpdate sub-updates.
type FlinkApplicationConfigUpdate struct {
	CheckpointConfigurationUpdate  *CheckpointConfigUpdate
	MonitoringConfigurationUpdate  *MonitoringConfigUpdate
	ParallelismConfigurationUpdate *ParallelismConfigUpdate
}

// InputUpdate describes updates to an existing application input, identified by InputID.
// Only the fields gopherstack's InputDescription models are supported (see
// models.go); InputSchemaUpdate/InputParallelismUpdate are not modeled
// anywhere in this backend (no different than at Add-time) and are ignored
// if present on the wire.
type InputUpdate struct {
	KinesisStreamsInputUpdate          *KinesisStreamsInputDesc
	KinesisFirehoseInputUpdate         *KinesisFirehoseInputDesc
	InputProcessingConfigurationUpdate *InputProcessingConfigurationDesc
	InputID                            string
	NamePrefixUpdate                   string
}

// OutputUpdate describes updates to an existing application output, identified by OutputID.
type OutputUpdate struct {
	KinesisStreamsOutputUpdate  *KinesisStreamsOutputDesc
	KinesisFirehoseOutputUpdate *KinesisFirehoseOutputDesc
	LambdaOutputUpdate          *LambdaOutputDesc
	DestinationSchemaUpdate     *DestinationSchemaDesc
	OutputID                    string
	NameUpdate                  string
}

// ReferenceDataSourceUpdate describes updates to an existing reference data
// source, identified by ReferenceID.
type ReferenceDataSourceUpdate struct {
	S3ReferenceDataSourceUpdate *S3ReferenceDataSourceDesc
	ReferenceID                 string
	TableNameUpdate             string
}

// SQLApplicationConfigUpdate bundles the three SQLApplicationConfigurationUpdate sub-updates.
type SQLApplicationConfigUpdate struct {
	InputUpdates               []InputUpdate
	OutputUpdates              []OutputUpdate
	ReferenceDataSourceUpdates []ReferenceDataSourceUpdate
}

// VpcConfigUpdate describes updates to an existing VPC configuration, identified by VpcConfigurationID.
type VpcConfigUpdate struct {
	VpcConfigurationID     string
	SubnetIDUpdates        []string
	SecurityGroupIDUpdates []string
}

// CloudWatchLoggingOptionUpdate describes an update to an existing CloudWatch
// logging option's LogStreamARN, identified by CloudWatchLoggingOptionID.
// Real AWS's AddApplicationCloudWatchLoggingOption/
// DeleteApplicationCloudWatchLoggingOption are the only ways to add/remove
// entries -- UpdateApplication can only update an existing one's ARN.
type CloudWatchLoggingOptionUpdate struct {
	CloudWatchLoggingOptionID string
	LogStreamARNUpdate        string
}

// ApplicationConfigurationUpdate bundles every optional delta accepted by
// UpdateApplication's ApplicationConfigurationUpdate request field.
type ApplicationConfigurationUpdate struct {
	ApplicationCodeConfigurationUpdate           *ApplicationCodeConfigUpdate
	FlinkApplicationConfigurationUpdate          *FlinkApplicationConfigUpdate
	ApplicationSnapshotConfigurationUpdate       *bool
	ApplicationSystemRollbackConfigurationUpdate *bool
	ApplicationEncryptionConfigurationUpdate     *ApplicationEncryptionConfigDesc
	SQLApplicationConfigurationUpdate            *SQLApplicationConfigUpdate
	EnvironmentPropertyUpdates                   []PropertyGroup
	VpcConfigurationUpdates                      []VpcConfigUpdate
	// hasEnvironmentPropertyUpdates distinguishes "no EnvironmentPropertyUpdates
	// object in the request" from "EnvironmentPropertyUpdates with zero
	// PropertyGroups" (which real AWS treats as clearing every group).
	HasEnvironmentPropertyUpdates bool
}

// RunConfigInput carries StartApplication's RunConfiguration and
// UpdateApplication's RunConfigurationUpdate request fields -- both share
// the same ApplicationRestoreConfiguration/FlinkRunConfiguration shape in
// real AWS.
type RunConfigInput struct {
	ApplicationRestoreConfiguration *ApplicationRestoreConfig
	FlinkRunConfiguration           *FlinkRunConfig
}

// UpdateApplicationParams bundles every UpdateApplication request field.
type UpdateApplicationParams struct {
	ApplicationConfigurationUpdate *ApplicationConfigurationUpdate
	RunConfigurationUpdate         *RunConfigInput
	Name                           string
	ConditionalToken               string
	ServiceExecutionRoleUpdate     string
	ApplicationDescription         string
	RuntimeEnvironmentUpdate       string
	CloudWatchLoggingOptionUpdates []CloudWatchLoggingOptionUpdate
	CurrentApplicationVersionID    int64
}

// SeedConfig bundles every piece of inline configuration CreateApplication's
// ApplicationConfiguration/CloudWatchLoggingOptions request fields can
// carry. See SeedApplicationConfiguration.
type SeedConfig struct {
	CodeConfig                *ApplicationCodeConfigDesc
	FlinkConfig               *FlinkApplicationConfigDesc
	SnapshotsEnabled          *bool
	RollbackEnabled           *bool
	EncryptionConfig          *ApplicationEncryptionConfigDesc
	Inputs                    []InputDescription
	Outputs                   []OutputDescription
	ReferenceDataSources      []ReferenceDataSourceDescription
	VpcConfigs                []VpcConfigurationDescription
	CWLOptions                []CloudWatchLoggingOptionDesc
	EnvironmentPropertyGroups []PropertyGroup
}

// IsEmpty reports whether cfg carries no inline configuration at all, so
// callers can skip the SeedApplicationConfiguration round-trip entirely
// (matching the pre-existing len(...)>0-checks convention in
// handleCreateApplication).
func (cfg SeedConfig) IsEmpty() bool {
	return len(cfg.Inputs) == 0 && len(cfg.Outputs) == 0 && len(cfg.ReferenceDataSources) == 0 &&
		len(cfg.VpcConfigs) == 0 && len(cfg.CWLOptions) == 0 &&
		cfg.CodeConfig == nil && cfg.FlinkConfig == nil && len(cfg.EnvironmentPropertyGroups) == 0 &&
		cfg.SnapshotsEnabled == nil && cfg.RollbackEnabled == nil && cfg.EncryptionConfig == nil
}
