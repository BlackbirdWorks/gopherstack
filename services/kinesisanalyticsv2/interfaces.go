package kinesisanalyticsv2

import "context"

// StorageBackend is the interface for the Kinesis Data Analytics v2 in-memory backend.
type StorageBackend interface {
	Region() string
	AccountID() string
	GenerateApplicationARN(name string) string

	CreateApplication(
		ctx context.Context, name, runtimeEnv, serviceRole, description, mode string, tags []Tag,
	) (*Application, error)
	SeedApplicationConfiguration(ctx context.Context, name string, cfg SeedConfig) error
	DescribeApplication(ctx context.Context, name string) (*Application, error)
	ListApplications(ctx context.Context, nextToken string) ([]*Application, string)
	UpdateApplication(ctx context.Context, params UpdateApplicationParams) (*Application, string, error)
	DeleteApplication(ctx context.Context, name string, createTimestampSeconds *float64) error
	StartApplication(
		ctx context.Context, name string, runConfig *RunConfigInput, sqlRunConfigs []SQLRunConfigInput,
	) (string, error)
	StopApplication(ctx context.Context, name string, force bool) (string, error)

	CreateApplicationSnapshot(ctx context.Context, appName, snapshotName string) (*Snapshot, error)
	DescribeApplicationSnapshot(ctx context.Context, appName, snapshotName string) (*Snapshot, error)
	ListApplicationSnapshots(ctx context.Context, appName, nextToken string) ([]*Snapshot, string, error)
	DeleteApplicationSnapshot(ctx context.Context, appName, snapshotName string) error

	TagResource(ctx context.Context, resourceARN string, tags []Tag) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	ListTagsForResource(ctx context.Context, resourceARN string) ([]Tag, error)

	// AddApplicationCloudWatchLoggingOption/AddApplicationVpcConfiguration/
	// DeleteApplicationCloudWatchLoggingOption/DeleteApplicationVpcConfiguration
	// return an OperationID -- real AWS's outputs for these four ops (and only
	// these four among the Add*/Delete* config family) carry an OperationId
	// field, verified against aws-sdk-go-v2's api_op_*.go.
	AddApplicationCloudWatchLoggingOption(
		ctx context.Context, name string, currentVersionID int64, logStreamARN, roleARN string,
	) (string, error)
	AddApplicationInput(ctx context.Context, name string, currentVersionID int64, input InputDescription) error
	AddApplicationInputProcessingConfiguration(
		ctx context.Context,
		name string,
		currentVersionID int64,
		inputID string,
		config *InputProcessingConfigurationDesc,
	) error
	AddApplicationOutput(ctx context.Context, name string, currentVersionID int64, output OutputDescription) error
	AddApplicationReferenceDataSource(
		ctx context.Context, name string, currentVersionID int64, ref ReferenceDataSourceDescription,
	) error
	AddApplicationVpcConfiguration(
		ctx context.Context, name string, currentVersionID int64, vpc VpcConfigurationDescription,
	) (string, error)

	DeleteApplicationCloudWatchLoggingOption(
		ctx context.Context, name string, currentVersionID int64, loggingOptionID string,
	) (string, error)
	DeleteApplicationInputProcessingConfiguration(
		ctx context.Context, name string, currentVersionID int64, inputID string,
	) error
	DeleteApplicationOutput(ctx context.Context, name string, currentVersionID int64, outputID string) error
	DeleteApplicationReferenceDataSource(
		ctx context.Context, name string, currentVersionID int64, referenceID string,
	) error
	DeleteApplicationVpcConfiguration(
		ctx context.Context, name string, currentVersionID int64, vpcConfigurationID string,
	) (string, error)

	DescribeApplicationOperation(ctx context.Context, name, operationID string) (*ApplicationOperation, error)
	ListApplicationOperations(ctx context.Context, name, nextToken string) ([]*ApplicationOperation, string, error)
	DescribeApplicationVersion(ctx context.Context, name string, versionID int64) (*Application, error)
	ListApplicationVersions(ctx context.Context, name, nextToken string) ([]*ApplicationVersionSummary, string, error)
	RollbackApplication(ctx context.Context, name string, currentVersionID int64) (*Application, string, error)
	UpdateApplicationMaintenanceConfiguration(
		ctx context.Context, name string, maintenanceWindowStartTime string,
	) (*Application, error)
	DiscoverInputSchema(
		ctx context.Context, resourceARN, roleARN, inputStartingPosition string,
	) (*DiscoveredSchema, error)
}

// compile-time interface check.
var _ StorageBackend = (*InMemoryBackend)(nil)
