package kinesisanalyticsv2

// StorageBackend is the interface for the Kinesis Data Analytics v2 in-memory backend.
type StorageBackend interface {
	Region() string
	AccountID() string
	GenerateApplicationARN(name string) string

	CreateApplication(name, runtimeEnv, serviceRole, description, mode string, tags []Tag) (*Application, error)
	DescribeApplication(name string) (*Application, error)
	ListApplications(nextToken string) ([]*Application, string)
	UpdateApplication(name string, serviceRole, description string) (*Application, error)
	DeleteApplication(name string) error
	StartApplication(name string) error
	StopApplication(name string) error

	CreateApplicationSnapshot(appName, snapshotName string) (*Snapshot, error)
	DescribeApplicationSnapshot(appName, snapshotName string) (*Snapshot, error)
	ListApplicationSnapshots(appName, nextToken string) ([]*Snapshot, string, error)
	DeleteApplicationSnapshot(appName, snapshotName string) error

	TagResource(resourceARN string, tags []Tag) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) ([]Tag, error)

	AddApplicationCloudWatchLoggingOption(name string, currentVersionID int64, logStreamARN, roleARN string) error
	AddApplicationInput(name string, currentVersionID int64, input InputDescription) error
	AddApplicationInputProcessingConfiguration(
		name string,
		currentVersionID int64,
		inputID string,
		config *InputProcessingConfigurationDesc,
	) error
	AddApplicationOutput(name string, currentVersionID int64, output OutputDescription) error
	AddApplicationReferenceDataSource(name string, currentVersionID int64, ref ReferenceDataSourceDescription) error
	AddApplicationVpcConfiguration(name string, currentVersionID int64, vpc VpcConfigurationDescription) error

	DeleteApplicationCloudWatchLoggingOption(name string, currentVersionID int64, loggingOptionID string) error
	DeleteApplicationInputProcessingConfiguration(name string, currentVersionID int64, inputID string) error
	DeleteApplicationOutput(name string, currentVersionID int64, outputID string) error
}

// compile-time interface check.
var _ StorageBackend = (*InMemoryBackend)(nil)
