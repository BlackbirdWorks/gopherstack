package iotanalytics

import "context"

// StorageBackend is the interface for the IoT Analytics backend.
type StorageBackend interface {
	CreateChannel(
		ctx context.Context,
		name string,
		tags map[string]string,
		storage *ChannelStorage,
		retention *RetentionPeriod,
	) (*Channel, error)
	DescribeChannel(name string) (*Channel, error)
	UpdateChannel(name string, storage *ChannelStorage, retention *RetentionPeriod) error
	DeleteChannel(name string) error
	ListChannels() []*Channel

	CreateDatastore(
		ctx context.Context,
		name string,
		tags map[string]string,
		storage *DatastoreStorage,
		retention *RetentionPeriod,
		fileFormat *FileFormatConfiguration,
		partitions *DatastorePartitions,
	) (*Datastore, error)
	DescribeDatastore(name string) (*Datastore, error)
	UpdateDatastore(
		name string,
		storage *DatastoreStorage,
		retention *RetentionPeriod,
		fileFormat *FileFormatConfiguration,
		partitions *DatastorePartitions,
	) error
	DeleteDatastore(name string) error
	ListDatastores() []*Datastore

	CreateDataset(
		ctx context.Context,
		name string,
		tags map[string]string,
		actions []DatasetAction,
		triggers []DatasetTrigger,
		contentDeliveryRules []ContentDeliveryRule,
		versioningConfig *VersioningConfiguration,
		lateDataRules []LateDataRule,
	) (*Dataset, error)
	DescribeDataset(name string) (*Dataset, error)
	UpdateDataset(
		name string,
		actions []DatasetAction,
		triggers []DatasetTrigger,
		contentDeliveryRules []ContentDeliveryRule,
		versioningConfig *VersioningConfiguration,
		lateDataRules []LateDataRule,
	) error
	DeleteDataset(name string) error
	ListDatasets() []*Dataset

	CreatePipeline(
		ctx context.Context,
		name string,
		tags map[string]string,
		activities []PipelineActivity,
	) (*Pipeline, error)
	DescribePipeline(name string) (*Pipeline, error)
	UpdatePipeline(name string, activities []PipelineActivity) error
	DeletePipeline(name string) error
	ListPipelines() []*Pipeline

	ListTagsForResource(resourceARN string) ([]TagDTO, error)
	TagResource(resourceARN string, tags []TagDTO) error
	UntagResource(resourceARN string, tagKeys []string) error

	BatchPutMessage(channelName string, messages []messageInput) ([]BatchPutMessageErrorEntry, error)
	SampleChannelData(channelName string, maxMessages int) ([][]byte, error)

	StartPipelineReprocessing(pipelineName string, startTime, endTime *float64) (string, error)
	CancelPipelineReprocessing(pipelineName, reprocessingID string) error

	CreateDatasetContent(datasetName string) (*DatasetContent, error)
	GetDatasetContent(datasetName, versionID string) (*DatasetContent, error)
	ListDatasetContents(datasetName string) ([]*DatasetContent, error)
	DeleteDatasetContent(datasetName, versionID string) error

	DescribeLoggingOptions() (*LoggingOptions, error)
	PutLoggingOptions(options *LoggingOptions) error

	RunPipelineActivity(payloads [][]byte) ([][]byte, error)

	Reset()
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
