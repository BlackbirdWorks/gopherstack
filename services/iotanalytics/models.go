package iotanalytics

import (
	"errors"
	"time"
)

// Sentinel errors for IoT Analytics backend operations.
var (
	// ErrChannelNotFound is returned when a channel does not exist.
	ErrChannelNotFound = newNotFoundError("channel not found")
	// ErrDatastoreNotFound is returned when a datastore does not exist.
	ErrDatastoreNotFound = newNotFoundError("datastore not found")
	// ErrDatasetNotFound is returned when a dataset does not exist.
	ErrDatasetNotFound = newNotFoundError("dataset not found")
	// ErrPipelineNotFound is returned when a pipeline does not exist.
	ErrPipelineNotFound = newNotFoundError("pipeline not found")
	// ErrDatasetContentNotFound is returned when a dataset content version does not exist.
	ErrDatasetContentNotFound = newNotFoundError("dataset content not found")
	// ErrLoggingOptionsNotFound is returned when logging options have not been configured.
	ErrLoggingOptionsNotFound = newNotFoundError("logging options not found")
	// ErrReprocessingNotFound is returned when a pipeline reprocessing job does not exist.
	ErrReprocessingNotFound = newNotFoundError("reprocessing not found")
	// ErrResourceNotFound is returned when a tagged resource ARN does not match any known resource.
	ErrResourceNotFound = newNotFoundError("resource not found")
	// ErrAlreadyExists is returned when a resource with the given name already exists.
	ErrAlreadyExists = errors.New("resource already exists")
	// ErrValidation is returned when request input fails validation.
	ErrValidation = errors.New("validation error")
)

// notFoundError represents a resource-not-found error.
type notFoundError struct {
	msg string
}

func (e *notFoundError) Error() string { return e.msg }

// newNotFoundError creates a new notFoundError.
func newNotFoundError(msg string) *notFoundError {
	return &notFoundError{msg: msg}
}

// isNotFound returns true if the error is a notFoundError.
func isNotFound(err error) bool {
	_, ok := err.(*notFoundError) //nolint:errorlint // direct type check for sentinel

	return ok
}

// ----------------------------------------
// Storage types
// ----------------------------------------

// ServiceManagedS3Storage indicates AWS-managed S3 storage (marker type).
type ServiceManagedS3Storage struct{}

// CustomerManagedS3ChannelStorage is customer-managed S3 for channels.
type CustomerManagedS3ChannelStorage struct {
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"keyPrefix,omitempty"`
	RoleArn   string `json:"roleArn"`
}

// ChannelStorage is the storage configuration for a channel.
type ChannelStorage struct {
	ServiceManagedS3  *ServiceManagedS3Storage         `json:"serviceManagedS3,omitempty"`
	CustomerManagedS3 *CustomerManagedS3ChannelStorage `json:"customerManagedS3,omitempty"`
}

// CustomerManagedS3DatastoreStorage is customer-managed S3 for datastores.
type CustomerManagedS3DatastoreStorage struct {
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"keyPrefix,omitempty"`
	RoleArn   string `json:"roleArn"`
}

// IotSiteWiseMultiLayerStorage is IoT SiteWise multi-layer storage for datastores.
type IotSiteWiseMultiLayerStorage struct {
	CustomerManagedS3Storage *CustomerManagedS3DatastoreStorage `json:"customerManagedS3Storage,omitempty"`
}

// DatastoreStorage is the storage configuration for a datastore.
type DatastoreStorage struct {
	ServiceManagedS3             *ServiceManagedS3Storage           `json:"serviceManagedS3,omitempty"`
	CustomerManagedS3            *CustomerManagedS3DatastoreStorage `json:"customerManagedS3,omitempty"`
	IotSiteWiseMultiLayerStorage *IotSiteWiseMultiLayerStorage      `json:"iotSiteWiseMultiLayerStorage,omitempty"`
}

// ----------------------------------------
// Retention and format types
// ----------------------------------------

// RetentionPeriod defines how long data is retained.
// Exactly one of Unlimited or NumberOfDays must be set.
type RetentionPeriod struct {
	NumberOfDays int  `json:"numberOfDays,omitempty"`
	Unlimited    bool `json:"unlimited,omitempty"`
}

// ColumnSchema defines a column in a Parquet schema.
type ColumnSchema struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// SchemaDefinition defines the schema for Parquet format.
type SchemaDefinition struct {
	Columns []ColumnSchema `json:"columns"`
}

// ParquetConfiguration defines Parquet file format settings.
type ParquetConfiguration struct {
	SchemaDefinition *SchemaDefinition `json:"schemaDefinition,omitempty"`
}

// JSONConfiguration defines JSON file format settings (marker type).
type JSONConfiguration struct{}

// FileFormatConfiguration defines the file format for a datastore.
type FileFormatConfiguration struct {
	JSONConfiguration    *JSONConfiguration    `json:"jsonConfiguration,omitempty"`
	ParquetConfiguration *ParquetConfiguration `json:"parquetConfiguration,omitempty"`
}

// ----------------------------------------
// Partition types
// ----------------------------------------

// AttributePartition defines a datastore partition by message attribute.
type AttributePartition struct {
	AttributeName string `json:"attributeName"`
}

// TimestampPartition defines a datastore partition by timestamp attribute.
type TimestampPartition struct {
	AttributeName   string `json:"attributeName"`
	TimestampFormat string `json:"timestampFormat,omitempty"`
}

// DatastorePartitionEntry is one partition definition (union).
type DatastorePartitionEntry struct {
	AttributePartition *AttributePartition `json:"attributePartition,omitempty"`
	TimestampPartition *TimestampPartition `json:"timestampPartition,omitempty"`
}

// DatastorePartitions holds all partition definitions for a datastore.
type DatastorePartitions struct {
	Partitions []DatastorePartitionEntry `json:"partitions"`
}

// ----------------------------------------
// Pipeline activity types
// ----------------------------------------

// PipelineChannelActivity is the pipeline channel source activity.
type PipelineChannelActivity struct {
	ChannelName string `json:"channelName"`
	Name        string `json:"name"`
	Next        string `json:"next,omitempty"`
}

// PipelineLambdaActivity invokes a Lambda function on messages.
type PipelineLambdaActivity struct {
	LambdaName string `json:"lambdaName"`
	Name       string `json:"name"`
	Next       string `json:"next,omitempty"`
	BatchSize  int    `json:"batchSize,omitempty"`
}

// PipelineDatastoreActivity is the pipeline sink activity.
type PipelineDatastoreActivity struct {
	DatastoreName string `json:"datastoreName"`
	Name          string `json:"name"`
}

// PipelineAddAttributesActivity adds attributes to messages.
type PipelineAddAttributesActivity struct {
	Attributes map[string]string `json:"attributes"`
	Name       string            `json:"name"`
	Next       string            `json:"next,omitempty"`
}

// PipelineRemoveAttributesActivity removes attributes from messages.
type PipelineRemoveAttributesActivity struct {
	Name       string   `json:"name"`
	Next       string   `json:"next,omitempty"`
	Attributes []string `json:"attributes"`
}

// PipelineSelectAttributesActivity selects specific attributes from messages.
type PipelineSelectAttributesActivity struct {
	Name       string   `json:"name"`
	Next       string   `json:"next,omitempty"`
	Attributes []string `json:"attributes"`
}

// PipelineFilterActivity filters messages based on a condition.
type PipelineFilterActivity struct {
	Filter string `json:"filter"`
	Name   string `json:"name"`
	Next   string `json:"next,omitempty"`
}

// PipelineMathActivity computes a math expression and adds result as an attribute.
type PipelineMathActivity struct {
	Attribute string `json:"attribute"`
	Math      string `json:"math"`
	Name      string `json:"name"`
	Next      string `json:"next,omitempty"`
}

// PipelineDeviceRegistryEnrichActivity enriches messages with Device Registry data.
type PipelineDeviceRegistryEnrichActivity struct {
	Attribute string `json:"attribute"`
	ThingName string `json:"thingName"`
	RoleArn   string `json:"roleArn"`
	Name      string `json:"name"`
	Next      string `json:"next,omitempty"`
}

// PipelineDeviceShadowEnrichActivity enriches messages with Device Shadow data.
type PipelineDeviceShadowEnrichActivity struct {
	Attribute string `json:"attribute"`
	ThingName string `json:"thingName"`
	RoleArn   string `json:"roleArn"`
	Name      string `json:"name"`
	Next      string `json:"next,omitempty"`
}

// PipelineActivity is a typed pipeline activity union.
type PipelineActivity struct {
	Channel              *PipelineChannelActivity              `json:"channel,omitempty"`
	Lambda               *PipelineLambdaActivity               `json:"lambda,omitempty"`
	Datastore            *PipelineDatastoreActivity            `json:"datastore,omitempty"`
	AddAttributes        *PipelineAddAttributesActivity        `json:"addAttributes,omitempty"`
	RemoveAttributes     *PipelineRemoveAttributesActivity     `json:"removeAttributes,omitempty"`
	SelectAttributes     *PipelineSelectAttributesActivity     `json:"selectAttributes,omitempty"`
	Filter               *PipelineFilterActivity               `json:"filter,omitempty"`
	Math                 *PipelineMathActivity                 `json:"math,omitempty"`
	DeviceRegistryEnrich *PipelineDeviceRegistryEnrichActivity `json:"deviceRegistryEnrich,omitempty"`
	DeviceShadowEnrich   *PipelineDeviceShadowEnrichActivity   `json:"deviceShadowEnrich,omitempty"`
}

// ----------------------------------------
// Dataset action and trigger types
// ----------------------------------------

// DeltaTime defines an offset for dataset query filters.
type DeltaTime struct {
	TimeExpression string `json:"timeExpression"`
	OffsetSeconds  int    `json:"offsetSeconds"`
}

// DatasetQueryFilter is a filter applied to a query action.
type DatasetQueryFilter struct {
	DeltaTime *DeltaTime `json:"deltaTime,omitempty"`
}

// DatasetQueryAction defines an SQL query action on a dataset.
type DatasetQueryAction struct {
	SQLQuery string               `json:"sqlQuery"`
	Filters  []DatasetQueryFilter `json:"filters,omitempty"`
}

// ResourceConfiguration defines compute resources for container actions.
type ResourceConfiguration struct {
	ComputeType    string `json:"computeType"`
	VolumeSizeInGB int    `json:"volumeSizeInGB"`
}

// DatasetVariable is a variable passed to a container action.
type DatasetVariable struct {
	StringValue                *string  `json:"stringValue,omitempty"`
	DoubleValue                *float64 `json:"doubleValue,omitempty"`
	DatasetContentVersionValue *string  `json:"datasetContentVersionValue,omitempty"`
	OutputFileURIValue         *string  `json:"outputFileUriValue,omitempty"`
	Name                       string   `json:"name"`
}

// DatasetContainerAction defines a container execution action on a dataset.
type DatasetContainerAction struct {
	Image                 string                 `json:"image"`
	ExecutionRoleArn      string                 `json:"executionRoleArn"`
	ResourceConfiguration *ResourceConfiguration `json:"resourceConfiguration"`
	Variables             []DatasetVariable      `json:"variables,omitempty"`
}

// DatasetAction is an action on a dataset (query or container).
type DatasetAction struct {
	QueryAction     *DatasetQueryAction     `json:"queryAction,omitempty"`
	ContainerAction *DatasetContainerAction `json:"containerAction,omitempty"`
	ActionName      string                  `json:"actionName"`
}

// ScheduleExpression defines a cron-based schedule trigger.
type ScheduleExpression struct {
	Expression string `json:"expression"`
}

// DatasetTriggerDataset triggers a dataset when another dataset produces content.
type DatasetTriggerDataset struct {
	Name string `json:"name"`
}

// DatasetTrigger triggers automatic dataset content creation.
type DatasetTrigger struct {
	Schedule *ScheduleExpression    `json:"schedule,omitempty"`
	Dataset  *DatasetTriggerDataset `json:"dataset,omitempty"`
}

// IotEventsDestination delivers content to IoT Events.
type IotEventsDestination struct {
	InputName string `json:"inputName"`
	RoleArn   string `json:"roleArn"`
}

// GlueConfiguration defines AWS Glue catalog settings for S3 delivery.
type GlueConfiguration struct {
	TableName    string `json:"tableName"`
	DatabaseName string `json:"databaseName"`
}

// S3DestinationConfiguration delivers content to S3.
type S3DestinationConfiguration struct {
	GlueConfiguration *GlueConfiguration `json:"glueConfiguration,omitempty"`
	Bucket            string             `json:"bucket"`
	Key               string             `json:"key"`
	RoleArn           string             `json:"roleArn"`
}

// ContentDeliveryDestination is the destination for a content delivery rule.
type ContentDeliveryDestination struct {
	IotEventsDestinationConfiguration *IotEventsDestination       `json:"iotEventsDestinationConfiguration,omitempty"`
	S3DestinationConfiguration        *S3DestinationConfiguration `json:"s3DestinationConfiguration,omitempty"`
}

// ContentDeliveryRule defines where dataset content is delivered on creation.
type ContentDeliveryRule struct {
	Destination *ContentDeliveryDestination `json:"destination"`
	EntryName   string                      `json:"entryName,omitempty"`
}

// VersioningConfiguration controls how many content versions to retain.
type VersioningConfiguration struct {
	MaxVersions int  `json:"maxVersions,omitempty"`
	Unlimited   bool `json:"unlimited,omitempty"`
}

// DeltaTimeSessionWindowConfiguration defines a session window for late data.
type DeltaTimeSessionWindowConfiguration struct {
	TimeoutInMinutes int `json:"timeoutInMinutes"`
}

// LateDataRuleConfiguration is the configuration for a late data rule.
type LateDataRuleConfiguration struct {
	DeltaTimeSessionWindowConfiguration *DeltaTimeSessionWindowConfiguration `json:"deltaTimeSessionWindowConfiguration,omitempty"` //nolint:lll // AWS field name
}

// LateDataRule defines conditions under which late data triggers dataset refresh.
type LateDataRule struct {
	RuleConfiguration *LateDataRuleConfiguration `json:"ruleConfiguration"`
	RuleName          string                     `json:"ruleName,omitempty"`
}

// ----------------------------------------
// Core resource types
// ----------------------------------------

// Channel stores all metadata and state for a single IoT Analytics channel.
type Channel struct {
	Tags                   map[string]string `json:"tags"`
	Storage                *ChannelStorage   `json:"storage,omitempty"`
	RetentionPeriod        *RetentionPeriod  `json:"retentionPeriod,omitempty"`
	Name                   string            `json:"name"`
	ARN                    string            `json:"arn"`
	Status                 string            `json:"status"`
	CreationTime           float64           `json:"creationTime"`
	LastUpdate             float64           `json:"lastUpdate"`
	LastMessageArrivalTime float64           `json:"lastMessageArrivalTime,omitempty"`
}

// Datastore stores all metadata and state for a single IoT Analytics datastore.
type Datastore struct {
	Tags                    map[string]string        `json:"tags"`
	Storage                 *DatastoreStorage        `json:"storage,omitempty"`
	RetentionPeriod         *RetentionPeriod         `json:"retentionPeriod,omitempty"`
	FileFormatConfiguration *FileFormatConfiguration `json:"fileFormatConfiguration,omitempty"`
	Partitions              *DatastorePartitions     `json:"partitions,omitempty"`
	Name                    string                   `json:"name"`
	ARN                     string                   `json:"arn"`
	Status                  string                   `json:"status"`
	CreationTime            float64                  `json:"creationTime"`
	LastUpdate              float64                  `json:"lastUpdate"`
}

// Dataset stores all metadata and state for a single IoT Analytics dataset.
type Dataset struct {
	Tags                    map[string]string        `json:"tags"`
	VersioningConfiguration *VersioningConfiguration `json:"versioningConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	ARN                     string                   `json:"arn"`
	Status                  string                   `json:"status"`
	Actions                 []DatasetAction          `json:"actions,omitempty"`
	Triggers                []DatasetTrigger         `json:"triggers,omitempty"`
	ContentDeliveryRules    []ContentDeliveryRule    `json:"contentDeliveryRules,omitempty"`
	LateDataRules           []LateDataRule           `json:"lateDataRules,omitempty"`
	CreationTime            float64                  `json:"creationTime"`
	LastUpdate              float64                  `json:"lastUpdate"`
}

// Pipeline stores all metadata and state for a single IoT Analytics pipeline.
type Pipeline struct {
	Tags          map[string]string                `json:"tags"`
	Reprocessings map[string]*PipelineReprocessing `json:"reprocessings"`
	Name          string                           `json:"name"`
	ARN           string                           `json:"arn"`
	Activities    []PipelineActivity               `json:"activities,omitempty"`
	CreationTime  float64                          `json:"creationTime"`
	LastUpdate    float64                          `json:"lastUpdate"`
}

// LoggingOptions stores the IoT Analytics logging configuration.
type LoggingOptions struct {
	RoleARN string `json:"roleArn"`
	Level   string `json:"level"`
	Enabled bool   `json:"enabled"`
}

// DatasetContent stores a single content version of an IoT Analytics dataset.
type DatasetContent struct {
	VersionID      string  `json:"versionId"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	CompletionTime float64 `json:"completionTime"`
}

// PipelineReprocessing stores state for a single pipeline reprocessing job.
type PipelineReprocessing struct {
	ID           string  `json:"id"`
	Status       string  `json:"status"`
	CreationTime float64 `json:"creationTime"`
	EndTime      float64 `json:"endTime,omitempty"`
	StartTime    float64 `json:"startTime,omitempty"`
}

// ChannelMessage stores a single message ingested into a channel.
type ChannelMessage struct {
	MessageID string
	Payload   []byte
}

// epochSeconds converts a [time.Time] to a float64 Unix epoch seconds value.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// ----------------------------------------
// DTO types for request/response serialization
// ----------------------------------------

// createChannelRequest is the request body for CreateChannel.
type createChannelRequest struct {
	ChannelStorage  *ChannelStorage  `json:"channelStorage,omitempty"`
	RetentionPeriod *RetentionPeriod `json:"retentionPeriod,omitempty"`
	ChannelName     string           `json:"channelName"`
	Tags            []tagDTO         `json:"tags,omitempty"`
}

// createChannelResponse is the response body for CreateChannel.
type createChannelResponse struct {
	ChannelName string `json:"channelName"`
	ChannelARN  string `json:"channelArn"`
}

// channelSummary is a summary of a channel for list operations.
type channelSummary struct {
	ChannelName            string  `json:"channelName"`
	ChannelARN             string  `json:"channelArn,omitempty"`
	Status                 string  `json:"status"`
	CreationTime           float64 `json:"creationTime"`
	LastUpdateTime         float64 `json:"lastUpdateTime,omitempty"`
	LastMessageArrivalTime float64 `json:"lastMessageArrivalTime,omitempty"`
}

// listChannelsResponse is the response body for ListChannels.
type listChannelsResponse struct {
	NextToken        *string          `json:"nextToken,omitempty"`
	ChannelSummaries []channelSummary `json:"channelSummaries"`
}

// describeChannelResponse is the response body for DescribeChannel.
type describeChannelResponse struct {
	Channel channelDetail `json:"channel"`
}

// channelDetail is a detailed view of a channel.
type channelDetail struct {
	Storage                *ChannelStorage  `json:"storage,omitempty"`
	RetentionPeriod        *RetentionPeriod `json:"retentionPeriod,omitempty"`
	Name                   string           `json:"name"`
	ARN                    string           `json:"arn"`
	Status                 string           `json:"status"`
	Tags                   []tagDTO         `json:"tags,omitempty"`
	CreationTime           float64          `json:"creationTime"`
	LastUpdateTime         float64          `json:"lastUpdateTime,omitempty"`
	LastMessageArrivalTime float64          `json:"lastMessageArrivalTime,omitempty"`
}

// updateChannelRequest is the request body for UpdateChannel.
type updateChannelRequest struct {
	ChannelStorage  *ChannelStorage  `json:"channelStorage,omitempty"`
	RetentionPeriod *RetentionPeriod `json:"retentionPeriod,omitempty"`
}

// createDatastoreRequest is the request body for CreateDatastore.
type createDatastoreRequest struct {
	DatastoreStorage        *DatastoreStorage        `json:"datastoreStorage,omitempty"`
	RetentionPeriod         *RetentionPeriod         `json:"retentionPeriod,omitempty"`
	FileFormatConfiguration *FileFormatConfiguration `json:"fileFormatConfiguration,omitempty"`
	Partitions              *DatastorePartitions     `json:"partitions,omitempty"`
	DatastoreName           string                   `json:"datastoreName"`
	Tags                    []tagDTO                 `json:"tags,omitempty"`
}

// createDatastoreResponse is the response body for CreateDatastore.
type createDatastoreResponse struct {
	DatastoreName string `json:"datastoreName"`
	DatastoreARN  string `json:"datastoreArn"`
}

// datastoreSummary is a summary of a datastore for list operations.
type datastoreSummary struct {
	DatastoreName  string  `json:"datastoreName"`
	DatastoreARN   string  `json:"datastoreArn,omitempty"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// listDatastoresResponse is the response body for ListDatastores.
type listDatastoresResponse struct {
	NextToken          *string            `json:"nextToken,omitempty"`
	DatastoreSummaries []datastoreSummary `json:"datastoreSummaries"`
}

// describeDatastoreResponse is the response body for DescribeDatastore.
type describeDatastoreResponse struct {
	Datastore datastoreDetail `json:"datastore"`
}

// datastoreDetail is a detailed view of a datastore.
type datastoreDetail struct {
	Storage                 *DatastoreStorage        `json:"storage,omitempty"`
	RetentionPeriod         *RetentionPeriod         `json:"retentionPeriod,omitempty"`
	FileFormatConfiguration *FileFormatConfiguration `json:"fileFormatConfiguration,omitempty"`
	Partitions              *DatastorePartitions     `json:"partitions,omitempty"`
	Name                    string                   `json:"name"`
	ARN                     string                   `json:"arn"`
	Status                  string                   `json:"status"`
	Tags                    []tagDTO                 `json:"tags,omitempty"`
	CreationTime            float64                  `json:"creationTime"`
	LastUpdateTime          float64                  `json:"lastUpdateTime,omitempty"`
}

// updateDatastoreRequest is the request body for UpdateDatastore.
type updateDatastoreRequest struct {
	DatastoreStorage        *DatastoreStorage        `json:"datastoreStorage,omitempty"`
	RetentionPeriod         *RetentionPeriod         `json:"retentionPeriod,omitempty"`
	FileFormatConfiguration *FileFormatConfiguration `json:"fileFormatConfiguration,omitempty"`
	Partitions              *DatastorePartitions     `json:"partitions,omitempty"`
}

// createDatasetRequest is the request body for CreateDataset.
type createDatasetRequest struct {
	Actions                 []DatasetAction          `json:"actions,omitempty"`
	Triggers                []DatasetTrigger         `json:"triggers,omitempty"`
	ContentDeliveryRules    []ContentDeliveryRule    `json:"contentDeliveryRules,omitempty"`
	LateDataRules           []LateDataRule           `json:"lateDataRules,omitempty"`
	VersioningConfiguration *VersioningConfiguration `json:"versioningConfiguration,omitempty"`
	DatasetName             string                   `json:"datasetName"`
	Tags                    []tagDTO                 `json:"tags,omitempty"`
}

// createDatasetResponse is the response body for CreateDataset.
type createDatasetResponse struct {
	DatasetName string `json:"datasetName"`
	DatasetARN  string `json:"datasetArn"`
}

// datasetSummary is a summary of a dataset for list operations.
type datasetSummary struct {
	DatasetName    string  `json:"datasetName"`
	DatasetARN     string  `json:"datasetArn,omitempty"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// listDatasetsResponse is the response body for ListDatasets.
type listDatasetsResponse struct {
	NextToken        *string          `json:"nextToken,omitempty"`
	DatasetSummaries []datasetSummary `json:"datasetSummaries"`
}

// describeDatasetResponse is the response body for DescribeDataset.
type describeDatasetResponse struct {
	Dataset datasetDetail `json:"dataset"`
}

// datasetDetail is a detailed view of a dataset.
type datasetDetail struct {
	VersioningConfiguration *VersioningConfiguration `json:"versioningConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	ARN                     string                   `json:"arn"`
	Status                  string                   `json:"status"`
	Actions                 []DatasetAction          `json:"actions,omitempty"`
	Triggers                []DatasetTrigger         `json:"triggers,omitempty"`
	ContentDeliveryRules    []ContentDeliveryRule    `json:"contentDeliveryRules,omitempty"`
	LateDataRules           []LateDataRule           `json:"lateDataRules,omitempty"`
	Tags                    []tagDTO                 `json:"tags,omitempty"`
	CreationTime            float64                  `json:"creationTime"`
	LastUpdateTime          float64                  `json:"lastUpdateTime,omitempty"`
}

// updateDatasetRequest is the request body for UpdateDataset.
type updateDatasetRequest struct {
	VersioningConfiguration *VersioningConfiguration `json:"versioningConfiguration,omitempty"`
	Actions                 []DatasetAction          `json:"actions,omitempty"`
	Triggers                []DatasetTrigger         `json:"triggers,omitempty"`
	ContentDeliveryRules    []ContentDeliveryRule    `json:"contentDeliveryRules,omitempty"`
	LateDataRules           []LateDataRule           `json:"lateDataRules,omitempty"`
}

// createPipelineRequest is the request body for CreatePipeline.
type createPipelineRequest struct {
	PipelineActivities []PipelineActivity `json:"pipelineActivities,omitempty"`
	PipelineName       string             `json:"pipelineName"`
	Tags               []tagDTO           `json:"tags,omitempty"`
}

// createPipelineResponse is the response body for CreatePipeline.
type createPipelineResponse struct {
	PipelineName string `json:"pipelineName"`
	PipelineARN  string `json:"pipelineArn"`
}

// pipelineReprocessingSummary is a typed reprocessing summary for list/describe responses.
type pipelineReprocessingSummary struct {
	ID           string  `json:"id"`
	Status       string  `json:"status"`
	CreationTime float64 `json:"creationTime"`
	EndTime      float64 `json:"endTime,omitempty"`
}

// pipelineSummary is a summary of a pipeline for list operations.
type pipelineSummary struct {
	PipelineName          string                        `json:"pipelineName"`
	PipelineARN           string                        `json:"pipelineArn,omitempty"`
	ReprocessingSummaries []pipelineReprocessingSummary `json:"reprocessingSummaries,omitempty"`
	CreationTime          float64                       `json:"creationTime"`
	LastUpdateTime        float64                       `json:"lastUpdateTime,omitempty"`
}

// listPipelinesResponse is the response body for ListPipelines.
type listPipelinesResponse struct {
	NextToken         *string           `json:"nextToken,omitempty"`
	PipelineSummaries []pipelineSummary `json:"pipelineSummaries"`
}

// describePipelineResponse is the response body for DescribePipeline.
type describePipelineResponse struct {
	Pipeline pipelineDetail `json:"pipeline"`
}

// pipelineDetail is a detailed view of a pipeline.
type pipelineDetail struct {
	Name                  string                        `json:"name"`
	ARN                   string                        `json:"arn"`
	Activities            []PipelineActivity            `json:"pipelineActivities,omitempty"`
	ReprocessingSummaries []pipelineReprocessingSummary `json:"reprocessingSummaries,omitempty"`
	Tags                  []tagDTO                      `json:"tags,omitempty"`
	CreationTime          float64                       `json:"creationTime"`
	LastUpdateTime        float64                       `json:"lastUpdateTime,omitempty"`
}

// updatePipelineRequest is the request body for UpdatePipeline.
type updatePipelineRequest struct {
	PipelineActivities []PipelineActivity `json:"pipelineActivities,omitempty"`
}

// TagDTO is a key-value tag for IoT Analytics resources.
type TagDTO struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// tagDTO is an alias for TagDTO for internal use.
type tagDTO = TagDTO

// listTagsResponse is the response body for ListTagsForResource.
type listTagsResponse struct {
	Tags []tagDTO `json:"tags"`
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Tags []tagDTO `json:"tags"`
}

// errorResponse is the standard IoT Analytics error response with AWS __type field.
type errorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// ----------------------------------------
// BatchPutMessage DTOs
// ----------------------------------------

// messageInput is a single message to ingest.
type messageInput struct {
	MessageID string `json:"messageId"`
	Payload   []byte `json:"payload"`
}

// batchPutMessageRequest is the request body for BatchPutMessage.
type batchPutMessageRequest struct {
	ChannelName string         `json:"channelName"`
	Messages    []messageInput `json:"messages"`
}

// BatchPutMessageErrorEntry is a per-message error in BatchPutMessage.
type BatchPutMessageErrorEntry struct {
	ChannelName  string `json:"channelName,omitempty"`
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	MessageID    string `json:"messageId,omitempty"`
}

// batchPutMessageResponse is the response for BatchPutMessage.
type batchPutMessageResponse struct {
	BatchPutMessageErrorEntries []BatchPutMessageErrorEntry `json:"batchPutMessageErrorEntries"`
}

// ----------------------------------------
// SampleChannelData DTOs
// ----------------------------------------

// sampleChannelDataResponse is the response for SampleChannelData.
type sampleChannelDataResponse struct {
	Payloads [][]byte `json:"payloads"`
}

// ----------------------------------------
// StartPipelineReprocessing DTOs
// ----------------------------------------

// reprocessingChannelMessages specifies S3 paths of archived channel messages.
type reprocessingChannelMessages struct {
	S3Paths []string `json:"s3Paths,omitempty"`
}

// startPipelineReprocessingRequest is the request body for StartPipelineReprocessing.
type startPipelineReprocessingRequest struct {
	StartTime       *float64                     `json:"startTime,omitempty"`
	EndTime         *float64                     `json:"endTime,omitempty"`
	ChannelMessages *reprocessingChannelMessages `json:"channelMessages,omitempty"`
}

// startPipelineReprocessingResponse is the response for StartPipelineReprocessing.
type startPipelineReprocessingResponse struct {
	ReprocessingID string `json:"reprocessingId"`
}

// ----------------------------------------
// DatasetContent DTOs
// ----------------------------------------

// datasetContentStatusDTO is the nested status object in dataset content responses.
type datasetContentStatusDTO struct {
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

// createDatasetContentResponse is the response for CreateDatasetContent.
type createDatasetContentResponse struct {
	VersionID string `json:"versionId"`
}

// getDatasetContentResponse is the response for GetDatasetContent.
type getDatasetContentResponse struct {
	Status    *datasetContentStatusDTO `json:"status"`
	Timestamp float64                  `json:"timestamp,omitempty"`
}

// datasetContentSummary is a summary entry for ListDatasetContents.
type datasetContentSummary struct {
	Status         *datasetContentStatusDTO `json:"status"`
	Version        string                   `json:"version"`
	CreationTime   float64                  `json:"creationTime,omitempty"`
	CompletionTime float64                  `json:"completionTime,omitempty"`
}

// listDatasetContentsResponse is the response for ListDatasetContents.
type listDatasetContentsResponse struct {
	NextToken               *string                 `json:"nextToken,omitempty"`
	DatasetContentSummaries []datasetContentSummary `json:"datasetContentSummaries"`
}

// ----------------------------------------
// LoggingOptions DTOs
// ----------------------------------------

// loggingOptionsDTO is the serialized form of IoT Analytics logging options.
type loggingOptionsDTO struct {
	RoleARN string `json:"roleArn"`
	Level   string `json:"level"`
	Enabled bool   `json:"enabled"`
}

// putLoggingOptionsRequest is the request body for PutLoggingOptions.
type putLoggingOptionsRequest struct {
	LoggingOptions loggingOptionsDTO `json:"loggingOptions"`
}

// describeLoggingOptionsResponse is the response for DescribeLoggingOptions.
type describeLoggingOptionsResponse struct {
	LoggingOptions loggingOptionsDTO `json:"loggingOptions"`
}

// ----------------------------------------
// RunPipelineActivity DTOs
// ----------------------------------------

// runPipelineActivityRequest is the request body for RunPipelineActivity.
type runPipelineActivityRequest struct {
	PipelineActivity map[string]any `json:"pipelineActivity"`
	Payloads         [][]byte       `json:"payloads"`
}

// runPipelineActivityResponse is the response for RunPipelineActivity.
type runPipelineActivityResponse struct {
	LogResult string   `json:"logResult"`
	Payloads  [][]byte `json:"payloads"`
}
