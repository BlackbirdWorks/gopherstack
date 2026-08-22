package iotanalytics

import "time"

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

// IotSiteWiseCustomerManagedS3Storage is customer-managed S3 for the IoT SiteWise
// multi-layer storage variant. Unlike [CustomerManagedS3DatastoreStorage], the real AWS
// type (types.IotSiteWiseCustomerManagedDatastoreS3Storage) has no roleArn member.
type IotSiteWiseCustomerManagedS3Storage struct {
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"keyPrefix,omitempty"`
}

// IotSiteWiseMultiLayerStorage is IoT SiteWise multi-layer storage for datastores.
type IotSiteWiseMultiLayerStorage struct {
	CustomerManagedS3Storage *IotSiteWiseCustomerManagedS3Storage `json:"customerManagedS3Storage,omitempty"`
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
	LastMessageArrivalTime  float64                  `json:"lastMessageArrivalTime,omitempty"`
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
//
// ScheduleTime is the time the content generation was scheduled to start (AWS
// docs: "the time the creation of the dataset contents was scheduled to
// start", distinct from CreationTime, "the actual time the creation ... was
// started"). This backend only creates dataset content synchronously via a
// direct CreateDatasetContent call (there is no background cron-trigger
// simulation for DatasetTrigger.Schedule), so ScheduleTime is always set
// equal to CreationTime -- the same behavior AWS exhibits for a manually
// invoked CreateDatasetContent that wasn't fired by a schedule trigger.
type DatasetContent struct {
	VersionID      string  `json:"versionId"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	CompletionTime float64 `json:"completionTime"`
	ScheduleTime   float64 `json:"scheduleTime"`
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
	RetentionPeriod *RetentionPeriod `json:"retentionPeriod,omitempty"`
	ChannelName     string           `json:"channelName"`
	ChannelARN      string           `json:"channelArn"`
}

// channelSummary is a summary of a channel for list operations. AWS's
// ChannelSummary shape has no channelArn member (deserializers.go
// awsRestjson1_deserializeDocumentChannelSummary) -- unlike the full Channel
// detail, the summary omits it.
type channelSummary struct {
	ChannelStorage         *ChannelStorage `json:"channelStorage,omitempty"`
	ChannelName            string          `json:"channelName"`
	Status                 string          `json:"status"`
	CreationTime           float64         `json:"creationTime"`
	LastUpdateTime         float64         `json:"lastUpdateTime,omitempty"`
	LastMessageArrivalTime float64         `json:"lastMessageArrivalTime,omitempty"`
}

// listChannelsResponse is the response body for ListChannels.
type listChannelsResponse struct {
	NextToken        *string          `json:"nextToken,omitempty"`
	ChannelSummaries []channelSummary `json:"channelSummaries"`
}

// describeChannelResponse is the response body for DescribeChannel. AWS returns
// statistics as a sibling of channel, not nested inside it (api_op_DescribeChannel.go:
// DescribeChannelOutput has separate Channel and Statistics members).
type describeChannelResponse struct {
	Statistics *channelStatistics `json:"statistics,omitempty"`
	Channel    channelDetail      `json:"channel"`
}

// channelStatisticsSize is the estimated storage size of a channel.
type channelStatisticsSize struct {
	EstimatedSizeInBytes float64 `json:"estimatedSizeInBytes,omitempty"`
	EstimatedOn          float64 `json:"estimatedOn,omitempty"`
}

// channelStatistics holds statistics for a channel.
type channelStatistics struct {
	Size *channelStatisticsSize `json:"size,omitempty"`
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

// createDatastoreRequest is the request body for CreateDatastore. AWS's wire key
// for the partitions member is "datastorePartitions" (serializers.go
// awsRestjson1_serializeOpDocumentCreateDatastoreInput), not "partitions".
type createDatastoreRequest struct {
	DatastoreStorage        *DatastoreStorage        `json:"datastoreStorage,omitempty"`
	RetentionPeriod         *RetentionPeriod         `json:"retentionPeriod,omitempty"`
	FileFormatConfiguration *FileFormatConfiguration `json:"fileFormatConfiguration,omitempty"`
	Partitions              *DatastorePartitions     `json:"datastorePartitions,omitempty"`
	DatastoreName           string                   `json:"datastoreName"`
	Tags                    []tagDTO                 `json:"tags,omitempty"`
}

// createDatastoreResponse is the response body for CreateDatastore.
type createDatastoreResponse struct {
	RetentionPeriod *RetentionPeriod `json:"retentionPeriod,omitempty"`
	DatastoreName   string           `json:"datastoreName"`
	DatastoreARN    string           `json:"datastoreArn"`
}

// datastoreSummary is a summary of a datastore for list operations. AWS's
// DatastoreSummary shape has no datastoreArn member (deserializers.go
// awsRestjson1_deserializeDocumentDatastoreSummary) -- unlike the full Datastore
// detail, the summary omits it.
type datastoreSummary struct {
	DatastoreStorage       *DatastoreStorage `json:"datastoreStorage,omitempty"`
	DatastoreName          string            `json:"datastoreName"`
	Status                 string            `json:"status"`
	CreationTime           float64           `json:"creationTime"`
	LastUpdateTime         float64           `json:"lastUpdateTime,omitempty"`
	LastMessageArrivalTime float64           `json:"lastMessageArrivalTime,omitempty"`
}

// listDatastoresResponse is the response body for ListDatastores.
type listDatastoresResponse struct {
	NextToken          *string            `json:"nextToken,omitempty"`
	DatastoreSummaries []datastoreSummary `json:"datastoreSummaries"`
}

// describeDatastoreResponse is the response body for DescribeDatastore. AWS returns
// statistics as a sibling of datastore, not nested inside it (api_op_DescribeDatastore.go:
// DescribeDatastoreOutput has separate Datastore and Statistics members).
type describeDatastoreResponse struct {
	Statistics *datastoreStatistics `json:"statistics,omitempty"`
	Datastore  datastoreDetail      `json:"datastore"`
}

// datastoreStatisticsSize is the estimated storage size of a datastore.
type datastoreStatisticsSize struct {
	EstimatedSizeInBytes float64 `json:"estimatedSizeInBytes,omitempty"`
	EstimatedOn          float64 `json:"estimatedOn,omitempty"`
}

// datastoreStatistics holds statistics for a datastore.
type datastoreStatistics struct {
	Size *datastoreStatisticsSize `json:"size,omitempty"`
}

// datastoreDetail is a detailed view of a datastore. AWS's wire key for the
// partitions member is "datastorePartitions" (deserializers.go
// awsRestjson1_deserializeDocumentDatastore), not "partitions".
type datastoreDetail struct {
	Storage                 *DatastoreStorage        `json:"storage,omitempty"`
	RetentionPeriod         *RetentionPeriod         `json:"retentionPeriod,omitempty"`
	FileFormatConfiguration *FileFormatConfiguration `json:"fileFormatConfiguration,omitempty"`
	Partitions              *DatastorePartitions     `json:"datastorePartitions,omitempty"`
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

// datasetSummary is a summary of a dataset for list operations. AWS's DatasetSummary
// shape has no datasetArn member (deserializers.go
// awsRestjson1_deserializeDocumentDatasetSummary) -- unlike the full Dataset detail,
// the summary omits it.
type datasetSummary struct {
	DatasetName    string  `json:"datasetName"`
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
	StartTime    float64 `json:"startTime,omitempty"`
	EndTime      float64 `json:"endTime,omitempty"`
}

// pipelineSummary is a summary of a pipeline for list operations. AWS's
// PipelineSummary shape has no pipelineArn member (deserializers.go
// awsRestjson1_deserializeDocumentPipelineSummary) -- unlike the full Pipeline
// detail, the summary omits it.
type pipelineSummary struct {
	PipelineName          string                        `json:"pipelineName"`
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
	ChannelName  string `json:"-"`
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

// createDatasetContentRequest is the request body for CreateDatasetContent. AWS docs: "To
// specify versionId for a dataset content, the dataset must use a DeltaTimer filter" -- this
// backend accepts an explicit versionId unconditionally (regardless of the dataset's
// trigger/action configuration) rather than replicating that DeltaTimer-only restriction,
// since enforcing it would require modeling DeltaTimer-driven dataset content generation
// this backend does not otherwise simulate.
type createDatasetContentRequest struct {
	VersionID string `json:"versionId,omitempty"`
}

// createDatasetContentResponse is the response for CreateDatasetContent.
type createDatasetContentResponse struct {
	VersionID string `json:"versionId"`
}

// datasetContentEntry is a single data entry in a GetDatasetContent response.
type datasetContentEntry struct {
	EntryName string `json:"entryName,omitempty"`
	DataURI   string `json:"dataURI,omitempty"`
}

// getDatasetContentResponse is the response for GetDatasetContent. AWS's
// GetDatasetContentOutput has no versionId member (deserializers.go
// awsRestjson1_deserializeOpDocumentGetDatasetContentOutput: entries, status,
// timestamp only).
type getDatasetContentResponse struct {
	Status    *datasetContentStatusDTO `json:"status"`
	Entries   []datasetContentEntry    `json:"entries"`
	Timestamp float64                  `json:"timestamp,omitempty"`
}

// datasetContentSummary is a summary entry for ListDatasetContents.
type datasetContentSummary struct {
	Status         *datasetContentStatusDTO `json:"status"`
	Version        string                   `json:"version"`
	CreationTime   float64                  `json:"creationTime,omitempty"`
	CompletionTime float64                  `json:"completionTime,omitempty"`
	ScheduleTime   float64                  `json:"scheduleTime,omitempty"`
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
	PipelineActivity PipelineActivity `json:"pipelineActivity"`
	Payloads         [][]byte         `json:"payloads"`
}

// runPipelineActivityResponse is the response for RunPipelineActivity.
type runPipelineActivityResponse struct {
	LogResult string   `json:"logResult"`
	Payloads  [][]byte `json:"payloads"`
}
