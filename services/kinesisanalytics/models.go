package kinesisanalytics

import "time"

// Application represents a Kinesis Analytics v1 application.
type Application struct {
	LastUpdateTimestamp      *time.Time                       `json:"LastUpdateTimestamp,omitempty"`
	Tags                     map[string]string                `json:"Tags,omitempty"`
	CreateTimestamp          *time.Time                       `json:"CreateTimestamp,omitempty"`
	ApplicationARN           string                           `json:"ApplicationARN"`
	ApplicationStatus        string                           `json:"ApplicationStatus"`
	ApplicationDescription   string                           `json:"ApplicationDescription,omitempty"`
	ApplicationCode          string                           `json:"ApplicationCode,omitempty"`
	ApplicationName          string                           `json:"ApplicationName"`
	CloudWatchLoggingOptions []CloudWatchLoggingOptionDesc    `json:"CloudWatchLoggingOptions,omitempty"`
	ReferenceDataSources     []ReferenceDataSourceDescription `json:"ReferenceDataSources,omitempty"`
	Outputs                  []OutputDescription              `json:"Outputs,omitempty"`
	Inputs                   []InputDescription               `json:"Inputs,omitempty"`
	ApplicationVersionID     int64                            `json:"ApplicationVersionId"`
}

// CloudWatchLoggingOptionDesc describes a CloudWatch logging option.
type CloudWatchLoggingOptionDesc struct {
	CloudWatchLoggingOptionID string `json:"CloudWatchLoggingOptionId"`
	LogStreamARN              string `json:"LogStreamARN"`
	RoleARN                   string `json:"RoleARN,omitempty"`
}

// LambdaProcessorDesc describes a Lambda input processor.
type LambdaProcessorDesc struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN,omitempty"`
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
	RoleARN     string `json:"RoleARN,omitempty"`
}

// KinesisFirehoseOutputDesc describes a Kinesis Firehose output.
type KinesisFirehoseOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN,omitempty"`
}

// LambdaOutputDesc describes a Lambda output.
type LambdaOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN,omitempty"`
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

// RecordColumn describes a column mapping in the schema.
type RecordColumn struct {
	Name    string `json:"Name"`
	SQLType string `json:"SqlType"`
	Mapping string `json:"Mapping,omitempty"`
}

// JSONMappingParameters holds the JSON row path.
type JSONMappingParameters struct {
	RecordRowPath string `json:"RecordRowPath"`
}

// CSVMappingParameters holds CSV delimiters.
type CSVMappingParameters struct {
	RecordColumnDelimiter string `json:"RecordColumnDelimiter"`
	RecordRowDelimiter    string `json:"RecordRowDelimiter"`
}

// MappingParameters holds format-specific mapping parameters.
type MappingParameters struct {
	JSONMappingParameters *JSONMappingParameters `json:"JSONMappingParameters,omitempty"`
	CSVMappingParameters  *CSVMappingParameters  `json:"CSVMappingParameters,omitempty"`
}

// RecordFormat describes the record format type and mapping.
type RecordFormat struct {
	MappingParameters *MappingParameters `json:"MappingParameters,omitempty"`
	RecordFormatType  string             `json:"RecordFormatType"`
}

// SourceSchema describes the schema of the input records.
type SourceSchema struct {
	RecordFormat   RecordFormat   `json:"RecordFormat"`
	RecordEncoding string         `json:"RecordEncoding,omitempty"`
	RecordColumns  []RecordColumn `json:"RecordColumns"`
}

// S3ReferenceDataSourceDesc describes the S3 source for reference data.
type S3ReferenceDataSourceDesc struct {
	BucketARN string `json:"BucketARN"`
	FileKey   string `json:"FileKey"`
	RoleARN   string `json:"RoleARN,omitempty"`
}

// ReferenceDataSourceDescription describes a reference data source.
type ReferenceDataSourceDescription struct {
	S3ReferenceDataSourceDescription *S3ReferenceDataSourceDesc `json:"S3ReferenceDataSourceDescription,omitempty"`
	ReferenceSchema                  *SourceSchema              `json:"ReferenceSchema,omitempty"`
	ReferenceID                      string                     `json:"ReferenceId"`
	TableName                        string                     `json:"TableName,omitempty"`
}

// applicationSummary is the short form returned by ListApplications.
type applicationSummary struct {
	ApplicationARN    string `json:"ApplicationARN"`
	ApplicationName   string `json:"ApplicationName"`
	ApplicationStatus string `json:"ApplicationStatus"`
}

// createApplicationInput is the request body for CreateApplication.
type createApplicationInput struct {
	ApplicationName        string     `json:"ApplicationName"`
	ApplicationDescription string     `json:"ApplicationDescription"`
	ApplicationCode        string     `json:"ApplicationCode"`
	Tags                   []tagEntry `json:"Tags"`
}

// createApplicationOutput is the response body for CreateApplication.
type createApplicationOutput struct {
	ApplicationSummary applicationSummary `json:"ApplicationSummary"`
}

// deleteApplicationInput is the request body for DeleteApplication.
type deleteApplicationInput struct {
	ApplicationName string  `json:"ApplicationName"`
	CreateTimestamp float64 `json:"CreateTimestamp"`
}

// describeApplicationInput is the request body for DescribeApplication.
type describeApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// applicationDetail is the full application detail returned by DescribeApplication.
type applicationDetail struct {
	ApplicationARN                      string                           `json:"ApplicationARN"`
	ApplicationName                     string                           `json:"ApplicationName"`
	ApplicationStatus                   string                           `json:"ApplicationStatus"`
	ApplicationCode                     string                           `json:"ApplicationCode,omitempty"`
	ApplicationDescription              string                           `json:"ApplicationDescription,omitempty"`
	CloudWatchLoggingOptionDescriptions []CloudWatchLoggingOptionDesc    `json:"CloudWatchLoggingOptionDescriptions,omitempty"` //nolint:lll // AWS API name
	InputDescriptions                   []InputDescription               `json:"InputDescriptions,omitempty"`
	OutputDescriptions                  []OutputDescription              `json:"OutputDescriptions,omitempty"`
	ReferenceDataSourceDescriptions     []ReferenceDataSourceDescription `json:"ReferenceDataSourceDescriptions,omitempty"`
	ApplicationVersionID                int64                            `json:"ApplicationVersionId"`
	CreateTimestamp                     float64                          `json:"CreateTimestamp,omitempty"`
	LastUpdateTimestamp                 float64                          `json:"LastUpdateTimestamp,omitempty"`
}

// describeApplicationOutput is the response body for DescribeApplication.
type describeApplicationOutput struct {
	ApplicationDetail applicationDetail `json:"ApplicationDetail"`
}

// listApplicationsInput is the request body for ListApplications.
type listApplicationsInput struct {
	ExclusiveStartApplicationName string `json:"ExclusiveStartApplicationName"`
	Limit                         int    `json:"Limit"`
}

// listApplicationsOutput is the response body for ListApplications.
type listApplicationsOutput struct {
	ApplicationSummaries []applicationSummary `json:"ApplicationSummaries"`
	HasMoreApplications  bool                 `json:"HasMoreApplications"`
}

// startApplicationInput is the request body for StartApplication.
type startApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// stopApplicationInput is the request body for StopApplication.
type stopApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// updateApplicationInput is the request body for UpdateApplication.
type updateApplicationInput struct {
	ApplicationUpdate           *applicationUpdate `json:"ApplicationUpdate"`
	ApplicationName             string             `json:"ApplicationName"`
	CurrentApplicationVersionID int64              `json:"CurrentApplicationVersionId"`
}

// applicationUpdate holds optional update fields.
type applicationUpdate struct {
	ApplicationCodeUpdate string `json:"ApplicationCodeUpdate,omitempty"`
}

// tagEntry is a key-value tag pair.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// listTagsForResourceInput is the request body for ListTagsForResource.
type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

// listTagsForResourceOutput is the response body for ListTagsForResource.
type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"Tags"`
}

// tagResourceInput is the request body for TagResource.
type tagResourceInput struct {
	ResourceARN string     `json:"ResourceARN"`
	Tags        []tagEntry `json:"Tags"`
}

// untagResourceInput is the request body for UntagResource.
type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

// errorResponse is the standard Kinesis Analytics error response body.
type errorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

type addApplicationCloudWatchLoggingOptionInput struct {
	CloudWatchLoggingOption     *cwlOptionInput `json:"CloudWatchLoggingOption"`
	ApplicationName             string          `json:"ApplicationName"`
	CurrentApplicationVersionID int64           `json:"CurrentApplicationVersionId"`
}

type cwlOptionInput struct {
	LogStreamARN string `json:"LogStreamARN"`
	RoleARN      string `json:"RoleARN"`
}

type addApplicationInputInput struct {
	Input                       *applicationInputConfig `json:"Input"`
	ApplicationName             string                  `json:"ApplicationName"`
	CurrentApplicationVersionID int64                   `json:"CurrentApplicationVersionId"`
}

type applicationInputConfig struct {
	KinesisStreamsInput  *kinesisStreamsInputConfig  `json:"KinesisStreamsInput,omitempty"`
	KinesisFirehoseInput *kinesisFirehoseInputConfig `json:"KinesisFirehoseInput,omitempty"`
	NamePrefix           string                      `json:"NamePrefix,omitempty"`
}

type kinesisStreamsInputConfig struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN"`
}

type kinesisFirehoseInputConfig struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN"`
}

type addApplicationInputProcessingConfigurationInput struct {
	InputProcessingConfiguration *inputProcessingConfigInput `json:"InputProcessingConfiguration"`
	ApplicationName              string                      `json:"ApplicationName"`
	InputID                      string                      `json:"InputId"`
	CurrentApplicationVersionID  int64                       `json:"CurrentApplicationVersionId"`
}

type inputProcessingConfigInput struct {
	InputLambdaProcessor *lambdaProcessorInput `json:"InputLambdaProcessor"`
}

type lambdaProcessorInput struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN"`
}

type addApplicationOutputInput struct {
	Output                      *applicationOutputConfig `json:"Output"`
	ApplicationName             string                   `json:"ApplicationName"`
	CurrentApplicationVersionID int64                    `json:"CurrentApplicationVersionId"`
}

type applicationOutputConfig struct {
	KinesisStreamsOutput  *kinesisStreamsOutputConfig  `json:"KinesisStreamsOutput,omitempty"`
	KinesisFirehoseOutput *kinesisFirehoseOutputConfig `json:"KinesisFirehoseOutput,omitempty"`
	LambdaOutput          *lambdaOutputConfig          `json:"LambdaOutput,omitempty"`
	DestinationSchema     *destinationSchemaInput      `json:"DestinationSchema,omitempty"`
	Name                  string                       `json:"Name,omitempty"`
}

type kinesisStreamsOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN"`
}

type kinesisFirehoseOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN"`
}

type lambdaOutputConfig struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN"`
}

type destinationSchemaInput struct {
	RecordFormatType string `json:"RecordFormatType"`
}

type addApplicationReferenceDataSourceInput struct {
	ReferenceDataSource         *referenceDataSourceConfig `json:"ReferenceDataSource"`
	ApplicationName             string                     `json:"ApplicationName"`
	CurrentApplicationVersionID int64                      `json:"CurrentApplicationVersionId"`
}

type referenceDataSourceConfig struct {
	S3ReferenceDataSource *s3ReferenceDataSourceConfig `json:"S3ReferenceDataSource,omitempty"`
	ReferenceSchema       *sourceSchemaInput           `json:"ReferenceSchema,omitempty"`
	TableName             string                       `json:"TableName,omitempty"`
}

type s3ReferenceDataSourceConfig struct {
	BucketARN string `json:"BucketARN"`
	FileKey   string `json:"FileKey"`
	RoleARN   string `json:"RoleARN"`
}

type sourceSchemaInput struct {
	RecordFormat   recordFormatInput `json:"RecordFormat"`
	RecordEncoding string            `json:"RecordEncoding,omitempty"`
	RecordColumns  []RecordColumn    `json:"RecordColumns"`
}

type recordFormatInput struct {
	MappingParameters *MappingParameters `json:"MappingParameters,omitempty"`
	RecordFormatType  string             `json:"RecordFormatType"`
}

type deleteApplicationCloudWatchLoggingOptionInput struct {
	CloudWatchLoggingOptionID   string `json:"CloudWatchLoggingOptionId"`
	ApplicationName             string `json:"ApplicationName"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type deleteApplicationInputProcessingConfigurationInput struct {
	ApplicationName             string `json:"ApplicationName"`
	InputID                     string `json:"InputId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type deleteApplicationOutputInput struct {
	ApplicationName             string `json:"ApplicationName"`
	OutputID                    string `json:"OutputId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type deleteApplicationReferenceDataSourceInput struct {
	ApplicationName             string `json:"ApplicationName"`
	ReferenceID                 string `json:"ReferenceId"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type discoverInputSchemaInput struct {
	InputProcessingConfiguration *inputProcessingConfigInput `json:"InputProcessingConfiguration,omitempty"`
	ResourceARN                  string                      `json:"ResourceARN,omitempty"`
	RoleARN                      string                      `json:"RoleARN,omitempty"`
}

type discoverInputSchemaOutput struct {
	InputSchema           *SourceSchema `json:"InputSchema,omitempty"`
	ParsedInputRecords    [][]string    `json:"ParsedInputRecords"`
	ProcessedInputRecords []string      `json:"ProcessedInputRecords"`
	RawInputRecords       []string      `json:"RawInputRecords"`
}
