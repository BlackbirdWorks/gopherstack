package firehose

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// maxRecordBytes is the maximum size of a single Firehose record (1,000 KB).
const maxRecordBytes = 1_000 * 1024

// maxBatchRecords is the maximum number of records allowed in a single PutRecordBatch call.
const maxBatchRecords = 500

// maxBatchBytes is the AWS Firehose limit on the combined payload of a
// PutRecordBatch request (4 MiB).
const maxBatchBytes = 4 * 1024 * 1024

const (
	// deliveryStreamTypeDirectPut is the default stream type for direct-put streams.
	deliveryStreamTypeDirectPut     = "DirectPut"
	deliveryStreamTypeKinesisSource = "KinesisStreamAsSource"
)

// BufferingHints controls when buffered records are delivered to S3.
type BufferingHints struct {
	SizeInMBs         int `json:"SizeInMBs"`
	IntervalInSeconds int `json:"IntervalInSeconds"`
}

// ProcessorParameter is a key-value parameter for a processor.
type ProcessorParameter struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
}

// Processor describes a single transformation step.
type Processor struct {
	Type       string               `json:"Type"`
	Parameters []ProcessorParameter `json:"Parameters,omitempty"`
}

// ProcessingConfiguration describes Lambda-based transformation.
type ProcessingConfiguration struct {
	Processors []Processor `json:"Processors,omitempty"`
	Enabled    bool        `json:"Enabled"`
}

// CloudWatchLoggingOptions configures CloudWatch logging for a destination.
type CloudWatchLoggingOptions struct {
	LogGroupName  string `json:"LogGroupName,omitempty"`
	LogStreamName string `json:"LogStreamName,omitempty"`
	Enabled       bool   `json:"Enabled"`
}

// KMSEncryptionConfig holds a KMS key ARN for S3 encryption.
type KMSEncryptionConfig struct {
	AWSKMSKeyARN string `json:"AWSKMSKeyARN"`
}

// S3EncryptionConfiguration holds the S3 object encryption config.
type S3EncryptionConfiguration struct {
	KMSEncryptionConfig *KMSEncryptionConfig `json:"KMSEncryptionConfig,omitempty"`
	NoEncryptionConfig  string               `json:"NoEncryptionConfig,omitempty"`
}

// DynamicPartitioningConfiguration controls dynamic partitioning.
type DynamicPartitioningConfiguration struct {
	RetryOptions *RetryOptions `json:"RetryOptions,omitempty"`
	Enabled      bool          `json:"Enabled"`
}

// RetryOptions holds a retry duration.
type RetryOptions struct {
	DurationInSeconds int `json:"DurationInSeconds"`
}

// EncryptionConfigInput holds the optional SSE configuration for a delivery stream.
type EncryptionConfigInput struct {
	KeyARN  string `json:"KeyARN,omitempty"`
	KeyType string `json:"KeyType"`
}

// EncryptionConfig holds the effective SSE configuration for a delivery stream.
type EncryptionConfig struct {
	FailureDescription *FailureDescription `json:"FailureDescription,omitempty"`
	KeyARN             string              `json:"KeyARN,omitempty"`
	KeyType            string              `json:"KeyType"`
	Status             string              `json:"Status"`
}

// FailureDescription holds error context for SSE failures.
type FailureDescription struct {
	Details string `json:"Details,omitempty"`
	Type    string `json:"Type,omitempty"`
}

// S3DestinationDescription holds the effective S3 destination config stored on the stream.
type S3DestinationDescription struct {
	BufferingHints                   *BufferingHints                   `json:"BufferingHints,omitempty"`
	ProcessingConfiguration          *ProcessingConfiguration          `json:"ProcessingConfiguration,omitempty"`
	S3BackupDescription              *S3BackupDescription              `json:"S3BackupDescription,omitempty"`
	EncryptionConfiguration          *S3EncryptionConfiguration        `json:"EncryptionConfiguration,omitempty"`
	CloudWatchLoggingOptions         *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions,omitempty"`
	DynamicPartitioningConfiguration *DynamicPartitioningConfiguration `json:"DynamicPartitioningConfiguration,omitempty"`
	DataFormatConversion             *DataFormatConversionConfig       `json:"DataFormatConversionConfiguration,omitempty"`
	BucketARN                        string                            `json:"BucketARN,omitempty"`
	RoleARN                          string                            `json:"RoleARN,omitempty"`
	Prefix                           string                            `json:"Prefix,omitempty"`
	ErrorOutputPrefix                string                            `json:"ErrorOutputPrefix,omitempty"`
	CompressionFormat                string                            `json:"CompressionFormat,omitempty"`
	FileExtension                    string                            `json:"FileExtension,omitempty"`
	CustomTimeZone                   string                            `json:"CustomTimeZone,omitempty"`
	DestinationID                    string                            `json:"DestinationId,omitempty"`
	S3BackupMode                     string                            `json:"S3BackupMode,omitempty"`
}

// S3BackupDescription holds the S3 backup destination configuration.
type S3BackupDescription struct {
	BufferingHints    *BufferingHints `json:"BufferingHints,omitempty"`
	BucketARN         string          `json:"BucketARN,omitempty"`
	RoleARN           string          `json:"RoleARN,omitempty"`
	Prefix            string          `json:"Prefix,omitempty"`
	CompressionFormat string          `json:"CompressionFormat,omitempty"`
}

// HTTPEndpointRequestConfiguration holds the content-encoding and attributes for HTTP requests.
type HTTPEndpointRequestConfiguration struct {
	ContentEncoding  string                        `json:"ContentEncoding,omitempty"`
	CommonAttributes []HTTPEndpointCommonAttribute `json:"CommonAttributes,omitempty"`
}

// HTTPEndpointCommonAttribute is a key-value attribute sent with HTTP requests.
type HTTPEndpointCommonAttribute struct {
	AttributeName  string `json:"AttributeName"`
	AttributeValue string `json:"AttributeValue"`
}

// HTTPEndpointDestinationDescription holds the HTTP endpoint destination config.
type HTTPEndpointDestinationDescription struct {
	ProcessingConfiguration  *ProcessingConfiguration          `json:"ProcessingConfiguration,omitempty"`
	EndpointConfiguration    *HTTPEndpointConfiguration        `json:"EndpointConfiguration,omitempty"`
	RequestConfiguration     *HTTPEndpointRequestConfiguration `json:"RequestConfiguration,omitempty"`
	BufferingHints           *BufferingHints                   `json:"BufferingHints,omitempty"`
	RetryOptions             *RetryOptions                     `json:"RetryOptions,omitempty"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions,omitempty"`
	S3BackupMode             string                            `json:"S3BackupMode,omitempty"`
	S3BackupDescription      *S3BackupDescription              `json:"S3BackupDescription,omitempty"`
	DestinationID            string                            `json:"DestinationId,omitempty"`
}

// HTTPEndpointConfiguration holds the HTTP endpoint URL and name.
type HTTPEndpointConfiguration struct {
	URL       string `json:"Url,omitempty"`
	Name      string `json:"Name,omitempty"`
	AccessKey string `json:"AccessKey,omitempty"`
}

// KinesisStreamSourceDescription describes a Kinesis stream source.
type KinesisStreamSourceDescription struct {
	DeliveryStartTimestamp string `json:"DeliveryStartTimestamp,omitempty"`
	KinesisStreamARN       string `json:"KinesisStreamARN,omitempty"`
	RoleARN                string `json:"RoleARN,omitempty"`
}

// MSKSourceDescription describes an MSK cluster source.
type MSKSourceDescription struct {
	AuthenticationConfiguration *MSKAuthenticationConfiguration `json:"AuthenticationConfiguration,omitempty"`
	MSKClusterARN               string                          `json:"MSKClusterARN,omitempty"`
	TopicName                   string                          `json:"TopicName,omitempty"`
	ReadFromTimestamp           string                          `json:"ReadFromTimestamp,omitempty"`
}

// MSKAuthenticationConfiguration holds MSK connectivity and role config.
type MSKAuthenticationConfiguration struct {
	Connectivity string `json:"Connectivity,omitempty"`
	RoleARN      string `json:"RoleARN,omitempty"`
}

// SourceDescription holds source details for non-DirectPut streams.
type SourceDescription struct {
	KinesisStreamSourceDescription *KinesisStreamSourceDescription `json:"KinesisStreamSourceDescription,omitempty"`
	MSKSourceDescription           *MSKSourceDescription           `json:"MSKSourceDescription,omitempty"`
}

// RedshiftCopyCommand holds the Redshift COPY command configuration. On the wire this
// nests under RedshiftDestinationDescription.CopyCommand (and, on the request side,
// RedshiftDestinationConfiguration.CopyCommand) rather than as flat fields.
type RedshiftCopyCommand struct {
	DataTableName    string `json:"DataTableName"`
	DataTableColumns string `json:"DataTableColumns,omitempty"`
	CopyOptions      string `json:"CopyOptions,omitempty"`
}

// RedshiftDestinationDescription holds a Redshift destination config.
type RedshiftDestinationDescription struct {
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration,omitempty"`
	RetryOptions            *RetryOptions            `json:"RetryOptions,omitempty"`
	S3BackupDescription     *S3BackupDescription     `json:"S3BackupDescription,omitempty"`
	// S3Destination is the required intermediate S3 staging location that Amazon
	// Redshift's COPY command reads from (wire field "S3DestinationDescription").
	S3Destination  *S3DestinationDescription `json:"S3DestinationDescription,omitempty"`
	CopyCommand    *RedshiftCopyCommand      `json:"CopyCommand,omitempty"`
	ClusterJDBCURL string                    `json:"ClusterJDBCURL,omitempty"`
	Username       string                    `json:"Username,omitempty"`
	RoleARN        string                    `json:"RoleARN,omitempty"`
	S3BackupMode   string                    `json:"S3BackupMode,omitempty"`
	DestinationID  string                    `json:"DestinationId,omitempty"`
}

// OpenSearchDestinationDescription holds an OpenSearch (Elasticsearch) destination config.
type OpenSearchDestinationDescription struct {
	ProcessingConfiguration  *ProcessingConfiguration  `json:"ProcessingConfiguration,omitempty"`
	BufferingHints           *BufferingHints           `json:"BufferingHints,omitempty"`
	RetryOptions             *RetryOptions             `json:"RetryOptions,omitempty"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions,omitempty"`
	S3BackupDescription      *S3BackupDescription      `json:"S3BackupDescription,omitempty"`
	DomainARN                string                    `json:"DomainARN,omitempty"`
	ClusterEndpoint          string                    `json:"ClusterEndpoint,omitempty"`
	IndexName                string                    `json:"IndexName,omitempty"`
	TypeName                 string                    `json:"TypeName,omitempty"`
	IndexRotationPeriod      string                    `json:"IndexRotationPeriod,omitempty"`
	S3BackupMode             string                    `json:"S3BackupMode,omitempty"`
	RoleARN                  string                    `json:"RoleARN,omitempty"`
	DestinationID            string                    `json:"DestinationId,omitempty"`
}

// SplunkDestinationDescription holds a Splunk HEC destination config.
type SplunkDestinationDescription struct {
	ProcessingConfiguration           *ProcessingConfiguration  `json:"ProcessingConfiguration,omitempty"`
	RetryOptions                      *RetryOptions             `json:"RetryOptions,omitempty"`
	CloudWatchLoggingOptions          *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions,omitempty"`
	S3BackupDescription               *S3BackupDescription      `json:"S3BackupDescription,omitempty"`
	HECEndpoint                       string                    `json:"HECEndpoint,omitempty"`
	HECEndpointType                   string                    `json:"HECEndpointType,omitempty"`
	HECToken                          string                    `json:"HECToken,omitempty"`
	S3BackupMode                      string                    `json:"S3BackupMode,omitempty"`
	DestinationID                     string                    `json:"DestinationId,omitempty"`
	HECAcknowledgmentTimeoutInSeconds int                       `json:"HECAcknowledgmentTimeoutInSeconds,omitempty"`
}

// DeliveryMetrics tracks delivery statistics for a stream.
type DeliveryMetrics struct {
	TotalRecords  int64 `json:"TotalRecords"`
	FailedRecords int64 `json:"FailedRecords"`
	TotalBytes    int64 `json:"TotalBytes"`
}

// DeliveryStream represents a Kinesis Firehose delivery stream.
type DeliveryStream struct {
	lastFlush               time.Time
	CreateTimestamp         time.Time                           `json:"createTimestamp"`
	LastUpdateTimestamp     time.Time                           `json:"lastUpdateTimestamp"`
	Tags                    *tags.Tags                          `json:"tags,omitempty"`
	S3Destination           *S3DestinationDescription           `json:"s3Destination,omitempty"`
	HTTPEndpointDestination *HTTPEndpointDestinationDescription `json:"httpEndpointDestination,omitempty"`
	RedshiftDestination     *RedshiftDestinationDescription     `json:"redshiftDestination,omitempty"`
	OpenSearchDestination   *OpenSearchDestinationDescription   `json:"openSearchDestination,omitempty"`
	SplunkDestination       *SplunkDestinationDescription       `json:"splunkDestination,omitempty"`
	Encryption              *EncryptionConfig                   `json:"encryption,omitempty"`
	Source                  *SourceDescription                  `json:"source,omitempty"`
	DeliveryStreamType      string                              `json:"deliveryStreamType,omitempty"`
	Name                    string                              `json:"name"`
	ARN                     string                              `json:"arn"`
	VersionID               string                              `json:"versionID,omitempty"`
	Status                  string                              `json:"status"`
	AccountID               string                              `json:"accountID"`
	Region                  string                              `json:"region"`
	Records                 [][]byte                            `json:"records,omitempty"`
	BackupRecords           [][]byte                            `json:"backupRecords,omitempty"`
	Metrics                 DeliveryMetrics                     `json:"metrics"`
	bufferSizeBytes         int
}
