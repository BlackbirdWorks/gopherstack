package firehose

import (
	"context"
	"fmt"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const maxListLimit = 10000

// s3DestinationInput holds the S3 destination configuration from the API request.
// It maps both S3DestinationConfiguration and ExtendedS3DestinationConfiguration fields.
type s3DestinationInput struct {
	BufferingHints                    *BufferingHints                   `json:"BufferingHints"`
	ProcessingConfiguration           *ProcessingConfiguration          `json:"ProcessingConfiguration"`
	S3BackupConfiguration             *s3BackupInput                    `json:"S3BackupConfiguration"`
	EncryptionConfiguration           *S3EncryptionConfiguration        `json:"EncryptionConfiguration"`
	CloudWatchLoggingOptions          *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions"`
	DynamicPartitioningConfiguration  *DynamicPartitioningConfiguration `json:"DynamicPartitioningConfiguration"`
	DataFormatConversionConfiguration *DataFormatConversionConfig       `json:"DataFormatConversionConfiguration"`
	BucketARN                         string                            `json:"BucketARN"`
	RoleARN                           string                            `json:"RoleARN"`
	Prefix                            string                            `json:"Prefix"`
	ErrorOutputPrefix                 string                            `json:"ErrorOutputPrefix"`
	CompressionFormat                 string                            `json:"CompressionFormat"`
	FileExtension                     string                            `json:"FileExtension"`
	CustomTimeZone                    string                            `json:"CustomTimeZone"`
	S3BackupMode                      string                            `json:"S3BackupMode"`
}

// s3BackupInput holds the S3 backup destination configuration.
type s3BackupInput struct {
	BufferingHints    *BufferingHints `json:"BufferingHints"`
	BucketARN         string          `json:"BucketARN"`
	RoleARN           string          `json:"RoleARN"`
	Prefix            string          `json:"Prefix"`
	CompressionFormat string          `json:"CompressionFormat"`
}

// httpEndpointDestinationInput holds HTTP endpoint destination configuration.
type httpEndpointDestinationInput struct {
	EndpointConfiguration    *httpEndpointConfigurationInput   `json:"EndpointConfiguration"`
	ProcessingConfiguration  *ProcessingConfiguration          `json:"ProcessingConfiguration"`
	S3BackupConfiguration    *s3BackupInput                    `json:"S3BackupConfiguration"`
	RequestConfiguration     *HTTPEndpointRequestConfiguration `json:"RequestConfiguration"`
	BufferingHints           *BufferingHints                   `json:"BufferingHints"`
	RetryOptions             *RetryOptions                     `json:"RetryOptions"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions"`
	S3BackupMode             string                            `json:"S3BackupMode"`
}

// httpEndpointConfigurationInput holds the HTTP endpoint URL and name.
type httpEndpointConfigurationInput struct {
	URL       string `json:"Url"`
	Name      string `json:"Name"`
	AccessKey string `json:"AccessKey"`
}

// kinesisStreamSrcInput holds Kinesis stream source config.
type kinesisStreamSrcInput struct {
	KinesisStreamARN string `json:"KinesisStreamARN"`
	RoleARN          string `json:"RoleARN"`
}

// mskSourceConfigurationInput holds MSK cluster source config.
type mskSourceConfigurationInput struct {
	AuthenticationConfiguration *MSKAuthenticationConfiguration `json:"AuthenticationConfiguration"`
	MSKClusterARN               string                          `json:"MSKClusterARN"`
	TopicName                   string                          `json:"TopicName"`
	ReadFromTimestamp           string                          `json:"ReadFromTimestamp"`
}

// redshiftDestinationInput holds the Redshift destination configuration.
// redshiftCopyCommandInput holds the Redshift COPY command configuration. AWS nests
// these fields under RedshiftDestinationConfiguration.CopyCommand on the wire, not as
// flat fields on the destination configuration itself.
type redshiftCopyCommandInput struct {
	DataTableName    string `json:"DataTableName"`
	DataTableColumns string `json:"DataTableColumns"`
	CopyOptions      string `json:"CopyOptions"`
}

type redshiftDestinationInput struct {
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration"`
	RetryOptions            *RetryOptions            `json:"RetryOptions"`
	// S3Configuration is the required intermediate S3 staging location Redshift's COPY
	// command reads from; distinct from S3BackupConfiguration (used only in backup mode).
	S3Configuration       *s3DestinationInput       `json:"S3Configuration"`
	S3BackupConfiguration *s3BackupInput            `json:"S3BackupConfiguration"`
	CopyCommand           *redshiftCopyCommandInput `json:"CopyCommand"`
	ClusterJDBCURL        string                    `json:"ClusterJDBCURL"`
	RoleARN               string                    `json:"RoleARN"`
	S3BackupMode          string                    `json:"S3BackupMode"`
	Username              string                    `json:"Username"`
}

// openSearchDestinationInput holds the OpenSearch destination configuration.
type openSearchDestinationInput struct {
	ProcessingConfiguration  *ProcessingConfiguration  `json:"ProcessingConfiguration"`
	BufferingHints           *BufferingHints           `json:"BufferingHints"`
	RetryOptions             *RetryOptions             `json:"RetryOptions"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions"`
	S3BackupConfiguration    *s3BackupInput            `json:"S3BackupConfiguration"`
	DomainARN                string                    `json:"DomainARN"`
	ClusterEndpoint          string                    `json:"ClusterEndpoint"`
	IndexName                string                    `json:"IndexName"`
	TypeName                 string                    `json:"TypeName"`
	IndexRotationPeriod      string                    `json:"IndexRotationPeriod"`
	S3BackupMode             string                    `json:"S3BackupMode"`
	RoleARN                  string                    `json:"RoleARN"`
}

// elasticsearchDestinationInput holds the legacy Elasticsearch destination configuration.
type elasticsearchDestinationInput struct {
	ProcessingConfiguration  *ProcessingConfiguration  `json:"ProcessingConfiguration"`
	BufferingHints           *BufferingHints           `json:"BufferingHints"`
	RetryOptions             *RetryOptions             `json:"RetryOptions"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions"`
	S3Configuration          *s3DestinationInput       `json:"S3Configuration"`
	DomainARN                string                    `json:"DomainARN"`
	ClusterEndpoint          string                    `json:"ClusterEndpoint"`
	IndexName                string                    `json:"IndexName"`
	TypeName                 string                    `json:"TypeName"`
	IndexRotationPeriod      string                    `json:"IndexRotationPeriod"`
	S3BackupMode             string                    `json:"S3BackupMode"`
	RoleARN                  string                    `json:"RoleARN"`
}

// splunkDestinationInput holds the Splunk HEC destination configuration.
type splunkDestinationInput struct {
	ProcessingConfiguration           *ProcessingConfiguration  `json:"ProcessingConfiguration"`
	RetryOptions                      *RetryOptions             `json:"RetryOptions"`
	CloudWatchLoggingOptions          *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions"`
	S3BackupConfiguration             *s3BackupInput            `json:"S3BackupConfiguration"`
	HECEndpoint                       string                    `json:"HECEndpoint"`
	HECEndpointType                   string                    `json:"HECEndpointType"`
	HECToken                          string                    `json:"HECToken"`
	S3BackupMode                      string                    `json:"S3BackupMode"`
	HECAcknowledgmentTimeoutInSeconds int                       `json:"HECAcknowledgmentTimeoutInSeconds"`
}

// aosDeliveryField holds the AmazonOpenSearch field separately so its long name
// does not drive gofmt alignment in createDeliveryStreamInput. Embedding keeps
// JSON marshaling transparent.
type aosDeliveryField struct {
	AmazonOpenSearchServiceDestinationConfiguration *openSearchDestinationInput `json:"AmazonOpenSearchServiceDestinationConfiguration"` //nolint:lll // AWS field name
}

// icebergDestinationInput holds the Apache Iceberg Tables destination configuration.
type icebergDestinationInput struct {
	BufferingHints                    *BufferingHints                 `json:"BufferingHints"`
	CatalogConfiguration              *CatalogConfiguration           `json:"CatalogConfiguration"`
	CloudWatchLoggingOptions          *CloudWatchLoggingOptions       `json:"CloudWatchLoggingOptions"`
	ProcessingConfiguration           *ProcessingConfiguration        `json:"ProcessingConfiguration"`
	RetryOptions                      *RetryOptions                   `json:"RetryOptions"`
	S3Configuration                   *s3DestinationInput             `json:"S3Configuration"`
	SchemaEvolutionConfiguration      *SchemaEvolutionConfiguration   `json:"SchemaEvolutionConfiguration"`
	TableCreationConfiguration        *TableCreationConfiguration     `json:"TableCreationConfiguration"`
	RoleARN                           string                          `json:"RoleARN"`
	S3BackupMode                      string                          `json:"S3BackupMode"`
	DestinationTableConfigurationList []DestinationTableConfiguration `json:"DestinationTableConfigurationList"`
	AppendOnly                        bool                            `json:"AppendOnly"`
}

// snowflakeDestinationInput holds the Snowflake destination configuration.
type snowflakeDestinationInput struct {
	BufferingHints              *SnowflakeBufferingHints     `json:"BufferingHints"`
	CloudWatchLoggingOptions    *CloudWatchLoggingOptions    `json:"CloudWatchLoggingOptions"`
	ProcessingConfiguration     *ProcessingConfiguration     `json:"ProcessingConfiguration"`
	RetryOptions                *SnowflakeRetryOptions       `json:"RetryOptions"`
	S3Configuration             *s3DestinationInput          `json:"S3Configuration"`
	SecretsManagerConfiguration *SecretsManagerConfiguration `json:"SecretsManagerConfiguration"`
	SnowflakeRoleConfiguration  *SnowflakeRoleConfiguration  `json:"SnowflakeRoleConfiguration"`
	SnowflakeVpcConfiguration   *SnowflakeVpcConfiguration   `json:"SnowflakeVpcConfiguration"`
	AccountURL                  string                       `json:"AccountUrl"`
	ContentColumnName           string                       `json:"ContentColumnName"`
	DataLoadingOption           string                       `json:"DataLoadingOption"`
	Database                    string                       `json:"Database"`
	KeyPassphrase               string                       `json:"KeyPassphrase"`
	MetaDataColumnName          string                       `json:"MetaDataColumnName"`
	PrivateKey                  string                       `json:"PrivateKey"`
	RoleARN                     string                       `json:"RoleARN"`
	S3BackupMode                string                       `json:"S3BackupMode"`
	Schema                      string                       `json:"Schema"`
	Table                       string                       `json:"Table"`
	User                        string                       `json:"User"`
}

type createDeliveryStreamInput struct {
	aosDeliveryField
	S3DestinationConfiguration            *s3DestinationInput            `json:"S3DestinationConfiguration"`
	ExtendedS3DestinationConfiguration    *s3DestinationInput            `json:"ExtendedS3DestinationConfiguration"`
	HTTPEndpointDestinationConfiguration  *httpEndpointDestinationInput  `json:"HTTPEndpointDestinationConfiguration"`
	KinesisStreamSourceConfiguration      *kinesisStreamSrcInput         `json:"KinesisStreamSourceConfiguration"`
	MSKSourceConfiguration                *mskSourceConfigurationInput   `json:"MSKSourceConfiguration"`
	RedshiftDestinationConfiguration      *redshiftDestinationInput      `json:"RedshiftDestinationConfiguration"`
	ElasticsearchDestinationConfiguration *elasticsearchDestinationInput `json:"ElasticsearchDestinationConfiguration"` //nolint:lll // AWS field name
	SplunkDestinationConfiguration        *splunkDestinationInput        `json:"SplunkDestinationConfiguration"`
	IcebergDestinationConfiguration       *icebergDestinationInput       `json:"IcebergDestinationConfiguration"`
	SnowflakeDestinationConfiguration     *snowflakeDestinationInput     `json:"SnowflakeDestinationConfiguration"`
	DeliveryStreamName                    string                         `json:"DeliveryStreamName"`
	DeliveryStreamType                    string                         `json:"DeliveryStreamType"`
	Tags                                  []svcTags.KV                   `json:"Tags"`
}

type createDeliveryStreamOutput struct {
	DeliveryStreamARN string `json:"DeliveryStreamARN"`
}

// buildS3DestinationDescription converts an s3DestinationInput to the backend type.
func buildS3DestinationDescription(raw *s3DestinationInput) *S3DestinationDescription {
	if raw == nil {
		return nil
	}

	dest := &S3DestinationDescription{
		BucketARN:                        raw.BucketARN,
		RoleARN:                          raw.RoleARN,
		Prefix:                           raw.Prefix,
		ErrorOutputPrefix:                raw.ErrorOutputPrefix,
		CompressionFormat:                raw.CompressionFormat,
		FileExtension:                    raw.FileExtension,
		CustomTimeZone:                   raw.CustomTimeZone,
		BufferingHints:                   raw.BufferingHints,
		ProcessingConfiguration:          raw.ProcessingConfiguration,
		S3BackupMode:                     raw.S3BackupMode,
		EncryptionConfiguration:          raw.EncryptionConfiguration,
		CloudWatchLoggingOptions:         raw.CloudWatchLoggingOptions,
		DynamicPartitioningConfiguration: raw.DynamicPartitioningConfiguration,
		DataFormatConversion:             raw.DataFormatConversionConfiguration,
	}

	dest.S3BackupDescription = buildS3BackupDescription(raw.S3BackupConfiguration)

	return dest
}

// buildHTTPEndpointDestination converts httpEndpointDestinationInput to the backend type.
func buildHTTPEndpointDestination(ep *httpEndpointDestinationInput) *HTTPEndpointDestinationDescription {
	if ep == nil {
		return nil
	}

	dest := &HTTPEndpointDestinationDescription{
		ProcessingConfiguration:  ep.ProcessingConfiguration,
		S3BackupMode:             ep.S3BackupMode,
		RequestConfiguration:     ep.RequestConfiguration,
		BufferingHints:           ep.BufferingHints,
		RetryOptions:             ep.RetryOptions,
		CloudWatchLoggingOptions: ep.CloudWatchLoggingOptions,
	}

	if ep.EndpointConfiguration != nil {
		dest.EndpointConfiguration = &HTTPEndpointConfiguration{
			URL:       ep.EndpointConfiguration.URL,
			Name:      ep.EndpointConfiguration.Name,
			AccessKey: ep.EndpointConfiguration.AccessKey,
		}
	}

	if ep.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(ep.S3BackupConfiguration)
	}

	return dest
}

// buildRedshiftDestination converts redshiftDestinationInput to the backend type.
func buildRedshiftDestination(rs *redshiftDestinationInput) *RedshiftDestinationDescription {
	if rs == nil {
		return nil
	}

	dest := &RedshiftDestinationDescription{
		ClusterJDBCURL:          rs.ClusterJDBCURL,
		RoleARN:                 rs.RoleARN,
		S3BackupMode:            rs.S3BackupMode,
		ProcessingConfiguration: rs.ProcessingConfiguration,
		RetryOptions:            rs.RetryOptions,
		Username:                rs.Username,
		S3Destination:           buildS3DestinationDescription(rs.S3Configuration),
	}

	if rs.CopyCommand != nil {
		dest.CopyCommand = &RedshiftCopyCommand{
			DataTableName:    rs.CopyCommand.DataTableName,
			DataTableColumns: rs.CopyCommand.DataTableColumns,
			CopyOptions:      rs.CopyCommand.CopyOptions,
		}
	}

	if rs.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(rs.S3BackupConfiguration)
	}

	return dest
}

// buildOpenSearchDestination converts openSearchDestinationInput to the backend type.
func buildOpenSearchDestination(os *openSearchDestinationInput) *OpenSearchDestinationDescription {
	if os == nil {
		return nil
	}

	dest := &OpenSearchDestinationDescription{
		DomainARN:                os.DomainARN,
		ClusterEndpoint:          os.ClusterEndpoint,
		IndexName:                os.IndexName,
		TypeName:                 os.TypeName,
		IndexRotationPeriod:      os.IndexRotationPeriod,
		S3BackupMode:             os.S3BackupMode,
		RoleARN:                  os.RoleARN,
		ProcessingConfiguration:  os.ProcessingConfiguration,
		BufferingHints:           os.BufferingHints,
		RetryOptions:             os.RetryOptions,
		CloudWatchLoggingOptions: os.CloudWatchLoggingOptions,
	}

	if os.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(os.S3BackupConfiguration)
	}

	return dest
}

// buildElasticsearchDestination converts elasticsearchDestinationInput to the backend type.
// This is the legacy Elasticsearch destination shape, wire-distinct from the newer
// AmazonopensearchserviceDestinationConfiguration family (see ElasticsearchDestinationDescription).
func buildElasticsearchDestination(es *elasticsearchDestinationInput) *ElasticsearchDestinationDescription {
	if es == nil {
		return nil
	}

	dest := &ElasticsearchDestinationDescription{
		DomainARN:                es.DomainARN,
		ClusterEndpoint:          es.ClusterEndpoint,
		IndexName:                es.IndexName,
		TypeName:                 es.TypeName,
		IndexRotationPeriod:      es.IndexRotationPeriod,
		S3BackupMode:             es.S3BackupMode,
		RoleARN:                  es.RoleARN,
		ProcessingConfiguration:  es.ProcessingConfiguration,
		BufferingHints:           es.BufferingHints,
		RetryOptions:             es.RetryOptions,
		CloudWatchLoggingOptions: es.CloudWatchLoggingOptions,
	}

	// AWS models S3Configuration as the required backup destination for legacy
	// Elasticsearch (distinct from the optional-backup pattern used elsewhere).
	if es.S3Configuration != nil {
		dest.S3BackupDescription = &S3BackupDescription{
			BucketARN:         es.S3Configuration.BucketARN,
			RoleARN:           es.S3Configuration.RoleARN,
			Prefix:            es.S3Configuration.Prefix,
			CompressionFormat: es.S3Configuration.CompressionFormat,
			BufferingHints:    es.S3Configuration.BufferingHints,
		}
	}

	return dest
}

// buildSplunkDestination converts splunkDestinationInput to the backend type.
func buildSplunkDestination(sp *splunkDestinationInput) *SplunkDestinationDescription {
	if sp == nil {
		return nil
	}

	dest := &SplunkDestinationDescription{
		HECEndpoint:                       sp.HECEndpoint,
		HECEndpointType:                   sp.HECEndpointType,
		HECToken:                          sp.HECToken,
		S3BackupMode:                      sp.S3BackupMode,
		HECAcknowledgmentTimeoutInSeconds: sp.HECAcknowledgmentTimeoutInSeconds,
		ProcessingConfiguration:           sp.ProcessingConfiguration,
		RetryOptions:                      sp.RetryOptions,
		CloudWatchLoggingOptions:          sp.CloudWatchLoggingOptions,
	}

	if sp.S3BackupConfiguration != nil {
		dest.S3BackupDescription = buildS3BackupDescription(sp.S3BackupConfiguration)
	}

	return dest
}

// buildIcebergDestination converts icebergDestinationInput to the backend type.
func buildIcebergDestination(ic *icebergDestinationInput) *IcebergDestinationDescription {
	if ic == nil {
		return nil
	}

	return &IcebergDestinationDescription{
		BufferingHints:                    ic.BufferingHints,
		CatalogConfiguration:              ic.CatalogConfiguration,
		CloudWatchLoggingOptions:          ic.CloudWatchLoggingOptions,
		ProcessingConfiguration:           ic.ProcessingConfiguration,
		RetryOptions:                      ic.RetryOptions,
		S3Destination:                     buildS3DestinationDescription(ic.S3Configuration),
		SchemaEvolutionConfiguration:      ic.SchemaEvolutionConfiguration,
		TableCreationConfiguration:        ic.TableCreationConfiguration,
		DestinationTableConfigurationList: ic.DestinationTableConfigurationList,
		RoleARN:                           ic.RoleARN,
		S3BackupMode:                      ic.S3BackupMode,
		AppendOnly:                        ic.AppendOnly,
	}
}

// buildSnowflakeDestination converts snowflakeDestinationInput to the backend type.
func buildSnowflakeDestination(sf *snowflakeDestinationInput) *SnowflakeDestinationDescription {
	if sf == nil {
		return nil
	}

	return &SnowflakeDestinationDescription{
		BufferingHints:              sf.BufferingHints,
		CloudWatchLoggingOptions:    sf.CloudWatchLoggingOptions,
		ProcessingConfiguration:     sf.ProcessingConfiguration,
		RetryOptions:                sf.RetryOptions,
		S3Destination:               buildS3DestinationDescription(sf.S3Configuration),
		SecretsManagerConfiguration: sf.SecretsManagerConfiguration,
		SnowflakeRoleConfiguration:  sf.SnowflakeRoleConfiguration,
		SnowflakeVpcConfiguration:   sf.SnowflakeVpcConfiguration,
		AccountURL:                  sf.AccountURL,
		ContentColumnName:           sf.ContentColumnName,
		DataLoadingOption:           sf.DataLoadingOption,
		Database:                    sf.Database,
		MetaDataColumnName:          sf.MetaDataColumnName,
		RoleARN:                     sf.RoleARN,
		S3BackupMode:                sf.S3BackupMode,
		Schema:                      sf.Schema,
		Table:                       sf.Table,
		User:                        sf.User,
	}
}

// buildS3BackupDescription converts an s3BackupInput to the backend type.
func buildS3BackupDescription(b *s3BackupInput) *S3BackupDescription {
	if b == nil {
		return nil
	}

	return &S3BackupDescription{
		BucketARN:         b.BucketARN,
		RoleARN:           b.RoleARN,
		Prefix:            b.Prefix,
		CompressionFormat: b.CompressionFormat,
		BufferingHints:    b.BufferingHints,
	}
}

// buildSourceDescription converts source config inputs to the backend type.
func buildSourceDescription(
	ks *kinesisStreamSrcInput,
	msk *mskSourceConfigurationInput,
) *SourceDescription {
	if ks != nil {
		return &SourceDescription{
			KinesisStreamSourceDescription: &KinesisStreamSourceDescription{
				KinesisStreamARN: ks.KinesisStreamARN,
				RoleARN:          ks.RoleARN,
			},
		}
	}

	if msk != nil {
		return &SourceDescription{
			MSKSourceDescription: &MSKSourceDescription{
				MSKClusterARN:               msk.MSKClusterARN,
				TopicName:                   msk.TopicName,
				ReadFromTimestamp:           msk.ReadFromTimestamp,
				AuthenticationConfiguration: msk.AuthenticationConfiguration,
			},
		}
	}

	return nil
}

// validateSingleDestination rejects a CreateDeliveryStream request that names more than
// one destination configuration. Real AWS accepts exactly one destination type per call
// (S3DestinationConfiguration and ExtendedS3DestinationConfiguration are mutually exclusive
// aliases for the same "S3 family" slot and are counted together).
func validateSingleDestination(in *createDeliveryStreamInput) error {
	provided := 0
	if in.S3DestinationConfiguration != nil || in.ExtendedS3DestinationConfiguration != nil {
		provided++
	}
	if in.HTTPEndpointDestinationConfiguration != nil {
		provided++
	}
	if in.RedshiftDestinationConfiguration != nil {
		provided++
	}
	if in.AmazonOpenSearchServiceDestinationConfiguration != nil {
		provided++
	}
	if in.ElasticsearchDestinationConfiguration != nil {
		provided++
	}
	if in.SplunkDestinationConfiguration != nil {
		provided++
	}
	if in.IcebergDestinationConfiguration != nil {
		provided++
	}
	if in.SnowflakeDestinationConfiguration != nil {
		provided++
	}

	if provided > 1 {
		return fmt.Errorf("%w: at most one destination configuration may be specified, got %d",
			ErrValidation, provided)
	}

	return nil
}

func (h *Handler) handleCreateDeliveryStream(
	ctx context.Context,
	in *createDeliveryStreamInput,
) (*createDeliveryStreamOutput, error) {
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

	// ExtendedS3 takes precedence over plain S3.
	rawS3 := in.ExtendedS3DestinationConfiguration
	if rawS3 == nil {
		rawS3 = in.S3DestinationConfiguration
	}

	if rawS3 != nil {
		if err := validateDataFormatConversion(rawS3.DataFormatConversionConfiguration); err != nil {
			return nil, err
		}
	}

	if err := validateSingleDestination(in); err != nil {
		return nil, err
	}

	s, err := h.Backend.CreateDeliveryStream(ctx, CreateDeliveryStreamInput{
		Name:                     in.DeliveryStreamName,
		DeliveryStreamType:       in.DeliveryStreamType,
		S3Destination:            buildS3DestinationDescription(rawS3),
		HTTPEndpointDestination:  buildHTTPEndpointDestination(in.HTTPEndpointDestinationConfiguration),
		RedshiftDestination:      buildRedshiftDestination(in.RedshiftDestinationConfiguration),
		OpenSearchDestination:    buildOpenSearchDestination(in.AmazonOpenSearchServiceDestinationConfiguration),
		ElasticsearchDestination: buildElasticsearchDestination(in.ElasticsearchDestinationConfiguration),
		SplunkDestination:        buildSplunkDestination(in.SplunkDestinationConfiguration),
		IcebergDestination:       buildIcebergDestination(in.IcebergDestinationConfiguration),
		SnowflakeDestination:     buildSnowflakeDestination(in.SnowflakeDestinationConfiguration),
		Source: buildSourceDescription(
			in.KinesisStreamSourceConfiguration, in.MSKSourceConfiguration,
		),
	})
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		tagMap := make(map[string]string, len(in.Tags))
		for _, t := range in.Tags {
			tagMap[t.Key] = t.Value
		}

		_ = h.Backend.TagDeliveryStream(ctx, in.DeliveryStreamName, tagMap)
	}

	return &createDeliveryStreamOutput{DeliveryStreamARN: s.ARN}, nil
}

type deleteDeliveryStreamOutput struct{}

func (h *Handler) handleDeleteDeliveryStream(
	ctx context.Context,
	in *deliveryStreamNameInput,
) (*deleteDeliveryStreamOutput, error) {
	if err := h.Backend.DeleteDeliveryStream(ctx, in.DeliveryStreamName); err != nil {
		return nil, err
	}

	return &deleteDeliveryStreamOutput{}, nil
}

// destinationDescriptionOutput mirrors AWS's DestinationDescription shape: a
// DestinationId plus at most one populated type-specific description. Real
// DescribeDeliveryStream responses nest every destination type under a single
// "Destinations" list on the wire rather than exposing separate per-type lists.
type destinationDescriptionOutput struct {
	ExtendedS3DestinationDescription              *S3DestinationDescription            `json:"ExtendedS3DestinationDescription,omitempty"`              //nolint:lll // AWS field name
	HTTPEndpointDestinationDescription            *HTTPEndpointDestinationDescription  `json:"HttpEndpointDestinationDescription,omitempty"`            //nolint:lll // AWS field name (note "Http" casing)
	RedshiftDestinationDescription                *RedshiftDestinationDescription      `json:"RedshiftDestinationDescription,omitempty"`                //nolint:lll // AWS field name
	AmazonopensearchserviceDestinationDescription *OpenSearchDestinationDescription    `json:"AmazonopensearchserviceDestinationDescription,omitempty"` //nolint:lll // AWS field name (exact casing)
	ElasticsearchDestinationDescription           *ElasticsearchDestinationDescription `json:"ElasticsearchDestinationDescription,omitempty"`           //nolint:lll // AWS field name
	SplunkDestinationDescription                  *SplunkDestinationDescription        `json:"SplunkDestinationDescription,omitempty"`                  //nolint:lll // AWS field name
	IcebergDestinationDescription                 *IcebergDestinationDescription       `json:"IcebergDestinationDescription,omitempty"`                 //nolint:lll // AWS field name
	SnowflakeDestinationDescription               *SnowflakeDestinationDescription     `json:"SnowflakeDestinationDescription,omitempty"`               //nolint:lll // AWS field name
	DestinationID                                 string                               `json:"DestinationId"`
}

type deliveryStreamDescriptionFields struct {
	EncryptionConfiguration *EncryptionConfig              `json:"DeliveryStreamEncryptionConfiguration,omitempty"` //nolint:lll // AWS field name
	Source                  *SourceDescription             `json:"Source,omitempty"`
	CreateTimestamp         *int64                         `json:"CreateTimestamp,omitempty"`
	LastUpdateTimestamp     *int64                         `json:"LastUpdateTimestamp,omitempty"` //nolint:lll // AWS field name
	DeliveryStreamName      string                         `json:"DeliveryStreamName"`
	DeliveryStreamARN       string                         `json:"DeliveryStreamARN"`
	DeliveryStreamStatus    string                         `json:"DeliveryStreamStatus"`
	DeliveryStreamType      string                         `json:"DeliveryStreamType,omitempty"` //nolint:lll // AWS field name
	VersionID               string                         `json:"VersionId,omitempty"`
	Destinations            []destinationDescriptionOutput `json:"Destinations"`
	HasMoreDestinations     bool                           `json:"HasMoreDestinations"`
}

type describeDeliveryStreamInput struct {
	DeliveryStreamName          string `json:"DeliveryStreamName"`
	ExclusiveStartDestinationID string `json:"ExclusiveStartDestinationId"`
	Limit                       int    `json:"Limit"`
}

type describeDeliveryStreamOutput struct {
	DeliveryStreamDescription deliveryStreamDescriptionFields `json:"DeliveryStreamDescription"`
}

func (h *Handler) handleDescribeDeliveryStream(
	ctx context.Context,
	in *describeDeliveryStreamInput,
) (*describeDeliveryStreamOutput, error) {
	s, err := h.Backend.DescribeDeliveryStream(ctx, in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}

	var createTS, updateTS *int64
	if !s.CreateTimestamp.IsZero() {
		ts := s.CreateTimestamp.Unix()
		createTS = &ts
	}

	if !s.LastUpdateTimestamp.IsZero() {
		ts := s.LastUpdateTimestamp.Unix()
		updateTS = &ts
	}

	desc := deliveryStreamDescriptionFields{
		DeliveryStreamName:      s.Name,
		DeliveryStreamARN:       s.ARN,
		DeliveryStreamStatus:    s.Status,
		DeliveryStreamType:      s.DeliveryStreamType,
		VersionID:               s.VersionID,
		EncryptionConfiguration: s.Encryption,
		Source:                  s.Source,
		CreateTimestamp:         createTS,
		LastUpdateTimestamp:     updateTS,
		Destinations:            buildDestinationDescriptions(s),
		HasMoreDestinations:     false,
	}

	return &describeDeliveryStreamOutput{DeliveryStreamDescription: desc}, nil
}

// defaultDestinationID is the synthetic DestinationId AWS assigns to a stream's first
// (and, in this backend, only) destination when none has been explicitly stamped yet.
const defaultDestinationID = "destinationId-000000000001"

// destinationIDOrDefault returns id, or defaultDestinationID when id is empty.
func destinationIDOrDefault(id string) string {
	if id == "" {
		return defaultDestinationID
	}

	return id
}

// buildDestinationDescriptions converts a DeliveryStream's per-type destination fields
// into the AWS wire shape: a single "Destinations" list of DestinationDescription
// entries, each carrying a DestinationId plus exactly one populated type-specific
// description. AWS never exposes separate top-level lists per destination type.
func buildDestinationDescriptions(s *DeliveryStream) []destinationDescriptionOutput {
	destinations := make([]destinationDescriptionOutput, 0, 1)

	if s.S3Destination != nil {
		d := *s.S3Destination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID:                    destinationIDOrDefault(d.DestinationID),
			ExtendedS3DestinationDescription: &d,
		})
	}

	if s.HTTPEndpointDestination != nil {
		d := *s.HTTPEndpointDestination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID:                      destinationIDOrDefault(d.DestinationID),
			HTTPEndpointDestinationDescription: &d,
		})
	}

	if s.RedshiftDestination != nil {
		d := *s.RedshiftDestination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID:                  destinationIDOrDefault(d.DestinationID),
			RedshiftDestinationDescription: &d,
		})
	}

	if s.OpenSearchDestination != nil {
		d := *s.OpenSearchDestination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID: destinationIDOrDefault(d.DestinationID),
			AmazonopensearchserviceDestinationDescription: &d,
		})
	}

	if s.ElasticsearchDestination != nil {
		d := *s.ElasticsearchDestination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID:                       destinationIDOrDefault(d.DestinationID),
			ElasticsearchDestinationDescription: &d,
		})
	}

	if s.SplunkDestination != nil {
		d := *s.SplunkDestination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID:                destinationIDOrDefault(d.DestinationID),
			SplunkDestinationDescription: &d,
		})
	}

	if s.IcebergDestination != nil {
		d := *s.IcebergDestination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID:                 destinationIDOrDefault(d.DestinationID),
			IcebergDestinationDescription: &d,
		})
	}

	if s.SnowflakeDestination != nil {
		d := *s.SnowflakeDestination
		destinations = append(destinations, destinationDescriptionOutput{
			DestinationID:                   destinationIDOrDefault(d.DestinationID),
			SnowflakeDestinationDescription: &d,
		})
	}

	return destinations
}

type listDeliveryStreamsInput struct {
	ExclusiveStartDeliveryStreamName string `json:"ExclusiveStartDeliveryStreamName"`
	DeliveryStreamType               string `json:"DeliveryStreamType"`
	Limit                            int    `json:"Limit"`
}

type listDeliveryStreamsOutput struct {
	DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
	HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
}

// isValidDeliveryStreamType reports whether s is a DeliveryStreamType filter value AWS
// accepts on ListDeliveryStreams.
func isValidDeliveryStreamType(s string) bool {
	return s == deliveryStreamTypeDirectPut || s == deliveryStreamTypeKinesisSource
}

func (h *Handler) handleListDeliveryStreams(
	ctx context.Context,
	in *listDeliveryStreamsInput,
) (*listDeliveryStreamsOutput, error) {
	if in.DeliveryStreamType != "" && !isValidDeliveryStreamType(in.DeliveryStreamType) {
		return nil, fmt.Errorf("%w: invalid DeliveryStreamType %q", ErrValidation, in.DeliveryStreamType)
	}

	names := h.Backend.ListDeliveryStreamsByType(ctx, in.DeliveryStreamType)

	// Apply ExclusiveStartDeliveryStreamName cursor.
	if in.ExclusiveStartDeliveryStreamName != "" {
		startIdx := -1
		for i, n := range names {
			if n == in.ExclusiveStartDeliveryStreamName {
				startIdx = i

				break
			}
		}
		if startIdx >= 0 {
			names = names[startIdx+1:]
		}
	}

	hasMore := false
	limit := in.Limit
	if limit <= 0 || limit > maxListLimit {
		limit = maxListLimit
	}

	if len(names) > limit {
		names = names[:limit]
		hasMore = true
	}

	return &listDeliveryStreamsOutput{
		DeliveryStreamNames:    names,
		HasMoreDeliveryStreams: hasMore,
	}, nil
}
