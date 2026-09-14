package kinesis

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- wire types (field names verified against kinesis@v1.53.0 serializers.go/deserializers.go) ---

type jsonRecordConfig struct {
	RecordFormatType string `json:"RecordFormatType"`
	GSRSchemaARN     string `json:"GSRSchemaARN,omitempty"`
}

type jsonChannelStreamConfiguration struct {
	StreamARN           string           `json:"StreamARN"`
	RecordConfiguration jsonRecordConfig `json:"RecordConfiguration"`
}

type jsonChannelStreamDescription struct {
	RecordConfiguration     jsonRecordConfig `json:"RecordConfiguration"`
	StreamARN               string           `json:"StreamARN"`
	StreamCreationTimestamp float64          `json:"StreamCreationTimestamp"`
}

type jsonChannelStreamIdentifier struct {
	StreamARN               string  `json:"StreamARN"`
	StreamCreationTimestamp float64 `json:"StreamCreationTimestamp"`
}

type jsonChannelEncryptionConfiguration struct {
	EncryptionType string `json:"EncryptionType"`
	KeyID          string `json:"KeyId"`
}

type jsonCloudWatchLogs struct {
	Enabled       *bool  `json:"Enabled"`
	LogGroupName  string `json:"LogGroupName,omitempty"`
	LogStreamName string `json:"LogStreamName,omitempty"`
}

type jsonChannelLoggingConfiguration struct {
	CloudWatchLogs *jsonCloudWatchLogs `json:"CloudWatchLogs"`
}

type jsonS3StorageConfiguration struct {
	BucketARN           string `json:"BucketARN"`
	CompressionType     string `json:"CompressionType"`
	ExpectedBucketOwner string `json:"ExpectedBucketOwner,omitempty"`
	OutputKeyTemplate   string `json:"OutputKeyTemplate,omitempty"`
	StorageClass        string `json:"StorageClass,omitempty"`
}

type jsonDeadLetterQueueS3Configuration struct {
	BucketARN           string `json:"BucketARN"`
	ExpectedBucketOwner string `json:"ExpectedBucketOwner,omitempty"`
	ErrorOutputPrefix   string `json:"ErrorOutputPrefix,omitempty"`
}

type jsonS3DestinationConfiguration struct {
	StorageConfiguration           *jsonS3StorageConfiguration         `json:"StorageConfiguration"`
	DeadLetterQueueS3Configuration *jsonDeadLetterQueueS3Configuration `json:"DeadLetterQueueS3Configuration,omitempty"`
	DataFreshnessInSeconds         int                                 `json:"DataFreshnessInSeconds,omitempty"`
}

type jsonPartitionField struct {
	SourceName string `json:"SourceName"`
	Transform  string `json:"Transform"`
}

type jsonPartitionSpec struct {
	PartitionFields []jsonPartitionField `json:"PartitionFields"`
}

type jsonS3TablesConfiguration struct {
	PartitionSpec   *jsonPartitionSpec `json:"PartitionSpec,omitempty"`
	TableBucketARN  string             `json:"TableBucketARN"`
	Namespace       string             `json:"Namespace"`
	TableName       string             `json:"TableName"`
	CompressionType string             `json:"CompressionType"`
}

type jsonS3TablesDestinationConfiguration struct {
	DeadLetterQueueS3Configuration *jsonDeadLetterQueueS3Configuration `json:"DeadLetterQueueS3Configuration,omitempty"`
	S3TablesConfigurationList      []jsonS3TablesConfiguration         `json:"S3TablesConfigurationList"`
	DataFreshnessInSeconds         int                                 `json:"DataFreshnessInSeconds,omitempty"`
}

type jsonChannelDescription struct {
	EncryptionConfiguration          *jsonChannelEncryptionConfiguration   `json:"EncryptionConfiguration,omitempty"`
	S3DestinationConfiguration       *jsonS3DestinationConfiguration       `json:"S3DestinationConfiguration,omitempty"`
	S3TablesDestinationConfiguration *jsonS3TablesDestinationConfiguration `json:"S3TablesDestinationConfiguration,omitempty"` //nolint:lll // SDK field name
	LoggingConfiguration             jsonChannelLoggingConfiguration       `json:"LoggingConfiguration"`
	ChannelARN                       string                                `json:"ChannelARN"`
	ChannelID                        string                                `json:"ChannelId"`
	ChannelName                      string                                `json:"ChannelName"`
	ChannelStatus                    string                                `json:"ChannelStatus"`
	ServiceExecutionRoleARN          string                                `json:"ServiceExecutionRoleARN"`
	StreamConfigurationList          []jsonChannelStreamDescription        `json:"StreamConfigurationList"`
	ChannelCreationTimestamp         float64                               `json:"ChannelCreationTimestamp"`
}

type jsonChannelDescriptionResp struct {
	ChannelDescription jsonChannelDescription `json:"ChannelDescription"`
}

type jsonChannelSummary struct {
	ChannelARN               string                        `json:"ChannelARN"`
	ChannelID                string                        `json:"ChannelId"`
	ChannelName              string                        `json:"ChannelName"`
	ChannelStatus            string                        `json:"ChannelStatus"`
	ChannelDestinationType   string                        `json:"ChannelDestinationType"`
	Streams                  []jsonChannelStreamIdentifier `json:"Streams"`
	ChannelCreationTimestamp float64                       `json:"ChannelCreationTimestamp"`
}

type jsonCreateChannelReq struct {
	EncryptionConfiguration          *jsonChannelEncryptionConfiguration   `json:"EncryptionConfiguration,omitempty"`
	LoggingConfiguration             *jsonChannelLoggingConfiguration      `json:"LoggingConfiguration,omitempty"`
	S3DestinationConfiguration       *jsonS3DestinationConfiguration       `json:"S3DestinationConfiguration,omitempty"`
	S3TablesDestinationConfiguration *jsonS3TablesDestinationConfiguration `json:"S3TablesDestinationConfiguration,omitempty"` //nolint:lll // SDK field name
	Tags                             map[string]string                     `json:"Tags,omitempty"`
	ChannelName                      string                                `json:"ChannelName"`
	ServiceExecutionRoleARN          string                                `json:"ServiceExecutionRoleARN"`
	StreamConfigurationList          []jsonChannelStreamConfiguration      `json:"StreamConfigurationList"`
}

type jsonChannelARNReq struct {
	ChannelARN string `json:"ChannelARN"`
}

type jsonStreamFilter struct {
	StreamARN string `json:"StreamARN"`
}

type jsonListChannelsReq struct {
	NextToken    string             `json:"NextToken,omitempty"`
	MaxResults   *int               `json:"MaxResults,omitempty"`
	StreamFilter []jsonStreamFilter `json:"StreamFilter,omitempty"`
}

type jsonListChannelsResp struct {
	NextToken        string               `json:"NextToken,omitempty"`
	ChannelSummaries []jsonChannelSummary `json:"ChannelSummaries"`
}

type jsonS3DestinationUpdateInput struct {
	DataFreshnessInSeconds int `json:"DataFreshnessInSeconds"`
}

type jsonS3TablesDestinationUpdateInput struct {
	DataFreshnessInSeconds int `json:"DataFreshnessInSeconds"`
}

type jsonUpdateChannelReq struct {
	LoggingConfiguration             *jsonChannelLoggingConfiguration    `json:"LoggingConfiguration,omitempty"`
	S3DestinationConfiguration       *jsonS3DestinationUpdateInput       `json:"S3DestinationConfiguration,omitempty"`
	S3TablesDestinationConfiguration *jsonS3TablesDestinationUpdateInput `json:"S3TablesDestinationConfiguration,omitempty"` //nolint:lll // SDK field name
	ChannelARN                       string                              `json:"ChannelARN"`
}

// --- wire <-> domain conversions ---

func recordConfigFromJSON(r jsonRecordConfig) ChannelRecordConfig {
	return ChannelRecordConfig(r)
}

func recordConfigToJSON(r ChannelRecordConfig) jsonRecordConfig {
	return jsonRecordConfig(r)
}

func channelStreamConfigsFromJSON(list []jsonChannelStreamConfiguration) []ChannelStreamConfig {
	out := make([]ChannelStreamConfig, 0, len(list))
	for _, c := range list {
		out = append(out, ChannelStreamConfig{
			StreamARN:           c.StreamARN,
			RecordConfiguration: recordConfigFromJSON(c.RecordConfiguration),
		})
	}

	return out
}

func channelStreamDescriptionsToJSON(list []ChannelStreamConfig) []jsonChannelStreamDescription {
	out := make([]jsonChannelStreamDescription, 0, len(list))
	for _, c := range list {
		out = append(out, jsonChannelStreamDescription{
			StreamARN:               c.StreamARN,
			StreamCreationTimestamp: awstime.Epoch(c.StreamCreationTimestamp),
			RecordConfiguration:     recordConfigToJSON(c.RecordConfiguration),
		})
	}

	return out
}

func channelStreamIdentifiersToJSON(list []ChannelStreamConfig) []jsonChannelStreamIdentifier {
	out := make([]jsonChannelStreamIdentifier, 0, len(list))
	for _, c := range list {
		out = append(out, jsonChannelStreamIdentifier{
			StreamARN:               c.StreamARN,
			StreamCreationTimestamp: awstime.Epoch(c.StreamCreationTimestamp),
		})
	}

	return out
}

func channelEncryptionFromJSON(e *jsonChannelEncryptionConfiguration) *ChannelEncryptionConfig {
	if e == nil {
		return nil
	}

	return &ChannelEncryptionConfig{EncryptionType: e.EncryptionType, KeyID: e.KeyID}
}

func channelEncryptionToJSON(e *ChannelEncryptionConfig) *jsonChannelEncryptionConfiguration {
	if e == nil {
		return nil
	}

	return &jsonChannelEncryptionConfiguration{EncryptionType: e.EncryptionType, KeyID: e.KeyID}
}

func cloudWatchLogsConfigFromJSON(l *jsonChannelLoggingConfiguration) *ChannelCloudWatchLogsConfig {
	if l == nil || l.CloudWatchLogs == nil {
		return nil
	}

	cw := l.CloudWatchLogs

	return &ChannelCloudWatchLogsConfig{
		Enabled:       cw.Enabled != nil && *cw.Enabled,
		LogGroupName:  cw.LogGroupName,
		LogStreamName: cw.LogStreamName,
	}
}

func cloudWatchLogsConfigToJSON(l ChannelCloudWatchLogsConfig) jsonChannelLoggingConfiguration {
	enabled := l.Enabled

	return jsonChannelLoggingConfiguration{
		CloudWatchLogs: &jsonCloudWatchLogs{
			Enabled:       &enabled,
			LogGroupName:  l.LogGroupName,
			LogStreamName: l.LogStreamName,
		},
	}
}

func deadLetterQueueFromJSON(d *jsonDeadLetterQueueS3Configuration) *ChannelDeadLetterQueueS3Config {
	if d == nil {
		return nil
	}

	return &ChannelDeadLetterQueueS3Config{
		BucketARN:           d.BucketARN,
		ExpectedBucketOwner: d.ExpectedBucketOwner,
		ErrorOutputPrefix:   d.ErrorOutputPrefix,
	}
}

func deadLetterQueueToJSON(d *ChannelDeadLetterQueueS3Config) *jsonDeadLetterQueueS3Configuration {
	if d == nil {
		return nil
	}

	return &jsonDeadLetterQueueS3Configuration{
		BucketARN:           d.BucketARN,
		ExpectedBucketOwner: d.ExpectedBucketOwner,
		ErrorOutputPrefix:   d.ErrorOutputPrefix,
	}
}

func s3DestinationFromJSON(d *jsonS3DestinationConfiguration) *ChannelS3Destination {
	if d == nil {
		return nil
	}

	out := &ChannelS3Destination{
		DataFreshnessInSeconds:         d.DataFreshnessInSeconds,
		DeadLetterQueueS3Configuration: deadLetterQueueFromJSON(d.DeadLetterQueueS3Configuration),
	}
	if d.StorageConfiguration != nil {
		out.StorageConfiguration = ChannelS3StorageConfig{
			BucketARN:           d.StorageConfiguration.BucketARN,
			CompressionType:     d.StorageConfiguration.CompressionType,
			ExpectedBucketOwner: d.StorageConfiguration.ExpectedBucketOwner,
			OutputKeyTemplate:   d.StorageConfiguration.OutputKeyTemplate,
			StorageClass:        d.StorageConfiguration.StorageClass,
		}
	}

	return out
}

func s3DestinationToJSON(d *ChannelS3Destination) *jsonS3DestinationConfiguration {
	if d == nil {
		return nil
	}

	return &jsonS3DestinationConfiguration{
		DataFreshnessInSeconds:         d.DataFreshnessInSeconds,
		DeadLetterQueueS3Configuration: deadLetterQueueToJSON(d.DeadLetterQueueS3Configuration),
		StorageConfiguration: &jsonS3StorageConfiguration{
			BucketARN:           d.StorageConfiguration.BucketARN,
			CompressionType:     d.StorageConfiguration.CompressionType,
			ExpectedBucketOwner: d.StorageConfiguration.ExpectedBucketOwner,
			OutputKeyTemplate:   d.StorageConfiguration.OutputKeyTemplate,
			StorageClass:        d.StorageConfiguration.StorageClass,
		},
	}
}

func partitionSpecFromJSON(p *jsonPartitionSpec) *ChannelPartitionSpec {
	if p == nil {
		return nil
	}

	fields := make([]ChannelPartitionField, 0, len(p.PartitionFields))
	for _, f := range p.PartitionFields {
		fields = append(fields, ChannelPartitionField(f))
	}

	return &ChannelPartitionSpec{PartitionFields: fields}
}

func partitionSpecToJSON(p *ChannelPartitionSpec) *jsonPartitionSpec {
	if p == nil {
		return nil
	}

	fields := make([]jsonPartitionField, 0, len(p.PartitionFields))
	for _, f := range p.PartitionFields {
		fields = append(fields, jsonPartitionField(f))
	}

	return &jsonPartitionSpec{PartitionFields: fields}
}

func s3TablesDestinationFromJSON(d *jsonS3TablesDestinationConfiguration) *ChannelS3TablesDestination {
	if d == nil {
		return nil
	}

	list := make([]ChannelS3TablesConfig, 0, len(d.S3TablesConfigurationList))
	for _, c := range d.S3TablesConfigurationList {
		list = append(list, ChannelS3TablesConfig{
			TableBucketARN:  c.TableBucketARN,
			Namespace:       c.Namespace,
			TableName:       c.TableName,
			CompressionType: c.CompressionType,
			PartitionSpec:   partitionSpecFromJSON(c.PartitionSpec),
		})
	}

	return &ChannelS3TablesDestination{
		DataFreshnessInSeconds:         d.DataFreshnessInSeconds,
		DeadLetterQueueS3Configuration: deadLetterQueueFromJSON(d.DeadLetterQueueS3Configuration),
		S3TablesConfigurationList:      list,
	}
}

func s3TablesDestinationToJSON(d *ChannelS3TablesDestination) *jsonS3TablesDestinationConfiguration {
	if d == nil {
		return nil
	}

	list := make([]jsonS3TablesConfiguration, 0, len(d.S3TablesConfigurationList))
	for _, c := range d.S3TablesConfigurationList {
		list = append(list, jsonS3TablesConfiguration{
			TableBucketARN:  c.TableBucketARN,
			Namespace:       c.Namespace,
			TableName:       c.TableName,
			CompressionType: c.CompressionType,
			PartitionSpec:   partitionSpecToJSON(c.PartitionSpec),
		})
	}

	return &jsonS3TablesDestinationConfiguration{
		DataFreshnessInSeconds:         d.DataFreshnessInSeconds,
		DeadLetterQueueS3Configuration: deadLetterQueueToJSON(d.DeadLetterQueueS3Configuration),
		S3TablesConfigurationList:      list,
	}
}

func channelToJSON(c *Channel) jsonChannelDescription {
	return jsonChannelDescription{
		ChannelARN:                       c.ChannelARN,
		ChannelID:                        c.ChannelID,
		ChannelName:                      c.ChannelName,
		ChannelStatus:                    c.ChannelStatus,
		ChannelCreationTimestamp:         awstime.Epoch(c.ChannelCreationTimestamp),
		ServiceExecutionRoleARN:          c.ServiceExecutionRoleARN,
		StreamConfigurationList:          channelStreamDescriptionsToJSON(c.StreamConfigurationList),
		LoggingConfiguration:             cloudWatchLogsConfigToJSON(c.LoggingConfiguration),
		EncryptionConfiguration:          channelEncryptionToJSON(c.EncryptionConfiguration),
		S3DestinationConfiguration:       s3DestinationToJSON(c.S3DestinationConfiguration),
		S3TablesDestinationConfiguration: s3TablesDestinationToJSON(c.S3TablesDestinationConfiguration),
	}
}

func channelSummaryToJSON(c *Channel) jsonChannelSummary {
	return jsonChannelSummary{
		ChannelARN:               c.ChannelARN,
		ChannelID:                c.ChannelID,
		ChannelName:              c.ChannelName,
		ChannelStatus:            c.ChannelStatus,
		ChannelDestinationType:   channelDestinationType(c),
		ChannelCreationTimestamp: awstime.Epoch(c.ChannelCreationTimestamp),
		Streams:                  channelStreamIdentifiersToJSON(c.StreamConfigurationList),
	}
}

// --- handlers ---

func (h *Handler) handleCreateChannel(ctx context.Context, _ *http.Request, body []byte) (any, error) {
	var req jsonCreateChannelReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if req.LoggingConfiguration != nil &&
		(req.LoggingConfiguration.CloudWatchLogs == nil || req.LoggingConfiguration.CloudWatchLogs.Enabled == nil) {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.CreateChannel(ctx, &CreateChannelInput{
		ChannelName:                      req.ChannelName,
		ServiceExecutionRoleARN:          req.ServiceExecutionRoleARN,
		StreamConfigurationList:          channelStreamConfigsFromJSON(req.StreamConfigurationList),
		EncryptionConfiguration:          channelEncryptionFromJSON(req.EncryptionConfiguration),
		LoggingConfiguration:             cloudWatchLogsConfigFromJSON(req.LoggingConfiguration),
		S3DestinationConfiguration:       s3DestinationFromJSON(req.S3DestinationConfiguration),
		S3TablesDestinationConfiguration: s3TablesDestinationFromJSON(req.S3TablesDestinationConfiguration),
		Tags:                             req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return jsonChannelDescriptionResp{ChannelDescription: channelToJSON(&out.ChannelDescription)}, nil
}

func (h *Handler) handleDeleteChannel(ctx context.Context, _ *http.Request, body []byte) (any, error) {
	var req jsonChannelARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.DeleteChannel(ctx, &DeleteChannelInput{ChannelARN: req.ChannelARN}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleDescribeChannel(ctx context.Context, _ *http.Request, body []byte) (any, error) {
	var req jsonChannelARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.DescribeChannel(ctx, &DescribeChannelInput{ChannelARN: req.ChannelARN})
	if err != nil {
		return nil, err
	}

	return jsonChannelDescriptionResp{ChannelDescription: channelToJSON(&out.ChannelDescription)}, nil
}

func channelStreamFiltersFromJSON(filters []jsonStreamFilter) []ChannelStreamFilter {
	out := make([]ChannelStreamFilter, 0, len(filters))
	for _, f := range filters {
		out = append(out, ChannelStreamFilter(f))
	}

	return out
}

func (h *Handler) handleListChannels(ctx context.Context, _ *http.Request, body []byte) (any, error) {
	var req jsonListChannelsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	maxResults := 0
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}

	out, err := h.Backend.ListChannels(ctx, &ListChannelsInput{
		NextToken:    req.NextToken,
		MaxResults:   maxResults,
		StreamFilter: channelStreamFiltersFromJSON(req.StreamFilter),
	})
	if err != nil {
		return nil, err
	}

	summaries := make([]jsonChannelSummary, 0, len(out.ChannelSummaries))
	for i := range out.ChannelSummaries {
		summaries = append(summaries, channelSummaryToJSON(&out.ChannelSummaries[i]))
	}

	return jsonListChannelsResp{ChannelSummaries: summaries, NextToken: out.NextToken}, nil
}

func (h *Handler) handleUpdateChannel(ctx context.Context, _ *http.Request, body []byte) (any, error) {
	var req jsonUpdateChannelReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if req.LoggingConfiguration != nil &&
		(req.LoggingConfiguration.CloudWatchLogs == nil || req.LoggingConfiguration.CloudWatchLogs.Enabled == nil) {
		return nil, ErrInvalidArgument
	}

	input := &UpdateChannelInput{ChannelARN: req.ChannelARN}
	input.LoggingConfiguration = cloudWatchLogsConfigFromJSON(req.LoggingConfiguration)

	if req.S3DestinationConfiguration != nil {
		input.S3DestinationConfiguration = &ChannelS3Destination{
			DataFreshnessInSeconds: req.S3DestinationConfiguration.DataFreshnessInSeconds,
		}
	}
	if req.S3TablesDestinationConfiguration != nil {
		input.S3TablesDestinationConfiguration = &ChannelS3TablesDestination{
			DataFreshnessInSeconds: req.S3TablesDestinationConfiguration.DataFreshnessInSeconds,
		}
	}

	out, err := h.Backend.UpdateChannel(ctx, input)
	if err != nil {
		return nil, err
	}

	return jsonChannelDescriptionResp{ChannelDescription: channelToJSON(&out.ChannelDescription)}, nil
}
