package kinesis

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// channelNameRe validates Kinesis channel names. The real SDK enforces no
// pattern client-side (validators.go only checks presence), so this mirrors
// the charset already used for stream/consumer names in this backend.
var channelNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

func isValidChannelName(s string) bool {
	return channelNameRe.MatchString(s)
}

// buildChannelARN builds a Kinesis channel ARN. The real SDK doc comments
// give no ARN format for channels (unlike stream/consumer ARNs, which are
// documented in the IAM access-control guide); this follows the same
// "{service}/{resource-name}" convention AWS uses for every other Kinesis
// resource type (stream/{name}, stream/{name}/consumer/{name}).
func buildChannelARN(region, accountID, channelName string) string {
	return arn.Build("kinesis", region, accountID, "channel/"+channelName)
}

// isChannelARN reports whether a resolves to a channel resource, i.e. its
// ARN resource segment starts with "channel/".
func isChannelARN(a string) bool {
	const arnResourceIdx = 5

	parts := strings.Split(a, ":")
	if len(parts) <= arnResourceIdx {
		return false
	}

	return strings.HasPrefix(parts[arnResourceIdx], "channel/")
}

// isValidRecordFormatType reports whether t is one of RecordConfiguration's
// documented RecordFormatType values (kinesis@v1.53.0 types/enums.go:196-217).
func isValidRecordFormatType(t string) bool {
	switch t {
	case recordFormatTypeGSRJSON, recordFormatTypeJSON, recordFormatTypeString, recordFormatTypeByteArray:
		return true
	default:
		return false
	}
}

// validateChannelStreamConfigList validates CreateChannelInput's
// StreamConfigurationList ("Currently, one stream is supported per channel.").
func validateChannelStreamConfigList(list []ChannelStreamConfig) error {
	if len(list) != maxChannelStreams {
		return ErrInvalidArgument
	}

	cfg := list[0]
	if cfg.StreamARN == "" {
		return ErrInvalidArgument
	}

	if !isValidRecordFormatType(cfg.RecordConfiguration.RecordFormatType) {
		return ErrInvalidArgument
	}

	return nil
}

// resolveChannelDataFreshness applies S3DestinationConfiguration/
// S3TablesDestinationConfiguration's shared DataFreshnessInSeconds default
// and range ("Valid range is 300 to 900 seconds ... default value is 300").
func resolveChannelDataFreshness(seconds int) (int, error) {
	if seconds == 0 {
		return defaultChannelDataFreshnessSeconds, nil
	}
	if seconds < minChannelDataFreshnessSeconds || seconds > maxChannelDataFreshnessSeconds {
		return 0, ErrInvalidArgument
	}

	return seconds, nil
}

// validateDeadLetterQueueS3Config validates an optional/required
// DeadLetterQueueS3Configuration: BucketARN and ExpectedBucketOwner are both
// required whenever the block itself is present (kinesis@v1.53.0
// types/types.go:364-381; validators.go validateDeadLetterQueueS3Configuration).
func validateDeadLetterQueueS3Config(d *ChannelDeadLetterQueueS3Config) error {
	if d == nil {
		return nil
	}

	if d.BucketARN == "" || d.ExpectedBucketOwner == "" {
		return ErrInvalidArgument
	}

	return nil
}

// validateS3Destination validates and normalizes a CreateChannel
// S3DestinationConfiguration. StorageConfiguration is required, with
// BucketARN/CompressionType/ExpectedBucketOwner all required within it
// (kinesis@v1.53.0 types/types.go:626-663; validators.go
// validateS3StorageConfiguration -- ExpectedBucketOwner is required
// client-side even though its own doc comment reads like an optional
// safety check). DeadLetterQueueS3Configuration is optional here.
func validateS3Destination(d *ChannelS3Destination) error {
	sc := d.StorageConfiguration
	if sc.BucketARN == "" || sc.CompressionType == "" || sc.ExpectedBucketOwner == "" {
		return ErrInvalidArgument
	}

	if err := validateDeadLetterQueueS3Config(d.DeadLetterQueueS3Configuration); err != nil {
		return err
	}

	freshness, err := resolveChannelDataFreshness(d.DataFreshnessInSeconds)
	if err != nil {
		return err
	}
	d.DataFreshnessInSeconds = freshness

	return nil
}

// validateS3TablesDestination validates and normalizes a CreateChannel
// S3TablesDestinationConfiguration (kinesis@v1.53.0 types/types.go:762-780:
// DeadLetterQueueS3Configuration and S3TablesConfigurationList are required;
// "Currently, one table is supported per channel.").
func validateS3TablesDestination(d *ChannelS3TablesDestination) error {
	if err := validateDeadLetterQueueS3Config(d.DeadLetterQueueS3Configuration); err != nil {
		return err
	}
	if d.DeadLetterQueueS3Configuration == nil {
		return ErrInvalidArgument
	}
	if len(d.S3TablesConfigurationList) != maxS3TablesConfigs {
		return ErrInvalidArgument
	}

	cfg := d.S3TablesConfigurationList[0]
	if cfg.TableBucketARN == "" || cfg.Namespace == "" || cfg.TableName == "" || cfg.CompressionType == "" {
		return ErrInvalidArgument
	}

	freshness, err := resolveChannelDataFreshness(d.DataFreshnessInSeconds)
	if err != nil {
		return err
	}
	d.DataFreshnessInSeconds = freshness

	return nil
}

// validateChannelEncryption validates an optional
// ChannelEncryptionConfiguration ("The only valid value is KMS.", KeyId
// required -- kinesis@v1.53.0 types/types.go:83-97).
func validateChannelEncryption(cfg *ChannelEncryptionConfig) error {
	if cfg == nil {
		return nil
	}

	if cfg.EncryptionType != channelEncryptionTypeKMS || cfg.KeyID == "" {
		return ErrInvalidArgument
	}

	return nil
}

// resolveChannelDestination enforces "you must specify either
// S3DestinationConfiguration or S3TablesDestinationConfiguration, but not
// both" (CreateChannelInput doc comment) and validates whichever is given.
func resolveChannelDestination(input *CreateChannelInput) error {
	hasS3 := input.S3DestinationConfiguration != nil
	hasS3Tables := input.S3TablesDestinationConfiguration != nil

	if hasS3 == hasS3Tables {
		return ErrInvalidArgument
	}

	if hasS3 {
		return validateS3Destination(input.S3DestinationConfiguration)
	}

	return validateS3TablesDestination(input.S3TablesDestinationConfiguration)
}

// channelDestinationType returns c's destination type
// (types.ChannelDestinationType, used on ChannelSummary): "S3" or
// "S3_TABLES" depending on which destination configuration is set.
func channelDestinationType(c *Channel) string {
	if c.S3DestinationConfiguration != nil {
		return channelDestinationTypeS3
	}

	return channelDestinationTypeS3Tables
}

// resolveChannelLogging applies CloudWatchLogs' documented defaults
// ("Defaults to /aws/kinesis/{channelName}/{channelId}" and "Defaults to
// DestinationDelivery", kinesis@v1.53.0 types/types.go:263-269) when omitted
// or left blank.
func resolveChannelLogging(
	cfg *ChannelCloudWatchLogsConfig, channelName, channelID string,
) ChannelCloudWatchLogsConfig {
	if cfg == nil {
		return ChannelCloudWatchLogsConfig{}
	}

	out := *cfg
	if out.LogGroupName == "" {
		out.LogGroupName = "/aws/kinesis/" + channelName + "/" + channelID
	}
	if out.LogStreamName == "" {
		out.LogStreamName = defaultChannelLogStreamName
	}

	return out
}

// defaultChannelLogStreamName is CloudWatchLogs.LogStreamName's documented
// default (kinesis@v1.53.0 types/types.go:267-269).
const defaultChannelLogStreamName = "DestinationDelivery"

// findChannelByName returns the channel with the given name in region, or
// nil. Callers must hold b.mu.
func (b *InMemoryBackend) findChannelByName(region, name string) *Channel {
	for _, c := range b.channelsByRegion.Get(region) {
		if c.ChannelName == name {
			return c
		}
	}

	return nil
}

// CreateChannel creates a channel that delivers records from a Kinesis data
// stream to an S3 or S3-Tables destination (api_op_CreateChannel.go).
// Records are not actually delivered -- see PARITY.md. CreateChannel is
// documented as asynchronous (CREATING then ACTIVE); this backend applies it
// synchronously, matching the precedent already set for
// UpdateStreamWarmThroughput/UpdateStreamMode.
func (b *InMemoryBackend) CreateChannel(ctx context.Context, input *CreateChannelInput) (*CreateChannelOutput, error) {
	if !isValidChannelName(input.ChannelName) || input.ServiceExecutionRoleARN == "" {
		return nil, ErrInvalidArgument
	}

	if err := validateChannelStreamConfigList(input.StreamConfigurationList); err != nil {
		return nil, err
	}

	if err := validateTagKVs(input.Tags); err != nil {
		return nil, err
	}

	if err := validateChannelEncryption(input.EncryptionConfiguration); err != nil {
		return nil, err
	}

	if err := resolveChannelDestination(input); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)
	streamARN := input.StreamConfigurationList[0].StreamARN
	streamName := streamNameFromARN(streamARN)

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("CreateChannel.stream")
	streamMode := stream.StreamMode
	stream.mu.RUnlock()

	// "This operation is only supported for data streams with the on-demand
	// capacity mode." (CreateChannel doc comment). The doc comment names no
	// specific exception for this case; InvalidArgumentException is used
	// here as the same catch-all this backend already uses for the
	// sibling "exactly one destination" configuration error -- see PARITY.md.
	if streamMode != streamModeOnDemand {
		return nil, ErrInvalidArgument
	}

	if b.findChannelByName(region, input.ChannelName) != nil {
		return nil, ErrChannelAlreadyExists
	}

	now := time.Now()
	channelID := uuid.NewString()

	streamConfig := ChannelStreamConfig{
		StreamARN:               streamARN,
		StreamCreationTimestamp: stream.CreatedAt,
		RecordConfiguration:     input.StreamConfigurationList[0].RecordConfiguration,
	}

	channel := &Channel{
		ChannelID:                channelID,
		ChannelARN:               buildChannelARN(region, b.accountID, input.ChannelName),
		ChannelName:              input.ChannelName,
		ChannelStatus:            channelStatusActive,
		ServiceExecutionRoleARN:  input.ServiceExecutionRoleARN,
		Region:                   region,
		ChannelCreationTimestamp: now,
		StreamConfigurationList:  []ChannelStreamConfig{streamConfig},
		LoggingConfiguration: resolveChannelLogging(
			input.LoggingConfiguration,
			input.ChannelName,
			channelID,
		),
		EncryptionConfiguration:          input.EncryptionConfiguration,
		S3DestinationConfiguration:       input.S3DestinationConfiguration,
		S3TablesDestinationConfiguration: input.S3TablesDestinationConfiguration,
		Tags:                             input.Tags,
	}

	b.channels.Put(channel)

	return &CreateChannelOutput{ChannelDescription: *channel}, nil
}

// DeleteChannel deletes the specified channel (api_op_DeleteChannel.go).
// Deletion is synchronous -- DeleteChannel's own doc comment, unlike
// CreateChannel/UpdateChannel's, describes no CREATING/UPDATING-style
// asynchronous transition, so there is no documented DELETING state to model.
// Any buffered records are flushed to S3 (best-effort) after the channel row
// is removed, so records already accepted are not silently dropped.
func (b *InMemoryBackend) DeleteChannel(ctx context.Context, input *DeleteChannelInput) error {
	ch, err := b.removeChannelLocked(input.ChannelARN)
	if err != nil {
		return err
	}

	b.deliveryMu.Lock("DeleteChannel.delivery")
	snap := extractChannelBufferLocked(b.channelBuffers, ch)
	delete(b.channelBuffers, ch.ChannelARN)
	b.deliveryMu.Unlock()

	if snap != nil {
		b.deliverChannelSnapshot(ctx, snap)
	}

	return nil
}

// removeChannelLocked validates and removes a channel row under b.mu,
// returning the removed Channel so the caller can flush its buffer outside
// the lock (see DeleteChannel).
func (b *InMemoryBackend) removeChannelLocked(channelARN string) (*Channel, error) {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelARN)
	if !ok {
		return nil, ErrChannelNotFound
	}

	b.channels.Delete(channelARN)

	return ch, nil
}

// DescribeChannel describes the specified channel (api_op_DescribeChannel.go).
func (b *InMemoryBackend) DescribeChannel(
	_ context.Context,
	input *DescribeChannelInput,
) (*DescribeChannelOutput, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	channel, ok := b.channels.Get(input.ChannelARN)
	if !ok {
		return nil, ErrChannelNotFound
	}

	return &DescribeChannelOutput{ChannelDescription: *channel}, nil
}

// channelMatchesStreamFilter reports whether c is associated with any of the
// stream ARNs in filter. An empty filter matches every channel.
func channelMatchesStreamFilter(c *Channel, filter []ChannelStreamFilter) bool {
	if len(filter) == 0 {
		return true
	}

	for _, f := range filter {
		for _, sc := range c.StreamConfigurationList {
			if sc.StreamARN == f.StreamARN {
				return true
			}
		}
	}

	return false
}

// ListChannels lists the channels in the caller's account/region, optionally
// filtered by source stream (api_op_ListChannels.go).
func (b *InMemoryBackend) ListChannels(ctx context.Context, input *ListChannelsInput) (*ListChannelsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListChannels")
	matched := make([]Channel, 0, b.channelsByRegion.Len())
	for _, c := range b.channelsByRegion.Get(region) {
		if channelMatchesStreamFilter(c, input.StreamFilter) {
			matched = append(matched, *c)
		}
	}
	b.mu.RUnlock()

	sort.Slice(matched, func(i, j int) bool { return matched[i].ChannelName < matched[j].ChannelName })

	if input.NextToken != "" {
		start := 0
		for start < len(matched) && matched[start].ChannelName <= input.NextToken {
			start++
		}
		matched = matched[start:]
	}

	limit := input.MaxResults
	if limit <= 0 || limit > maxChannelsListResults {
		limit = maxChannelsListResults
	}

	var nextToken string
	if len(matched) > limit {
		nextToken = matched[limit-1].ChannelName
		matched = matched[:limit]
	}

	return &ListChannelsOutput{ChannelSummaries: matched, NextToken: nextToken}, nil
}

// applyUpdateChannelDestination validates and applies input's destination
// update onto channel, enforcing UpdateChannel's documented restriction:
// "You cannot change the destination ... of an existing channel", only
// DataFreshnessInSeconds.
func applyUpdateChannelDestination(channel *Channel, input *UpdateChannelInput) error {
	hasS3 := input.S3DestinationConfiguration != nil
	hasS3Tables := input.S3TablesDestinationConfiguration != nil

	if hasS3 && hasS3Tables {
		return ErrInvalidArgument
	}

	if hasS3 {
		if channel.S3DestinationConfiguration == nil {
			return ErrInvalidArgument
		}
		freshness, err := resolveChannelDataFreshness(input.S3DestinationConfiguration.DataFreshnessInSeconds)
		if err != nil {
			return err
		}
		channel.S3DestinationConfiguration.DataFreshnessInSeconds = freshness
	}

	if hasS3Tables {
		if channel.S3TablesDestinationConfiguration == nil {
			return ErrInvalidArgument
		}
		freshness, err := resolveChannelDataFreshness(input.S3TablesDestinationConfiguration.DataFreshnessInSeconds)
		if err != nil {
			return err
		}
		channel.S3TablesDestinationConfiguration.DataFreshnessInSeconds = freshness
	}

	return nil
}

// UpdateChannel updates a channel's data-freshness interval or CloudWatch
// Logs configuration (api_op_UpdateChannel.go). UpdateChannel is documented
// as asynchronous (UPDATING then ACTIVE); this backend applies it
// synchronously, matching CreateChannel's disclosed simplification.
func (b *InMemoryBackend) UpdateChannel(_ context.Context, input *UpdateChannelInput) (*UpdateChannelOutput, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	channel, ok := b.channels.Get(input.ChannelARN)
	if !ok {
		return nil, ErrChannelNotFound
	}

	if err := applyUpdateChannelDestination(channel, input); err != nil {
		return nil, err
	}

	if input.LoggingConfiguration != nil {
		channel.LoggingConfiguration = resolveChannelLogging(
			input.LoggingConfiguration,
			channel.ChannelName,
			channel.ChannelID,
		)
	}

	return &UpdateChannelOutput{ChannelDescription: *channel}, nil
}
