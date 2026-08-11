package kafka

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// channelARN builds arn:{partition}:kafka:{region}:{account}:channel/{clusterName}/{clusterUUID}/{channelName},
// reusing the cluster's own path segment like topicARN. Deterministic per
// (clusterArn, channelName), which doubles as CreateChannel's duplicate check.
func channelARN(clusterArn, channelName string) string {
	const clusterMarker = ":cluster/"

	prefix, clusterPath, ok := strings.Cut(clusterArn, clusterMarker)
	if !ok {
		// Malformed/test ARN with no "cluster/" marker: append directly.
		return clusterArn + "/channel/" + channelName
	}

	return prefix + ":channel/" + clusterPath + "/" + channelName
}

// CreateChannel creates a channel on an MSK cluster. Real MSK creation is
// async (Status starts CREATING); this emulator makes it ACTIVE immediately,
// matching CreateTopic's simplification. ClusterOperationArn is returned but
// not persisted — real DescribeChannel/ListChannels would show it empty too
// once Status is ACTIVE (it's only set while CREATING/UPDATING/DELETING, per
// types.go's DescribeChannelOutput doc comment).
func (b *InMemoryBackend) CreateChannel(
	ctx context.Context,
	clusterArn, channelName string,
	topicConfigurationList []TopicConfiguration,
	encryptionConfiguration *ChannelEncryptionConfiguration,
	icebergDestinationConfiguration *IcebergDestinationConfiguration,
	s3DestinationConfiguration *S3DestinationConfiguration,
	loggingInfo *ChannelLoggingInfo,
	tags map[string]string,
) (*Channel, error) {
	if err := validateCreateChannelInput(
		channelName,
		topicConfigurationList,
		encryptionConfiguration,
		icebergDestinationConfiguration,
		s3DestinationConfiguration,
	); err != nil {
		return nil, err
	}

	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	channelArn := channelARN(clusterArn, channelName)
	if b.channels.Has(channelArn) {
		return nil, ErrAlreadyExists
	}

	destinationType := ChannelDestinationTypeS3
	if icebergDestinationConfiguration != nil {
		destinationType = ChannelDestinationTypeIceberg
	}

	ch := &Channel{
		ChannelArn:                      channelArn,
		ChannelName:                     channelName,
		ClusterArn:                      clusterArn,
		DestinationType:                 destinationType,
		Status:                          ChannelStatusActive,
		CreationTime:                    time.Now().UTC().Format(time.RFC3339),
		TopicConfigurationList:          cloneTopicConfigurationList(topicConfigurationList),
		EncryptionConfiguration:         cloneChannelEncryptionConfiguration(encryptionConfiguration),
		IcebergDestinationConfiguration: cloneIcebergDestinationConfiguration(icebergDestinationConfiguration),
		S3DestinationConfiguration:      cloneS3DestinationConfiguration(s3DestinationConfiguration),
		LoggingInfo:                     cloneChannelLoggingInfo(loggingInfo),
		Tags:                            nonNilTagsCopy(tags),
	}
	b.channels.Put(ch)

	op := b.newClusterOperationLocked(region, clusterArn, "CREATE_CHANNEL", nil, nil)

	result := cloneChannel(ch)
	result.ClusterOperationArn = op.ClusterOperationArn

	return result, nil
}

// validateCreateChannelInput applies validators.go's required-field rules
// plus the server-side-only "exactly one destination" rule (not enforced by
// the SDK's client-side validator, since neither field is marked required).
func validateCreateChannelInput(
	channelName string,
	topicConfigurationList []TopicConfiguration,
	encryptionConfiguration *ChannelEncryptionConfiguration,
	icebergDestinationConfiguration *IcebergDestinationConfiguration,
	s3DestinationConfiguration *S3DestinationConfiguration,
) error {
	if channelName == "" {
		return fmt.Errorf("channelName is required: %w", ErrValidation)
	}

	if err := validateTopicConfigurationList(topicConfigurationList); err != nil {
		return err
	}

	if encryptionConfiguration != nil && encryptionConfiguration.KmsKeyArn == "" {
		return fmt.Errorf("encryptionConfiguration.kmsKeyArn is required: %w", ErrValidation)
	}

	switch {
	case s3DestinationConfiguration == nil && icebergDestinationConfiguration == nil:
		return fmt.Errorf(
			"exactly one of s3DestinationConfiguration or icebergDestinationConfiguration is required: %w",
			ErrValidation,
		)
	case s3DestinationConfiguration != nil && icebergDestinationConfiguration != nil:
		return fmt.Errorf(
			"s3DestinationConfiguration and icebergDestinationConfiguration are mutually exclusive: %w",
			ErrValidation,
		)
	case s3DestinationConfiguration != nil:
		return validateS3DestinationConfig(s3DestinationConfiguration)
	default:
		return validateIcebergDestinationConfig(icebergDestinationConfiguration)
	}
}

// validateTopicConfigurationList mirrors validate__listOfTopicConfiguration
// plus CreateChannelInput.TopicConfigurationList's doc comment ("Currently
// exactly one topic must be specified").
func validateTopicConfigurationList(topicConfigurationList []TopicConfiguration) error {
	if len(topicConfigurationList) != 1 {
		return fmt.Errorf("exactly one topicConfigurationList entry is required: %w", ErrValidation)
	}

	tc := topicConfigurationList[0]
	if tc.RecordConverter == nil || tc.RecordConverter.ValueConverter == "" {
		return fmt.Errorf("topicConfigurationList[0].recordConverter.valueConverter is required: %w", ErrValidation)
	}

	if tc.TopicArn == "" {
		return fmt.Errorf("topicConfigurationList[0].topicArn is required: %w", ErrValidation)
	}

	if tc.RecordSchema != nil && tc.RecordSchema.GsrArn == "" {
		return fmt.Errorf("topicConfigurationList[0].recordSchema.gsrArn is required: %w", ErrValidation)
	}

	return nil
}

// validateS3DestinationConfig mirrors validateS3DestinationConfiguration.
func validateS3DestinationConfig(cfg *S3DestinationConfiguration) error {
	if cfg.DeadLetterQueueS3 == nil || cfg.DeadLetterQueueS3.BucketArn == "" {
		return fmt.Errorf("s3DestinationConfiguration.deadLetterQueueS3.bucketArn is required: %w", ErrValidation)
	}

	if cfg.ServiceExecutionRoleArn == "" {
		return fmt.Errorf("s3DestinationConfiguration.serviceExecutionRoleArn is required: %w", ErrValidation)
	}

	if cfg.Storage == nil {
		return fmt.Errorf("s3DestinationConfiguration.storage is required: %w", ErrValidation)
	}

	if cfg.Storage.BucketArn == "" {
		return fmt.Errorf("s3DestinationConfiguration.storage.bucketArn is required: %w", ErrValidation)
	}

	if cfg.Storage.CompressionType == "" {
		return fmt.Errorf("s3DestinationConfiguration.storage.compressionType is required: %w", ErrValidation)
	}

	if cfg.Storage.StorageClass == "" {
		return fmt.Errorf("s3DestinationConfiguration.storage.storageClass is required: %w", ErrValidation)
	}

	return nil
}

// validateIcebergDestinationConfig mirrors validateIcebergDestinationConfiguration.
func validateIcebergDestinationConfig(cfg *IcebergDestinationConfiguration) error {
	if cfg.DeadLetterQueueS3 == nil || cfg.DeadLetterQueueS3.BucketArn == "" {
		return fmt.Errorf(
			"icebergDestinationConfiguration.deadLetterQueueS3.bucketArn is required: %w", ErrValidation,
		)
	}

	if len(cfg.DestinationTableList) == 0 {
		return fmt.Errorf(
			"icebergDestinationConfiguration.destinationTableList is required: %w", ErrValidation,
		)
	}

	if cfg.SchemaEvolution == nil {
		return fmt.Errorf("icebergDestinationConfiguration.schemaEvolution is required: %w", ErrValidation)
	}

	if cfg.ServiceExecutionRoleArn == "" {
		return fmt.Errorf(
			"icebergDestinationConfiguration.serviceExecutionRoleArn is required: %w", ErrValidation,
		)
	}

	if cfg.TableCreation == nil {
		return fmt.Errorf("icebergDestinationConfiguration.tableCreation is required: %w", ErrValidation)
	}

	return nil
}

// DeleteChannel deletes a channel from an MSK cluster. Real MSK deletion is
// async (DELETING); this emulator removes it immediately.
func (b *InMemoryBackend) DeleteChannel(ctx context.Context, clusterArn, channelArn string) (*Channel, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelArn)
	if !ok || ch.ClusterArn != clusterArn {
		return nil, ErrNotFound
	}

	b.channels.Delete(channelArn)

	op := b.newClusterOperationLocked(region, clusterArn, "DELETE_CHANNEL", nil, nil)

	return &Channel{ChannelArn: channelArn, ClusterOperationArn: op.ClusterOperationArn}, nil
}

// DescribeChannel retrieves a channel by cluster ARN and channel ARN. A
// channelArn that exists but belongs to a different cluster is reported as
// not found, matching real MSK's cluster-scoped Channel resource model.
func (b *InMemoryBackend) DescribeChannel(_ context.Context, clusterArn, channelArn string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	ch, ok := b.channels.Get(channelArn)
	if !ok || ch.ClusterArn != clusterArn {
		return nil, ErrNotFound
	}

	return cloneChannel(ch), nil
}

// ListChannels returns channels for a cluster sorted by channel name,
// optionally filtered by topicNameFilter. Unlike ListTopics' prefix-match
// topicNameFilter, ListChannels' doc comment says "matches the specified
// value" with no "starting with" qualifier, so this is an exact match.
func (b *InMemoryBackend) ListChannels(_ context.Context, clusterArn, topicNameFilter string) ([]*Channel, error) {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	channels := b.channelsByCluster.Get(clusterArn)
	out := make([]*Channel, 0, len(channels))

	for _, ch := range channels {
		if topicNameFilter != "" && !channelMatchesTopicName(ch, topicNameFilter) {
			continue
		}

		out = append(out, cloneChannel(ch))
	}

	slices.SortFunc(out, func(a, b *Channel) int { return strings.Compare(a.ChannelName, b.ChannelName) })

	return out, nil
}

// channelMatchesTopicName reports whether any of ch's topic configurations
// references a topic named name, recovered from the ARN's trailing
// "/"-delimited segment (topicARN always ends "/{topicName}", see topics.go).
func channelMatchesTopicName(ch *Channel, name string) bool {
	for _, tc := range ch.TopicConfigurationList {
		if idx := strings.LastIndex(tc.TopicArn, "/"); idx != -1 && idx+1 < len(tc.TopicArn) {
			if tc.TopicArn[idx+1:] == name {
				return true
			}
		} else if tc.TopicArn == name {
			return true
		}
	}

	return false
}

// UpdateChannel updates the destination-freshness setting of an existing
// channel. The destination type cannot be changed (api_op_UpdateChannel.go
// doc comment); enforced here server-side since neither update field is
// marked required in validators.go.
func (b *InMemoryBackend) UpdateChannel(
	ctx context.Context,
	clusterArn, channelArn string,
	icebergDestinationUpdate *IcebergDestinationUpdate,
	s3DestinationUpdate *S3DestinationUpdate,
) (*Channel, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelArn)
	if !ok || ch.ClusterArn != clusterArn {
		return nil, ErrNotFound
	}

	if err := applyChannelDestinationUpdateLocked(ch, icebergDestinationUpdate, s3DestinationUpdate); err != nil {
		return nil, err
	}

	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_CHANNEL", nil, nil)

	result := cloneChannel(ch)
	result.ClusterOperationArn = op.ClusterOperationArn

	return result, nil
}

// applyChannelDestinationUpdateLocked mutates ch's destination-freshness
// setting from an UpdateChannel payload. Caller must hold the write lock.
func applyChannelDestinationUpdateLocked(
	ch *Channel,
	icebergDestinationUpdate *IcebergDestinationUpdate,
	s3DestinationUpdate *S3DestinationUpdate,
) error {
	switch {
	case icebergDestinationUpdate == nil && s3DestinationUpdate == nil:
		return fmt.Errorf(
			"exactly one of icebergDestinationUpdate or s3DestinationUpdate is required: %w", ErrValidation,
		)
	case icebergDestinationUpdate != nil && s3DestinationUpdate != nil:
		return fmt.Errorf(
			"icebergDestinationUpdate and s3DestinationUpdate are mutually exclusive: %w", ErrValidation,
		)
	case icebergDestinationUpdate != nil:
		if ch.DestinationType != ChannelDestinationTypeIceberg || ch.IcebergDestinationConfiguration == nil {
			return fmt.Errorf(
				"channel destination type is %s, cannot apply icebergDestinationUpdate: %w",
				ch.DestinationType, ErrValidation,
			)
		}

		ch.IcebergDestinationConfiguration.DataFreshnessInSeconds = icebergDestinationUpdate.DataFreshnessInSeconds
	default:
		if ch.DestinationType != ChannelDestinationTypeS3 || ch.S3DestinationConfiguration == nil {
			return fmt.Errorf(
				"channel destination type is %s, cannot apply s3DestinationUpdate: %w",
				ch.DestinationType, ErrValidation,
			)
		}

		ch.S3DestinationConfiguration.DataFreshnessInSeconds = s3DestinationUpdate.DataFreshnessInSeconds
	}

	return nil
}

// Clone helpers below: deep copies so returned values never alias backend state.

func cloneChannel(ch *Channel) *Channel {
	return &Channel{
		ChannelArn:                      ch.ChannelArn,
		ChannelName:                     ch.ChannelName,
		ClusterArn:                      ch.ClusterArn,
		DestinationType:                 ch.DestinationType,
		Status:                          ch.Status,
		ClusterOperationArn:             ch.ClusterOperationArn,
		CreationTime:                    ch.CreationTime,
		TopicConfigurationList:          cloneTopicConfigurationList(ch.TopicConfigurationList),
		EncryptionConfiguration:         cloneChannelEncryptionConfiguration(ch.EncryptionConfiguration),
		IcebergDestinationConfiguration: cloneIcebergDestinationConfiguration(ch.IcebergDestinationConfiguration),
		S3DestinationConfiguration:      cloneS3DestinationConfiguration(ch.S3DestinationConfiguration),
		LoggingInfo:                     cloneChannelLoggingInfo(ch.LoggingInfo),
		StateInfo:                       cloneChannelStateInfo(ch.StateInfo),
		Tags:                            nonNilTagsCopy(ch.Tags),
	}
}

func cloneChannelEncryptionConfiguration(c *ChannelEncryptionConfiguration) *ChannelEncryptionConfiguration {
	if c == nil {
		return nil
	}

	clone := *c

	return &clone
}

func cloneDeadLetterQueueS3(d *DeadLetterQueueS3) *DeadLetterQueueS3 {
	if d == nil {
		return nil
	}

	clone := *d

	return &clone
}

func clonePartitionSpec(p *PartitionSpec) *PartitionSpec {
	if p == nil {
		return nil
	}

	return &PartitionSpec{
		PartitionStrategy: p.PartitionStrategy,
		SourceList:        append([]PartitionSource(nil), p.SourceList...),
	}
}

func cloneDestinationTableList(src []DestinationTable) []DestinationTable {
	if src == nil {
		return nil
	}

	out := make([]DestinationTable, len(src))
	for i, dt := range src {
		out[i] = DestinationTable{
			DestinationDatabaseName: dt.DestinationDatabaseName,
			DestinationTableName:    dt.DestinationTableName,
			PartitionSpec:           clonePartitionSpec(dt.PartitionSpec),
		}
	}

	return out
}

func cloneIcebergDestinationConfiguration(c *IcebergDestinationConfiguration) *IcebergDestinationConfiguration {
	if c == nil {
		return nil
	}

	clone := &IcebergDestinationConfiguration{
		AppendOnly:              c.AppendOnly,
		CompressionType:         c.CompressionType,
		DataFreshnessInSeconds:  c.DataFreshnessInSeconds,
		ServiceExecutionRoleArn: c.ServiceExecutionRoleArn,
		DeadLetterQueueS3:       cloneDeadLetterQueueS3(c.DeadLetterQueueS3),
		DestinationTableList:    cloneDestinationTableList(c.DestinationTableList),
	}

	if c.Catalog != nil {
		catalog := *c.Catalog
		clone.Catalog = &catalog
	}

	if c.SchemaEvolution != nil {
		se := *c.SchemaEvolution
		clone.SchemaEvolution = &se
	}

	if c.TableCreation != nil {
		tc := *c.TableCreation
		clone.TableCreation = &tc
	}

	return clone
}

func cloneS3DestinationConfiguration(c *S3DestinationConfiguration) *S3DestinationConfiguration {
	if c == nil {
		return nil
	}

	clone := &S3DestinationConfiguration{
		DataFreshnessInSeconds:  c.DataFreshnessInSeconds,
		ServiceExecutionRoleArn: c.ServiceExecutionRoleArn,
		DeadLetterQueueS3:       cloneDeadLetterQueueS3(c.DeadLetterQueueS3),
	}

	if c.Storage != nil {
		storage := *c.Storage
		clone.Storage = &storage
	}

	return clone
}

func cloneChannelLoggingInfo(l *ChannelLoggingInfo) *ChannelLoggingInfo {
	if l == nil {
		return nil
	}

	clone := &ChannelLoggingInfo{}

	if l.CloudWatchLogs != nil {
		cwl := *l.CloudWatchLogs
		clone.CloudWatchLogs = &cwl
	}

	if l.Firehose != nil {
		fh := *l.Firehose
		clone.Firehose = &fh
	}

	if l.S3 != nil {
		s3 := *l.S3
		clone.S3 = &s3
	}

	return clone
}

func cloneChannelStateInfo(s *ChannelStateInfo) *ChannelStateInfo {
	if s == nil {
		return nil
	}

	clone := *s

	return &clone
}

func cloneTopicConfigurationList(src []TopicConfiguration) []TopicConfiguration {
	if src == nil {
		return nil
	}

	out := make([]TopicConfiguration, len(src))
	for i, tc := range src {
		out[i] = TopicConfiguration{TopicArn: tc.TopicArn}

		if tc.RecordConverter != nil {
			rc := *tc.RecordConverter
			out[i].RecordConverter = &rc
		}

		if tc.RecordSchema != nil {
			rs := *tc.RecordSchema
			out[i].RecordSchema = &rs
		}
	}

	return out
}
