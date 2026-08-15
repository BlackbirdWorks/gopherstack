package kafka

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// CreateReplicator creates a new MSK replicator. Real aws-sdk-go-v2 clients
// always populate kafkaClusters/replicationInfoList (the SDK's client-side
// input-validation middleware rejects a CreateReplicatorInput missing either
// before the request is even sent), so both are accepted and fully persisted
// here but not hard-required server-side -- a nil/empty slice from a
// non-SDK caller is stored as-is rather than rejected.
func (b *InMemoryBackend) CreateReplicator(
	ctx context.Context,
	name, description, serviceExecutionRoleArn string,
	kafkaClusters []ClusterConfig,
	replicationInfoList []ReplicationInfoConfig,
	tags map[string]string,
	logDelivery *LogDelivery,
) (*Replicator, error) {
	if name == "" {
		return nil, fmt.Errorf("replicatorName is required: %w", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateReplicator")
	defer b.mu.Unlock()

	for _, r := range b.replicatorsByRegion.Get(region) {
		if r.ReplicatorName == name {
			return nil, ErrAlreadyExists
		}
	}

	replicatorArn := b.replicatorARN(region, name)
	replicator := &Replicator{
		ReplicatorArn:           replicatorArn,
		ReplicatorName:          name,
		Description:             description,
		ServiceExecutionRoleArn: serviceExecutionRoleArn,
		ReplicatorState:         ReplicatorStateRunning,
		CurrentVersion:          nextVersionToken(),
		CreationTime:            time.Now().UTC().Format(time.RFC3339),
		Tags:                    nonNilTagsCopy(tags),
		KafkaClusters:           cloneKafkaClusterConfigs(kafkaClusters),
		ReplicationInfoList:     cloneReplicationInfoConfigs(replicationInfoList),
		LogDelivery:             cloneLogDelivery(logDelivery),
	}
	b.replicators.Put(replicator)

	return b.describeReplicatorLocked(replicator), nil
}

// describeReplicatorLocked clones r and resolves KafkaClusterAlias /
// SourceKafkaClusterAlias / TargetKafkaClusterAlias fresh from the current
// cluster table, mirroring how real MSK derives KafkaClusterDescription /
// ReplicationInfoDescription aliases at read time rather than storing them.
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) describeReplicatorLocked(r *Replicator) *Replicator {
	out := cloneReplicator(r)

	for i := range out.KafkaClusters {
		out.KafkaClusters[i].Alias = b.clusterAliasForArn(out.KafkaClusters[i].MskClusterArn)
	}

	for i := range out.ReplicationInfoList {
		out.ReplicationInfoList[i].SourceAlias = b.clusterAliasForArn(out.ReplicationInfoList[i].SourceKafkaClusterArn)
		out.ReplicationInfoList[i].TargetAlias = b.clusterAliasForArn(out.ReplicationInfoList[i].TargetKafkaClusterArn)
	}

	return out
}

// DeleteReplicator deletes a replicator by ARN.
func (b *InMemoryBackend) DeleteReplicator(_ context.Context, replicatorArn string) error {
	b.mu.Lock("DeleteReplicator")
	defer b.mu.Unlock()

	if !b.replicators.Delete(replicatorArn) {
		return ErrNotFound
	}

	return nil
}

// DescribeReplicator retrieves a replicator by ARN.
func (b *InMemoryBackend) DescribeReplicator(_ context.Context, replicatorArn string) (*Replicator, error) {
	b.mu.RLock("DescribeReplicator")
	defer b.mu.RUnlock()

	r, ok := b.replicators.Get(replicatorArn)
	if !ok {
		return nil, ErrNotFound
	}

	return b.describeReplicatorLocked(r), nil
}

// ListReplicators returns all replicators in the request's region sorted by name.
func (b *InMemoryBackend) ListReplicators(ctx context.Context) []*Replicator {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListReplicators")
	defer b.mu.RUnlock()

	replicators := b.replicatorsByRegion.Get(region)
	out := make([]*Replicator, 0, len(replicators))
	for _, r := range replicators {
		out = append(out, b.describeReplicatorLocked(r))
	}

	slices.SortFunc(out, func(a, b *Replicator) int {
		if a.ReplicatorName < b.ReplicatorName {
			return -1
		}
		if a.ReplicatorName > b.ReplicatorName {
			return 1
		}

		return 0
	})

	return out
}

// clusterAliasForArn returns the KafkaClusterAlias real MSK derives for a
// kafkaClusters entry: the actual name of the referenced MSK cluster, looked
// up by ARN. If the cluster doesn't exist in this backend (e.g. cross-account/
// already-deleted), it falls back to the last "/"-delimited resource segment
// of the ARN so a value is always returned. Caller must hold b.mu.
func (b *InMemoryBackend) clusterAliasForArn(mskClusterArn string) string {
	if c, ok := b.clusters.Get(mskClusterArn); ok {
		return c.ClusterName
	}

	if idx := strings.LastIndex(mskClusterArn, "/"); idx != -1 && idx+1 < len(mskClusterArn) {
		return mskClusterArn[idx+1:]
	}

	return mskClusterArn
}

// UpdateReplicationInfo updates the topic/consumer-group replication settings
// for one source->target flow of a replicator, enforcing the same
// optimistic-lock CurrentVersion contract as the cluster Update* operations.
func (b *InMemoryBackend) UpdateReplicationInfo(
	_ context.Context,
	replicatorArn, currentVersion, sourceKafkaClusterArn, targetKafkaClusterArn string,
	topicReplication *TopicReplicationConfig,
	consumerGroupReplication *ConsumerGroupReplicationConfig,
) (*Replicator, error) {
	b.mu.Lock("UpdateReplicationInfo")
	defer b.mu.Unlock()

	r, ok := b.replicators.Get(replicatorArn)
	if !ok {
		return nil, ErrNotFound
	}

	if currentVersion == "" || currentVersion != r.CurrentVersion {
		return nil, fmt.Errorf(
			"the specified replicator version is not current: %w", ErrValidation,
		)
	}

	idx := slices.IndexFunc(r.ReplicationInfoList, func(ri ReplicationInfoConfig) bool {
		return ri.SourceKafkaClusterArn == sourceKafkaClusterArn &&
			ri.TargetKafkaClusterArn == targetKafkaClusterArn
	})
	if idx == -1 {
		return nil, ErrNotFound
	}

	if topicReplication != nil {
		r.ReplicationInfoList[idx].TopicReplication = *topicReplication
	}

	if consumerGroupReplication != nil {
		r.ReplicationInfoList[idx].ConsumerGroupReplication = *consumerGroupReplication
	}

	r.CurrentVersion = nextVersionToken()

	return b.describeReplicatorLocked(r), nil
}

// AddReplicatorInternal creates a replicator directly for testing purposes.
func (b *InMemoryBackend) AddReplicatorInternal(name string) *Replicator {
	b.mu.Lock("AddReplicatorInternal")
	defer b.mu.Unlock()

	replicatorArn := b.replicatorARN(b.region, name)
	replicator := &Replicator{
		ReplicatorArn:   replicatorArn,
		ReplicatorName:  name,
		ReplicatorState: ReplicatorStateRunning,
		CurrentVersion:  nextVersionToken(),
		CreationTime:    time.Now().UTC().Format(time.RFC3339),
		Tags:            make(map[string]string),
	}
	b.replicators.Put(replicator)

	return cloneReplicator(replicator)
}

// cloneReplicator creates a deep copy of a Replicator.
func cloneReplicator(r *Replicator) *Replicator {
	return &Replicator{
		ReplicatorArn:           r.ReplicatorArn,
		ReplicatorName:          r.ReplicatorName,
		Description:             r.Description,
		ServiceExecutionRoleArn: r.ServiceExecutionRoleArn,
		ReplicatorState:         r.ReplicatorState,
		CurrentVersion:          r.CurrentVersion,
		CreationTime:            r.CreationTime,
		StateInfoCode:           r.StateInfoCode,
		StateInfoMessage:        r.StateInfoMessage,
		Tags:                    nonNilTagsCopy(r.Tags),
		KafkaClusters:           cloneKafkaClusterConfigs(r.KafkaClusters),
		ReplicationInfoList:     cloneReplicationInfoConfigs(r.ReplicationInfoList),
		LogDelivery:             cloneLogDelivery(r.LogDelivery),
	}
}

// cloneLogDelivery deep-copies a *LogDelivery.
func cloneLogDelivery(ld *LogDelivery) *LogDelivery {
	if ld == nil || ld.ReplicatorLogDelivery == nil {
		return nil
	}

	rld := *ld.ReplicatorLogDelivery
	if rld.CloudWatchLogs != nil {
		cw := *rld.CloudWatchLogs
		rld.CloudWatchLogs = &cw
	}

	if rld.Firehose != nil {
		fh := *rld.Firehose
		rld.Firehose = &fh
	}

	if rld.S3 != nil {
		s3 := *rld.S3
		rld.S3 = &s3
	}

	return &LogDelivery{ReplicatorLogDelivery: &rld}
}

// cloneKafkaClusterConfigs deep-copies a []ClusterConfig.
func cloneKafkaClusterConfigs(src []ClusterConfig) []ClusterConfig {
	if src == nil {
		return nil
	}

	out := make([]ClusterConfig, len(src))
	for i, kc := range src {
		out[i] = ClusterConfig{
			MskClusterArn:    kc.MskClusterArn,
			Alias:            kc.Alias,
			SubnetIDs:        append([]string(nil), kc.SubnetIDs...),
			SecurityGroupIDs: append([]string(nil), kc.SecurityGroupIDs...),
		}
	}

	return out
}

// cloneReplicationInfoConfigs deep-copies a []ReplicationInfoConfig.
func cloneReplicationInfoConfigs(src []ReplicationInfoConfig) []ReplicationInfoConfig {
	if src == nil {
		return nil
	}

	out := make([]ReplicationInfoConfig, len(src))
	for i, ri := range src {
		out[i] = ReplicationInfoConfig{
			SourceKafkaClusterArn: ri.SourceKafkaClusterArn,
			TargetKafkaClusterArn: ri.TargetKafkaClusterArn,
			TargetCompressionType: ri.TargetCompressionType,
			SourceAlias:           ri.SourceAlias,
			TargetAlias:           ri.TargetAlias,
			TopicReplication: TopicReplicationConfig{
				TopicsToReplicate:               append([]string(nil), ri.TopicReplication.TopicsToReplicate...),
				TopicsToExclude:                 append([]string(nil), ri.TopicReplication.TopicsToExclude...),
				CopyAccessControlListsForTopics: ri.TopicReplication.CopyAccessControlListsForTopics,
				CopyTopicConfigurations:         ri.TopicReplication.CopyTopicConfigurations,
				DetectAndCopyNewTopics:          ri.TopicReplication.DetectAndCopyNewTopics,
				StartingPositionType:            ri.TopicReplication.StartingPositionType,
				TopicNameConfigurationType:      ri.TopicReplication.TopicNameConfigurationType,
			},
			ConsumerGroupReplication: ConsumerGroupReplicationConfig{
				ConsumerGroupsToReplicate: append(
					[]string(nil), ri.ConsumerGroupReplication.ConsumerGroupsToReplicate...,
				),
				ConsumerGroupsToExclude: append(
					[]string(nil), ri.ConsumerGroupReplication.ConsumerGroupsToExclude...,
				),
				DetectAndCopyNewConsumerGroups:  ri.ConsumerGroupReplication.DetectAndCopyNewConsumerGroups,
				SynchroniseConsumerGroupOffsets: ri.ConsumerGroupReplication.SynchroniseConsumerGroupOffsets,
			},
		}
	}

	return out
}
