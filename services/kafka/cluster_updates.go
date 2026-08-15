package kafka

import (
	"context"
)

// UpdateBrokerCount updates the number of broker nodes in a cluster.
func (b *InMemoryBackend) UpdateBrokerCount(
	ctx context.Context,
	clusterArn string,
	numBrokers int32,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateBrokerCount")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	source := &MutableClusterInfo{NumberOfBrokerNodes: c.NumberOfBrokerNodes}
	c.NumberOfBrokerNodes = numBrokers
	target := &MutableClusterInfo{NumberOfBrokerNodes: numBrokers}
	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_BROKER_COUNT", source, target)

	return op, nil
}

// UpdateBrokerStorage updates the EBS storage size for broker nodes.
func (b *InMemoryBackend) UpdateBrokerStorage(
	ctx context.Context,
	clusterArn string,
	volumeSize int32,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateBrokerStorage")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	if c.BrokerNodeGroupInfo.StorageInfo == nil {
		c.BrokerNodeGroupInfo.StorageInfo = &StorageInfo{}
	}

	if c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo == nil {
		c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo = &EBSStorageInfo{}
	}

	c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize = volumeSize
	target := &MutableClusterInfo{BrokerEBSVolumeInfo: []BrokerEBSVolumeInfo{{VolumeSizeGB: volumeSize}}}
	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_BROKER_STORAGE", nil, target)

	return op, nil
}

// UpdateBrokerType updates the instance type for broker nodes.
func (b *InMemoryBackend) UpdateBrokerType(
	ctx context.Context,
	clusterArn, instanceType string,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateBrokerType")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	c.BrokerNodeGroupInfo.InstanceType = instanceType
	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_BROKER_TYPE", nil, nil)

	return op, nil
}

// UpdateClusterConfiguration updates the configuration for a cluster.
func (b *InMemoryBackend) UpdateClusterConfiguration(
	ctx context.Context,
	clusterArn, configArn string,
	revision int64,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateClusterConfiguration")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	c.ConfigurationInfo = &ConfigurationInfo{
		Arn:      configArn,
		Revision: revision,
	}
	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_CLUSTER_CONFIGURATION", nil, nil)

	return op, nil
}

// UpdateClusterKafkaVersion updates the Kafka version for a cluster.
func (b *InMemoryBackend) UpdateClusterKafkaVersion(
	ctx context.Context,
	clusterArn, targetKafkaVersion string,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateClusterKafkaVersion")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	c.KafkaVersion = targetKafkaVersion
	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_CLUSTER_KAFKA_VERSION", nil, nil)

	return op, nil
}

// UpdateConnectivity updates broker connectivity settings for a cluster, persisting
// the new ConnectivityInfo onto the broker node group and recording an operation
// whose source/target reflect the before/after state.
func (b *InMemoryBackend) UpdateConnectivity(
	ctx context.Context,
	clusterArn string, settings UpdateConnectivitySettings,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateConnectivity")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	source := &MutableClusterInfo{
		ConnectivityInfo: cloneConnectivityInfo(c.BrokerNodeGroupInfo.ConnectivityInfo),
	}
	target := &MutableClusterInfo{ConnectivityInfo: cloneConnectivityInfo(settings.ConnectivityInfo)}

	if settings.ConnectivityInfo != nil {
		c.BrokerNodeGroupInfo.ConnectivityInfo = cloneConnectivityInfo(settings.ConnectivityInfo)
	}

	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_CONNECTIVITY", source, target)

	return op, nil
}

// UpdateMonitoring updates monitoring/logging settings for a cluster, persisting the
// new EnhancedMonitoring/OpenMonitoring/LoggingInfo and recording an operation.
func (b *InMemoryBackend) UpdateMonitoring(
	ctx context.Context,
	clusterArn string, settings UpdateMonitoringSettings,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateMonitoring")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	source := &MutableClusterInfo{
		EnhancedMonitoring: c.EnhancedMonitoring,
		OpenMonitoring:     cloneOpenMonitoring(c.OpenMonitoring),
		LoggingInfo:        cloneLoggingInfo(c.LoggingInfo),
	}
	target := &MutableClusterInfo{
		EnhancedMonitoring: settings.EnhancedMonitoring,
		OpenMonitoring:     cloneOpenMonitoring(settings.OpenMonitoring),
		LoggingInfo:        cloneLoggingInfo(settings.LoggingInfo),
	}

	if settings.EnhancedMonitoring != "" {
		c.EnhancedMonitoring = settings.EnhancedMonitoring
	}
	if settings.OpenMonitoring != nil {
		c.OpenMonitoring = cloneOpenMonitoring(settings.OpenMonitoring)
	}
	if settings.LoggingInfo != nil {
		c.LoggingInfo = cloneLoggingInfo(settings.LoggingInfo)
	}

	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_MONITORING", source, target)

	return op, nil
}

// UpdateRebalancing updates a cluster's intelligent rebalancing status,
// persisting the new Rebalancing.Status and recording an operation whose
// source/target reflect the before/after state (mirroring UpdateMonitoring).
func (b *InMemoryBackend) UpdateRebalancing(
	ctx context.Context, clusterArn, status string,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateRebalancing")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	source := &MutableClusterInfo{Rebalancing: cloneRebalancing(c.Rebalancing)}
	c.Rebalancing = &Rebalancing{Status: status}
	target := &MutableClusterInfo{Rebalancing: cloneRebalancing(c.Rebalancing)}

	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_REBALANCING", source, target)

	return op, nil
}

// UpdateSecurity updates authentication/encryption settings for a cluster, persisting
// the new ClientAuthentication/EncryptionInfo and recording an operation.
func (b *InMemoryBackend) UpdateSecurity(
	ctx context.Context,
	clusterArn string, settings UpdateSecuritySettings,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateSecurity")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	source := &MutableClusterInfo{
		ClientAuthentication: cloneClientAuth(c.ClientAuthentication),
		EncryptionInfo:       cloneEncryptionInfo(c.EncryptionInfo),
	}
	target := &MutableClusterInfo{
		ClientAuthentication: cloneClientAuth(settings.ClientAuthentication),
		EncryptionInfo:       cloneEncryptionInfo(settings.EncryptionInfo),
	}

	if settings.ClientAuthentication != nil {
		c.ClientAuthentication = cloneClientAuth(settings.ClientAuthentication)
	}
	if settings.EncryptionInfo != nil {
		c.EncryptionInfo = cloneEncryptionInfo(settings.EncryptionInfo)
	}

	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_SECURITY", source, target)

	return op, nil
}

// UpdateStorage updates broker storage settings for a cluster, persisting the new
// StorageMode and EBS volume size/throughput and recording an operation.
func (b *InMemoryBackend) UpdateStorage(
	ctx context.Context,
	clusterArn string, settings UpdateStorageSettings,
) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateStorage")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	source := &MutableClusterInfo{StorageMode: c.StorageMode}
	if si := c.BrokerNodeGroupInfo.StorageInfo; si != nil && si.EbsStorageInfo != nil {
		source.BrokerEBSVolumeInfo = []BrokerEBSVolumeInfo{{
			VolumeSizeGB:          si.EbsStorageInfo.VolumeSize,
			ProvisionedThroughput: cloneProvisionedThroughput(si.EbsStorageInfo.ProvisionedThroughput),
		}}
	}

	target := &MutableClusterInfo{StorageMode: settings.StorageMode}
	if settings.VolumeSizeGB > 0 || settings.ProvisionedThroughput != nil {
		target.BrokerEBSVolumeInfo = []BrokerEBSVolumeInfo{{
			VolumeSizeGB:          settings.VolumeSizeGB,
			ProvisionedThroughput: cloneProvisionedThroughput(settings.ProvisionedThroughput),
		}}
	}

	if settings.StorageMode != "" {
		c.StorageMode = settings.StorageMode
	}
	if settings.VolumeSizeGB > 0 || settings.ProvisionedThroughput != nil {
		applyStorageUpdateLocked(c, settings)
	}

	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_STORAGE", source, target)

	return op, nil
}

// applyStorageUpdateLocked mutates the cluster's EBS storage info from an
// UpdateStorage payload. The caller must hold the write lock.
func applyStorageUpdateLocked(c *Cluster, settings UpdateStorageSettings) {
	if c.BrokerNodeGroupInfo.StorageInfo == nil {
		c.BrokerNodeGroupInfo.StorageInfo = &StorageInfo{}
	}
	if c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo == nil {
		c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo = &EBSStorageInfo{}
	}
	ebs := c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo
	if settings.VolumeSizeGB > 0 {
		ebs.VolumeSize = settings.VolumeSizeGB
	}
	if settings.ProvisionedThroughput != nil {
		ebs.ProvisionedThroughput = cloneProvisionedThroughput(settings.ProvisionedThroughput)
	}
}

// cloneProvisionedThroughput returns a deep copy of a ProvisionedThroughput.
func cloneProvisionedThroughput(pt *ProvisionedThroughput) *ProvisionedThroughput {
	if pt == nil {
		return nil
	}
	clone := *pt

	return &clone
}

// RebootBroker initiates a broker reboot operation.
func (b *InMemoryBackend) RebootBroker(ctx context.Context, clusterArn string, _ []string) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("RebootBroker")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	op := b.newClusterOperationLocked(region, clusterArn, "REBOOT_BROKER", nil, nil)

	return op, nil
}
