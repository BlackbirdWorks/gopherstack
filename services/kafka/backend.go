// Package kafka provides an in-memory stub of AWS MSK (Managed Streaming for Apache Kafka).
package kafka

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// MSK resources are isolated per region: every backend operation resolves the
// caller's region from the request context (for create/list operations) or from
// the resource ARN (for operations that target an existing ARN) and operates only
// on that region's nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

const (
	// kafkaVersion360 is the MSK Kafka 3.6.0 version identifier.
	kafkaVersion360 = "3.6.0"
	// kafkaVersion351 is the MSK Kafka 3.5.1 version identifier.
	kafkaVersion351 = "3.5.1"
)

const (
	// ReplicatorStateRunning indicates a running replicator.
	ReplicatorStateRunning = "RUNNING"
	// VpcConnectionStateAvailable indicates a VPC connection that is available.
	VpcConnectionStateAvailable = "AVAILABLE"
	// ClusterOperationStateUpdateComplete indicates a completed cluster operation.
	ClusterOperationStateUpdateComplete = "UPDATE_COMPLETE"
	// DefaultClusterVersion is the default MSK cluster version identifier.
	DefaultClusterVersion = "K3AEGXETSR30VB"
)

const (
	// ClusterStateActive indicates a running cluster.
	ClusterStateActive = "ACTIVE"
	// ClusterStateCreating indicates a cluster being provisioned.
	ClusterStateCreating = "CREATING"
	// ClusterStateDeleting indicates a cluster being removed.
	ClusterStateDeleting = "DELETING"
	// ClusterStateFailed indicates a cluster in a failed state.
	ClusterStateFailed = "FAILED"
	// ClusterStateUpdating indicates a cluster undergoing an update.
	ClusterStateUpdating = "UPDATING"
	// ClusterStateRebootingBroker indicates a broker reboot in progress.
	ClusterStateRebootingBroker = "REBOOTING_BROKER"
	// ClusterStateMaintenance indicates a cluster in a maintenance window.
	ClusterStateMaintenance = "MAINTENANCE"
	// ClusterStateHealing indicates a cluster undergoing node healing.
	ClusterStateHealing = "HEALING"
)

const (
	// ClusterTypeProvisioned is the standard MSK cluster type.
	ClusterTypeProvisioned = "PROVISIONED"
	// ClusterTypeServerless is the MSK Serverless cluster type.
	ClusterTypeServerless = "SERVERLESS"
)

const (
	// EnhancedMonitoringDefault is the default monitoring level.
	EnhancedMonitoringDefault = "DEFAULT"
	// EnhancedMonitoringPerBroker enables per-broker metrics.
	EnhancedMonitoringPerBroker = "PER_BROKER"
	// EnhancedMonitoringPerTopicPerBroker enables per-topic-per-broker metrics.
	EnhancedMonitoringPerTopicPerBroker = "PER_TOPIC_PER_BROKER"
	// EnhancedMonitoringPerTopicPerPartition enables per-topic-per-partition metrics.
	EnhancedMonitoringPerTopicPerPartition = "PER_TOPIC_PER_PARTITION"
)

const (
	// StorageModeLocal is the standard EBS-only storage mode.
	StorageModeLocal = "LOCAL"
	// StorageModeTiered enables tiered storage (remote storage offload).
	StorageModeTiered = "TIERED"
)

const (
	// EncryptionInTransitTLS requires TLS for all client-broker traffic.
	EncryptionInTransitTLS = "TLS"
	// EncryptionInTransitTLSPlaintext allows both TLS and plaintext.
	EncryptionInTransitTLSPlaintext = "TLS_PLAINTEXT"
	// EncryptionInTransitPlaintext allows plaintext only (dev/test use).
	EncryptionInTransitPlaintext = "PLAINTEXT"
)

const (
	// defaultBrokerCount is used in seed helpers for testing.
	defaultBrokerCount = 3
	// defaultReplicationFactor is used in AddTopicInternal.
	defaultReplicationFactor = 3
	// defaultPartitionCount is used in AddTopicInternal.
	defaultPartitionCount = 1
	// maxClustersPerRegion caps the number of clusters per region to prevent unbounded growth.
	maxClustersPerRegion = 500
)

// ProvisionedThroughput holds EBS provisioned throughput config.
type ProvisionedThroughput struct {
	VolumeThroughput int32 `json:"volumeThroughput,omitempty"`
	Enabled          bool  `json:"enabled"`
}

// EBSStorageInfo holds EBS volume config.
type EBSStorageInfo struct {
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
	VolumeSize            int32                  `json:"volumeSize,omitempty"`
}

// StorageInfo holds broker storage config.
type StorageInfo struct {
	EbsStorageInfo *EBSStorageInfo `json:"ebsStorageInfo,omitempty"`
}

// PublicAccess holds public access configuration for broker connectivity.
type PublicAccess struct {
	Type string `json:"type,omitempty"`
}

// VpcConnectivitySaslIam holds IAM authentication settings for VPC connectivity.
type VpcConnectivitySaslIam struct {
	Enabled bool `json:"enabled"`
}

// VpcConnectivitySaslScram holds SCRAM authentication settings for VPC connectivity.
type VpcConnectivitySaslScram struct {
	Enabled bool `json:"enabled"`
}

// VpcConnectivitySasl holds SASL settings for VPC connectivity.
type VpcConnectivitySasl struct {
	Iam   *VpcConnectivitySaslIam   `json:"iam,omitempty"`
	Scram *VpcConnectivitySaslScram `json:"scram,omitempty"`
}

// VpcConnectivityTLS holds TLS settings for VPC connectivity.
type VpcConnectivityTLS struct {
	Enabled bool `json:"enabled"`
}

// VpcConnectivityClientAuthentication holds authentication settings for VPC connectivity.
type VpcConnectivityClientAuthentication struct {
	Sasl *VpcConnectivitySasl `json:"sasl,omitempty"`
	TLS  *VpcConnectivityTLS  `json:"tls,omitempty"`
}

// VpcConnectivity holds VPC connectivity configuration.
type VpcConnectivity struct {
	ClientAuthentication *VpcConnectivityClientAuthentication `json:"clientAuthentication,omitempty"`
}

// ConnectivityInfo holds broker connectivity configuration.
type ConnectivityInfo struct {
	PublicAccess    *PublicAccess    `json:"publicAccess,omitempty"`
	VpcConnectivity *VpcConnectivity `json:"vpcConnectivity,omitempty"`
}

// BrokerNodeGroupInfo holds broker node configuration.
type BrokerNodeGroupInfo struct {
	ConnectivityInfo     *ConnectivityInfo `json:"connectivityInfo,omitempty"`
	StorageInfo          *StorageInfo      `json:"storageInfo,omitempty"`
	BrokerAZDistribution string            `json:"brokerAZDistribution,omitempty"`
	InstanceType         string            `json:"instanceType"`
	ZoneIDs              []string          `json:"zoneIds,omitempty"`
	ClientSubnets        []string          `json:"clientSubnets"`
	SecurityGroups       []string          `json:"securityGroups,omitempty"`
}

// ConfigurationInfo holds a cluster configuration reference.
type ConfigurationInfo struct {
	Arn      string `json:"arn"`
	Revision int64  `json:"revision"`
}

// ClientAuthentication holds MSK cluster authentication configuration.
type ClientAuthentication struct {
	Sasl            *SaslSettings            `json:"sasl,omitempty"`
	TLS             *TLSSettings             `json:"tls,omitempty"`
	Unauthenticated *UnauthenticatedSettings `json:"unauthenticated,omitempty"`
}

// SaslSettings holds SASL authentication settings.
type SaslSettings struct {
	Scram *SaslScram `json:"scram,omitempty"`
	Iam   *SaslIam   `json:"iam,omitempty"`
}

// SaslScram holds SASL/SCRAM settings.
type SaslScram struct {
	Enabled bool `json:"enabled"`
}

// SaslIam holds SASL/IAM settings.
type SaslIam struct {
	Enabled bool `json:"enabled"`
}

// TLSSettings holds TLS authentication settings.
type TLSSettings struct {
	CertificateAuthorityArnList []string `json:"certificateAuthorityArnList,omitempty"`
	Enabled                     bool     `json:"enabled"`
}

// UnauthenticatedSettings holds unauthenticated access settings.
type UnauthenticatedSettings struct {
	Enabled bool `json:"enabled"`
}

// EncryptionAtRest holds at-rest encryption configuration.
type EncryptionAtRest struct {
	DataVolumeKMSKeyID string `json:"dataVolumeKMSKeyId,omitempty"`
}

// EncryptionInTransit holds in-transit encryption configuration.
type EncryptionInTransit struct {
	ClientBroker string `json:"clientBroker,omitempty"`
	InCluster    bool   `json:"inCluster"`
}

// EncryptionInfo holds cluster encryption configuration.
type EncryptionInfo struct {
	EncryptionAtRest    *EncryptionAtRest    `json:"encryptionAtRest,omitempty"`
	EncryptionInTransit *EncryptionInTransit `json:"encryptionInTransit,omitempty"`
}

// JmxExporter holds JMX exporter settings.
type JmxExporter struct {
	EnabledInBroker bool `json:"enabledInBroker"`
}

// NodeExporter holds Node exporter settings.
type NodeExporter struct {
	EnabledInBroker bool `json:"enabledInBroker"`
}

// PrometheusInfo holds Prometheus scraping configuration.
type PrometheusInfo struct {
	JmxExporter  *JmxExporter  `json:"jmxExporter,omitempty"`
	NodeExporter *NodeExporter `json:"nodeExporter,omitempty"`
}

// OpenMonitoring holds open monitoring configuration.
type OpenMonitoring struct {
	Prometheus *PrometheusInfo `json:"prometheus,omitempty"`
}

// CloudWatchLogs holds CloudWatch log delivery settings.
type CloudWatchLogs struct {
	LogGroup string `json:"logGroup,omitempty"`
	Enabled  bool   `json:"enabled"`
}

// Firehose holds Kinesis Data Firehose log delivery settings.
type Firehose struct {
	DeliveryStream string `json:"deliveryStream,omitempty"`
	Enabled        bool   `json:"enabled"`
}

// S3Logs holds S3 log delivery settings.
type S3Logs struct {
	Bucket  string `json:"bucket,omitempty"`
	Prefix  string `json:"prefix,omitempty"`
	Enabled bool   `json:"enabled"`
}

// BrokerLogs holds broker log delivery destinations.
type BrokerLogs struct {
	CloudWatchLogs *CloudWatchLogs `json:"cloudWatchLogs,omitempty"`
	Firehose       *Firehose       `json:"firehose,omitempty"`
	S3             *S3Logs         `json:"s3,omitempty"`
}

// LoggingInfo holds cluster logging configuration.
type LoggingInfo struct {
	BrokerLogs *BrokerLogs `json:"brokerLogs,omitempty"`
}

// StateInfo holds cluster state detail for error conditions.
type StateInfo struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// ServerlessVpcConfig holds VPC configuration for a serverless cluster.
type ServerlessVpcConfig struct {
	SubnetIDs        []string `json:"subnetIds,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
}

// ServerlessClientAuthentication holds authentication settings for a serverless cluster.
type ServerlessClientAuthentication struct {
	Sasl *SaslSettings `json:"sasl,omitempty"`
}

// ServerlessClusterInfo holds serverless-specific cluster configuration.
type ServerlessClusterInfo struct {
	ClientAuthentication *ServerlessClientAuthentication `json:"clientAuthentication,omitempty"`
	VpcConfigs           []ServerlessVpcConfig           `json:"vpcConfigs,omitempty"`
}

// Cluster represents an MSK cluster.
type Cluster struct {
	Tags                 map[string]string      `json:"-"`
	ClientAuthentication *ClientAuthentication  `json:"clientAuthentication,omitempty"`
	EncryptionInfo       *EncryptionInfo        `json:"encryptionInfo,omitempty"`
	OpenMonitoring       *OpenMonitoring        `json:"openMonitoring,omitempty"`
	LoggingInfo          *LoggingInfo           `json:"loggingInfo,omitempty"`
	StateInfo            *StateInfo             `json:"stateInfo,omitempty"`
	Serverless           *ServerlessClusterInfo `json:"serverless,omitempty"`
	ConfigurationInfo    *ConfigurationInfo     `json:"configurationInfo,omitempty"`
	ClusterArn           string                 `json:"clusterArn"`
	ClusterName          string                 `json:"clusterName"`
	ClusterType          string                 `json:"clusterType"`
	KafkaVersion         string                 `json:"kafkaVersion,omitempty"`
	State                string                 `json:"state"`
	CurrentVersion       string                 `json:"currentVersion"`
	ActiveOperationArn   string                 `json:"activeOperationArn,omitempty"`
	EnhancedMonitoring   string                 `json:"enhancedMonitoring,omitempty"`
	StorageMode          string                 `json:"storageMode,omitempty"`
	CreationTime         string                 `json:"creationTime,omitempty"`
	BrokerNodeGroupInfo  BrokerNodeGroupInfo    `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes  int32                  `json:"numberOfBrokerNodes"`
	pollCount            int                    // tracks CREATING→ACTIVE progression; not serialized
}

// Configuration represents an MSK configuration.
type Configuration struct {
	Tags             map[string]string `json:"-"`
	Arn              string            `json:"arn"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	ServerProperties string            `json:"serverProperties"`
	KafkaVersions    []string          `json:"kafkaVersions"`
}

// InMemoryBackend stores MSK state in memory.
//
// Every ARN-keyed resource table below is keyed by the resource's own ARN,
// which already encodes its region (arn:partition:service:region:account:
// resource, see regionFromARN) — so same-named resources in different
// regions are fully isolated without an outer region-keyed map layer.
// Operations that take an existing resource ARN resolve their region from
// the ARN itself when they need to build a NEW related ARN; point lookups by
// an existing ARN need no region resolution at all. See store_setup.go for
// the full table/index registration and the rationale for each one.
type InMemoryBackend struct {
	registry                   *store.Registry
	clusters                   *store.Table[Cluster]
	clustersByRegion           *store.Index[Cluster]
	clustersByName             *store.Index[Cluster]
	configurations             *store.Table[Configuration]
	configurationsByRegion     *store.Index[Configuration]
	replicators                *store.Table[Replicator]
	replicatorsByRegion        *store.Index[Replicator]
	topics                     *store.Table[Topic]
	topicsByCluster            *store.Index[Topic]
	vpcConnections             *store.Table[VpcConnection]
	vpcConnectionsByRegion     *store.Index[VpcConnection]
	vpcConnectionsByCluster    *store.Index[VpcConnection]
	clusterOperations          *store.Table[ClusterOperation]
	clusterOperationsByCluster *store.Index[ClusterOperation]
	scramSecrets               map[string][]string // clusterArn → []secretArn (raw: slice-valued, not *T)
	clusterPolicies            map[string]string   // clusterArn → policy document (raw: string-valued, not *T)
	mu                         *lockmetrics.RWMutex
	accountID                  string
	region                     string
}

// NewInMemoryBackend creates a new in-memory MSK backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		scramSecrets:    make(map[string][]string),
		clusterPolicies: make(map[string]string),
		mu:              lockmetrics.New("kafka"),
		accountID:       accountID,
		region:          region,
	}
	registerAllTables(b)

	return b
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all state, returning the backend to a clean empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.scramSecrets = make(map[string][]string)
	b.clusterPolicies = make(map[string]string)
}

// clusterARN builds an ARN for an MSK cluster in region.
func (b *InMemoryBackend) clusterARN(region, name string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("cluster/%s/%s", name, uuid.New().String()),
	)
}

// configurationARN builds an ARN for an MSK configuration in region.
func (b *InMemoryBackend) configurationARN(region, name string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("configuration/%s/%s", name, uuid.New().String()),
	)
}

// replicatorARN builds an ARN for an MSK replicator in region.
func (b *InMemoryBackend) replicatorARN(region, name string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("replicator/%s/%s", name, uuid.New().String()),
	)
}

// vpcConnectionARN builds an ARN for an MSK VPC connection in region.
func (b *InMemoryBackend) vpcConnectionARN(region, clusterArn, vpcID string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("vpc-connection/%s/%s/%s", clusterArn, vpcID, uuid.New().String()),
	)
}

// clusterOperationARN builds an ARN for an MSK cluster operation in region.
func (b *InMemoryBackend) clusterOperationARN(region, clusterArn string) string {
	return arn.Build(
		"kafka",
		region,
		b.accountID,
		fmt.Sprintf("cluster-operation/%s/%s", clusterArn, uuid.New().String()),
	)
}

// topicKey returns the composite key used to store a topic in memory.
func topicKey(clusterArn, topicName string) string {
	return clusterArn + "|" + topicName
}

// --- Cluster operations ---

// CreateCluster creates a new MSK cluster.
func (b *InMemoryBackend) CreateCluster(
	ctx context.Context,
	name, kafkaVersion string,
	numBrokers int32,
	brokerInfo BrokerNodeGroupInfo,
	clientAuth *ClientAuthentication,
	tags map[string]string,
) (*Cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("clusterName is required: %w", ErrValidation)
	}

	if numBrokers < 1 {
		return nil, fmt.Errorf("numberOfBrokerNodes must be at least 1: %w", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if len(b.clustersByName.Get(region+"|"+name)) > 0 {
		return nil, ErrAlreadyExists
	}

	if regionClusters := b.clustersByRegion.Get(region); len(regionClusters) >= maxClustersPerRegion {
		victim := regionClusters[0]
		b.clusters.Delete(victim.ClusterArn)
	}

	clusterArn := b.clusterARN(region, name)
	safeInfo := BrokerNodeGroupInfo{
		BrokerAZDistribution: brokerInfo.BrokerAZDistribution,
		InstanceType:         brokerInfo.InstanceType,
		ClientSubnets:        append([]string(nil), brokerInfo.ClientSubnets...),
		SecurityGroups:       append([]string(nil), brokerInfo.SecurityGroups...),
		ZoneIDs:              append([]string(nil), brokerInfo.ZoneIDs...),
		StorageInfo:          brokerInfo.StorageInfo,
		ConnectivityInfo:     brokerInfo.ConnectivityInfo,
	}
	if brokerInfo.StorageInfo != nil {
		safeInfo.StorageInfo = cloneStorageInfo(brokerInfo.StorageInfo)
	}
	if brokerInfo.ConnectivityInfo != nil {
		safeInfo.ConnectivityInfo = cloneConnectivityInfo(brokerInfo.ConnectivityInfo)
	}
	cluster := &Cluster{
		ClusterArn:           clusterArn,
		ClusterName:          name,
		ClusterType:          ClusterTypeProvisioned,
		KafkaVersion:         kafkaVersion,
		NumberOfBrokerNodes:  numBrokers,
		BrokerNodeGroupInfo:  safeInfo,
		ClientAuthentication: cloneClientAuth(clientAuth),
		State:                ClusterStateCreating,
		CurrentVersion:       DefaultClusterVersion,
		Tags:                 nonNilTagsCopy(tags),
	}
	b.clusters.Put(cluster)

	return cloneCluster(cluster), nil
}

// CreateServerlessCluster creates a new MSK Serverless cluster.
func (b *InMemoryBackend) CreateServerlessCluster(
	ctx context.Context,
	name string,
	serverless *ServerlessClusterInfo,
	tags map[string]string,
) (*Cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("clusterName is required: %w", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateServerlessCluster")
	defer b.mu.Unlock()

	if len(b.clustersByName.Get(region+"|"+name)) > 0 {
		return nil, ErrAlreadyExists
	}

	if regionClusters := b.clustersByRegion.Get(region); len(regionClusters) >= maxClustersPerRegion {
		victim := regionClusters[0]
		b.clusters.Delete(victim.ClusterArn)
	}

	clusterArn := b.clusterARN(region, name)
	cluster := &Cluster{
		ClusterArn:     clusterArn,
		ClusterName:    name,
		ClusterType:    ClusterTypeServerless,
		State:          ClusterStateCreating,
		CurrentVersion: DefaultClusterVersion,
		Tags:           nonNilTagsCopy(tags),
		Serverless:     cloneServerless(serverless),
	}
	b.clusters.Put(cluster)

	return cloneCluster(cluster), nil
}

// DescribeCluster retrieves a cluster by ARN, advancing CREATING→ACTIVE on first poll.
func (b *InMemoryBackend) DescribeCluster(_ context.Context, clusterArn string) (*Cluster, error) {
	b.mu.Lock("DescribeCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	if c.State == ClusterStateCreating {
		c.pollCount++
		if c.pollCount >= 1 {
			c.State = ClusterStateActive
		}
	}

	return cloneCluster(c), nil
}

// ListClusters returns all MSK clusters in the request's region sorted by name.
func (b *InMemoryBackend) ListClusters(ctx context.Context) []*Cluster {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	clusters := b.clustersByRegion.Get(region)
	out := make([]*Cluster, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, cloneCluster(c))
	}

	slices.SortFunc(out, func(a, b *Cluster) int {
		if a.ClusterName < b.ClusterName {
			return -1
		}
		if a.ClusterName > b.ClusterName {
			return 1
		}

		return 0
	})

	return out
}

// DeleteCluster deletes a cluster by ARN, cascading to its SCRAM secrets, topics and cluster policy.
func (b *InMemoryBackend) DeleteCluster(_ context.Context, clusterArn string) error {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return ErrNotFound
	}

	b.clusters.Delete(clusterArn)
	delete(b.scramSecrets, clusterArn)
	delete(b.clusterPolicies, clusterArn)

	// Remove all topics belonging to this cluster. The index's slice is
	// cloned before deleting from it, since Table.Delete mutates
	// topicsByCluster's backing group in place and would otherwise corrupt
	// this loop's own iteration.
	for _, t := range slices.Clone(b.topicsByCluster.Get(clusterArn)) {
		b.topics.Delete(topicKey(t.ClusterArn, t.TopicName))
	}

	return nil
}

// --- Configuration operations ---

// CreateConfiguration creates a new MSK configuration.
func (b *InMemoryBackend) CreateConfiguration(
	ctx context.Context,
	name, description string,
	kafkaVersions []string,
	serverProperties string,
) (*Configuration, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required: %w", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	for _, c := range b.configurationsByRegion.Get(region) {
		if c.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	configArn := b.configurationARN(region, name)
	kvs := make([]string, len(kafkaVersions))
	copy(kvs, kafkaVersions)
	config := &Configuration{
		Arn:              configArn,
		Name:             name,
		Description:      description,
		KafkaVersions:    kvs,
		ServerProperties: serverProperties,
		Tags:             make(map[string]string),
	}
	b.configurations.Put(config)

	return cloneConfiguration(config), nil
}

// DescribeConfiguration retrieves a configuration by ARN.
func (b *InMemoryBackend) DescribeConfiguration(_ context.Context, configArn string) (*Configuration, error) {
	b.mu.RLock("DescribeConfiguration")
	defer b.mu.RUnlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneConfiguration(c), nil
}

// ListConfigurations returns all MSK configurations in the request's region sorted by name.
func (b *InMemoryBackend) ListConfigurations(ctx context.Context) []*Configuration {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	configurations := b.configurationsByRegion.Get(region)
	out := make([]*Configuration, 0, len(configurations))
	for _, c := range configurations {
		out = append(out, cloneConfiguration(c))
	}

	slices.SortFunc(out, func(a, b *Configuration) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// DeleteConfiguration deletes a configuration by ARN.
func (b *InMemoryBackend) DeleteConfiguration(_ context.Context, configArn string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	if !b.configurations.Delete(configArn) {
		return ErrNotFound
	}

	return nil
}

// --- Tag operations ---

// TagResource adds tags to a cluster, configuration, replicator, or VPC connection by ARN.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if c, ok := b.clusters.Get(resourceArn); ok {
		maps.Copy(c.Tags, tags)

		return nil
	}

	if c, ok := b.configurations.Get(resourceArn); ok {
		maps.Copy(c.Tags, tags)

		return nil
	}

	if r, ok := b.replicators.Get(resourceArn); ok {
		maps.Copy(r.Tags, tags)

		return nil
	}

	if v, ok := b.vpcConnections.Get(resourceArn); ok {
		maps.Copy(v.Tags, tags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a cluster, configuration, replicator, or VPC connection by ARN.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if c, ok := b.clusters.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if c, ok := b.configurations.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if r, ok := b.replicators.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	if v, ok := b.vpcConnections.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(v.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// GetTags retrieves tags for a cluster, configuration, replicator, or VPC connection by ARN.
func (b *InMemoryBackend) GetTags(_ context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if c, ok := b.clusters.Get(resourceArn); ok {
		return maps.Clone(c.Tags), nil
	}

	if c, ok := b.configurations.Get(resourceArn); ok {
		return maps.Clone(c.Tags), nil
	}

	if r, ok := b.replicators.Get(resourceArn); ok {
		return maps.Clone(r.Tags), nil
	}

	if v, ok := b.vpcConnections.Get(resourceArn); ok {
		return maps.Clone(v.Tags), nil
	}

	return nil, ErrNotFound
}

// --- SCRAM secret operations ---

// BatchAssociateScramSecret associates a list of SCRAM secrets with a cluster.
// It returns any errors that occurred for individual secrets.
func (b *InMemoryBackend) BatchAssociateScramSecret(
	_ context.Context,
	clusterArn string,
	secretArnList []string,
) ([]ScramSecretError, error) {
	b.mu.Lock("BatchAssociateScramSecret")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	existing := b.scramSecrets[clusterArn]
	existingSet := make(map[string]struct{}, len(existing))

	for _, s := range existing {
		existingSet[s] = struct{}{}
	}

	for _, secretArn := range secretArnList {
		if _, found := existingSet[secretArn]; !found {
			existing = append(existing, secretArn)
			existingSet[secretArn] = struct{}{}
		}
	}

	b.scramSecrets[clusterArn] = existing

	return []ScramSecretError{}, nil
}

// BatchDisassociateScramSecret disassociates a list of SCRAM secrets from a cluster.
// It returns any errors that occurred for individual secrets.
func (b *InMemoryBackend) BatchDisassociateScramSecret(
	_ context.Context,
	clusterArn string,
	secretArnList []string,
) ([]ScramSecretError, error) {
	b.mu.Lock("BatchDisassociateScramSecret")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	removeSet := make(map[string]struct{}, len(secretArnList))

	for _, s := range secretArnList {
		removeSet[s] = struct{}{}
	}

	existing := b.scramSecrets[clusterArn]
	kept := make([]string, 0, len(existing))

	for _, s := range existing {
		if _, remove := removeSet[s]; !remove {
			kept = append(kept, s)
		}
	}

	b.scramSecrets[clusterArn] = kept

	return []ScramSecretError{}, nil
}

// --- Replicator operations ---

// CreateReplicator creates a new MSK replicator.
func (b *InMemoryBackend) CreateReplicator(
	ctx context.Context,
	name, description, serviceExecutionRoleArn string,
	tags map[string]string,
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
		Tags:                    nonNilTagsCopy(tags),
	}
	b.replicators.Put(replicator)

	return cloneReplicator(replicator), nil
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

// --- Topic operations ---

// CreateTopic creates a topic on an MSK cluster.
func (b *InMemoryBackend) CreateTopic(
	_ context.Context,
	clusterArn, topicName string,
	replicationFactor, numPartitions int32,
	configEntries map[string]string,
) (*Topic, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topicName is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateTopic")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	key := topicKey(clusterArn, topicName)
	if b.topics.Has(key) {
		return nil, ErrAlreadyExists
	}

	topic := &Topic{
		TopicName:         topicName,
		ClusterArn:        clusterArn,
		ReplicationFactor: replicationFactor,
		NumPartitions:     numPartitions,
		ConfigEntries:     nonNilMapCopy(configEntries),
	}
	b.topics.Put(topic)

	return cloneTopic(topic), nil
}

// DeleteTopic deletes a topic from an MSK cluster.
func (b *InMemoryBackend) DeleteTopic(_ context.Context, clusterArn, topicName string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return ErrNotFound
	}

	if !b.topics.Delete(topicKey(clusterArn, topicName)) {
		return ErrNotFound
	}

	return nil
}

// --- VPC connection operations ---

// CreateVpcConnection creates a new VPC connection to an MSK cluster.
func (b *InMemoryBackend) CreateVpcConnection(
	ctx context.Context,
	targetClusterArn, vpcID, authentication string,
	tags map[string]string,
) (*VpcConnection, error) {
	region := regionFromARN(targetClusterArn, getRegion(ctx, b.region))

	b.mu.Lock("CreateVpcConnection")
	defer b.mu.Unlock()

	if !b.clusters.Has(targetClusterArn) {
		return nil, ErrNotFound
	}

	vpcConnectionArn := b.vpcConnectionARN(region, targetClusterArn, vpcID)
	conn := &VpcConnection{
		VpcConnectionArn: vpcConnectionArn,
		TargetClusterArn: targetClusterArn,
		VpcID:            vpcID,
		Authentication:   authentication,
		State:            VpcConnectionStateAvailable,
		Tags:             nonNilTagsCopy(tags),
	}
	b.vpcConnections.Put(conn)

	return cloneVpcConnection(conn), nil
}

// DeleteVpcConnection deletes a VPC connection by ARN.
func (b *InMemoryBackend) DeleteVpcConnection(_ context.Context, vpcConnectionArn string) error {
	b.mu.Lock("DeleteVpcConnection")
	defer b.mu.Unlock()

	if !b.vpcConnections.Delete(vpcConnectionArn) {
		return ErrNotFound
	}

	return nil
}

// --- Replicator describe/list/update operations ---

// DescribeReplicator retrieves a replicator by ARN.
func (b *InMemoryBackend) DescribeReplicator(_ context.Context, replicatorArn string) (*Replicator, error) {
	b.mu.RLock("DescribeReplicator")
	defer b.mu.RUnlock()

	r, ok := b.replicators.Get(replicatorArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneReplicator(r), nil
}

// ListReplicators returns all replicators in the request's region sorted by name.
func (b *InMemoryBackend) ListReplicators(ctx context.Context) []*Replicator {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListReplicators")
	defer b.mu.RUnlock()

	replicators := b.replicatorsByRegion.Get(region)
	out := make([]*Replicator, 0, len(replicators))
	for _, r := range replicators {
		out = append(out, cloneReplicator(r))
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

// UpdateReplicationInfo updates the replicator description.
func (b *InMemoryBackend) UpdateReplicationInfo(
	_ context.Context,
	replicatorArn, description string,
) (*Replicator, error) {
	b.mu.Lock("UpdateReplicationInfo")
	defer b.mu.Unlock()

	r, ok := b.replicators.Get(replicatorArn)
	if !ok {
		return nil, ErrNotFound
	}

	r.Description = description

	return cloneReplicator(r), nil
}

// --- Topic describe/list/update operations ---

// DescribeTopic retrieves a topic by cluster ARN and topic name.
func (b *InMemoryBackend) DescribeTopic(_ context.Context, clusterArn, topicName string) (*Topic, error) {
	b.mu.RLock("DescribeTopic")
	defer b.mu.RUnlock()

	t, ok := b.topics.Get(topicKey(clusterArn, topicName))
	if !ok {
		return nil, ErrNotFound
	}

	return cloneTopic(t), nil
}

// DescribeTopicPartitions retrieves a topic's partition count.
func (b *InMemoryBackend) DescribeTopicPartitions(ctx context.Context, clusterArn, topicName string) (*Topic, error) {
	return b.DescribeTopic(ctx, clusterArn, topicName)
}

// ListTopics returns all topics for a cluster sorted by topic name.
func (b *InMemoryBackend) ListTopics(_ context.Context, clusterArn string) ([]*Topic, error) {
	b.mu.RLock("ListTopics")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	topics := b.topicsByCluster.Get(clusterArn)
	out := make([]*Topic, 0, len(topics))

	for _, t := range topics {
		out = append(out, cloneTopic(t))
	}

	slices.SortFunc(out, func(a, b *Topic) int {
		if a.TopicName < b.TopicName {
			return -1
		}
		if a.TopicName > b.TopicName {
			return 1
		}

		return 0
	})

	return out, nil
}

// UpdateTopic updates a topic's config entries and/or partition count.
func (b *InMemoryBackend) UpdateTopic(
	_ context.Context,
	clusterArn, topicName string,
	numPartitions int32,
	configEntries map[string]string,
) (*Topic, error) {
	b.mu.Lock("UpdateTopic")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(clusterArn, topicName))
	if !ok {
		return nil, ErrNotFound
	}

	if numPartitions > 0 {
		t.NumPartitions = numPartitions
	}

	if configEntries != nil {
		maps.Copy(t.ConfigEntries, configEntries)
	}

	return cloneTopic(t), nil
}

// --- VPC connection describe/list/reject operations ---

// DescribeVpcConnection retrieves a VPC connection by ARN.
func (b *InMemoryBackend) DescribeVpcConnection(_ context.Context, vpcConnectionArn string) (*VpcConnection, error) {
	b.mu.RLock("DescribeVpcConnection")
	defer b.mu.RUnlock()

	v, ok := b.vpcConnections.Get(vpcConnectionArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneVpcConnection(v), nil
}

// ListVpcConnections returns all VPC connections in the request's region sorted by ARN.
func (b *InMemoryBackend) ListVpcConnections(ctx context.Context) []*VpcConnection {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListVpcConnections")
	defer b.mu.RUnlock()

	conns := b.vpcConnectionsByRegion.Get(region)
	out := make([]*VpcConnection, 0, len(conns))
	for _, v := range conns {
		out = append(out, cloneVpcConnection(v))
	}

	slices.SortFunc(out, func(a, b *VpcConnection) int {
		if a.VpcConnectionArn < b.VpcConnectionArn {
			return -1
		}
		if a.VpcConnectionArn > b.VpcConnectionArn {
			return 1
		}

		return 0
	})

	return out
}

// collectClusterChildrenLocked verifies the cluster exists, then returns the
// clones of every value idx groups under clusterArn, sorted ascending by
// sortKey. Callers must hold b.mu (read lock).
func collectClusterChildrenLocked[T any](
	clusters *store.Table[Cluster],
	idx *store.Index[T],
	clusterArn string,
	clone func(*T) *T,
	sortKey func(*T) string,
) ([]*T, error) {
	if !clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	items := idx.Get(clusterArn)
	out := make([]*T, 0, len(items))

	for _, item := range items {
		out = append(out, clone(item))
	}

	slices.SortFunc(out, func(a, b *T) int { return strings.Compare(sortKey(a), sortKey(b)) })

	return out, nil
}

// ListClientVpcConnections returns all VPC connections for a given cluster.
func (b *InMemoryBackend) ListClientVpcConnections(_ context.Context, clusterArn string) ([]*VpcConnection, error) {
	b.mu.RLock("ListClientVpcConnections")
	defer b.mu.RUnlock()

	return collectClusterChildrenLocked(
		b.clusters,
		b.vpcConnectionsByCluster,
		clusterArn,
		cloneVpcConnection,
		func(v *VpcConnection) string { return v.VpcConnectionArn },
	)
}

// RejectClientVpcConnection rejects (deletes) a VPC connection.
func (b *InMemoryBackend) RejectClientVpcConnection(ctx context.Context, vpcConnectionArn string) error {
	return b.DeleteVpcConnection(ctx, vpcConnectionArn)
}

// --- Cluster policy get/put operations ---

// GetClusterPolicy retrieves the policy document for a cluster.
// Returns ErrNotFound when the cluster exists but has no policy set — matching AWS behavior.
func (b *InMemoryBackend) GetClusterPolicy(_ context.Context, clusterArn string) (string, error) {
	b.mu.RLock("GetClusterPolicy")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterArn) {
		return "", ErrNotFound
	}

	policy, ok := b.clusterPolicies[clusterArn]
	if !ok {
		return "", fmt.Errorf("no resource-based policy found for cluster %q: %w", clusterArn, ErrNotFound)
	}

	return policy, nil
}

// PutClusterPolicy sets the policy document for a cluster.
func (b *InMemoryBackend) PutClusterPolicy(_ context.Context, clusterArn, policy string) error {
	b.mu.Lock("PutClusterPolicy")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return ErrNotFound
	}

	b.clusterPolicies[clusterArn] = policy

	return nil
}

// --- Cluster operation list operations ---

// ListClusterOperations returns all cluster operations for a cluster.
func (b *InMemoryBackend) ListClusterOperations(_ context.Context, clusterArn string) ([]*ClusterOperation, error) {
	b.mu.RLock("ListClusterOperations")
	defer b.mu.RUnlock()

	return collectClusterChildrenLocked(
		b.clusters,
		b.clusterOperationsByCluster,
		clusterArn,
		cloneClusterOperation,
		func(op *ClusterOperation) string { return op.ClusterOperationArn },
	)
}

// DescribeClusterOperationV2 retrieves a cluster operation (V2) by ARN.
func (b *InMemoryBackend) DescribeClusterOperationV2(
	ctx context.Context,
	clusterOperationArn string,
) (*ClusterOperation, error) {
	return b.DescribeClusterOperation(ctx, clusterOperationArn)
}

// ListClusterOperationsV2 returns all cluster operations for a cluster (V2).
func (b *InMemoryBackend) ListClusterOperationsV2(ctx context.Context, clusterArn string) ([]*ClusterOperation, error) {
	return b.ListClusterOperations(ctx, clusterArn)
}

// --- Configuration revision operations ---

// ConfigurationRevision represents a revision of an MSK configuration.
type ConfigurationRevision struct {
	ConfigurationArn string `json:"configurationArn"`
	Description      string `json:"description,omitempty"`
	ServerProperties string `json:"serverProperties,omitempty"`
	Revision         int64  `json:"revision"`
}

// DescribeConfigurationRevision retrieves a configuration revision.
// In this stub, revision 1 always refers to the current configuration state.
func (b *InMemoryBackend) DescribeConfigurationRevision(
	_ context.Context,
	configArn string,
	revision int64,
) (*ConfigurationRevision, error) {
	b.mu.RLock("DescribeConfigurationRevision")
	defer b.mu.RUnlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	return &ConfigurationRevision{
		ConfigurationArn: c.Arn,
		Revision:         revision,
		Description:      c.Description,
		ServerProperties: c.ServerProperties,
	}, nil
}

// UpdateConfiguration updates a configuration's server properties and description.
func (b *InMemoryBackend) UpdateConfiguration(
	_ context.Context,
	configArn, description, serverProperties string,
) (*Configuration, error) {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	if description != "" {
		c.Description = description
	}

	if serverProperties != "" {
		c.ServerProperties = serverProperties
	}

	return cloneConfiguration(c), nil
}

// ListConfigurationRevisions lists revisions for a configuration.
// In this stub, every configuration has a single revision (revision 1).
func (b *InMemoryBackend) ListConfigurationRevisions(
	_ context.Context,
	configArn string,
) ([]*ConfigurationRevision, error) {
	b.mu.RLock("ListConfigurationRevisions")
	defer b.mu.RUnlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	return []*ConfigurationRevision{
		{
			ConfigurationArn: c.Arn,
			Revision:         1,
			Description:      c.Description,
			ServerProperties: c.ServerProperties,
		},
	}, nil
}

// --- Broker / cluster update operations ---

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

// UpdateConnectivitySettings is the payload for UpdateConnectivity.
type UpdateConnectivitySettings struct {
	ConnectivityInfo *ConnectivityInfo
}

// UpdateMonitoringSettings is the payload for UpdateMonitoring.
type UpdateMonitoringSettings struct {
	OpenMonitoring     *OpenMonitoring
	LoggingInfo        *LoggingInfo
	EnhancedMonitoring string
}

// UpdateSecuritySettings is the payload for UpdateSecurity.
type UpdateSecuritySettings struct {
	ClientAuthentication *ClientAuthentication
	EncryptionInfo       *EncryptionInfo
}

// UpdateStorageSettings is the payload for UpdateStorage.
type UpdateStorageSettings struct {
	ProvisionedThroughput *ProvisionedThroughput
	StorageMode           string
	VolumeSizeGB          int32
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

// UpdateRebalancing records a rebalancing operation for a cluster. AWS MSK exposes
// no per-field rebalancing configuration to persist (it is an action, not a setting),
// so this validates the cluster and records the operation.
func (b *InMemoryBackend) UpdateRebalancing(ctx context.Context, clusterArn string) (*ClusterOperation, error) {
	region := regionFromARN(clusterArn, getRegion(ctx, b.region))

	b.mu.Lock("UpdateRebalancing")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	op := b.newClusterOperationLocked(region, clusterArn, "UPDATE_REBALANCING", nil, nil)

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

// --- SCRAM secret list operations ---

// ListScramSecrets returns all SCRAM secrets for a cluster.
func (b *InMemoryBackend) ListScramSecrets(_ context.Context, clusterArn string) ([]string, error) {
	b.mu.RLock("ListScramSecrets")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	secrets := b.scramSecrets[clusterArn]
	out := make([]string, len(secrets))
	copy(out, secrets)

	return out, nil
}

// --- Misc read ops ---

// ListNodes returns broker node stubs for a cluster.
func (b *InMemoryBackend) ListNodes(_ context.Context, clusterArn string) ([]*BrokerNode, error) {
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	out := make([]*BrokerNode, 0, int(c.NumberOfBrokerNodes))

	for i := range c.NumberOfBrokerNodes {
		out = append(out, &BrokerNode{
			BrokerID:     i + 1,
			InstanceType: c.BrokerNodeGroupInfo.InstanceType,
		})
	}

	return out, nil
}

// ListKafkaVersions returns supported Kafka versions, matching current MSK availability.
// Kafka versions are global (not region-scoped), so ctx is unused.
func (b *InMemoryBackend) ListKafkaVersions(_ context.Context) []*MSKVersion {
	return []*MSKVersion{
		{Version: "3.8.0.kraft", Status: ClusterStateActive},
		{Version: "3.7.x.kraft", Status: ClusterStateActive},
		{Version: kafkaVersion360, Status: ClusterStateActive},
		{Version: kafkaVersion351, Status: ClusterStateActive},
		{Version: "3.4.0", Status: ClusterStateActive},
		{Version: "3.3.2", Status: ClusterStateActive},
		{Version: "3.3.1", Status: ClusterStateActive},
		{Version: "2.8.2.tiered", Status: ClusterStateActive},
		{Version: "2.8.1", Status: ClusterStateActive},
		{Version: "2.8.0", Status: "DEPRECATED"},
		{Version: "2.6.0", Status: "DEPRECATED"},
	}
}

// GetCompatibleKafkaVersions returns Kafka versions compatible with the cluster's current version.
// KRaft clusters can only target KRaft versions. ZooKeeper clusters can target ZooKeeper versions up to 3.x.
func (b *InMemoryBackend) GetCompatibleKafkaVersions(_ context.Context, clusterArn string) ([]*MSKVersion, error) {
	b.mu.RLock("GetCompatibleKafkaVersions")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	current := c.KafkaVersion
	isKRaft := strings.HasSuffix(current, ".kraft")

	if isKRaft {
		return []*MSKVersion{
			{Version: "3.8.0.kraft", Status: ClusterStateActive},
			{Version: "3.7.x.kraft", Status: ClusterStateActive},
		}, nil
	}

	// ZooKeeper-based: return non-KRaft active versions higher than current.
	// For simplicity, return a curated list of ZooKeeper-compatible upgrades.
	return []*MSKVersion{
		{Version: kafkaVersion360, Status: ClusterStateActive},
		{Version: kafkaVersion351, Status: ClusterStateActive},
		{Version: "3.4.0", Status: ClusterStateActive},
		{Version: "3.3.2", Status: ClusterStateActive},
		{Version: "2.8.2.tiered", Status: ClusterStateActive},
	}, nil
}

// BrokerNode represents a stub broker node.
type BrokerNode struct {
	InstanceType string `json:"instanceType,omitempty"`
	BrokerID     int32  `json:"brokerId"`
}

// MSKVersion represents an available Kafka version.
type MSKVersion struct {
	Version string `json:"version"`
	Status  string `json:"status"`
}

// newClusterOperationLocked creates and stores a cluster operation.
// MUST be called with b.mu write lock held.
func (b *InMemoryBackend) newClusterOperationLocked(
	region, clusterArn, operationType string, source, target *MutableClusterInfo,
) *ClusterOperation {
	clusterOperationArn := b.clusterOperationARN(region, clusterArn)
	op := &ClusterOperation{
		ClusterOperationArn: clusterOperationArn,
		ClusterArn:          clusterArn,
		OperationType:       operationType,
		OperationState:      ClusterOperationStateUpdateComplete,
		SourceClusterInfo:   source,
		TargetClusterInfo:   target,
	}
	b.clusterOperations.Put(op)

	return cloneClusterOperation(op)
}

// --- Cluster policy operations ---

// DeleteClusterPolicy deletes the policy attached to an MSK cluster.
func (b *InMemoryBackend) DeleteClusterPolicy(_ context.Context, clusterArn string) error {
	b.mu.Lock("DeleteClusterPolicy")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return ErrNotFound
	}

	delete(b.clusterPolicies, clusterArn)

	return nil
}

// --- Cluster operation operations ---

// DescribeClusterOperation retrieves a cluster operation by ARN.
func (b *InMemoryBackend) DescribeClusterOperation(
	_ context.Context,
	clusterOperationArn string,
) (*ClusterOperation, error) {
	b.mu.RLock("DescribeClusterOperation")
	defer b.mu.RUnlock()

	op, ok := b.clusterOperations.Get(clusterOperationArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneClusterOperation(op), nil
}

// AddClusterOperationInternal adds a cluster operation for testing purposes.
func (b *InMemoryBackend) AddClusterOperationInternal(
	clusterArn, operationType string,
) *ClusterOperation {
	b.mu.Lock("AddClusterOperationInternal")
	defer b.mu.Unlock()

	region := regionFromARN(clusterArn, b.region)
	clusterOperationArn := b.clusterOperationARN(region, clusterArn)
	op := &ClusterOperation{
		ClusterOperationArn: clusterOperationArn,
		ClusterArn:          clusterArn,
		OperationType:       operationType,
		OperationState:      ClusterOperationStateUpdateComplete,
	}
	b.clusterOperations.Put(op)

	return cloneClusterOperation(op)
}

// AddClusterInternal creates a cluster directly for testing purposes.
func (b *InMemoryBackend) AddClusterInternal(name, kafkaVersion string) *Cluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	clusterArn := b.clusterARN(b.region, name)
	cluster := &Cluster{
		ClusterArn:          clusterArn,
		ClusterName:         name,
		ClusterType:         ClusterTypeProvisioned,
		KafkaVersion:        kafkaVersion,
		NumberOfBrokerNodes: defaultBrokerCount,
		State:               ClusterStateActive,
		CurrentVersion:      DefaultClusterVersion,
		Tags:                make(map[string]string),
	}
	b.clusters.Put(cluster)

	return cloneCluster(cluster)
}

func (b *InMemoryBackend) AddConfigurationInternal(name string) *Configuration {
	b.mu.Lock("AddConfigurationInternal")
	defer b.mu.Unlock()

	configArn := b.configurationARN(b.region, name)
	config := &Configuration{
		Arn:           configArn,
		Name:          name,
		KafkaVersions: []string{"2.8.0"},
		Tags:          make(map[string]string),
	}
	b.configurations.Put(config)

	return cloneConfiguration(config)
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
		Tags:            make(map[string]string),
	}
	b.replicators.Put(replicator)

	return cloneReplicator(replicator)
}

// AddTopicInternal creates a topic directly for testing purposes.
func (b *InMemoryBackend) AddTopicInternal(clusterArn, topicName string) *Topic {
	b.mu.Lock("AddTopicInternal")
	defer b.mu.Unlock()

	topic := &Topic{
		TopicName:         topicName,
		ClusterArn:        clusterArn,
		ReplicationFactor: defaultReplicationFactor,
		NumPartitions:     defaultPartitionCount,
		ConfigEntries:     make(map[string]string),
	}
	b.topics.Put(topic)

	return cloneTopic(topic)
}

// AddVpcConnectionInternal creates a VPC connection directly for testing purposes.
func (b *InMemoryBackend) AddVpcConnectionInternal(clusterArn, vpcID string) *VpcConnection {
	b.mu.Lock("AddVpcConnectionInternal")
	defer b.mu.Unlock()

	region := regionFromARN(clusterArn, b.region)
	vpcConnectionArn := b.vpcConnectionARN(region, clusterArn, vpcID)
	conn := &VpcConnection{
		VpcConnectionArn: vpcConnectionArn,
		TargetClusterArn: clusterArn,
		VpcID:            vpcID,
		State:            VpcConnectionStateAvailable,
		Tags:             make(map[string]string),
	}
	b.vpcConnections.Put(conn)

	return cloneVpcConnection(conn)
}

// ScramSecretError represents an error that occurred while associating or disassociating a SCRAM secret.
type ScramSecretError struct {
	SecretArn    string `json:"secretArn"`
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// Replicator represents an MSK replicator.
type Replicator struct {
	Tags                    map[string]string `json:"-"`
	ReplicatorArn           string            `json:"replicatorArn"`
	ReplicatorName          string            `json:"replicatorName"`
	Description             string            `json:"description,omitempty"`
	ServiceExecutionRoleArn string            `json:"serviceExecutionRoleArn"`
	ReplicatorState         string            `json:"replicatorState"`
}

// Topic represents an MSK topic on a cluster.
type Topic struct {
	ConfigEntries     map[string]string `json:"configEntries,omitempty"`
	TopicName         string            `json:"topicName"`
	ClusterArn        string            `json:"clusterArn"`
	ReplicationFactor int32             `json:"replicationFactor"`
	NumPartitions     int32             `json:"numPartitions"`
}

// VpcConnection represents an MSK VPC connection.
type VpcConnection struct {
	Tags             map[string]string `json:"-"`
	VpcConnectionArn string            `json:"vpcConnectionArn"`
	TargetClusterArn string            `json:"targetClusterArn"`
	VpcID            string            `json:"vpcId"`
	Authentication   string            `json:"authentication,omitempty"`
	State            string            `json:"state"`
}

// ClusterOperation represents an MSK cluster operation.
type ClusterOperation struct {
	SourceClusterInfo   *MutableClusterInfo `json:"sourceClusterInfo,omitempty"`
	TargetClusterInfo   *MutableClusterInfo `json:"targetClusterInfo,omitempty"`
	ClusterOperationArn string              `json:"clusterOperationArn"`
	ClusterArn          string              `json:"clusterArn"`
	OperationType       string              `json:"operationType"`
	OperationState      string              `json:"operationState"`
}

// MutableClusterInfo captures the subset of cluster configuration that an update
// operation changes. DescribeClusterOperation returns it as SourceClusterInfo
// (the state before the operation) and TargetClusterInfo (the requested state).
type MutableClusterInfo struct {
	ConnectivityInfo     *ConnectivityInfo     `json:"connectivityInfo,omitempty"`
	OpenMonitoring       *OpenMonitoring       `json:"openMonitoring,omitempty"`
	LoggingInfo          *LoggingInfo          `json:"loggingInfo,omitempty"`
	ClientAuthentication *ClientAuthentication `json:"clientAuthentication,omitempty"`
	EncryptionInfo       *EncryptionInfo       `json:"encryptionInfo,omitempty"`
	StorageMode          string                `json:"storageMode,omitempty"`
	EnhancedMonitoring   string                `json:"enhancedMonitoring,omitempty"`
	BrokerEBSVolumeInfo  []BrokerEBSVolumeInfo `json:"brokerEBSVolumeInfo,omitempty"`
	NumberOfBrokerNodes  int32                 `json:"numberOfBrokerNodes,omitempty"`
}

// BrokerEBSVolumeInfo describes a per-broker EBS volume target for UpdateStorage.
type BrokerEBSVolumeInfo struct {
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
	KafkaBrokerNodeID     string                 `json:"kafkaBrokerNodeId,omitempty"`
	VolumeSizeGB          int32                  `json:"volumeSizeGB,omitempty"`
}

// cloneCluster creates a deep copy of a cluster.
func cloneCluster(c *Cluster) *Cluster {
	clone := &Cluster{
		ClusterArn:          c.ClusterArn,
		ClusterName:         c.ClusterName,
		ClusterType:         c.ClusterType,
		KafkaVersion:        c.KafkaVersion,
		State:               c.State,
		CurrentVersion:      c.CurrentVersion,
		ActiveOperationArn:  c.ActiveOperationArn,
		EnhancedMonitoring:  c.EnhancedMonitoring,
		StorageMode:         c.StorageMode,
		CreationTime:        c.CreationTime,
		NumberOfBrokerNodes: c.NumberOfBrokerNodes,
		Tags:                nonNilTagsCopy(c.Tags),
		BrokerNodeGroupInfo: BrokerNodeGroupInfo{
			BrokerAZDistribution: c.BrokerNodeGroupInfo.BrokerAZDistribution,
			InstanceType:         c.BrokerNodeGroupInfo.InstanceType,
			ClientSubnets:        append([]string(nil), c.BrokerNodeGroupInfo.ClientSubnets...),
			SecurityGroups:       append([]string(nil), c.BrokerNodeGroupInfo.SecurityGroups...),
			ZoneIDs:              append([]string(nil), c.BrokerNodeGroupInfo.ZoneIDs...),
		},
	}

	if c.BrokerNodeGroupInfo.StorageInfo != nil {
		clone.BrokerNodeGroupInfo.StorageInfo = cloneStorageInfo(c.BrokerNodeGroupInfo.StorageInfo)
	}

	if c.BrokerNodeGroupInfo.ConnectivityInfo != nil {
		clone.BrokerNodeGroupInfo.ConnectivityInfo = cloneConnectivityInfo(c.BrokerNodeGroupInfo.ConnectivityInfo)
	}

	clone.ClientAuthentication = cloneClientAuth(c.ClientAuthentication)
	clone.EncryptionInfo = cloneEncryptionInfo(c.EncryptionInfo)
	clone.OpenMonitoring = cloneOpenMonitoring(c.OpenMonitoring)
	clone.LoggingInfo = cloneLoggingInfo(c.LoggingInfo)
	clone.Serverless = cloneServerless(c.Serverless)

	if c.StateInfo != nil {
		si := *c.StateInfo
		clone.StateInfo = &si
	}

	if c.ConfigurationInfo != nil {
		ci := *c.ConfigurationInfo
		clone.ConfigurationInfo = &ci
	}

	return clone
}

// cloneStorageInfo deep-copies a StorageInfo.
func cloneStorageInfo(s *StorageInfo) *StorageInfo {
	if s == nil {
		return nil
	}

	clone := &StorageInfo{}
	if s.EbsStorageInfo != nil {
		ebs := &EBSStorageInfo{VolumeSize: s.EbsStorageInfo.VolumeSize}
		if s.EbsStorageInfo.ProvisionedThroughput != nil {
			pt := *s.EbsStorageInfo.ProvisionedThroughput
			ebs.ProvisionedThroughput = &pt
		}
		clone.EbsStorageInfo = ebs
	}

	return clone
}

// cloneConnectivityInfo deep-copies a ConnectivityInfo.
func cloneConnectivityInfo(ci *ConnectivityInfo) *ConnectivityInfo {
	if ci == nil {
		return nil
	}

	clone := &ConnectivityInfo{}
	if ci.PublicAccess != nil {
		pa := *ci.PublicAccess
		clone.PublicAccess = &pa
	}

	if ci.VpcConnectivity != nil {
		clone.VpcConnectivity = cloneVpcConnectivity(ci.VpcConnectivity)
	}

	return clone
}

// cloneVpcConnectivity deep-copies a VpcConnectivity.
func cloneVpcConnectivity(src *VpcConnectivity) *VpcConnectivity {
	vc := &VpcConnectivity{}

	if src.ClientAuthentication == nil {
		return vc
	}

	ca := &VpcConnectivityClientAuthentication{}
	srcCA := src.ClientAuthentication

	if srcCA.Sasl != nil {
		sasl := &VpcConnectivitySasl{}

		if srcCA.Sasl.Iam != nil {
			iam := *srcCA.Sasl.Iam
			sasl.Iam = &iam
		}

		if srcCA.Sasl.Scram != nil {
			scram := *srcCA.Sasl.Scram
			sasl.Scram = &scram
		}

		ca.Sasl = sasl
	}

	if srcCA.TLS != nil {
		tls := *srcCA.TLS
		ca.TLS = &tls
	}

	vc.ClientAuthentication = ca

	return vc
}

// cloneEncryptionInfo deep-copies an EncryptionInfo.
func cloneEncryptionInfo(ei *EncryptionInfo) *EncryptionInfo {
	if ei == nil {
		return nil
	}

	clone := &EncryptionInfo{}
	if ei.EncryptionAtRest != nil {
		ear := *ei.EncryptionAtRest
		clone.EncryptionAtRest = &ear
	}

	if ei.EncryptionInTransit != nil {
		eit := *ei.EncryptionInTransit
		clone.EncryptionInTransit = &eit
	}

	return clone
}

// cloneOpenMonitoring deep-copies an OpenMonitoring.
func cloneOpenMonitoring(om *OpenMonitoring) *OpenMonitoring {
	if om == nil {
		return nil
	}

	clone := &OpenMonitoring{}
	if om.Prometheus != nil {
		p := &PrometheusInfo{}
		if om.Prometheus.JmxExporter != nil {
			jmx := *om.Prometheus.JmxExporter
			p.JmxExporter = &jmx
		}
		if om.Prometheus.NodeExporter != nil {
			ne := *om.Prometheus.NodeExporter
			p.NodeExporter = &ne
		}
		clone.Prometheus = p
	}

	return clone
}

// cloneLoggingInfo deep-copies a LoggingInfo.
func cloneLoggingInfo(li *LoggingInfo) *LoggingInfo {
	if li == nil {
		return nil
	}

	clone := &LoggingInfo{}
	if li.BrokerLogs != nil {
		bl := &BrokerLogs{}
		if li.BrokerLogs.CloudWatchLogs != nil {
			cwl := *li.BrokerLogs.CloudWatchLogs
			bl.CloudWatchLogs = &cwl
		}
		if li.BrokerLogs.Firehose != nil {
			fh := *li.BrokerLogs.Firehose
			bl.Firehose = &fh
		}
		if li.BrokerLogs.S3 != nil {
			s3 := *li.BrokerLogs.S3
			bl.S3 = &s3
		}
		clone.BrokerLogs = bl
	}

	return clone
}

// cloneServerless deep-copies a ServerlessClusterInfo.
func cloneServerless(s *ServerlessClusterInfo) *ServerlessClusterInfo {
	if s == nil {
		return nil
	}

	clone := &ServerlessClusterInfo{}
	if s.ClientAuthentication != nil {
		ca := &ServerlessClientAuthentication{}
		if s.ClientAuthentication.Sasl != nil {
			ca.Sasl = cloneSasl(s.ClientAuthentication.Sasl)
		}
		clone.ClientAuthentication = ca
	}

	if len(s.VpcConfigs) > 0 {
		clone.VpcConfigs = make([]ServerlessVpcConfig, len(s.VpcConfigs))
		for i, vc := range s.VpcConfigs {
			clone.VpcConfigs[i] = ServerlessVpcConfig{
				SubnetIDs:        append([]string(nil), vc.SubnetIDs...),
				SecurityGroupIDs: append([]string(nil), vc.SecurityGroupIDs...),
			}
		}
	}

	return clone
}

// cloneClientAuth deep-copies a ClientAuthentication value.
func cloneClientAuth(auth *ClientAuthentication) *ClientAuthentication {
	if auth == nil {
		return nil
	}

	authCopy := &ClientAuthentication{}

	if auth.Sasl != nil {
		authCopy.Sasl = cloneSasl(auth.Sasl)
	}

	if auth.TLS != nil {
		tlsCopy := TLSSettings{
			Enabled:                     auth.TLS.Enabled,
			CertificateAuthorityArnList: append([]string(nil), auth.TLS.CertificateAuthorityArnList...),
		}
		authCopy.TLS = &tlsCopy
	}

	if auth.Unauthenticated != nil {
		ua := *auth.Unauthenticated
		authCopy.Unauthenticated = &ua
	}

	return authCopy
}

// cloneSasl deep-copies a SaslSettings value.
func cloneSasl(s *SaslSettings) *SaslSettings {
	if s == nil {
		return nil
	}

	c := *s

	if s.Scram != nil {
		scramCopy := *s.Scram
		c.Scram = &scramCopy
	}

	if s.Iam != nil {
		iamCopy := *s.Iam
		c.Iam = &iamCopy
	}

	return &c
}

// cloneConfiguration creates a deep copy of a Configuration.
func cloneConfiguration(c *Configuration) *Configuration {
	kvs := make([]string, len(c.KafkaVersions))
	copy(kvs, c.KafkaVersions)

	return &Configuration{
		Arn:              c.Arn,
		Name:             c.Name,
		Description:      c.Description,
		ServerProperties: c.ServerProperties,
		KafkaVersions:    kvs,
		Tags:             nonNilTagsCopy(c.Tags),
	}
}

// cloneReplicator creates a deep copy of a Replicator.
func cloneReplicator(r *Replicator) *Replicator {
	return &Replicator{
		ReplicatorArn:           r.ReplicatorArn,
		ReplicatorName:          r.ReplicatorName,
		Description:             r.Description,
		ServiceExecutionRoleArn: r.ServiceExecutionRoleArn,
		ReplicatorState:         r.ReplicatorState,
		Tags:                    nonNilTagsCopy(r.Tags),
	}
}

// cloneTopic creates a deep copy of a Topic.
func cloneTopic(t *Topic) *Topic {
	return &Topic{
		TopicName:         t.TopicName,
		ClusterArn:        t.ClusterArn,
		ReplicationFactor: t.ReplicationFactor,
		NumPartitions:     t.NumPartitions,
		ConfigEntries:     nonNilMapCopy(t.ConfigEntries),
	}
}

// cloneVpcConnection creates a deep copy of a VpcConnection.
func cloneVpcConnection(v *VpcConnection) *VpcConnection {
	return &VpcConnection{
		VpcConnectionArn: v.VpcConnectionArn,
		TargetClusterArn: v.TargetClusterArn,
		VpcID:            v.VpcID,
		Authentication:   v.Authentication,
		State:            v.State,
		Tags:             nonNilTagsCopy(v.Tags),
	}
}

// cloneClusterOperation creates a deep copy of a ClusterOperation.
func cloneClusterOperation(op *ClusterOperation) *ClusterOperation {
	return &ClusterOperation{
		ClusterOperationArn: op.ClusterOperationArn,
		ClusterArn:          op.ClusterArn,
		OperationType:       op.OperationType,
		OperationState:      op.OperationState,
		SourceClusterInfo:   cloneMutableClusterInfo(op.SourceClusterInfo),
		TargetClusterInfo:   cloneMutableClusterInfo(op.TargetClusterInfo),
	}
}

// cloneMutableClusterInfo deep-copies a MutableClusterInfo, reusing the existing
// per-field clone helpers so the returned operation does not alias backend state.
func cloneMutableClusterInfo(m *MutableClusterInfo) *MutableClusterInfo {
	if m == nil {
		return nil
	}

	clone := &MutableClusterInfo{
		ConnectivityInfo:     cloneConnectivityInfo(m.ConnectivityInfo),
		OpenMonitoring:       cloneOpenMonitoring(m.OpenMonitoring),
		LoggingInfo:          cloneLoggingInfo(m.LoggingInfo),
		ClientAuthentication: cloneClientAuth(m.ClientAuthentication),
		EncryptionInfo:       cloneEncryptionInfo(m.EncryptionInfo),
		StorageMode:          m.StorageMode,
		EnhancedMonitoring:   m.EnhancedMonitoring,
		NumberOfBrokerNodes:  m.NumberOfBrokerNodes,
	}

	if len(m.BrokerEBSVolumeInfo) > 0 {
		clone.BrokerEBSVolumeInfo = make([]BrokerEBSVolumeInfo, len(m.BrokerEBSVolumeInfo))
		for i, v := range m.BrokerEBSVolumeInfo {
			cv := BrokerEBSVolumeInfo{
				KafkaBrokerNodeID: v.KafkaBrokerNodeID,
				VolumeSizeGB:      v.VolumeSizeGB,
			}
			if v.ProvisionedThroughput != nil {
				pt := *v.ProvisionedThroughput
				cv.ProvisionedThroughput = &pt
			}
			clone.BrokerEBSVolumeInfo[i] = cv
		}
	}

	return clone
}

// nonNilTagsCopy returns a new non-nil copy of tags; an empty map if tags is nil.
func nonNilTagsCopy(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	return maps.Clone(tags)
}

// nonNilMapCopy returns a new non-nil copy of a string map; an empty map if nil.
func nonNilMapCopy(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}

	return maps.Clone(m)
}
