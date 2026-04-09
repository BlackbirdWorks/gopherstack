// Package kafka provides an in-memory stub of AWS MSK (Managed Streaming for Apache Kafka).
package kafka

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

const (
	// ReplicatorStateRunning indicates a running replicator.
	ReplicatorStateRunning = "RUNNING"
	// VpcConnectionStateAvailable indicates a VPC connection that is available.
	VpcConnectionStateAvailable = "AVAILABLE"
	// ClusterOperationStateUpdateComplete indicates a completed cluster operation.
	ClusterOperationStateUpdateComplete = "UPDATE_COMPLETE"
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
)

const (
	// defaultBrokerCount is used in seed helpers for testing.
	defaultBrokerCount = 3
	// defaultReplicationFactor is used in AddTopicInternal.
	defaultReplicationFactor = 3
	// defaultPartitionCount is used in AddTopicInternal.
	defaultPartitionCount = 1
)

// BrokerNodeGroupInfo holds broker node configuration.
type BrokerNodeGroupInfo struct {
	StorageInfo          *StorageInfo `json:"storageInfo,omitempty"`
	BrokerAZDistribution string       `json:"brokerAZDistribution,omitempty"`
	InstanceType         string       `json:"instanceType"`
	ClientSubnets        []string     `json:"clientSubnets"`
	SecurityGroups       []string     `json:"securityGroups,omitempty"`
}

// StorageInfo holds broker storage config.
type StorageInfo struct {
	EbsStorageInfo *EBSStorageInfo `json:"ebsStorageInfo,omitempty"`
}

// EBSStorageInfo holds EBS volume config.
type EBSStorageInfo struct {
	VolumeSize int32 `json:"volumeSize,omitempty"`
}

// ConfigurationInfo holds a cluster configuration reference.
type ConfigurationInfo struct {
	Arn      string `json:"arn"`
	Revision int64  `json:"revision"`
}

// Cluster represents an MSK cluster.
type Cluster struct {
	Tags                map[string]string   `json:"-"`
	ClusterArn          string              `json:"clusterArn"`
	ClusterName         string              `json:"clusterName"`
	KafkaVersion        string              `json:"kafkaVersion"`
	State               string              `json:"state"`
	CurrentVersion      string              `json:"currentVersion"`
	BrokerNodeGroupInfo BrokerNodeGroupInfo `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes int32               `json:"numberOfBrokerNodes"`
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
type InMemoryBackend struct {
	clusters          map[string]*Cluster          // key: clusterArn
	configurations    map[string]*Configuration    // key: configArn
	scramSecrets      map[string][]string          // key: clusterArn → []secretArn
	replicators       map[string]*Replicator       // key: replicatorArn
	topics            map[string]*Topic            // key: clusterArn + "|" + topicName
	vpcConnections    map[string]*VpcConnection    // key: vpcConnectionArn
	clusterPolicies   map[string]string            // key: clusterArn → policy document
	clusterOperations map[string]*ClusterOperation // key: clusterOperationArn
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
}

// NewInMemoryBackend creates a new in-memory MSK backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:          make(map[string]*Cluster),
		configurations:    make(map[string]*Configuration),
		scramSecrets:      make(map[string][]string),
		replicators:       make(map[string]*Replicator),
		topics:            make(map[string]*Topic),
		vpcConnections:    make(map[string]*VpcConnection),
		clusterPolicies:   make(map[string]string),
		clusterOperations: make(map[string]*ClusterOperation),
		mu:                lockmetrics.New("kafka"),
		accountID:         accountID,
		region:            region,
	}
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all state, returning the backend to a clean empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*Cluster)
	b.configurations = make(map[string]*Configuration)
	b.scramSecrets = make(map[string][]string)
	b.replicators = make(map[string]*Replicator)
	b.topics = make(map[string]*Topic)
	b.vpcConnections = make(map[string]*VpcConnection)
	b.clusterPolicies = make(map[string]string)
	b.clusterOperations = make(map[string]*ClusterOperation)
}

// clusterARN builds an ARN for an MSK cluster.
func (b *InMemoryBackend) clusterARN(name string) string {
	return arn.Build("kafka", b.region, b.accountID, fmt.Sprintf("cluster/%s/%s", name, uuid.New().String()))
}

// configurationARN builds an ARN for an MSK configuration.
func (b *InMemoryBackend) configurationARN(name string) string {
	return arn.Build("kafka", b.region, b.accountID, fmt.Sprintf("configuration/%s/%s", name, uuid.New().String()))
}

// replicatorARN builds an ARN for an MSK replicator.
func (b *InMemoryBackend) replicatorARN(name string) string {
	return arn.Build("kafka", b.region, b.accountID, fmt.Sprintf("replicator/%s/%s", name, uuid.New().String()))
}

// vpcConnectionARN builds an ARN for an MSK VPC connection.
func (b *InMemoryBackend) vpcConnectionARN(clusterArn, vpcID string) string {
	return arn.Build(
		"kafka",
		b.region,
		b.accountID,
		fmt.Sprintf("vpc-connection/%s/%s/%s", clusterArn, vpcID, uuid.New().String()),
	)
}

// clusterOperationARN builds an ARN for an MSK cluster operation.
func (b *InMemoryBackend) clusterOperationARN(clusterArn string) string {
	return arn.Build(
		"kafka",
		b.region,
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
	name, kafkaVersion string,
	numBrokers int32,
	brokerInfo BrokerNodeGroupInfo,
	tags map[string]string,
) (*Cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("clusterName is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	for _, c := range b.clusters {
		if c.ClusterName == name {
			return nil, ErrAlreadyExists
		}
	}

	clusterArn := b.clusterARN(name)
	cluster := &Cluster{
		ClusterArn:          clusterArn,
		ClusterName:         name,
		KafkaVersion:        kafkaVersion,
		NumberOfBrokerNodes: numBrokers,
		BrokerNodeGroupInfo: brokerInfo,
		State:               ClusterStateActive,
		CurrentVersion:      "K3AEGXETSR30VB",
		Tags:                nonNilTagsCopy(tags),
	}
	b.clusters[clusterArn] = cluster

	return cloneCluster(cluster), nil
}

// DescribeCluster retrieves a cluster by ARN.
func (b *InMemoryBackend) DescribeCluster(clusterArn string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	c, ok := b.clusters[clusterArn]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneCluster(c), nil
}

// ListClusters returns all MSK clusters sorted by name.
func (b *InMemoryBackend) ListClusters() []*Cluster {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	out := make([]*Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
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
func (b *InMemoryBackend) DeleteCluster(clusterArn string) error {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterArn]; !ok {
		return ErrNotFound
	}

	delete(b.clusters, clusterArn)
	delete(b.scramSecrets, clusterArn)
	delete(b.clusterPolicies, clusterArn)

	// Remove all topics belonging to this cluster.
	prefix := clusterArn + topicKeySeparator
	for k := range b.topics {
		if strings.HasPrefix(k, prefix) {
			delete(b.topics, k)
		}
	}

	return nil
}

// --- Configuration operations ---

// CreateConfiguration creates a new MSK configuration.
func (b *InMemoryBackend) CreateConfiguration(
	name, description string,
	kafkaVersions []string,
	serverProperties string,
) (*Configuration, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	for _, c := range b.configurations {
		if c.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	configArn := b.configurationARN(name)
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
	b.configurations[configArn] = config

	return cloneConfiguration(config), nil
}

// DescribeConfiguration retrieves a configuration by ARN.
func (b *InMemoryBackend) DescribeConfiguration(configArn string) (*Configuration, error) {
	b.mu.RLock("DescribeConfiguration")
	defer b.mu.RUnlock()

	c, ok := b.configurations[configArn]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneConfiguration(c), nil
}

// ListConfigurations returns all MSK configurations sorted by name.
func (b *InMemoryBackend) ListConfigurations() []*Configuration {
	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	out := make([]*Configuration, 0, len(b.configurations))
	for _, c := range b.configurations {
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
func (b *InMemoryBackend) DeleteConfiguration(configArn string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.configurations[configArn]; !ok {
		return ErrNotFound
	}

	delete(b.configurations, configArn)

	return nil
}

// --- Tag operations ---

// TagResource adds tags to a cluster, configuration, replicator, or VPC connection by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if c, ok := b.clusters[resourceArn]; ok {
		maps.Copy(c.Tags, tags)

		return nil
	}

	if c, ok := b.configurations[resourceArn]; ok {
		maps.Copy(c.Tags, tags)

		return nil
	}

	if r, ok := b.replicators[resourceArn]; ok {
		maps.Copy(r.Tags, tags)

		return nil
	}

	if v, ok := b.vpcConnections[resourceArn]; ok {
		maps.Copy(v.Tags, tags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a cluster, configuration, replicator, or VPC connection by ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if c, ok := b.clusters[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if c, ok := b.configurations[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if r, ok := b.replicators[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	if v, ok := b.vpcConnections[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(v.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// GetTags retrieves tags for a cluster, configuration, replicator, or VPC connection by ARN.
func (b *InMemoryBackend) GetTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if c, ok := b.clusters[resourceArn]; ok {
		return maps.Clone(c.Tags), nil
	}

	if c, ok := b.configurations[resourceArn]; ok {
		return maps.Clone(c.Tags), nil
	}

	if r, ok := b.replicators[resourceArn]; ok {
		return maps.Clone(r.Tags), nil
	}

	if v, ok := b.vpcConnections[resourceArn]; ok {
		return maps.Clone(v.Tags), nil
	}

	return nil, ErrNotFound
}

// --- SCRAM secret operations ---

// BatchAssociateScramSecret associates a list of SCRAM secrets with a cluster.
// It returns any errors that occurred for individual secrets.
func (b *InMemoryBackend) BatchAssociateScramSecret(
	clusterArn string,
	secretArnList []string,
) ([]ScramSecretError, error) {
	b.mu.Lock("BatchAssociateScramSecret")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterArn]; !ok {
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
	clusterArn string,
	secretArnList []string,
) ([]ScramSecretError, error) {
	b.mu.Lock("BatchDisassociateScramSecret")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterArn]; !ok {
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
	name, description, serviceExecutionRoleArn string,
	tags map[string]string,
) (*Replicator, error) {
	if name == "" {
		return nil, fmt.Errorf("replicatorName is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateReplicator")
	defer b.mu.Unlock()

	for _, r := range b.replicators {
		if r.ReplicatorName == name {
			return nil, ErrAlreadyExists
		}
	}

	replicatorArn := b.replicatorARN(name)
	replicator := &Replicator{
		ReplicatorArn:           replicatorArn,
		ReplicatorName:          name,
		Description:             description,
		ServiceExecutionRoleArn: serviceExecutionRoleArn,
		ReplicatorState:         ReplicatorStateRunning,
		Tags:                    nonNilTagsCopy(tags),
	}
	b.replicators[replicatorArn] = replicator

	return cloneReplicator(replicator), nil
}

// DeleteReplicator deletes a replicator by ARN.
func (b *InMemoryBackend) DeleteReplicator(replicatorArn string) error {
	b.mu.Lock("DeleteReplicator")
	defer b.mu.Unlock()

	if _, ok := b.replicators[replicatorArn]; !ok {
		return ErrNotFound
	}

	delete(b.replicators, replicatorArn)

	return nil
}

// --- Topic operations ---

// CreateTopic creates a topic on an MSK cluster.
func (b *InMemoryBackend) CreateTopic(
	clusterArn, topicName string,
	replicationFactor, numPartitions int32,
	configEntries map[string]string,
) (*Topic, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topicName is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateTopic")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterArn]; !ok {
		return nil, ErrNotFound
	}

	key := topicKey(clusterArn, topicName)
	if _, ok := b.topics[key]; ok {
		return nil, ErrAlreadyExists
	}

	topic := &Topic{
		TopicName:         topicName,
		ClusterArn:        clusterArn,
		ReplicationFactor: replicationFactor,
		NumPartitions:     numPartitions,
		ConfigEntries:     nonNilMapCopy(configEntries),
	}
	b.topics[key] = topic

	return cloneTopic(topic), nil
}

// DeleteTopic deletes a topic from an MSK cluster.
func (b *InMemoryBackend) DeleteTopic(clusterArn, topicName string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterArn]; !ok {
		return ErrNotFound
	}

	key := topicKey(clusterArn, topicName)
	if _, ok := b.topics[key]; !ok {
		return ErrNotFound
	}

	delete(b.topics, key)

	return nil
}

// --- VPC connection operations ---

// CreateVpcConnection creates a new VPC connection to an MSK cluster.
func (b *InMemoryBackend) CreateVpcConnection(
	targetClusterArn, vpcID, authentication string,
	tags map[string]string,
) (*VpcConnection, error) {
	b.mu.Lock("CreateVpcConnection")
	defer b.mu.Unlock()

	if _, ok := b.clusters[targetClusterArn]; !ok {
		return nil, ErrNotFound
	}

	vpcConnectionArn := b.vpcConnectionARN(targetClusterArn, vpcID)
	conn := &VpcConnection{
		VpcConnectionArn: vpcConnectionArn,
		TargetClusterArn: targetClusterArn,
		VpcID:            vpcID,
		Authentication:   authentication,
		State:            VpcConnectionStateAvailable,
		Tags:             nonNilTagsCopy(tags),
	}
	b.vpcConnections[vpcConnectionArn] = conn

	return cloneVpcConnection(conn), nil
}

// DeleteVpcConnection deletes a VPC connection by ARN.
func (b *InMemoryBackend) DeleteVpcConnection(vpcConnectionArn string) error {
	b.mu.Lock("DeleteVpcConnection")
	defer b.mu.Unlock()

	if _, ok := b.vpcConnections[vpcConnectionArn]; !ok {
		return ErrNotFound
	}

	delete(b.vpcConnections, vpcConnectionArn)

	return nil
}

// --- Cluster policy operations ---

// DeleteClusterPolicy deletes the policy attached to an MSK cluster.
func (b *InMemoryBackend) DeleteClusterPolicy(clusterArn string) error {
	b.mu.Lock("DeleteClusterPolicy")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterArn]; !ok {
		return ErrNotFound
	}

	delete(b.clusterPolicies, clusterArn)

	return nil
}

// --- Cluster operation operations ---

// DescribeClusterOperation retrieves a cluster operation by ARN.
func (b *InMemoryBackend) DescribeClusterOperation(clusterOperationArn string) (*ClusterOperation, error) {
	b.mu.RLock("DescribeClusterOperation")
	defer b.mu.RUnlock()

	op, ok := b.clusterOperations[clusterOperationArn]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneClusterOperation(op), nil
}

// AddClusterOperationInternal adds a cluster operation for testing purposes.
func (b *InMemoryBackend) AddClusterOperationInternal(clusterArn, operationType string) *ClusterOperation {
	b.mu.Lock("AddClusterOperationInternal")
	defer b.mu.Unlock()

	clusterOperationArn := b.clusterOperationARN(clusterArn)
	op := &ClusterOperation{
		ClusterOperationArn: clusterOperationArn,
		ClusterArn:          clusterArn,
		OperationType:       operationType,
		OperationState:      ClusterOperationStateUpdateComplete,
	}
	b.clusterOperations[clusterOperationArn] = op

	return cloneClusterOperation(op)
}

// AddClusterInternal creates a cluster directly for testing purposes.
func (b *InMemoryBackend) AddClusterInternal(name, kafkaVersion string) *Cluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	clusterArn := b.clusterARN(name)
	cluster := &Cluster{
		ClusterArn:          clusterArn,
		ClusterName:         name,
		KafkaVersion:        kafkaVersion,
		NumberOfBrokerNodes: defaultBrokerCount,
		State:               ClusterStateActive,
		CurrentVersion:      "K3AEGXETSR30VB",
		Tags:                make(map[string]string),
	}
	b.clusters[clusterArn] = cluster

	return cloneCluster(cluster)
}
func (b *InMemoryBackend) AddConfigurationInternal(name string) *Configuration {
	b.mu.Lock("AddConfigurationInternal")
	defer b.mu.Unlock()

	configArn := b.configurationARN(name)
	config := &Configuration{
		Arn:           configArn,
		Name:          name,
		KafkaVersions: []string{"2.8.0"},
		Tags:          make(map[string]string),
	}
	b.configurations[configArn] = config

	return cloneConfiguration(config)
}

// AddReplicatorInternal creates a replicator directly for testing purposes.
func (b *InMemoryBackend) AddReplicatorInternal(name string) *Replicator {
	b.mu.Lock("AddReplicatorInternal")
	defer b.mu.Unlock()

	replicatorArn := b.replicatorARN(name)
	replicator := &Replicator{
		ReplicatorArn:   replicatorArn,
		ReplicatorName:  name,
		ReplicatorState: ReplicatorStateRunning,
		Tags:            make(map[string]string),
	}
	b.replicators[replicatorArn] = replicator

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
	b.topics[topicKey(clusterArn, topicName)] = topic

	return cloneTopic(topic)
}

// AddVpcConnectionInternal creates a VPC connection directly for testing purposes.
func (b *InMemoryBackend) AddVpcConnectionInternal(clusterArn, vpcID string) *VpcConnection {
	b.mu.Lock("AddVpcConnectionInternal")
	defer b.mu.Unlock()

	vpcConnectionArn := b.vpcConnectionARN(clusterArn, vpcID)
	conn := &VpcConnection{
		VpcConnectionArn: vpcConnectionArn,
		TargetClusterArn: clusterArn,
		VpcID:            vpcID,
		State:            VpcConnectionStateAvailable,
		Tags:             make(map[string]string),
	}
	b.vpcConnections[vpcConnectionArn] = conn

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
	ClusterOperationArn string `json:"clusterOperationArn"`
	ClusterArn          string `json:"clusterArn"`
	OperationType       string `json:"operationType"`
	OperationState      string `json:"operationState"`
}

// cloneCluster creates a deep copy of a cluster.
func cloneCluster(c *Cluster) *Cluster {
	clone := &Cluster{
		ClusterArn:          c.ClusterArn,
		ClusterName:         c.ClusterName,
		KafkaVersion:        c.KafkaVersion,
		State:               c.State,
		CurrentVersion:      c.CurrentVersion,
		NumberOfBrokerNodes: c.NumberOfBrokerNodes,
		Tags:                nonNilTagsCopy(c.Tags),
		BrokerNodeGroupInfo: BrokerNodeGroupInfo{
			BrokerAZDistribution: c.BrokerNodeGroupInfo.BrokerAZDistribution,
			InstanceType:         c.BrokerNodeGroupInfo.InstanceType,
			ClientSubnets:        append([]string(nil), c.BrokerNodeGroupInfo.ClientSubnets...),
			SecurityGroups:       append([]string(nil), c.BrokerNodeGroupInfo.SecurityGroups...),
		},
	}

	if c.BrokerNodeGroupInfo.StorageInfo != nil {
		clone.BrokerNodeGroupInfo.StorageInfo = &StorageInfo{}
		if c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo != nil {
			clone.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo = &EBSStorageInfo{
				VolumeSize: c.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize,
			}
		}
	}

	return clone
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
	}
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
