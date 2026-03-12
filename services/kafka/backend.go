// Package kafka provides an in-memory stub of AWS MSK (Managed Streaming for Apache Kafka).
package kafka

import (
	"fmt"
	"maps"

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
	clusters       map[string]*Cluster       // key: clusterArn
	configurations map[string]*Configuration // key: configArn
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new in-memory MSK backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:       make(map[string]*Cluster),
		configurations: make(map[string]*Configuration),
		mu:             lockmetrics.New("kafka"),
		accountID:      accountID,
		region:         region,
	}
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// clusterARN builds an ARN for an MSK cluster.
func (b *InMemoryBackend) clusterARN(name string) string {
	return arn.Build("kafka", b.region, b.accountID, fmt.Sprintf("cluster/%s/%s", name, uuid.New().String()))
}

// configurationARN builds an ARN for an MSK configuration.
func (b *InMemoryBackend) configurationARN(name string) string {
	return arn.Build("kafka", b.region, b.accountID, fmt.Sprintf("configuration/%s/%s", name, uuid.New().String()))
}

// --- Cluster operations ---

// CreateCluster creates a new MSK cluster.
func (b *InMemoryBackend) CreateCluster(
	name, kafkaVersion string,
	numBrokers int32,
	brokerInfo BrokerNodeGroupInfo,
	tags map[string]string,
) (*Cluster, error) {
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
		Tags:                maps.Clone(tags),
	}
	b.clusters[clusterArn] = cluster

	return cluster, nil
}

// DescribeCluster retrieves a cluster by ARN.
func (b *InMemoryBackend) DescribeCluster(clusterArn string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	c, ok := b.clusters[clusterArn]
	if !ok {
		return nil, ErrNotFound
	}

	return c, nil
}

// ListClusters returns all MSK clusters.
func (b *InMemoryBackend) ListClusters() []*Cluster {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	out := make([]*Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		out = append(out, c)
	}

	return out
}

// DeleteCluster deletes a cluster by ARN.
func (b *InMemoryBackend) DeleteCluster(clusterArn string) error {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterArn]; !ok {
		return ErrNotFound
	}

	delete(b.clusters, clusterArn)

	return nil
}

// --- Configuration operations ---

// CreateConfiguration creates a new MSK configuration.
func (b *InMemoryBackend) CreateConfiguration(
	name, description string,
	kafkaVersions []string,
	serverProperties string,
) (*Configuration, error) {
	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	for _, c := range b.configurations {
		if c.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	configArn := b.configurationARN(name)
	config := &Configuration{
		Arn:              configArn,
		Name:             name,
		Description:      description,
		KafkaVersions:    kafkaVersions,
		ServerProperties: serverProperties,
	}
	b.configurations[configArn] = config

	return config, nil
}

// DescribeConfiguration retrieves a configuration by ARN.
func (b *InMemoryBackend) DescribeConfiguration(configArn string) (*Configuration, error) {
	b.mu.RLock("DescribeConfiguration")
	defer b.mu.RUnlock()

	c, ok := b.configurations[configArn]
	if !ok {
		return nil, ErrNotFound
	}

	return c, nil
}

// ListConfigurations returns all MSK configurations.
func (b *InMemoryBackend) ListConfigurations() []*Configuration {
	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	out := make([]*Configuration, 0, len(b.configurations))
	for _, c := range b.configurations {
		out = append(out, c)
	}

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

// TagResource adds tags to a cluster or configuration by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if c, ok := b.clusters[resourceArn]; ok {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}

		maps.Copy(c.Tags, tags)

		return nil
	}

	if c, ok := b.configurations[resourceArn]; ok {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}

		maps.Copy(c.Tags, tags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a cluster or configuration by ARN.
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

	return ErrNotFound
}

// GetTags retrieves tags for a cluster or configuration by ARN.
func (b *InMemoryBackend) GetTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if c, ok := b.clusters[resourceArn]; ok {
		return maps.Clone(c.Tags), nil
	}

	if c, ok := b.configurations[resourceArn]; ok {
		return maps.Clone(c.Tags), nil
	}

	return nil, ErrNotFound
}
