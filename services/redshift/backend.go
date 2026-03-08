package redshift

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrClusterNotFound      = errors.New("ClusterNotFound")
	ErrClusterAlreadyExists = errors.New("ClusterAlreadyExists")
	ErrInvalidParameter     = errors.New("InvalidParameterValue")
)

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// Cluster represents a Redshift cluster.
type Cluster struct {
	Tags              *tags.Tags `json:"tags,omitempty"`
	ClusterIdentifier string     `json:"clusterIdentifier"`
	NodeType          string     `json:"nodeType"`
	Endpoint          string     `json:"endpoint"`
	Status            string     `json:"status"`
	DBName            string     `json:"dbName"`
	MasterUsername    string     `json:"masterUsername"`
}

// InMemoryBackend is the in-memory store for Redshift clusters.
type InMemoryBackend struct {
	dnsRegistrar DNSRegistrar
	clusters     map[string]*Cluster
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:  make(map[string]*Cluster),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("redshift"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// SetDNSRegistrar wires a DNS server so Redshift cluster hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = dns
	b.mu.Unlock()
}

// CreateCluster creates a new Redshift cluster.
func (b *InMemoryBackend) CreateCluster(id, nodeType, dbName, masterUser string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}

	if nodeType == "" {
		nodeType = "dc2.large"
	}
	if dbName == "" {
		dbName = "dev"
	}
	if masterUser == "" {
		masterUser = "admin"
	}

	endpoint := fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", id, b.accountID, b.region)
	cluster := &Cluster{
		ClusterIdentifier: id,
		NodeType:          nodeType,
		Endpoint:          endpoint,
		Status:            "available",
		DBName:            dbName,
		MasterUsername:    masterUser,
		Tags:              tags.New("redshift.cluster." + id + ".tags"),
	}
	b.clusters[id] = cluster

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *cluster

	return &cp, nil
}

// DeleteCluster removes the cluster with the given identifier.
func (b *InMemoryBackend) DeleteCluster(id string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cp := *cluster
	delete(b.clusters, id)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeClusters returns clusters. If id is non-empty, returns only that cluster.
func (b *InMemoryBackend) DescribeClusters(id string) ([]Cluster, error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []Cluster{*c}, nil
	}

	clusters := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		clusters = append(clusters, *c)
	}

	return clusters, nil
}

// DescribeTags returns all tags across all clusters.
func (b *InMemoryBackend) DescribeTags() map[string]map[string]string {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string]map[string]string, len(b.clusters))
	for id, c := range b.clusters {
		result[id] = c.Tags.Clone()
	}

	return result
}

// CreateTags adds or updates tags on the specified cluster.
func (b *InMemoryBackend) CreateTags(clusterID string, kv map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	c, exists := b.clusters[clusterID]
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	c.Tags.Merge(kv)

	return nil
}

// DeleteTags removes tag keys from the specified cluster.
func (b *InMemoryBackend) DeleteTags(clusterID string, keys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	c, exists := b.clusters[clusterID]
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	c.Tags.DeleteKeys(keys)

	return nil
}
