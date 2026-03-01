package redshift

import (
	"errors"
	"fmt"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrClusterNotFound      = errors.New("ClusterNotFound")
	ErrClusterAlreadyExists = errors.New("ClusterAlreadyExists")
	ErrInvalidParameter     = errors.New("InvalidParameterValue")
)

// Cluster represents a Redshift cluster.
type Cluster struct {
	Tags              *tags.Tags
	ClusterIdentifier string
	NodeType          string
	Endpoint          string
	Status            string
	DBName            string
	MasterUsername    string
}

// InMemoryBackend is the in-memory store for Redshift clusters.
type InMemoryBackend struct {
	clusters  map[string]*Cluster
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:  make(map[string]*Cluster),
		accountID: accountID,
		region:    region,
	}
}

// CreateCluster creates a new Redshift cluster.
func (b *InMemoryBackend) CreateCluster(id, nodeType, dbName, masterUser string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock()
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

	cp := *cluster

	return &cp, nil
}

// DeleteCluster removes the cluster with the given identifier.
func (b *InMemoryBackend) DeleteCluster(id string) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cp := *cluster
	delete(b.clusters, id)

	return &cp, nil
}

// DescribeClusters returns clusters. If id is non-empty, returns only that cluster.
func (b *InMemoryBackend) DescribeClusters(id string) ([]Cluster, error) {
	b.mu.RLock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make(map[string]map[string]string, len(b.clusters))
	for id, c := range b.clusters {
		result[id] = c.Tags.Clone()
	}

	return result
}

// CreateTags adds or updates tags on the specified cluster.
func (b *InMemoryBackend) CreateTags(clusterID string, kv map[string]string) error {
	b.mu.Lock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	c, exists := b.clusters[clusterID]
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	c.Tags.DeleteKeys(keys)

	return nil
}
