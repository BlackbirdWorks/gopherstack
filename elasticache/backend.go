package elasticache

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// Engine mode constants.
const (
	EngineEmbedded = "embedded"
	EngineDocker   = "docker"
	EngineStub     = "stub"
)

var (
	ErrClusterNotFound               = errors.New("CacheClusterNotFound")
	ErrClusterAlreadyExists          = errors.New("CacheClusterAlreadyExists")
	ErrReplicationGroupNotFound      = errors.New("ReplicationGroupNotFound")
	ErrReplicationGroupAlreadyExists = errors.New("ReplicationGroupAlreadyExists")
)

// Cluster represents an ElastiCache cluster.
type Cluster struct {
	ClusterID     string
	Engine        string
	EngineVersion string
	Status        string
	Endpoint      string
	Port          int
	NodeType      string
	NumCacheNodes int
	ARN           string
	Tags          map[string]string
	CreatedAt     time.Time
	mini          *miniredis.Miniredis // non-nil for embedded mode
}

// ReplicationGroup represents an ElastiCache replication group.
type ReplicationGroup struct {
	ReplicationGroupID string
	Description        string
	Status             string
	ARN                string
	Tags               map[string]string
	CreatedAt          time.Time
}

// StorageBackend defines the interface for the ElastiCache in-memory store.
type StorageBackend interface {
	CreateCluster(id, engine, nodeType string, port int) (*Cluster, error)
	DeleteCluster(id string) error
	DescribeClusters(id string) ([]Cluster, error)
	ListTagsForResource(arn string) (map[string]string, error)
	CreateReplicationGroup(id, description string) (*ReplicationGroup, error)
	DeleteReplicationGroup(id string) error
	DescribeReplicationGroups(id string) ([]ReplicationGroup, error)
}

// InMemoryBackend is an in-memory ElastiCache backend.
type InMemoryBackend struct {
	mu                sync.RWMutex
	clusters          map[string]*Cluster
	replicationGroups map[string]*ReplicationGroup
	engineMode        string
	accountID         string
	region            string
}

// NewInMemoryBackend creates a new backend with the given engine mode.
func NewInMemoryBackend(engineMode, accountID, region string) *InMemoryBackend {
	if engineMode == "" {
		engineMode = EngineEmbedded
	}
	return &InMemoryBackend{
		clusters:          make(map[string]*Cluster),
		replicationGroups: make(map[string]*ReplicationGroup),
		engineMode:        engineMode,
		accountID:         accountID,
		region:            region,
	}
}

func (b *InMemoryBackend) clusterARN(id string) string {
	return fmt.Sprintf("arn:aws:elasticache:%s:%s:cluster:%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) replicationGroupARN(id string) string {
	return fmt.Sprintf("arn:aws:elasticache:%s:%s:replicationgroup:%s", b.region, b.accountID, id)
}

// CreateCluster creates a new cache cluster.
func (b *InMemoryBackend) CreateCluster(id, engine, nodeType string, port int) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.clusters[id]; exists {
		return nil, ErrClusterAlreadyExists
	}

	if engine == "" {
		engine = "redis"
	}
	if nodeType == "" {
		nodeType = "cache.t3.micro"
	}

	c := &Cluster{
		ClusterID:     id,
		Engine:        engine,
		EngineVersion: "7.1.0",
		Status:        "available",
		NodeType:      nodeType,
		NumCacheNodes: 1,
		ARN:           b.clusterARN(id),
		Tags:          make(map[string]string),
		CreatedAt:     time.Now(),
	}

	switch b.engineMode {
	case EngineEmbedded:
		mr, err := miniredis.Run()
		if err != nil {
			return nil, fmt.Errorf("start miniredis: %w", err)
		}
		c.mini = mr
		c.Endpoint = "localhost"
		c.Port = mr.Server().Addr().Port
	default:
		// stub and docker: synthetic endpoint
		c.Endpoint = "localhost"
		if port > 0 {
			c.Port = port
		} else {
			c.Port = 6379
		}
	}

	b.clusters[id] = c
	return c, nil
}

// DeleteCluster stops and removes a cluster.
func (b *InMemoryBackend) DeleteCluster(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, exists := b.clusters[id]
	if !exists {
		return ErrClusterNotFound
	}

	if c.mini != nil {
		c.mini.Close()
	}
	delete(b.clusters, id)
	return nil
}

// DescribeClusters returns one or all clusters.
func (b *InMemoryBackend) DescribeClusters(id string) ([]Cluster, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, ErrClusterNotFound
		}
		out := *c
		return []Cluster{out}, nil
	}

	out := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		cp := *c
		out = append(out, cp)
	}
	return out, nil
}

// ListTagsForResource returns tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, c := range b.clusters {
		if c.ARN == arn {
			tags := make(map[string]string, len(c.Tags))
			for k, v := range c.Tags {
				tags[k] = v
			}
			return tags, nil
		}
	}
	for _, rg := range b.replicationGroups {
		if rg.ARN == arn {
			tags := make(map[string]string, len(rg.Tags))
			for k, v := range rg.Tags {
				tags[k] = v
			}
			return tags, nil
		}
	}
	return nil, fmt.Errorf("resource with ARN %s not found", arn)
}

// CreateReplicationGroup creates a replication group.
func (b *InMemoryBackend) CreateReplicationGroup(id, description string) (*ReplicationGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.replicationGroups[id]; exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	rg := &ReplicationGroup{
		ReplicationGroupID: id,
		Description:        description,
		Status:             "available",
		ARN:                b.replicationGroupARN(id),
		Tags:               make(map[string]string),
		CreatedAt:          time.Now(),
	}
	b.replicationGroups[id] = rg
	return rg, nil
}

// DeleteReplicationGroup removes a replication group.
func (b *InMemoryBackend) DeleteReplicationGroup(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.replicationGroups[id]; !exists {
		return ErrReplicationGroupNotFound
	}
	delete(b.replicationGroups, id)
	return nil
}

// DescribeReplicationGroups returns one or all replication groups.
func (b *InMemoryBackend) DescribeReplicationGroups(id string) ([]ReplicationGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if id != "" {
		rg, exists := b.replicationGroups[id]
		if !exists {
			return nil, ErrReplicationGroupNotFound
		}
		out := *rg
		return []ReplicationGroup{out}, nil
	}

	out := make([]ReplicationGroup, 0, len(b.replicationGroups))
	for _, rg := range b.replicationGroups {
		cp := *rg
		out = append(out, cp)
	}
	return out, nil
}

// ListAll returns all clusters (used by dashboard).
func (b *InMemoryBackend) ListAll() []Cluster {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		cp := *c
		out = append(out, cp)
	}
	return out
}
