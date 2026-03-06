package elasticache

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	randomSuffixLen = 3
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
	ErrResourceNotFound              = errors.New("resource not found")
)

// Cluster represents an ElastiCache cluster.
type Cluster struct {
	CreatedAt     time.Time
	Tags          *tags.Tags
	mini          *miniredis.Miniredis
	ClusterID     string
	Engine        string
	EngineVersion string
	Status        string
	Endpoint      string
	NodeType      string
	ARN           string
	Port          int
	NumCacheNodes int
}

// ReplicationGroup represents an ElastiCache replication group.
type ReplicationGroup struct {
	CreatedAt          time.Time  `json:"createdAt"`
	Tags               *tags.Tags `json:"tags,omitempty"`
	ReplicationGroupID string     `json:"replicationGroupID"`
	Description        string     `json:"description"`
	Status             string     `json:"status"`
	ARN                string     `json:"arn"`
}

// StorageBackend defines the interface for the ElastiCache in-memory store.
type StorageBackend interface {
	CreateCluster(id, engine, nodeType string, port int) (*Cluster, error)
	DeleteCluster(id string) error
	DescribeClusters(id, marker string, maxRecords int) (page.Page[Cluster], error)
	ListTagsForResource(arn string) (map[string]string, error)
	CreateReplicationGroup(id, description string) (*ReplicationGroup, error)
	DeleteReplicationGroup(id string) error
	DescribeReplicationGroups(id, marker string, maxRecords int) (page.Page[ReplicationGroup], error)
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// InMemoryBackend is an in-memory ElastiCache backend.
type InMemoryBackend struct {
	clusters          map[string]*Cluster
	replicationGroups map[string]*ReplicationGroup
	mu                *lockmetrics.RWMutex
	dnsRegistrar      DNSRegistrar
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
		mu:                lockmetrics.New("elasticache"),
	}
}

// SetDNSRegistrar wires a DNS server so cache cluster hostnames are
// automatically registered on create and deregistered on delete.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = r
	b.mu.Unlock()
}

func (b *InMemoryBackend) clusterARN(id string) string {
	return arn.Build("elasticache", b.region, b.accountID, "cluster:"+id)
}

func (b *InMemoryBackend) replicationGroupARN(id string) string {
	return arn.Build("elasticache", b.region, b.accountID, "replicationgroup:"+id)
}

// CreateCluster creates a new cache cluster.
func (b *InMemoryBackend) CreateCluster(id, engine, nodeType string, port int) (*Cluster, error) {
	b.mu.Lock("CreateCluster")
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
		Tags:          tags.New("elasticache.cluster." + id + ".tags"),
		CreatedAt:     time.Now(),
	}

	switch b.engineMode {
	case EngineEmbedded:
		mr, err := miniredis.Run()
		if err != nil {
			return nil, fmt.Errorf("start miniredis: %w", err)
		}
		c.mini = mr
		c.Port = mr.Server().Addr().Port
	default:
		if port > 0 {
			c.Port = port
		} else {
			c.Port = 6379
		}
	}

	// Generate an AWS-style hostname and register it with DNS if available.
	c.Endpoint = gopherDNS.SyntheticHostname(id, randomSuffix(), b.region, "cache")
	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(c.Endpoint)
	}

	b.clusters[id] = c

	return c, nil
}

// DeleteCluster stops and removes a cluster.
func (b *InMemoryBackend) DeleteCluster(id string) error {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, exists := b.clusters[id]
	if !exists {
		return ErrClusterNotFound
	}

	if b.dnsRegistrar != nil && c.Endpoint != "" {
		b.dnsRegistrar.Deregister(c.Endpoint)
	}

	if c.mini != nil {
		c.mini.Close()
	}
	delete(b.clusters, id)

	return nil
}

const elasticacheDefaultMaxRecords = 100

// DescribeClusters returns one cluster by id, or a paginated list of all clusters when id is empty.
func (b *InMemoryBackend) DescribeClusters(id, marker string, maxRecords int) (page.Page[Cluster], error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return page.Page[Cluster]{}, ErrClusterNotFound
		}
		out := *c

		return page.Page[Cluster]{Data: []Cluster{out}}, nil
	}

	out := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		cp := *c
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ClusterID < out[j].ClusterID })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ListTagsForResource returns tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, c := range b.clusters {
		if c.ARN == arn {
			return c.Tags.Clone(), nil
		}
	}
	for _, rg := range b.replicationGroups {
		if rg.ARN == arn {
			return rg.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("resource with ARN %s: %w", arn, ErrResourceNotFound)
}

// CreateReplicationGroup creates a replication group.
func (b *InMemoryBackend) CreateReplicationGroup(id, description string) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroup")
	defer b.mu.Unlock()

	if _, exists := b.replicationGroups[id]; exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	rg := &ReplicationGroup{
		ReplicationGroupID: id,
		Description:        description,
		Status:             "available",
		ARN:                b.replicationGroupARN(id),
		Tags:               tags.New("elasticache.rg." + id + ".tags"),
		CreatedAt:          time.Now(),
	}
	b.replicationGroups[id] = rg

	return rg, nil
}

// DeleteReplicationGroup removes a replication group.
func (b *InMemoryBackend) DeleteReplicationGroup(id string) error {
	b.mu.Lock("DeleteReplicationGroup")
	defer b.mu.Unlock()

	if _, exists := b.replicationGroups[id]; !exists {
		return ErrReplicationGroupNotFound
	}
	delete(b.replicationGroups, id)

	return nil
}

// DescribeReplicationGroups returns one replication group by id, or a paginated list of all when id is empty.
func (b *InMemoryBackend) DescribeReplicationGroups(id, marker string, maxRecords int) (page.Page[ReplicationGroup], error) {
	b.mu.RLock("DescribeReplicationGroups")
	defer b.mu.RUnlock()

	if id != "" {
		rg, exists := b.replicationGroups[id]
		if !exists {
			return page.Page[ReplicationGroup]{}, ErrReplicationGroupNotFound
		}
		out := *rg

		return page.Page[ReplicationGroup]{Data: []ReplicationGroup{out}}, nil
	}

	out := make([]ReplicationGroup, 0, len(b.replicationGroups))
	for _, rg := range b.replicationGroups {
		cp := *rg
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ReplicationGroupID < out[j].ReplicationGroupID })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// randomSuffix generates a short random hex string for synthetic hostnames.
func randomSuffix() string {
	b := make([]byte, randomSuffixLen)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// ListAll returns all clusters (used by dashboard).
func (b *InMemoryBackend) ListAll() []Cluster {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()
	out := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		cp := *c
		out = append(out, cp)
	}

	return out
}
