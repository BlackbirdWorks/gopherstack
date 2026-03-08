package elasticache

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
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
	ErrClusterNotFound                    = errors.New("CacheClusterNotFound")
	ErrClusterAlreadyExists               = errors.New("CacheClusterAlreadyExists")
	ErrReplicationGroupNotFound           = errors.New("ReplicationGroupNotFound")
	ErrReplicationGroupAlreadyExists      = errors.New("ReplicationGroupAlreadyExists")
	ErrResourceNotFound                   = errors.New("resource not found")
	ErrParameterGroupNotFound             = errors.New("CacheParameterGroupNotFound")
	ErrParameterGroupAlreadyExists        = errors.New("CacheParameterGroupAlreadyExists")
	ErrParameterGroupDefaultNotModifiable = errors.New("default parameter group cannot be deleted or modified")
	ErrSubnetGroupNotFound                = errors.New("CacheSubnetGroupNotFound")
	ErrSubnetGroupAlreadyExists           = errors.New("CacheSubnetGroupAlreadyExists")
	ErrSnapshotNotFound                   = errors.New("SnapshotNotFound")
	ErrSnapshotAlreadyExists              = errors.New("SnapshotAlreadyExistsFault")
)

// Cluster represents an ElastiCache cluster.
type Cluster struct {
	CreatedAt               time.Time
	Tags                    *tags.Tags
	mini                    *miniredis.Miniredis
	ClusterID               string
	Engine                  string
	EngineVersion           string
	Status                  string
	Endpoint                string
	NodeType                string
	ARN                     string
	CacheParameterGroupName string
	Port                    int
	NumCacheNodes           int
}

// ReplicationGroup represents an ElastiCache replication group.
type ReplicationGroup struct {
	CreatedAt               time.Time  `json:"createdAt"`
	Tags                    *tags.Tags `json:"tags,omitempty"`
	ReplicationGroupID      string     `json:"replicationGroupID"`
	Description             string     `json:"description"`
	Status                  string     `json:"status"`
	ARN                     string     `json:"arn"`
	CacheParameterGroupName string     `json:"cacheParameterGroupName,omitempty"`
}

// CacheParameterGroup represents an ElastiCache parameter group.
type CacheParameterGroup struct {
	Tags        *tags.Tags        `json:"tags,omitempty"`
	Parameters  map[string]string `json:"parameters"`
	Name        string            `json:"name"`
	Family      string            `json:"family"`
	Description string            `json:"description"`
	ARN         string            `json:"arn"`
	IsGlobal    bool              `json:"isGlobal"`
}

// CacheSubnetGroup represents an ElastiCache subnet group.
type CacheSubnetGroup struct {
	Tags        *tags.Tags `json:"tags,omitempty"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	VpcID       string     `json:"vpcId"`
	ARN         string     `json:"arn"`
	SubnetIDs   []string   `json:"subnetIds"`
}

// CacheSnapshot represents an ElastiCache snapshot.
type CacheSnapshot struct {
	CreatedAt          time.Time  `json:"createdAt"`
	Tags               *tags.Tags `json:"tags,omitempty"`
	SnapshotName       string     `json:"snapshotName"`
	CacheClusterID     string     `json:"cacheClusterId"`
	ReplicationGroupID string     `json:"replicationGroupId"`
	Status             string     `json:"status"`
	ARN                string     `json:"arn"`
	Engine             string     `json:"engine"`
	EngineVersion      string     `json:"engineVersion"`
	NodeType           string     `json:"nodeType"`
	SnapshotSource     string     `json:"snapshotSource"` // "manual" or "automated"
}

// StorageBackend defines the interface for the ElastiCache in-memory store.
type StorageBackend interface {
	CreateCluster(id, engine, nodeType string, port int) (*Cluster, error)
	CreateClusterWithOptions(id, engine, nodeType, paramGroupName string, port int) (*Cluster, error)
	DeleteCluster(id string) error
	DescribeClusters(id, marker string, maxRecords int) (page.Page[Cluster], error)
	ModifyCluster(id, nodeType, paramGroupName string) (*Cluster, error)
	ListTagsForResource(arn string) (map[string]string, error)
	CreateReplicationGroup(id, description string) (*ReplicationGroup, error)
	CreateReplicationGroupWithOptions(id, description, paramGroupName string) (*ReplicationGroup, error)
	DeleteReplicationGroup(id string) error
	DescribeReplicationGroups(id, marker string, maxRecords int) (page.Page[ReplicationGroup], error)
	ModifyReplicationGroup(id, description, paramGroupName string) (*ReplicationGroup, error)
	CreateParameterGroup(name, family, description string) (*CacheParameterGroup, error)
	DeleteParameterGroup(name string) error
	DescribeParameterGroups(name, marker string, maxRecords int) (page.Page[CacheParameterGroup], error)
	ModifyParameterGroup(name string, params map[string]string) (*CacheParameterGroup, error)
	ResetParameterGroup(name string, paramNames []string, resetAll bool) (*CacheParameterGroup, error)
	DescribeParameters(name, marker string, maxRecords int) (page.Page[CacheParameter], error)
	CreateSubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error)
	DeleteSubnetGroup(name string) error
	DescribeSubnetGroups(name, marker string, maxRecords int) (page.Page[CacheSubnetGroup], error)
	ModifySubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error)
	CreateSnapshot(snapshotName, clusterID, replicationGroupID string) (*CacheSnapshot, error)
	DeleteSnapshot(snapshotName string) (*CacheSnapshot, error)
	DescribeSnapshots(snapshotName, clusterID, marker string, maxRecords int) (page.Page[CacheSnapshot], error)
	CopySnapshot(sourceSnapshotName, targetSnapshotName string) (*CacheSnapshot, error)
}

// CacheParameter represents a single cache parameter (for DescribeParameters response).
type CacheParameter struct {
	Name          string
	Value         string
	Description   string
	DataType      string
	AllowedValues string
	IsModifiable  bool
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// builtinParameterGroupFamilies returns the well-known default parameter group families.
func builtinParameterGroupFamilies() []struct{ family, name string } {
	return []struct{ family, name string }{
		{"redis7", "default.redis7"},
		{"redis6.x", "default.redis6.x"},
		{"redis5.0", "default.redis5.0"},
		{"redis4.0", "default.redis4.0"},
		{"redis3.2", "default.redis3.2"},
		{"redis2.8", "default.redis2.8"},
		{"memcached1.6", "default.memcached1.6"},
		{"memcached1.5", "default.memcached1.5"},
	}
}

// InMemoryBackend is an in-memory ElastiCache backend.
type InMemoryBackend struct {
	clusters          map[string]*Cluster
	replicationGroups map[string]*ReplicationGroup
	parameterGroups   map[string]*CacheParameterGroup
	subnetGroups      map[string]*CacheSubnetGroup
	snapshots         map[string]*CacheSnapshot
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

	b := &InMemoryBackend{
		clusters:          make(map[string]*Cluster),
		replicationGroups: make(map[string]*ReplicationGroup),
		parameterGroups:   make(map[string]*CacheParameterGroup),
		subnetGroups:      make(map[string]*CacheSubnetGroup),
		snapshots:         make(map[string]*CacheSnapshot),
		engineMode:        engineMode,
		accountID:         accountID,
		region:            region,
		mu:                lockmetrics.New("elasticache"),
	}

	b.initDefaultParameterGroups()

	return b
}

// initDefaultParameterGroups seeds the well-known default parameter groups.
func (b *InMemoryBackend) initDefaultParameterGroups() {
	for _, dpg := range builtinParameterGroupFamilies() {
		pg := &CacheParameterGroup{
			Name:        dpg.name,
			Family:      dpg.family,
			Description: "Default parameter group for " + dpg.family,
			ARN:         b.parameterGroupARN(dpg.name),
			IsGlobal:    true,
			Parameters:  make(map[string]string),
			Tags:        tags.New("elasticache.pg." + dpg.name + ".tags"),
		}
		b.parameterGroups[dpg.name] = pg
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

func (b *InMemoryBackend) parameterGroupARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "parametergroup:"+name)
}

func (b *InMemoryBackend) subnetGroupARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "subnetgroup:"+name)
}

func (b *InMemoryBackend) snapshotARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "snapshot:"+name)
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

// CreateClusterWithOptions creates a new cache cluster with optional parameter group.
func (b *InMemoryBackend) CreateClusterWithOptions(
	id, engine, nodeType, paramGroupName string,
	port int,
) (*Cluster, error) {
	c, err := b.CreateCluster(id, engine, nodeType, port)
	if err != nil {
		return nil, err
	}

	if paramGroupName != "" {
		b.mu.Lock("CreateClusterWithOptions")
		if _, ok := b.parameterGroups[paramGroupName]; !ok {
			b.mu.Unlock()

			if cleanupErr := b.DeleteCluster(id); cleanupErr != nil {
				return nil, errors.Join(ErrParameterGroupNotFound, cleanupErr)
			}

			return nil, ErrParameterGroupNotFound
		}
		b.clusters[id].CacheParameterGroupName = paramGroupName
		c.CacheParameterGroupName = paramGroupName
		b.mu.Unlock()
	}

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

		return page.Page[Cluster]{Data: []Cluster{*c}}, nil
	}

	out := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		out = append(out, *c)
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
	for _, pg := range b.parameterGroups {
		if pg.ARN == arn {
			return pg.Tags.Clone(), nil
		}
	}
	for _, sg := range b.subnetGroups {
		if sg.ARN == arn {
			return sg.Tags.Clone(), nil
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

// CreateReplicationGroupWithOptions creates a replication group with optional parameter group.
func (b *InMemoryBackend) CreateReplicationGroupWithOptions(
	id, description, paramGroupName string,
) (*ReplicationGroup, error) {
	rg, err := b.CreateReplicationGroup(id, description)
	if err != nil {
		return nil, err
	}

	if paramGroupName != "" {
		b.mu.Lock("CreateReplicationGroupWithOptions")
		if _, ok := b.parameterGroups[paramGroupName]; !ok {
			b.mu.Unlock()

			if cleanupErr := b.DeleteReplicationGroup(id); cleanupErr != nil {
				return nil, errors.Join(ErrParameterGroupNotFound, cleanupErr)
			}

			return nil, ErrParameterGroupNotFound
		}
		b.replicationGroups[id].CacheParameterGroupName = paramGroupName
		rg.CacheParameterGroupName = paramGroupName
		b.mu.Unlock()
	}

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
func (b *InMemoryBackend) DescribeReplicationGroups(
	id, marker string,
	maxRecords int,
) (page.Page[ReplicationGroup], error) {
	b.mu.RLock("DescribeReplicationGroups")
	defer b.mu.RUnlock()

	if id != "" {
		rg, exists := b.replicationGroups[id]
		if !exists {
			return page.Page[ReplicationGroup]{}, ErrReplicationGroupNotFound
		}

		return page.Page[ReplicationGroup]{Data: []ReplicationGroup{*rg}}, nil
	}

	out := make([]ReplicationGroup, 0, len(b.replicationGroups))
	for _, rg := range b.replicationGroups {
		out = append(out, *rg)
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

// ModifyCluster modifies an existing cache cluster.
func (b *InMemoryBackend) ModifyCluster(id, nodeType, paramGroupName string) (*Cluster, error) {
	b.mu.Lock("ModifyCluster")
	defer b.mu.Unlock()

	c, exists := b.clusters[id]
	if !exists {
		return nil, ErrClusterNotFound
	}

	if nodeType != "" {
		c.NodeType = nodeType
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroups[paramGroupName]; !ok {
			return nil, ErrParameterGroupNotFound
		}
		c.CacheParameterGroupName = paramGroupName
	}

	cp := *c

	return &cp, nil
}

// ModifyReplicationGroup modifies an existing replication group.
func (b *InMemoryBackend) ModifyReplicationGroup(id, description, paramGroupName string) (*ReplicationGroup, error) {
	b.mu.Lock("ModifyReplicationGroup")
	defer b.mu.Unlock()

	rg, exists := b.replicationGroups[id]
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	if description != "" {
		rg.Description = description
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroups[paramGroupName]; !ok {
			return nil, ErrParameterGroupNotFound
		}
		rg.CacheParameterGroupName = paramGroupName
	}

	cp := *rg

	return &cp, nil
}

// CreateParameterGroup creates a new cache parameter group.
func (b *InMemoryBackend) CreateParameterGroup(name, family, description string) (*CacheParameterGroup, error) {
	b.mu.Lock("CreateParameterGroup")
	defer b.mu.Unlock()

	if _, exists := b.parameterGroups[name]; exists {
		return nil, ErrParameterGroupAlreadyExists
	}

	pg := &CacheParameterGroup{
		Name:        name,
		Family:      family,
		Description: description,
		ARN:         b.parameterGroupARN(name),
		IsGlobal:    false,
		Parameters:  make(map[string]string),
		Tags:        tags.New("elasticache.pg." + name + ".tags"),
	}
	b.parameterGroups[name] = pg

	return pg, nil
}

// DeleteParameterGroup removes a cache parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(name string) error {
	b.mu.Lock("DeleteParameterGroup")
	defer b.mu.Unlock()

	pg, exists := b.parameterGroups[name]
	if !exists {
		return ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return ErrParameterGroupDefaultNotModifiable
	}

	delete(b.parameterGroups, name)

	return nil
}

// DescribeParameterGroups returns one parameter group by name, or a paginated list of all.
func (b *InMemoryBackend) DescribeParameterGroups(
	name, marker string,
	maxRecords int,
) (page.Page[CacheParameterGroup], error) {
	b.mu.RLock("DescribeParameterGroups")
	defer b.mu.RUnlock()

	if name != "" {
		pg, exists := b.parameterGroups[name]
		if !exists {
			return page.Page[CacheParameterGroup]{}, ErrParameterGroupNotFound
		}

		return page.Page[CacheParameterGroup]{Data: []CacheParameterGroup{*pg}}, nil
	}

	out := make([]CacheParameterGroup, 0, len(b.parameterGroups))
	for _, pg := range b.parameterGroups {
		out = append(out, *pg)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ModifyParameterGroup updates parameters in a cache parameter group.
func (b *InMemoryBackend) ModifyParameterGroup(name string, params map[string]string) (*CacheParameterGroup, error) {
	b.mu.Lock("ModifyParameterGroup")
	defer b.mu.Unlock()

	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return nil, ErrParameterGroupDefaultNotModifiable
	}

	maps.Copy(pg.Parameters, params)

	cp := *pg

	return &cp, nil
}

// ResetParameterGroup resets parameters in a cache parameter group to defaults.
func (b *InMemoryBackend) ResetParameterGroup(
	name string,
	paramNames []string,
	resetAll bool,
) (*CacheParameterGroup, error) {
	b.mu.Lock("ResetParameterGroup")
	defer b.mu.Unlock()

	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return nil, ErrParameterGroupDefaultNotModifiable
	}

	if resetAll {
		pg.Parameters = make(map[string]string)
	} else {
		for _, pname := range paramNames {
			delete(pg.Parameters, pname)
		}
	}

	cp := *pg

	return &cp, nil
}

// DescribeParameters lists parameters in a cache parameter group.
func (b *InMemoryBackend) DescribeParameters(name, marker string, maxRecords int) (page.Page[CacheParameter], error) {
	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	pg, exists := b.parameterGroups[name]
	if !exists {
		return page.Page[CacheParameter]{}, ErrParameterGroupNotFound
	}

	out := make([]CacheParameter, 0, len(pg.Parameters))
	for k, v := range pg.Parameters {
		out = append(out, CacheParameter{
			Name:         k,
			Value:        v,
			DataType:     "string",
			IsModifiable: true,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// CreateSubnetGroup creates a new cache subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error) {
	b.mu.Lock("CreateSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sg := &CacheSubnetGroup{
		Name:        name,
		Description: description,
		SubnetIDs:   subnetIDs,
		ARN:         b.subnetGroupARN(name),
		Tags:        tags.New("elasticache.sg." + name + ".tags"),
	}
	b.subnetGroups[name] = sg

	return sg, nil
}

// DeleteSubnetGroup removes a cache subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(name string) error {
	b.mu.Lock("DeleteSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; !exists {
		return ErrSubnetGroupNotFound
	}

	delete(b.subnetGroups, name)

	return nil
}

// DescribeSubnetGroups returns one subnet group by name, or a paginated list of all.
func (b *InMemoryBackend) DescribeSubnetGroups(
	name, marker string,
	maxRecords int,
) (page.Page[CacheSubnetGroup], error) {
	b.mu.RLock("DescribeSubnetGroups")
	defer b.mu.RUnlock()

	if name != "" {
		sg, exists := b.subnetGroups[name]
		if !exists {
			return page.Page[CacheSubnetGroup]{}, ErrSubnetGroupNotFound
		}

		return page.Page[CacheSubnetGroup]{Data: []CacheSubnetGroup{*sg}}, nil
	}

	out := make([]CacheSubnetGroup, 0, len(b.subnetGroups))
	for _, sg := range b.subnetGroups {
		out = append(out, *sg)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ModifySubnetGroup updates a cache subnet group.
func (b *InMemoryBackend) ModifySubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error) {
	b.mu.Lock("ModifySubnetGroup")
	defer b.mu.Unlock()

	sg, exists := b.subnetGroups[name]
	if !exists {
		return nil, ErrSubnetGroupNotFound
	}

	if description != "" {
		sg.Description = description
	}

	if len(subnetIDs) > 0 {
		sg.SubnetIDs = subnetIDs
	}

	cp := *sg

	return &cp, nil
}

// CreateSnapshot creates a manual snapshot of a cluster or replication group.
func (b *InMemoryBackend) CreateSnapshot(snapshotName, clusterID, replicationGroupID string) (*CacheSnapshot, error) {
	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	if _, exists := b.snapshots[snapshotName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snap := &CacheSnapshot{
		SnapshotName:       snapshotName,
		CacheClusterID:     clusterID,
		ReplicationGroupID: replicationGroupID,
		Status:             "available",
		ARN:                b.snapshotARN(snapshotName),
		SnapshotSource:     "manual",
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.snapshot." + snapshotName + ".tags"),
	}

	if clusterID != "" {
		c, ok := b.clusters[clusterID]
		if !ok {
			return nil, ErrClusterNotFound
		}
		snap.Engine = c.Engine
		snap.EngineVersion = c.EngineVersion
		snap.NodeType = c.NodeType
	}

	if replicationGroupID != "" {
		rg, ok := b.replicationGroups[replicationGroupID]
		if !ok {
			return nil, ErrReplicationGroupNotFound
		}
		snap.Engine = "redis"
		snap.EngineVersion = "7.1.0"
		snap.ReplicationGroupID = rg.ReplicationGroupID
	}

	b.snapshots[snapshotName] = snap

	return snap, nil
}

// DeleteSnapshot removes a snapshot and returns the deleted snapshot.
func (b *InMemoryBackend) DeleteSnapshot(snapshotName string) (*CacheSnapshot, error) {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotName]
	if !exists {
		return nil, ErrSnapshotNotFound
	}

	cp := *snap
	delete(b.snapshots, snapshotName)

	return &cp, nil
}

// DescribeSnapshots returns one snapshot by name, or a paginated list filtered by cluster/rg.
func (b *InMemoryBackend) DescribeSnapshots(
	snapshotName, clusterID, marker string,
	maxRecords int,
) (page.Page[CacheSnapshot], error) {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	if snapshotName != "" {
		snap, exists := b.snapshots[snapshotName]
		if !exists {
			return page.Page[CacheSnapshot]{}, ErrSnapshotNotFound
		}

		return page.Page[CacheSnapshot]{Data: []CacheSnapshot{*snap}}, nil
	}

	out := make([]CacheSnapshot, 0, len(b.snapshots))
	for _, snap := range b.snapshots {
		if clusterID != "" && snap.CacheClusterID != clusterID {
			continue
		}
		out = append(out, *snap)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotName < out[j].SnapshotName })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// CopySnapshot copies an existing snapshot to a new name.
func (b *InMemoryBackend) CopySnapshot(sourceSnapshotName, targetSnapshotName string) (*CacheSnapshot, error) {
	b.mu.Lock("CopySnapshot")
	defer b.mu.Unlock()

	src, ok := b.snapshots[sourceSnapshotName]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if _, targetExists := b.snapshots[targetSnapshotName]; targetExists {
		return nil, ErrSnapshotAlreadyExists
	}

	cp := *src
	cp.SnapshotName = targetSnapshotName
	cp.ARN = b.snapshotARN(targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.Tags = tags.New("elasticache.snapshot." + targetSnapshotName + ".tags")
	b.snapshots[targetSnapshotName] = &cp

	result := cp

	return &result, nil
}
