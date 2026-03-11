package docdb

import (
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	ErrClusterNotFound                    = errors.New("DBClusterNotFound")
	ErrClusterAlreadyExists               = errors.New("DBClusterAlreadyExists")
	ErrInstanceNotFound                   = errors.New("DBInstanceNotFound")
	ErrInstanceAlreadyExists              = errors.New("DBInstanceAlreadyExists")
	ErrSubnetGroupNotFound                = errors.New("DBSubnetGroupNotFound")
	ErrSubnetGroupAlreadyExists           = errors.New("DBSubnetGroupAlreadyExists")
	ErrClusterParameterGroupNotFound      = errors.New("DBClusterParameterGroupNotFound")
	ErrClusterParameterGroupAlreadyExists = errors.New("DBClusterParameterGroupAlreadyExists")
	ErrClusterSnapshotNotFound            = errors.New("DBClusterSnapshotNotFound")
	ErrClusterSnapshotAlreadyExists       = errors.New("DBClusterSnapshotAlreadyExists")
	ErrInvalidParameter                   = errors.New("InvalidParameterValue")
	ErrUnknownAction                      = errors.New("InvalidAction")
)

const (
	defaultDocDBPort     = 27017
	defaultInstanceClass = "db.t3.medium"
	docDBEngine          = "docdb"
)

type DBCluster struct {
	DBClusterIdentifier         string
	Engine                      string
	Status                      string
	MasterUsername              string
	DatabaseName                string
	DBClusterParameterGroupName string
	Endpoint                    string
	Port                        int
}

type DBInstance struct {
	DBInstanceIdentifier string
	DBClusterIdentifier  string
	DBInstanceClass      string
	Engine               string
	DBInstanceStatus     string
	Endpoint             string
	Port                 int
}

type DBSubnetGroup struct {
	DBSubnetGroupName        string
	DBSubnetGroupDescription string
	VpcID                    string
	Status                   string
	SubnetIDs                []string
}

type Tag struct {
	Key   string
	Value string
}

type DBClusterParameterGroup struct {
	DBClusterParameterGroupName string
	DBParameterGroupFamily      string
	Description                 string
}

type DBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string
	DBClusterIdentifier         string
	Engine                      string
	Status                      string
}

type InMemoryBackend struct {
	clusters               map[string]*DBCluster
	instances              map[string]*DBInstance
	subnetGroups           map[string]*DBSubnetGroup
	clusterParameterGroups map[string]*DBClusterParameterGroup
	clusterSnapshots       map[string]*DBClusterSnapshot
	tags                   map[string][]Tag
	fisFailoverFaults      map[string]time.Time
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:               make(map[string]*DBCluster),
		instances:              make(map[string]*DBInstance),
		subnetGroups:           make(map[string]*DBSubnetGroup),
		clusterParameterGroups: make(map[string]*DBClusterParameterGroup),
		clusterSnapshots:       make(map[string]*DBClusterSnapshot),
		tags:                   make(map[string][]Tag),
		fisFailoverFaults:      make(map[string]time.Time),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("docdb"),
	}
}

func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) CreateDBCluster(
	id, engine, masterUser, dbName, paramGroupName string,
	port int,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = docDBEngine
	}
	if paramGroupName == "" {
		paramGroupName = "default.docdb4.0"
	}
	if port <= 0 {
		port = defaultDocDBPort
	}
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", id, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:         id,
		Engine:                      engine,
		Status:                      "available",
		MasterUsername:              masterUser,
		DatabaseName:                dbName,
		DBClusterParameterGroupName: paramGroupName,
		Endpoint:                    endpoint,
		Port:                        port,
	}
	b.clusters[id] = cluster
	cp := *cluster

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusters(id string) ([]DBCluster, error) {
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}
		cp := *c

		return []DBCluster{cp}, nil
	}
	result := make([]DBCluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		result = append(result, *c)
	}

	return result, nil
}

func (b *InMemoryBackend) DeleteDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := *c
	delete(b.clusters, id)

	return &cp, nil
}

func (b *InMemoryBackend) ModifyDBCluster(id, paramGroupName string) (*DBCluster, error) {
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if paramGroupName != "" {
		c.DBClusterParameterGroupName = paramGroupName
	}
	cp := *c

	return &cp, nil
}

func (b *InMemoryBackend) StopDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	c.Status = "stopped"
	cp := *c

	return &cp, nil
}

func (b *InMemoryBackend) StartDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	c.Status = "available"
	cp := *c

	return &cp, nil
}

func (b *InMemoryBackend) FailoverDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := *c

	return &cp, nil
}

func (b *InMemoryBackend) CreateDBInstance(id, clusterID, instanceClass, engine string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()
	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	if engine == "" {
		engine = docDBEngine
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	endpoint := fmt.Sprintf("%s.docdb.%s.amazonaws.com", id, b.region)
	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DBClusterIdentifier:  clusterID,
		DBInstanceClass:      instanceClass,
		Engine:               engine,
		DBInstanceStatus:     "available",
		Endpoint:             endpoint,
		Port:                 defaultDocDBPort,
	}
	b.instances[id] = inst
	cp := *inst

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	if id != "" {
		inst, exists := b.instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}
		cp := *inst

		return []DBInstance{cp}, nil
	}
	result := make([]DBInstance, 0, len(b.instances))
	for _, inst := range b.instances {
		result = append(result, *inst)
	}

	return result, nil
}

func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst
	delete(b.instances, id)

	return &cp, nil
}

func (b *InMemoryBackend) ModifyDBInstance(id, instanceClass string) (*DBInstance, error) {
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	cp := *inst

	return &cp, nil
}

func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst

	return &cp, nil
}

func (b *InMemoryBackend) CreateDBSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBSubnetGroup")
	defer b.mu.Unlock()
	if _, exists := b.subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: subnet group %s already exists", ErrSubnetGroupAlreadyExists, name)
	}
	ids := make([]string, len(subnetIDs))
	copy(ids, subnetIDs)
	sg := &DBSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		Status:                   "Complete",
		SubnetIDs:                ids,
	}
	b.subnetGroups[name] = sg
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBSubnetGroups(name string) ([]DBSubnetGroup, error) {
	b.mu.RLock("DescribeDBSubnetGroups")
	defer b.mu.RUnlock()
	if name != "" {
		sg, exists := b.subnetGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)

		return []DBSubnetGroup{cp}, nil
	}
	result := make([]DBSubnetGroup, 0, len(b.subnetGroups))
	for _, sg := range b.subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		result = append(result, cp)
	}

	return result, nil
}

func (b *InMemoryBackend) DeleteDBSubnetGroup(name string) error {
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	delete(b.subnetGroups, name)

	return nil
}

func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	name, family, description string,
) (*DBClusterParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 description,
	}
	b.clusterParameterGroups[name] = pg
	cp := *pg

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterParameterGroups(name string) ([]DBClusterParameterGroup, error) {
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
		}
		cp := *pg

		return []DBClusterParameterGroup{cp}, nil
	}
	result := make([]DBClusterParameterGroup, 0, len(b.clusterParameterGroups))
	for _, pg := range b.clusterParameterGroups {
		result = append(result, *pg)
	}

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterParameterGroup(name string) error {
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	delete(b.clusterParameterGroups, name)

	return nil
}

func (b *InMemoryBackend) ModifyDBClusterParameterGroup(name string) (*DBClusterParameterGroup, error) {
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

func (b *InMemoryBackend) CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	c, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterIdentifier:         clusterID,
		Engine:                      c.Engine,
		Status:                      "available",
	}
	b.clusterSnapshots[snapshotID] = snap
	cp := *snap

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterSnapshots(snapshotID string) ([]DBClusterSnapshot, error) {
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, len(b.clusterSnapshots))
	for _, snap := range b.clusterSnapshots {
		result = append(result, *snap)
	}

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	delete(b.clusterSnapshots, snapshotID)

	return &cp, nil
}

func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	current := b.tags[arn]
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}
	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}
	b.tags[arn] = current
}

func (b *InMemoryBackend) RemoveTagsFromResource(arn string, keys []string) {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	current := b.tags[arn]
	kept := current[:0]
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	b.tags[arn] = kept
}

func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp
}
