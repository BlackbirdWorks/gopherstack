package rds

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrInstanceNotFound is returned when an RDS instance does not exist.
	ErrInstanceNotFound = errors.New("DBInstanceNotFound")
	// ErrInstanceAlreadyExists is returned when an RDS instance already exists.
	ErrInstanceAlreadyExists = errors.New("DBInstanceAlreadyExists")
	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = errors.New("DBSnapshotNotFound")
	// ErrSnapshotAlreadyExists is returned when a snapshot already exists.
	ErrSnapshotAlreadyExists = errors.New("DBSnapshotAlreadyExists")
	// ErrSubnetGroupNotFound is returned when a subnet group does not exist.
	ErrSubnetGroupNotFound = errors.New("DBSubnetGroupNotFound")
	// ErrSubnetGroupAlreadyExists is returned when a subnet group already exists.
	ErrSubnetGroupAlreadyExists = errors.New("DBSubnetGroupAlreadyExists")
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = errors.New("InvalidParameterValue")
	// ErrUnknownAction is returned for unrecognized RDS actions.
	ErrUnknownAction = errors.New("InvalidAction")
)

const (
	defaultPort             = 5432
	mysqlPort               = 3306
	defaultInstanceClass    = "db.t3.micro"
	defaultAllocatedStorage = 20
)

// DBInstance represents an RDS database instance.
type DBInstance struct {
	DBInstanceIdentifier string `json:"dbInstanceIdentifier"`
	DbiResourceID        string `json:"dbiResourceID"`
	DBInstanceClass      string `json:"dbInstanceClass"`
	Engine               string `json:"engine"`
	DBInstanceStatus     string `json:"dbInstanceStatus"`
	MasterUsername       string `json:"masterUsername"`
	DBName               string `json:"dbName"`
	Endpoint             string `json:"endpoint"`
	VpcID                string `json:"vpcID"`
	DBSubnetGroupName    string `json:"dbSubnetGroupName"`
	Port                 int    `json:"port"`
	AllocatedStorage     int    `json:"allocatedStorage"`
}

// DBSnapshot represents an RDS database snapshot.
type DBSnapshot struct {
	DBSnapshotIdentifier string `json:"dbSnapshotIdentifier"`
	DBInstanceIdentifier string `json:"dbInstanceIdentifier"`
	Engine               string `json:"engine"`
	Status               string `json:"status"`
}

// DBSubnetGroup represents an RDS DB subnet group.
type DBSubnetGroup struct {
	DBSubnetGroupName        string   `json:"dbSubnetGroupName"`
	DBSubnetGroupDescription string   `json:"dbSubnetGroupDescription"`
	VpcID                    string   `json:"vpcID"`
	Status                   string   `json:"status"`
	SubnetIDs                []string `json:"subnetIDs"`
}

// Tag is a key/value tag attached to an RDS resource.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// InMemoryBackend is the in-memory store for RDS resources.
type InMemoryBackend struct {
	dnsRegistrar DNSRegistrar
	instances    map[string]*DBInstance
	snapshots    map[string]*DBSnapshot
	subnetGroups map[string]*DBSubnetGroup
	tags         map[string][]Tag
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		instances:    make(map[string]*DBInstance),
		snapshots:    make(map[string]*DBSnapshot),
		subnetGroups: make(map[string]*DBSubnetGroup),
		tags:         make(map[string][]Tag),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("rds"),
	}
}

// SetDNSRegistrar wires a DNS server so RDS instance hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = dns
	b.mu.Unlock()
}

// enginePort returns the default port for the given database engine.
func enginePort(engine string) int {
	switch engine {
	case "mysql", "mariadb", "aurora-mysql":
		return mysqlPort
	default:
		return defaultPort
	}
}

// CreateDBInstance creates a new RDS DB instance.
func (b *InMemoryBackend) CreateDBInstance(
	id, engine, instanceClass, dbName, masterUser string,
	allocatedStorage int,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()

	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	if engine == "" {
		engine = "postgres"
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	if allocatedStorage <= 0 {
		allocatedStorage = defaultAllocatedStorage
	}
	if masterUser == "" {
		masterUser = "admin"
	}

	port := enginePort(engine)
	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)

	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DbiResourceID:        id,
		DBInstanceClass:      instanceClass,
		Engine:               engine,
		DBInstanceStatus:     "available",
		MasterUsername:       masterUser,
		DBName:               dbName,
		Endpoint:             endpoint,
		Port:                 port,
		AllocatedStorage:     allocatedStorage,
	}
	b.instances[id] = inst

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *inst

	return &cp, nil
}

// DeleteDBInstance removes the DB instance with the given identifier.
func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	cp := *inst
	delete(b.instances, id)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeDBInstances returns instances. If id is non-empty, returns only that instance.
func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()

	if id != "" {
		inst, exists := b.instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}

		return []DBInstance{*inst}, nil
	}

	instances := make([]DBInstance, 0, len(b.instances))
	for _, inst := range b.instances {
		instances = append(instances, *inst)
	}

	return instances, nil
}

// ModifyDBInstance modifies properties of an existing DB instance.
func (b *InMemoryBackend) ModifyDBInstance(id, instanceClass string, allocatedStorage int) (*DBInstance, error) {
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	if allocatedStorage > 0 {
		inst.AllocatedStorage = allocatedStorage
	}

	cp := *inst

	return &cp, nil
}

// CreateDBSnapshot creates a snapshot of the given DB instance.
func (b *InMemoryBackend) CreateDBSnapshot(snapshotID, instanceID string) (*DBSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	if instanceID == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBSnapshot")
	defer b.mu.Unlock()

	if _, exists := b.snapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, snapshotID)
	}

	inst, exists := b.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	snap := &DBSnapshot{
		DBSnapshotIdentifier: snapshotID,
		DBInstanceIdentifier: instanceID,
		Engine:               inst.Engine,
		Status:               "available",
	}
	b.snapshots[snapshotID] = snap

	cp := *snap

	return &cp, nil
}

// DescribeDBSnapshots returns snapshots. If snapshotID is non-empty, returns only that snapshot.
func (b *InMemoryBackend) DescribeDBSnapshots(snapshotID string) ([]DBSnapshot, error) {
	b.mu.RLock("DescribeDBSnapshots")
	defer b.mu.RUnlock()

	if snapshotID != "" {
		snap, exists := b.snapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
		}

		return []DBSnapshot{*snap}, nil
	}

	snaps := make([]DBSnapshot, 0, len(b.snapshots))
	for _, snap := range b.snapshots {
		snaps = append(snaps, *snap)
	}

	return snaps, nil
}

// DeleteDBSnapshot removes the given snapshot.
func (b *InMemoryBackend) DeleteDBSnapshot(snapshotID string) (*DBSnapshot, error) {
	b.mu.Lock("DeleteDBSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	cp := *snap
	delete(b.snapshots, snapshotID)

	return &cp, nil
}

// CreateDBSubnetGroup creates a DB subnet group.
func (b *InMemoryBackend) CreateDBSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName must not be empty", ErrInvalidParameter)
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
		SubnetIDs:                ids,
		Status:                   "Complete",
	}
	b.subnetGroups[name] = sg

	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)

	return &cp, nil
}

// DescribeDBSubnetGroups returns subnet groups. If name is non-empty, returns only that group.
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

	sgs := make([]DBSubnetGroup, 0, len(b.subnetGroups))

	for _, sg := range b.subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		sgs = append(sgs, cp)
	}

	return sgs, nil
}

// DeleteDBSubnetGroup removes the given subnet group.
func (b *InMemoryBackend) DeleteDBSubnetGroup(name string) error {
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}

	delete(b.subnetGroups, name)

	return nil
}

// AddTagsToResource adds or overwrites tags on the resource identified by arn.
func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	current := b.tags[arn]
	// Build an index for O(1) key lookup.
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

// RemoveTagsFromResource removes the named tags from the resource identified by arn.
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

// ListTagsForResource returns the tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp
}
