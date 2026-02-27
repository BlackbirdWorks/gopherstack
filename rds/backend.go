package rds

import (
	"errors"
	"fmt"
	"sync"
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
	DBInstanceIdentifier string
	DBInstanceClass      string
	Engine               string
	DBInstanceStatus     string
	MasterUsername       string
	DBName               string
	Endpoint             string
	VpcID                string
	DBSubnetGroupName    string
	Port                 int
	AllocatedStorage     int
}

// DBSnapshot represents an RDS database snapshot.
type DBSnapshot struct {
	DBSnapshotIdentifier string
	DBInstanceIdentifier string
	Engine               string
	Status               string
}

// DBSubnetGroup represents an RDS DB subnet group.
type DBSubnetGroup struct {
	DBSubnetGroupName        string
	DBSubnetGroupDescription string
	VpcID                    string
	Status                   string
	SubnetIDs                []string
}

// InMemoryBackend is the in-memory store for RDS resources.
type InMemoryBackend struct {
	instances    map[string]*DBInstance
	snapshots    map[string]*DBSnapshot
	subnetGroups map[string]*DBSubnetGroup
	accountID    string
	region       string
	mu           sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		instances:    make(map[string]*DBInstance),
		snapshots:    make(map[string]*DBSnapshot),
		subnetGroups: make(map[string]*DBSubnetGroup),
		accountID:    accountID,
		region:       region,
	}
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

	b.mu.Lock()
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

	cp := *inst

	return &cp, nil
}

// DeleteDBInstance removes the DB instance with the given identifier.
func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	cp := *inst
	delete(b.instances, id)

	return &cp, nil
}

// DescribeDBInstances returns instances. If id is non-empty, returns only that instance.
func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	b.mu.RLock()
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
	b.mu.Lock()
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

	b.mu.Lock()
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
	b.mu.RLock()
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
	b.mu.Lock()
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

	b.mu.Lock()
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
	b.mu.RLock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}

	delete(b.subnetGroups, name)

	return nil
}
