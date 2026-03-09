package rds

import (
	"errors"
	"fmt"
	"maps"
	"time"

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

	ErrParameterGroupNotFound       = errors.New("DBParameterGroupNotFound")
	ErrParameterGroupAlreadyExists  = errors.New("DBParameterGroupAlreadyExists")
	ErrOptionGroupNotFound          = errors.New("OptionGroupNotFound")
	ErrOptionGroupAlreadyExists     = errors.New("OptionGroupAlreadyExists")
	ErrClusterNotFound              = errors.New("DBClusterNotFound")
	ErrClusterAlreadyExists         = errors.New("DBClusterAlreadyExists")
	ErrClusterSnapshotNotFound      = errors.New("DBClusterSnapshotNotFound")
	ErrClusterSnapshotAlreadyExists = errors.New("DBClusterSnapshotAlreadyExists")
)

const (
	defaultPort             = 5432
	mysqlPort               = 3306
	defaultInstanceClass    = "db.t3.micro"
	defaultAllocatedStorage = 20
)

// DBInstance represents an RDS database instance.
type DBInstance struct {
	DBInstanceIdentifier              string `json:"dbInstanceIdentifier"`
	DbiResourceID                     string `json:"dbiResourceID"`
	DBInstanceClass                   string `json:"dbInstanceClass"`
	Engine                            string `json:"engine"`
	DBInstanceStatus                  string `json:"dbInstanceStatus"`
	MasterUsername                    string `json:"masterUsername"`
	DBName                            string `json:"dbName"`
	Endpoint                          string `json:"endpoint"`
	VpcID                             string `json:"vpcID"`
	DBSubnetGroupName                 string `json:"dbSubnetGroupName"`
	DBParameterGroupName              string `json:"dbParameterGroupName"`
	ReplicaSourceDBInstanceIdentifier string `json:"replicaSourceDBInstanceIdentifier"`
	Port                              int    `json:"port"`
	AllocatedStorage                  int    `json:"allocatedStorage"`
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

// DBParameter represents a single RDS parameter.
type DBParameter struct {
	ParameterName  string `json:"parameterName"`
	ParameterValue string `json:"parameterValue"`
	Description    string `json:"description"`
	ApplyType      string `json:"applyType"`
	DataType       string `json:"dataType"`
	IsModifiable   bool   `json:"isModifiable"`
}

// DBParameterGroup represents an RDS DB parameter group.
type DBParameterGroup struct {
	Parameters             map[string]DBParameter `json:"parameters"`
	DBParameterGroupName   string                 `json:"dbParameterGroupName"`
	DBParameterGroupFamily string                 `json:"dbParameterGroupFamily"`
	Description            string                 `json:"description"`
}

// OptionGroupOption represents an option within an option group.
type OptionGroupOption struct {
	OptionName    string `json:"optionName"`
	OptionVersion string `json:"optionVersion"`
}

// OptionGroup represents an RDS option group.
type OptionGroup struct {
	OptionGroupName        string              `json:"optionGroupName"`
	OptionGroupDescription string              `json:"optionGroupDescription"`
	EngineName             string              `json:"engineName"`
	MajorEngineVersion     string              `json:"majorEngineVersion"`
	Options                []OptionGroupOption `json:"options"`
}

// DBCluster represents an Aurora-style RDS cluster.
type DBCluster struct {
	DBClusterIdentifier         string `json:"dbClusterIdentifier"`
	Engine                      string `json:"engine"`
	Status                      string `json:"status"`
	MasterUsername              string `json:"masterUsername"`
	DatabaseName                string `json:"databaseName"`
	DBClusterParameterGroupName string `json:"dbClusterParameterGroupName"`
	Endpoint                    string `json:"endpoint"`
	Port                        int    `json:"port"`
}

// DBClusterSnapshot represents an RDS cluster snapshot.
type DBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `json:"dbClusterSnapshotIdentifier"`
	DBClusterIdentifier         string `json:"dbClusterIdentifier"`
	Engine                      string `json:"engine"`
	Status                      string `json:"status"`
}

// DBEngineVersion represents an available RDS engine version.
type DBEngineVersion struct {
	Engine              string `json:"engine"`
	EngineVersion       string `json:"engineVersion"`
	DBEngineDescription string `json:"dbEngineDescription"`
}

// OrderableDBInstanceOption represents an orderable DB instance option.
type OrderableDBInstanceOption struct {
	Engine          string `json:"engine"`
	EngineVersion   string `json:"engineVersion"`
	DBInstanceClass string `json:"dbInstanceClass"`
	MultiAZCapable  bool   `json:"multiAZCapable"`
}

// DBLogFile represents a log file for a DB instance.
type DBLogFile struct {
	LogFileName string `json:"logFileName"`
	Size        int64  `json:"size"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// InMemoryBackend is the in-memory store for RDS resources.
type InMemoryBackend struct {
	dnsRegistrar           DNSRegistrar
	instances              map[string]*DBInstance
	snapshots              map[string]*DBSnapshot
	subnetGroups           map[string]*DBSubnetGroup
	tags                   map[string][]Tag
	parameterGroups        map[string]*DBParameterGroup
	clusterParameterGroups map[string]*DBParameterGroup
	optionGroups           map[string]*OptionGroup
	clusters               map[string]*DBCluster
	clusterSnapshots       map[string]*DBClusterSnapshot
	fisFailoverFaults      map[string]time.Time // keyed by cluster identifier; value is expiry (zero = permanent)
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		instances:              make(map[string]*DBInstance),
		snapshots:              make(map[string]*DBSnapshot),
		subnetGroups:           make(map[string]*DBSubnetGroup),
		tags:                   make(map[string][]Tag),
		parameterGroups:        make(map[string]*DBParameterGroup),
		clusterParameterGroups: make(map[string]*DBParameterGroup),
		optionGroups:           make(map[string]*OptionGroup),
		clusters:               make(map[string]*DBCluster),
		clusterSnapshots:       make(map[string]*DBClusterSnapshot),
		fisFailoverFaults:      make(map[string]time.Time),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("rds"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

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
	id, engine, instanceClass, dbName, masterUser, paramGroupName string,
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
		DBParameterGroupName: paramGroupName,
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

// CreateDBParameterGroup creates a new DB parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBParameterGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups[name]; exists {
		return nil, fmt.Errorf("%w: parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
		Parameters:             make(map[string]DBParameter),
	}
	b.parameterGroups[name] = pg
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// copyDBParameterGroup returns a deep copy of the given parameter group.
func copyDBParameterGroup(pg *DBParameterGroup) DBParameterGroup {
	cp := *pg
	cp.Parameters = make(map[string]DBParameter, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return cp
}

// DescribeDBParameterGroups returns parameter groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeDBParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.parameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, len(b.parameterGroups))
	for _, pg := range b.parameterGroups {
		result = append(result, copyDBParameterGroup(pg))
	}

	return result, nil
}

// DeleteDBParameterGroup removes the given parameter group.
func (b *InMemoryBackend) DeleteDBParameterGroup(name string) error {
	b.mu.Lock("DeleteDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups[name]; !exists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	delete(b.parameterGroups, name)

	return nil
}

// ModifyDBParameterGroup modifies parameters in a parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(name string, params []DBParameter) (*DBParameterGroup, error) {
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	for _, p := range params {
		pg.Parameters[p.ParameterName] = p
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// DescribeDBParameters returns parameters for a parameter group.
func (b *InMemoryBackend) DescribeDBParameters(groupName string) ([]DBParameter, error) {
	b.mu.RLock("DescribeDBParameters")
	defer b.mu.RUnlock()
	pg, exists := b.parameterGroups[groupName]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, groupName)
	}
	result := make([]DBParameter, 0, len(pg.Parameters))
	for _, p := range pg.Parameters {
		result = append(result, p)
	}

	return result, nil
}

// ResetDBParameterGroup resets parameters in a parameter group.
func (b *InMemoryBackend) ResetDBParameterGroup(
	name string,
	resetAll bool,
	params []string,
) (*DBParameterGroup, error) {
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	if resetAll {
		for k, p := range pg.Parameters {
			p.ParameterValue = ""
			pg.Parameters[k] = p
		}
	} else {
		for _, pName := range params {
			if p, ok := pg.Parameters[pName]; ok {
				p.ParameterValue = ""
				pg.Parameters[pName] = p
			}
		}
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// CreateOptionGroup creates a new option group.
func (b *InMemoryBackend) CreateOptionGroup(name, engine, majorVersion, description string) (*OptionGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: OptionGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups[name]; exists {
		return nil, fmt.Errorf("%w: option group %s already exists", ErrOptionGroupAlreadyExists, name)
	}
	og := &OptionGroup{
		OptionGroupName:        name,
		OptionGroupDescription: description,
		EngineName:             engine,
		MajorEngineVersion:     majorVersion,
		Options:                []OptionGroupOption{},
	}
	b.optionGroups[name] = og
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// DescribeOptionGroups returns option groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeOptionGroups(name string) ([]OptionGroup, error) {
	b.mu.RLock("DescribeOptionGroups")
	defer b.mu.RUnlock()
	if name != "" {
		og, exists := b.optionGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
		}
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)

		return []OptionGroup{cp}, nil
	}
	result := make([]OptionGroup, 0, len(b.optionGroups))
	for _, og := range b.optionGroups {
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)
		result = append(result, cp)
	}

	return result, nil
}

// DeleteOptionGroup removes the given option group.
func (b *InMemoryBackend) DeleteOptionGroup(name string) error {
	b.mu.Lock("DeleteOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups[name]; !exists {
		return fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	delete(b.optionGroups, name)

	return nil
}

// ModifyOptionGroup modifies an option group by adding/removing options.
func (b *InMemoryBackend) ModifyOptionGroup(
	name string,
	optionsToAdd []OptionGroupOption,
	optionsToRemove []string,
) (*OptionGroup, error) {
	b.mu.Lock("ModifyOptionGroup")
	defer b.mu.Unlock()
	og, exists := b.optionGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	removeSet := make(map[string]bool, len(optionsToRemove))
	for _, o := range optionsToRemove {
		removeSet[o] = true
	}
	kept := make([]OptionGroupOption, 0, len(og.Options))
	for _, o := range og.Options {
		if !removeSet[o.OptionName] {
			kept = append(kept, o)
		}
	}
	kept = append(kept, optionsToAdd...)
	og.Options = kept
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// CreateDBCluster creates a new DB cluster.
func (b *InMemoryBackend) CreateDBCluster(
	id, engine, masterUser, dbName, paramGroupName string,
	port int,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = "aurora-postgresql"
	}
	if paramGroupName == "" {
		paramGroupName = "default." + engine
	}
	if port <= 0 {
		port = enginePort(engine)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
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

// DescribeDBClusters returns clusters. If id is non-empty, returns only that cluster.
func (b *InMemoryBackend) DescribeDBClusters(id string) ([]DBCluster, error) {
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		cluster, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}
		cp := *cluster

		return []DBCluster{cp}, nil
	}
	result := make([]DBCluster, 0, len(b.clusters))
	for _, cluster := range b.clusters {
		result = append(result, *cluster)
	}

	return result, nil
}

// DeleteDBCluster removes the given cluster.
func (b *InMemoryBackend) DeleteDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := *cluster
	delete(b.clusters, id)

	return &cp, nil
}

// ModifyDBCluster modifies a DB cluster.
func (b *InMemoryBackend) ModifyDBCluster(id, paramGroupName string) (*DBCluster, error) {
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if paramGroupName != "" {
		cluster.DBClusterParameterGroupName = paramGroupName
	}
	cp := *cluster

	return &cp, nil
}

// CreateDBClusterParameterGroup creates a new cluster parameter group.
func (b *InMemoryBackend) CreateDBClusterParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
		Parameters:             make(map[string]DBParameter),
	}
	b.clusterParameterGroups[name] = pg
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// DescribeDBClusterParameterGroups returns cluster parameter groups.
func (b *InMemoryBackend) DescribeDBClusterParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, len(b.clusterParameterGroups))
	for _, pg := range b.clusterParameterGroups {
		result = append(result, copyDBParameterGroup(pg))
	}

	return result, nil
}

// CreateDBClusterSnapshot creates a snapshot of the given cluster.
func (b *InMemoryBackend) CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cluster, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterIdentifier:         clusterID,
		Engine:                      cluster.Engine,
		Status:                      "available",
	}
	b.clusterSnapshots[snapshotID] = snap
	cp := *snap

	return &cp, nil
}

// DescribeDBClusterSnapshots returns cluster snapshots.
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

// CreateDBInstanceReadReplica creates a read replica of the given source instance.
func (b *InMemoryBackend) CreateDBInstanceReadReplica(id, sourceID string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBInstanceReadReplica")
	defer b.mu.Unlock()
	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	source, exists := b.instances[sourceID]
	if !exists {
		return nil, fmt.Errorf("%w: source instance %s not found", ErrInstanceNotFound, sourceID)
	}
	port := source.Port
	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	replica := &DBInstance{
		DBInstanceIdentifier:              id,
		DbiResourceID:                     id,
		DBInstanceClass:                   source.DBInstanceClass,
		Engine:                            source.Engine,
		DBInstanceStatus:                  "available",
		MasterUsername:                    source.MasterUsername,
		Endpoint:                          endpoint,
		Port:                              port,
		AllocatedStorage:                  source.AllocatedStorage,
		ReplicaSourceDBInstanceIdentifier: sourceID,
	}
	b.instances[id] = replica
	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}
	cp := *replica

	return &cp, nil
}

// PromoteReadReplica promotes a read replica to a standalone instance.
func (b *InMemoryBackend) PromoteReadReplica(id string) (*DBInstance, error) {
	b.mu.Lock("PromoteReadReplica")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	inst.ReplicaSourceDBInstanceIdentifier = ""
	cp := *inst

	return &cp, nil
}

// RebootDBInstance reboots the given instance.
func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	inst.DBInstanceStatus = "available"
	cp := *inst

	return &cp, nil
}

// DescribeDBEngineVersions returns available engine versions, filtered by engine and/or version.
func (b *InMemoryBackend) DescribeDBEngineVersions(engine, engineVersion string) []DBEngineVersion {
	all := []DBEngineVersion{
		{Engine: "postgres", EngineVersion: "14.10", DBEngineDescription: "PostgreSQL 14.10"},
		{Engine: "postgres", EngineVersion: "15.5", DBEngineDescription: "PostgreSQL 15.5"},
		{Engine: "mysql", EngineVersion: "8.0.35", DBEngineDescription: "MySQL 8.0.35"},
		{Engine: "mariadb", EngineVersion: "10.6.14", DBEngineDescription: "MariaDB 10.6.14"},
		{Engine: "aurora-mysql", EngineVersion: "3.04.0", DBEngineDescription: "Aurora MySQL 3.04.0"},
		{Engine: "aurora-postgresql", EngineVersion: "14.9", DBEngineDescription: "Aurora PostgreSQL 14.9"},
		{Engine: "aurora-postgresql", EngineVersion: "15.4", DBEngineDescription: "Aurora PostgreSQL 15.4"},
	}
	if engine == "" && engineVersion == "" {
		return all
	}
	result := make([]DBEngineVersion, 0)
	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}
		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}
		result = append(result, v)
	}

	return result
}

// DescribeOrderableDBInstanceOptions returns orderable instance options for the given engine.
func (b *InMemoryBackend) DescribeOrderableDBInstanceOptions(engine, engineVersion string) []OrderableDBInstanceOption {
	classes := []string{"db.t3.micro", "db.t3.small", "db.t3.medium", "db.r5.large", "db.r5.xlarge"}
	if engine == "" {
		engine = "postgres"
	}
	versions := b.DescribeDBEngineVersions(engine, engineVersion)
	if len(versions) == 0 {
		versions = []DBEngineVersion{{Engine: engine, EngineVersion: engineVersion}}
	}
	result := make([]OrderableDBInstanceOption, 0, len(classes)*len(versions))
	for _, v := range versions {
		for _, class := range classes {
			result = append(result, OrderableDBInstanceOption{
				Engine:          v.Engine,
				EngineVersion:   v.EngineVersion,
				DBInstanceClass: class,
				MultiAZCapable:  true,
			})
		}
	}

	return result
}

// DescribeDBLogFiles returns the log files for the given instance.
func (b *InMemoryBackend) DescribeDBLogFiles(instanceID string) ([]DBLogFile, error) {
	b.mu.RLock("DescribeDBLogFiles")
	defer b.mu.RUnlock()
	if _, exists := b.instances[instanceID]; !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	return []DBLogFile{}, nil
}

// DownloadDBLogFilePortion returns log file content for the given instance.
func (b *InMemoryBackend) DownloadDBLogFilePortion(instanceID, _ string) (string, error) {
	b.mu.RLock("DownloadDBLogFilePortion")
	defer b.mu.RUnlock()
	if _, exists := b.instances[instanceID]; !exists {
		return "", fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	return "", nil
}
