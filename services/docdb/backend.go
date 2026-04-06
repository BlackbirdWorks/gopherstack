package docdb

import (
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	ErrClusterNotFound                    = awserr.New("DBClusterNotFoundFault", awserr.ErrNotFound)
	ErrClusterAlreadyExists               = awserr.New("DBClusterAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrInstanceNotFound                   = awserr.New("DBInstanceNotFound", awserr.ErrNotFound)
	ErrInstanceAlreadyExists              = awserr.New("DBInstanceAlreadyExists", awserr.ErrAlreadyExists)
	ErrSubnetGroupNotFound                = awserr.New("DBSubnetGroupNotFoundFault", awserr.ErrNotFound)
	ErrSubnetGroupAlreadyExists           = awserr.New("DBSubnetGroupAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrClusterParameterGroupNotFound      = awserr.New("DBClusterParameterGroupNotFoundFault", awserr.ErrNotFound)
	ErrClusterParameterGroupAlreadyExists = awserr.New(
		"DBClusterParameterGroupAlreadyExistsFault",
		awserr.ErrAlreadyExists,
	)
	ErrClusterSnapshotNotFound        = awserr.New("DBClusterSnapshotNotFoundFault", awserr.ErrNotFound)
	ErrClusterSnapshotAlreadyExists   = awserr.New("DBClusterSnapshotAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrEventSubscriptionNotFound      = awserr.New("SubscriptionNotFoundFault", awserr.ErrNotFound)
	ErrEventSubscriptionAlreadyExists = awserr.New("SubscriptionAlreadyExistFault", awserr.ErrAlreadyExists)
	ErrGlobalClusterNotFound          = awserr.New("GlobalClusterNotFoundFault", awserr.ErrNotFound)
	ErrGlobalClusterAlreadyExists     = awserr.New("GlobalClusterAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrInvalidParameter               = awserr.New("InvalidParameterValue", awserr.ErrInvalidParameter)
	ErrUnknownAction                  = awserr.New("InvalidAction", awserr.ErrInvalidParameter)
)

const (
	defaultDocDBPort     = 27017
	defaultInstanceClass = "db.t3.medium"
	defaultEngineVersion = "4.0.0"
	docDBEngine          = "docdb"

	// OptInType constants for ApplyPendingMaintenanceAction.
	optInTypeImmediate       = "immediate"
	optInTypeNextMaintenance = "next-maintenance"
	optInTypeUndoOptIn       = "undo-opt-in"
)

type DBCluster struct {
	Tags                        map[string]string `json:"tags"`
	DBClusterIdentifier         string            `json:"dbClusterIdentifier"`
	Engine                      string            `json:"engine"`
	Status                      string            `json:"status"`
	MasterUsername              string            `json:"masterUsername"`
	DatabaseName                string            `json:"databaseName"`
	DBClusterParameterGroupName string            `json:"dbClusterParameterGroupName"`
	Endpoint                    string            `json:"endpoint"`
	DBClusterArn                string            `json:"dbClusterArn"`
	EngineVersion               string            `json:"engineVersion"`
	Port                        int               `json:"port"`
	StorageEncrypted            bool              `json:"storageEncrypted"`
}

type DBInstance struct {
	Tags                 map[string]string `json:"tags"`
	DBInstanceIdentifier string            `json:"dbInstanceIdentifier"`
	DBClusterIdentifier  string            `json:"dbClusterIdentifier"`
	DBInstanceClass      string            `json:"dbInstanceClass"`
	Engine               string            `json:"engine"`
	DBInstanceStatus     string            `json:"dbInstanceStatus"`
	Endpoint             string            `json:"endpoint"`
	DBInstanceArn        string            `json:"dbInstanceArn"`
	EngineVersion        string            `json:"engineVersion"`
	Port                 int               `json:"port"`
	StorageEncrypted     bool              `json:"storageEncrypted"`
}

type DBSubnetGroup struct {
	Tags                     map[string]string `json:"tags"`
	DBSubnetGroupName        string            `json:"dbSubnetGroupName"`
	DBSubnetGroupDescription string            `json:"dbSubnetGroupDescription"`
	VpcID                    string            `json:"vpcID"`
	Status                   string            `json:"status"`
	SubnetIDs                []string          `json:"subnetIDs"`
}

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DBClusterParameterGroup struct {
	Tags                        map[string]string `json:"tags"`
	DBClusterParameterGroupName string            `json:"dbClusterParameterGroupName"`
	DBParameterGroupFamily      string            `json:"dbParameterGroupFamily"`
	Description                 string            `json:"description"`
}

type DBClusterSnapshot struct {
	Tags                        map[string]string `json:"tags"`
	DBClusterSnapshotIdentifier string            `json:"dbClusterSnapshotIdentifier"`
	DBClusterIdentifier         string            `json:"dbClusterIdentifier"`
	Engine                      string            `json:"engine"`
	Status                      string            `json:"status"`
	EngineVersion               string            `json:"engineVersion"`
	StorageEncrypted            bool              `json:"storageEncrypted"`
}

type EventSubscription struct {
	SubscriptionName string   `json:"subscriptionName"`
	SnsTopicARN      string   `json:"snsTopicARN"`
	Status           string   `json:"status"`
	SourceIDs        []string `json:"sourceIDs"`
}

type GlobalCluster struct {
	GlobalClusterIdentifier string `json:"globalClusterIdentifier"`
	SourceDBClusterID       string `json:"sourceDBClusterID"`
	Status                  string `json:"status"`
}

type Certificate struct {
	CertificateIdentifier string
	CertificateType       string
	Thumbprint            string
	ValidFrom             string
	ValidTill             string
}

type DBClusterParameter struct {
	ParameterName  string
	ParameterValue string
	Description    string
	Source         string
	ApplyType      string
	DataType       string
	IsModifiable   bool
}

type InMemoryBackend struct {
	clusters               map[string]*DBCluster
	instances              map[string]*DBInstance
	subnetGroups           map[string]*DBSubnetGroup
	clusterParameterGroups map[string]*DBClusterParameterGroup
	clusterSnapshots       map[string]*DBClusterSnapshot
	eventSubscriptions     map[string]*EventSubscription
	globalClusters         map[string]*GlobalCluster
	tags                   map[string][]Tag
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
		eventSubscriptions:     make(map[string]*EventSubscription),
		globalClusters:         make(map[string]*GlobalCluster),
		tags:                   make(map[string][]Tag),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("docdb"),
	}
}

// Reset clears all stored state, returning the backend to an empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*DBCluster)
	b.instances = make(map[string]*DBInstance)
	b.subnetGroups = make(map[string]*DBSubnetGroup)
	b.clusterParameterGroups = make(map[string]*DBClusterParameterGroup)
	b.clusterSnapshots = make(map[string]*DBClusterSnapshot)
	b.eventSubscriptions = make(map[string]*EventSubscription)
	b.globalClusters = make(map[string]*GlobalCluster)
	b.tags = make(map[string][]Tag)
}

func (b *InMemoryBackend) Region() string { return b.region }

// clusterARN returns the ARN for a DB cluster.
func (b *InMemoryBackend) clusterARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster:"+id)
}

// instanceARN returns the ARN for a DB instance.
func (b *InMemoryBackend) instanceARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "db:"+id)
}

// subnetGroupARN returns the ARN for a DB subnet group.
func (b *InMemoryBackend) subnetGroupARN(name string) string {
	return arn.Build("rds", b.region, b.accountID, "subgrp:"+name)
}

// clusterParameterGroupARN returns the ARN for a DB cluster parameter group.
func (b *InMemoryBackend) clusterParameterGroupARN(name string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster-pg:"+name)
}

// clusterSnapshotARN returns the ARN for a DB cluster snapshot.
func (b *InMemoryBackend) clusterSnapshotARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster-snapshot:"+id)
}

func (b *InMemoryBackend) CreateDBCluster(
	id, engine, masterUser, dbName, paramGroupName string,
	port int,
	tags map[string]string,
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
	clusterArn := b.clusterARN(id)
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
		DBClusterArn:                clusterArn,
		EngineVersion:               defaultEngineVersion,
		Tags:                        copyTags(tags),
	}
	b.clusters[id] = cluster
	if len(tags) > 0 {
		b.tags[clusterArn] = tagsFromMap(tags)
	}

	return copyCluster(cluster), nil
}

func (b *InMemoryBackend) DescribeDBClusters(id string) ([]DBCluster, error) {
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []DBCluster{*copyCluster(c)}, nil
	}
	result := make([]DBCluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		result = append(result, *copyCluster(c))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterIdentifier < result[j].DBClusterIdentifier
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBCluster(id string) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := copyCluster(c)
	delete(b.clusters, id)
	delete(b.tags, b.clusterARN(id))

	return cp, nil
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

	return copyCluster(c), nil
}

func (b *InMemoryBackend) StopDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	c.Status = "stopped"

	return copyCluster(c), nil
}

func (b *InMemoryBackend) StartDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	c.Status = "available"

	return copyCluster(c), nil
}

func (b *InMemoryBackend) FailoverDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	return copyCluster(c), nil
}

func (b *InMemoryBackend) CreateDBInstance(
	id, clusterID, instanceClass, engine string,
	tags map[string]string,
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
		engine = docDBEngine
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	instanceArn := b.instanceARN(id)
	endpoint := fmt.Sprintf("%s.docdb.%s.amazonaws.com", id, b.region)
	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DBClusterIdentifier:  clusterID,
		DBInstanceClass:      instanceClass,
		Engine:               engine,
		DBInstanceStatus:     "available",
		Endpoint:             endpoint,
		Port:                 defaultDocDBPort,
		DBInstanceArn:        instanceArn,
		EngineVersion:        defaultEngineVersion,
		Tags:                 copyTags(tags),
	}
	b.instances[id] = inst
	if len(tags) > 0 {
		b.tags[instanceArn] = tagsFromMap(tags)
	}

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	if id != "" {
		inst, exists := b.instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}

		return []DBInstance{*copyInstance(inst)}, nil
	}
	result := make([]DBInstance, 0, len(b.instances))
	for _, inst := range b.instances {
		result = append(result, *copyInstance(inst))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBInstanceIdentifier < result[j].DBInstanceIdentifier
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := copyInstance(inst)
	delete(b.instances, id)
	delete(b.tags, b.instanceARN(id))

	return cp, nil
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

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) CreateDBSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
	tags map[string]string,
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
		Tags:                     copyTags(tags),
	}
	b.subnetGroups[name] = sg
	sgArn := b.subnetGroupARN(name)
	if len(tags) > 0 {
		b.tags[sgArn] = tagsFromMap(tags)
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)
	cp.Tags = copyTags(sg.Tags)

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
		cp.Tags = copyTags(sg.Tags)

		return []DBSubnetGroup{cp}, nil
	}
	result := make([]DBSubnetGroup, 0, len(b.subnetGroups))
	for _, sg := range b.subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		cp.Tags = copyTags(sg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBSubnetGroupName < result[j].DBSubnetGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBSubnetGroup(name string) error {
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	delete(b.subnetGroups, name)
	delete(b.tags, b.subnetGroupARN(name))

	return nil
}

func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	name, family, description string,
	tags map[string]string,
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
		Tags:                        copyTags(tags),
	}
	b.clusterParameterGroups[name] = pg
	pgArn := b.clusterParameterGroupARN(name)
	if len(tags) > 0 {
		b.tags[pgArn] = tagsFromMap(tags)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

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
		cp.Tags = copyTags(pg.Tags)

		return []DBClusterParameterGroup{cp}, nil
	}
	result := make([]DBClusterParameterGroup, 0, len(b.clusterParameterGroups))
	for _, pg := range b.clusterParameterGroups {
		cp := *pg
		cp.Tags = copyTags(pg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterParameterGroupName < result[j].DBClusterParameterGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterParameterGroup(name string) error {
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	delete(b.clusterParameterGroups, name)
	delete(b.tags, b.clusterParameterGroupARN(name))

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
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) CreateDBClusterSnapshot(
	snapshotID, clusterID string,
	tags map[string]string,
) (*DBClusterSnapshot, error) {
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
		EngineVersion:               c.EngineVersion,
		StorageEncrypted:            c.StorageEncrypted,
		Tags:                        copyTags(tags),
	}
	b.clusterSnapshots[snapshotID] = snap
	snapArn := b.clusterSnapshotARN(snapshotID)
	if len(tags) > 0 {
		b.tags[snapArn] = tagsFromMap(tags)
	}
	cp := *snap
	cp.Tags = copyTags(snap.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterSnapshots(snapshotID, clusterID string) ([]DBClusterSnapshot, error) {
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap
		cp.Tags = copyTags(snap.Tags)

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, len(b.clusterSnapshots))
	for _, snap := range b.clusterSnapshots {
		if clusterID != "" && snap.DBClusterIdentifier != clusterID {
			continue
		}
		cp := *snap
		cp.Tags = copyTags(snap.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterSnapshotIdentifier < result[j].DBClusterSnapshotIdentifier
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	cp.Tags = copyTags(snap.Tags)
	delete(b.clusterSnapshots, snapshotID)
	delete(b.tags, b.clusterSnapshotARN(snapshotID))

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
	kept := make([]Tag, 0, len(current))
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
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].Key < cp[j].Key
	})

	return cp
}

// AddSourceIdentifierToSubscription adds a source identifier to an event subscription.
func (b *InMemoryBackend) AddSourceIdentifierToSubscription(
	subscriptionName, sourceID string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("AddSourceIdentifierToSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[subscriptionName]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, subscriptionName)
	}
	if slices.Contains(sub.SourceIDs, sourceID) {
		return copyEventSubscription(sub), nil
	}
	sub.SourceIDs = append(sub.SourceIDs, sourceID)

	return copyEventSubscription(sub), nil
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	resourceARN, action, optInType string,
) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceIdentifier is required", ErrInvalidParameter)
	}
	if action == "" {
		return fmt.Errorf("%w: ApplyAction is required", ErrInvalidParameter)
	}
	if optInType == "" {
		return fmt.Errorf("%w: OptInType is required", ErrInvalidParameter)
	}
	switch optInType {
	case optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn:
		// valid
	default:
		return fmt.Errorf(
			"%w: OptInType must be one of %s, %s, %s",
			ErrInvalidParameter,
			optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn,
		)
	}

	return nil
}

// CopyDBClusterParameterGroup copies a DB cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	sourceGroupName, targetName, targetDescription string,
) (*DBClusterParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	if targetName == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()
	src, exists := b.clusterParameterGroups[sourceGroupName]
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			sourceGroupName,
		)
	}
	if _, ok := b.clusterParameterGroups[targetName]; ok {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			targetName,
		)
	}
	desc := targetDescription
	if desc == "" {
		desc = src.Description
	}
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: targetName,
		DBParameterGroupFamily:      src.DBParameterGroupFamily,
		Description:                 desc,
	}
	b.clusterParameterGroups[targetName] = pg
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

// CopyDBClusterSnapshot copies a DB cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(
	sourceSnapshotID, targetSnapshotID string,
) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	src, exists := b.clusterSnapshots[sourceSnapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	if _, ok := b.clusterSnapshots[targetSnapshotID]; ok {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterIdentifier:         src.DBClusterIdentifier,
		Engine:                      src.Engine,
		Status:                      "available",
		EngineVersion:               src.EngineVersion,
		StorageEncrypted:            src.StorageEncrypted,
	}
	b.clusterSnapshots[targetSnapshotID] = snap
	cp := *snap
	cp.Tags = copyTags(snap.Tags)

	return &cp, nil
}

// CreateEventSubscription creates an event subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	name, snsTopicARN string,
	sourceIDs []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()
	if _, exists := b.eventSubscriptions[name]; exists {
		return nil, fmt.Errorf("%w: subscription %s already exists", ErrEventSubscriptionAlreadyExists, name)
	}
	ids := make([]string, len(sourceIDs))
	copy(ids, sourceIDs)
	sub := &EventSubscription{
		SubscriptionName: name,
		SnsTopicARN:      snsTopicARN,
		Status:           "active",
		SourceIDs:        ids,
	}
	b.eventSubscriptions[name] = sub

	return copyEventSubscription(sub), nil
}

// CreateGlobalCluster creates a global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
	id, sourceDBClusterID string,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()
	if _, exists := b.globalClusters[id]; exists {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, id)
	}
	gc := &GlobalCluster{
		GlobalClusterIdentifier: id,
		SourceDBClusterID:       sourceDBClusterID,
		Status:                  "available",
	}
	b.globalClusters[id] = gc
	cp := *gc

	return &cp, nil
}

// DeleteEventSubscription deletes an event subscription.
func (b *InMemoryBackend) DeleteEventSubscription(name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	cp := copyEventSubscription(sub)
	delete(b.eventSubscriptions, name)

	return cp, nil
}

// DeleteGlobalCluster deletes a global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(id string) (*GlobalCluster, error) {
	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	cp := *gc
	delete(b.globalClusters, id)

	return &cp, nil
}

// DescribeCertificates returns certificate information.
func (b *InMemoryBackend) DescribeCertificates(certificateID string) []Certificate {
	certs := []Certificate{
		{
			CertificateIdentifier: "rds-ca-2019",
			CertificateType:       "CA",
			Thumbprint:            "d404926ab3b1c6f0ad61f8d95dadf6c3eea47dbf",
			ValidFrom:             "2019-09-19T00:00:00Z",
			ValidTill:             "2024-08-22T00:00:00Z",
		},
		{
			CertificateIdentifier: "rds-ca-rsa2048-g1",
			CertificateType:       "CA",
			Thumbprint:            "cf5c7c1cf32cae39012fc84c8d9e76c25bce55fb",
			ValidFrom:             "2021-05-25T00:00:00Z",
			ValidTill:             "2061-05-25T00:00:00Z",
		},
	}
	if certificateID == "" {
		return certs
	}
	for _, c := range certs {
		if c.CertificateIdentifier == certificateID {
			return []Certificate{c}
		}
	}

	return []Certificate{}
}

// DescribeDBClusterParameters returns the parameters for a DB cluster parameter group.
func (b *InMemoryBackend) DescribeDBClusterParameters(groupName string) ([]DBClusterParameter, error) {
	b.mu.RLock("DescribeDBClusterParameters")
	defer b.mu.RUnlock()
	if groupName == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	if _, exists := b.clusterParameterGroups[groupName]; !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, groupName)
	}
	params := []DBClusterParameter{
		{
			ParameterName:  "tls",
			ParameterValue: "enabled",
			Description:    "Specifies the TLS setting",
			Source:         "system",
			ApplyType:      "static",
			DataType:       "string",
			IsModifiable:   true,
		},
		{
			ParameterName:  "ttl_monitor",
			ParameterValue: "enabled",
			Description:    "Specifies the TTL monitor setting",
			Source:         "system",
			ApplyType:      "dynamic",
			DataType:       "string",
			IsModifiable:   true,
		},
	}

	return params, nil
}

// DescribeGlobalClusters returns global clusters, optionally filtered by ID, sorted by identifier.
func (b *InMemoryBackend) DescribeGlobalClusters(id string) []GlobalCluster {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()
	if id != "" {
		gc, exists := b.globalClusters[id]
		if !exists {
			return []GlobalCluster{}
		}
		cp := *gc

		return []GlobalCluster{cp}
	}
	result := make([]GlobalCluster, 0, len(b.globalClusters))
	for _, gc := range b.globalClusters {
		result = append(result, *gc)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GlobalClusterIdentifier < result[j].GlobalClusterIdentifier
	})

	return result
}

// AddDBClusterInternal seeds a cluster directly for testing.
func (b *InMemoryBackend) AddDBClusterInternal(cluster *DBCluster) {
	b.mu.Lock("AddDBClusterInternal")
	defer b.mu.Unlock()
	b.clusters[cluster.DBClusterIdentifier] = cluster
}

// AddDBInstanceInternal seeds an instance directly for testing.
func (b *InMemoryBackend) AddDBInstanceInternal(inst *DBInstance) {
	b.mu.Lock("AddDBInstanceInternal")
	defer b.mu.Unlock()
	b.instances[inst.DBInstanceIdentifier] = inst
}

// AddDBSubnetGroupInternal seeds a subnet group directly for testing.
func (b *InMemoryBackend) AddDBSubnetGroupInternal(sg *DBSubnetGroup) {
	b.mu.Lock("AddDBSubnetGroupInternal")
	defer b.mu.Unlock()
	b.subnetGroups[sg.DBSubnetGroupName] = sg
}

// AddDBClusterParameterGroupInternal seeds a parameter group directly for testing.
func (b *InMemoryBackend) AddDBClusterParameterGroupInternal(pg *DBClusterParameterGroup) {
	b.mu.Lock("AddDBClusterParameterGroupInternal")
	defer b.mu.Unlock()
	b.clusterParameterGroups[pg.DBClusterParameterGroupName] = pg
}

// AddDBClusterSnapshotInternal seeds a snapshot directly for testing.
func (b *InMemoryBackend) AddDBClusterSnapshotInternal(snap *DBClusterSnapshot) {
	b.mu.Lock("AddDBClusterSnapshotInternal")
	defer b.mu.Unlock()
	b.clusterSnapshots[snap.DBClusterSnapshotIdentifier] = snap
}

// AddEventSubscriptionInternal seeds an event subscription directly for testing.
func (b *InMemoryBackend) AddEventSubscriptionInternal(sub *EventSubscription) {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	b.eventSubscriptions[sub.SubscriptionName] = sub
}

// AddGlobalClusterInternal seeds a global cluster directly for testing.
func (b *InMemoryBackend) AddGlobalClusterInternal(gc *GlobalCluster) {
	b.mu.Lock("AddGlobalClusterInternal")
	defer b.mu.Unlock()
	b.globalClusters[gc.GlobalClusterIdentifier] = gc
}

// copyCluster returns a deep copy of a DBCluster.
func copyCluster(c *DBCluster) *DBCluster {
	cp := *c
	cp.Tags = copyTags(c.Tags)

	return &cp
}

// copyInstance returns a deep copy of a DBInstance.
func copyInstance(inst *DBInstance) *DBInstance {
	cp := *inst
	cp.Tags = copyTags(inst.Tags)

	return &cp
}

// copyEventSubscription returns a deep copy of an EventSubscription.
func copyEventSubscription(sub *EventSubscription) *EventSubscription {
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp
}

// copyTags returns a deep copy of a string map (tags).
func copyTags(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)

	return dst
}

// tagsFromMap converts a map[string]string to []Tag.
func tagsFromMap(m map[string]string) []Tag {
	tags := make([]Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, Tag{Key: k, Value: v})
	}

	return tags
}
