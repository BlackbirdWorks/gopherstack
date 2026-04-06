package docdb

import (
	"errors"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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
	ErrEventSubscriptionNotFound          = errors.New("SubscriptionNotFoundFault")
	ErrEventSubscriptionAlreadyExists     = errors.New("SubscriptionAlreadyExistFault")
	ErrGlobalClusterNotFound              = errors.New("GlobalClusterNotFoundFault")
	ErrGlobalClusterAlreadyExists         = errors.New("GlobalClusterAlreadyExistsFault")
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

type EventSubscription struct {
	SubscriptionName string
	SnsTopicARN      string
	Status           string
	SourceIDs        []string
}

type GlobalCluster struct {
	GlobalClusterIdentifier string
	SourceDBClusterID       string
	Status                  string
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
	delete(b.tags, b.clusterARN(id))

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
	delete(b.tags, b.instanceARN(id))

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
	delete(b.tags, b.subnetGroupARN(name))

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
		cp := *sub
		cp.SourceIDs = make([]string, len(sub.SourceIDs))
		copy(cp.SourceIDs, sub.SourceIDs)

		return &cp, nil
	}
	sub.SourceIDs = append(sub.SourceIDs, sourceID)
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
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
	}
	b.clusterSnapshots[targetSnapshotID] = snap
	cp := *snap

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
	cp := *sub
	cp.SourceIDs = make([]string, len(ids))
	copy(cp.SourceIDs, ids)

	return &cp, nil
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
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)
	delete(b.eventSubscriptions, name)

	return &cp, nil
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
