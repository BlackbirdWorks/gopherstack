package timestreaminfluxdb

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	clusterNodes = 3
	maxClusters  = 10
)

// ClusterInput holds CreateDbCluster parameters.
type ClusterInput struct {
	Tags             map[string]string
	LogDelivery      *LogDelivery
	Maintenance      *Maintenance
	AllocatedStorage *int32
	Port             *int32
	Name             string
	Username         string
	Password         string
	Organization     string
	Bucket           string
	DBInstanceType   string
	DBStorageType    string
	DeploymentType   string
	NetworkType      string
	FailoverMode     string
	KMSKeyID         string
	ParameterGroupID string
	SubnetIDs        []string
	SecurityGroupIDs []string
	BackupConfigs    []BackupConfig
	Publicly         bool
}

const defaultClusterStorage = 400

func (in *ClusterInput) defaults() {
	in.DBStorageType = cmp.Or(in.DBStorageType, storageTypes()[0])
	in.DeploymentType = cmp.Or(in.DeploymentType, deployReadReplicas)
	in.NetworkType = cmp.Or(in.NetworkType, networkTypes()[0])
	in.FailoverMode = cmp.Or(in.FailoverMode, failoverMods()[0])
	in.Username = cmp.Or(in.Username, defaultUsername)
	in.Organization = cmp.Or(in.Organization, defaultOrg)
	in.Bucket = cmp.Or(in.Bucket, defaultBucket)
}

func validateClusterInput(in *ClusterInput) error {
	checks := []error{
		validateName(in.Name),
		validateCredentials(in.Username, in.Password, in.Organization, in.Bucket),
		validateNetwork(in.SubnetIDs, in.SecurityGroupIDs),
		oneOf("dbInstanceType", in.DBInstanceType, instanceTypes()),
		oneOf("dbStorageType", in.DBStorageType, storageTypes()),
		oneOf("deploymentType", in.DeploymentType, []string{deployReadReplicas}),
		oneOf("networkType", in.NetworkType, networkTypes()),
		oneOf("failoverMode", in.FailoverMode, failoverMods()),
		validateLogDelivery(in.LogDelivery),
		validateMaintenance(in.Maintenance),
		validateBackupConfigs(in.BackupConfigs),
		validateTags(in.Tags),
	}

	for _, err := range checks {
		if err != nil {
			return err
		}
	}

	if in.AllocatedStorage != nil {
		if err := validateStorage(*in.AllocatedStorage); err != nil {
			return err
		}
	}

	if in.Port != nil {
		return validatePort(*in.Port)
	}

	return nil
}

func (b *InMemoryBackend) clusterLocked(sc scope, id string) (*DBCluster, error) {
	c, ok := b.clusters.Get(id)
	if !ok || !sc.owns(c.AccountID, c.Region) || b.gone(c.Pending) {
		return nil, notFound("DB cluster", id)
	}

	return c, nil
}

func (b *InMemoryBackend) membersLocked(clusterID string) []*DBInstance {
	var out []*DBInstance

	b.instances.Range(func(i *DBInstance) bool {
		if i.ClusterID == clusterID {
			out = append(out, i)
		}

		return true
	})

	slices.SortFunc(out, func(x, y *DBInstance) int { return cmp.Compare(x.Name, y.Name) })

	return out
}

func (b *InMemoryBackend) setMembersLocked(clusterID string, p Pending, only []string) {
	for _, m := range b.membersLocked(clusterID) {
		if len(only) == 0 || slices.Contains(only, m.ID) {
			m.Pending = p
		}
	}
}

// CreateDBCluster creates a cluster and its member instances in CREATING state.
func (b *InMemoryBackend) CreateDBCluster(ctx context.Context, in ClusterInput) (string, string, error) {
	in.defaults()

	if err := validateClusterInput(&in); err != nil {
		return "", "", err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateDbCluster")
	defer b.mu.Unlock()

	b.purgeLocked()

	if err := b.checkSubnets(sc, in.SubnetIDs); err != nil {
		return "", "", err
	}

	if err := b.groupExistsLocked(sc, in.ParameterGroupID); err != nil {
		return "", "", err
	}

	if b.clusterNameTakenLocked(sc, in.Name) {
		return "", "", conflict("DB cluster %s already exists", in.Name)
	}

	if b.clusterCountLocked(sc) >= maxClusters {
		return "", "", quota("DB clusters per account and region", maxClusters)
	}

	c := b.buildClusterLocked(sc, &in)
	b.clusters.Put(c)

	for n := 1; n <= clusterNodes; n++ {
		b.instances.Put(b.buildMemberLocked(sc, c, n))
	}

	return c.ID, b.state(c.Pending), nil
}

func (b *InMemoryBackend) buildClusterLocked(sc scope, in *ClusterInput) *DBCluster {
	id := newID()
	p := Placement{
		LogDelivery: in.LogDelivery, Maintenance: normalizeMaintenance(in.Maintenance),
		DBInstanceType: in.DBInstanceType, DBStorageType: in.DBStorageType, DeploymentType: in.DeploymentType,
		NetworkType: in.NetworkType, KMSKeyID: in.KMSKeyID, ParameterGroupID: in.ParameterGroupID,
		SubnetIDs: slices.Clone(in.SubnetIDs), SecurityGroupIDs: slices.Clone(in.SecurityGroupIDs),
		BackupConfigs: slices.Clone(in.BackupConfigs), AllocatedStorage: defaultClusterStorage, Port: defaultPort,
		PubliclyAccessible: in.Publicly,
	}

	if in.AllocatedStorage != nil {
		p.AllocatedStorage = *in.AllocatedStorage
	}

	if in.Port != nil {
		p.Port = *in.Port
	}

	one, two := int32(1), int32(clusterNodes-1)
	no := false

	return &DBCluster{
		ID: id, Name: in.Name, ARN: sc.arn("db-cluster/" + id), AccountID: sc.account, Region: sc.region,
		Username: in.Username, Organization: in.Organization, Bucket: in.Bucket, Password: in.Password,
		EngineType: engineV2, FailoverMode: in.FailoverMode, Tags: maps.Clone(in.Tags), CreatedAt: b.now(),
		Config:    &ClusterConfig{DedicatedCompactor: &no, IngestQueryInstances: &one, QueryOnlyInstances: &two},
		Placement: p, Pending: b.pend(statusCreating, false),
	}
}

func (b *InMemoryBackend) buildMemberLocked(sc scope, c *DBCluster, n int) *DBInstance {
	id := newID()
	mode := modeReplica

	if n == 1 {
		mode = modePrimary
	}

	p := c.Placement
	p.DeploymentType = deploySingleAZ
	p.SubnetIDs, p.SecurityGroupIDs = slices.Clone(p.SubnetIDs), slices.Clone(p.SecurityGroupIDs)

	return &DBInstance{
		ID: id, Name: fmt.Sprintf("%s-%d", c.Name, n), ARN: sc.arn("db-instance/" + id), AccountID: sc.account,
		Region: sc.region, ClusterID: c.ID, Mode: mode, Username: c.Username, Organization: c.Organization,
		Bucket: c.Bucket, AZ: sc.region + string(rune('a'+(n-1)%clusterNodes)), EngineType: c.EngineType,
		CreatedAt: b.now(), Placement: p, Pending: c.Pending,
	}
}

func (b *InMemoryBackend) clusterNameTakenLocked(sc scope, name string) bool {
	taken := false

	b.clusters.Range(func(c *DBCluster) bool {
		if sc.owns(c.AccountID, c.Region) && lowerEq(c.Name, name) && !b.gone(c.Pending) {
			taken = true
		}

		return !taken
	})

	return taken
}

func (b *InMemoryBackend) clusterCountLocked(sc scope) int {
	n := 0

	b.clusters.Range(func(c *DBCluster) bool {
		if sc.owns(c.AccountID, c.Region) && !b.gone(c.Pending) {
			n++
		}

		return true
	})

	return n
}

// GetDBCluster returns a cluster.
func (b *InMemoryBackend) GetDBCluster(ctx context.Context, id string) (wCluster, error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("GetDbCluster")
	defer b.mu.RUnlock()

	c, err := b.clusterLocked(sc, id)
	if err != nil {
		return wCluster{}, err
	}

	return b.wireCluster(sc, c), nil
}

// ListDBClusters lists clusters ordered by name.
func (b *InMemoryBackend) ListDBClusters(
	ctx context.Context, token string, maxResults int32,
) (page.Page[wClusterSummary], error) {
	if err := validateListArgs(maxResults); err != nil {
		return page.Page[wClusterSummary]{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("ListDbClusters")
	defer b.mu.RUnlock()

	var items []*DBCluster

	b.clusters.Range(func(c *DBCluster) bool {
		if sc.owns(c.AccountID, c.Region) && !b.gone(c.Pending) {
			items = append(items, c)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *DBCluster) int {
		return cmp.Or(cmp.Compare(x.Name, y.Name), cmp.Compare(x.ID, y.ID))
	})

	pg, err := paginate(items, token, maxResults)
	if err != nil {
		return page.Page[wClusterSummary]{}, err
	}

	out := page.Page[wClusterSummary]{Next: pg.Next, Data: make([]wClusterSummary, 0, len(pg.Data))}
	for _, c := range pg.Data {
		out.Data = append(out.Data, b.summarizeCluster(sc, c))
	}

	return out, nil
}

// ClusterUpdate holds UpdateDbCluster parameters.
type ClusterUpdate struct {
	LogDelivery      *LogDelivery
	Maintenance      *Maintenance
	Port             *int32
	DBInstanceType   string
	ParameterGroupID string
	FailoverMode     string
	BackupConfigs    []BackupConfig
	HasBackupConfigs bool
}

// UpdateDBCluster changes a cluster and moves it to an updating status.
func (b *InMemoryBackend) UpdateDBCluster(ctx context.Context, id string, u ClusterUpdate) (string, error) {
	for _, err := range []error{
		validateLogDelivery(u.LogDelivery), validateMaintenance(u.Maintenance), validateBackupConfigs(u.BackupConfigs),
	} {
		if err != nil {
			return "", err
		}
	}

	if u.Port != nil {
		if err := validatePort(*u.Port); err != nil {
			return "", err
		}
	}

	if u.DBInstanceType != "" {
		if err := oneOf("dbInstanceType", u.DBInstanceType, instanceTypes()); err != nil {
			return "", err
		}
	}

	if u.FailoverMode != "" {
		if err := oneOf("failoverMode", u.FailoverMode, failoverMods()); err != nil {
			return "", err
		}
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("UpdateDbCluster")
	defer b.mu.Unlock()

	b.purgeLocked()

	c, err := b.clusterLocked(sc, id)
	if err != nil {
		return "", err
	}

	if st := b.state(c.Pending); st != statusAvailable {
		return "", conflict("DB cluster %s is %s", id, st)
	}

	if ierr := b.groupExistsLocked(sc, u.ParameterGroupID); ierr != nil {
		return "", ierr
	}

	kind := statusUpdating

	if u.DBInstanceType != "" && u.DBInstanceType != c.Placement.DBInstanceType {
		kind = statusUpdatingInstanceType
		b.setMemberType(c.ID, u.DBInstanceType)
		c.Placement.DBInstanceType = u.DBInstanceType
	}

	b.applyClusterUpdate(c, u)
	c.Pending = b.pend(kind, false)
	b.setMembersLocked(id, c.Pending, nil)

	return b.state(c.Pending), nil
}

func (b *InMemoryBackend) setMemberType(clusterID, t string) {
	for _, m := range b.membersLocked(clusterID) {
		m.Placement.DBInstanceType = t
	}
}

func (b *InMemoryBackend) applyClusterUpdate(c *DBCluster, u ClusterUpdate) {
	if u.Port != nil {
		c.Placement.Port = *u.Port
	}

	if u.ParameterGroupID != "" {
		c.Placement.ParameterGroupID = u.ParameterGroupID
	}

	if u.FailoverMode != "" {
		c.FailoverMode = u.FailoverMode
	}

	if u.LogDelivery != nil {
		c.Placement.LogDelivery = u.LogDelivery
	}

	if u.Maintenance != nil {
		c.Placement.Maintenance = normalizeMaintenance(u.Maintenance)
	}

	if u.HasBackupConfigs {
		c.Placement.BackupConfigs = slices.Clone(u.BackupConfigs)
	}

	for _, m := range b.membersLocked(c.ID) {
		m.Placement.Port = c.Placement.Port
		m.Placement.ParameterGroupID = c.Placement.ParameterGroupID
	}
}

// DeleteDBCluster starts cluster deletion with its member instances.
func (b *InMemoryBackend) DeleteDBCluster(ctx context.Context, id string) (string, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteDbCluster")
	defer b.mu.Unlock()

	b.purgeLocked()

	c, err := b.clusterLocked(sc, id)
	if err != nil {
		return "", err
	}

	if st := b.state(c.Pending); st != statusAvailable && st != statusFailed {
		return "", conflict("DB cluster %s is %s", id, st)
	}

	c.Pending = b.pend(statusDeleting, false)
	b.setMembersLocked(id, c.Pending, nil)

	return statusDeleting, nil
}

// RebootDBCluster reboots a cluster, or only the listed member instances.
func (b *InMemoryBackend) RebootDBCluster(ctx context.Context, id string, instanceIDs []string) (string, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("RebootDbCluster")
	defer b.mu.Unlock()

	b.purgeLocked()

	c, err := b.clusterLocked(sc, id)
	if err != nil {
		return "", err
	}

	if st := b.state(c.Pending); st != statusAvailable {
		return "", conflict("DB cluster %s is %s", id, st)
	}

	members := b.membersLocked(id)

	for _, iid := range instanceIDs {
		if !slices.ContainsFunc(members, func(m *DBInstance) bool { return m.ID == iid }) {
			return "", invalid("instance %s is not a member of cluster %s", iid, id)
		}
	}

	c.Pending = b.pend(statusRebooting, false)
	b.setMembersLocked(id, c.Pending, instanceIDs)

	return statusRebooting, nil
}
