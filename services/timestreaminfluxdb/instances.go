package timestreaminfluxdb

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultOrg      = "organization"
	defaultBucket   = "bucket"
	defaultUsername = "admin"
	defaultWindow   = "Sun:02:00-Sun:06:00"
	defaultZone     = "UTC"
	maxInstances    = 50
)

// InstanceInput holds CreateDbInstance parameters.
type InstanceInput struct {
	Tags               map[string]string
	LogDelivery        *LogDelivery
	Maintenance        *Maintenance
	Name               string
	Username           string
	Password           string
	Organization       string
	Bucket             string
	DBInstanceType     string
	DBStorageType      string
	DeploymentType     string
	NetworkType        string
	KMSKeyID           string
	ParameterGroupID   string
	SubnetIDs          []string
	SecurityGroupIDs   []string
	BackupConfigs      []BackupConfig
	AllocatedStorage   int32
	Port               int32
	PubliclyAccessible bool
}

func (in *InstanceInput) defaults() {
	in.Port = cmpOrInt(in.Port, defaultPort)
	in.DBStorageType = cmp.Or(in.DBStorageType, storageTypes()[0])
	in.DeploymentType = cmp.Or(in.DeploymentType, deploySingleAZ)
	in.NetworkType = cmp.Or(in.NetworkType, networkTypes()[0])
	in.Username = cmp.Or(in.Username, defaultUsername)
	in.Organization = cmp.Or(in.Organization, defaultOrg)
	in.Bucket = cmp.Or(in.Bucket, defaultBucket)
}

func cmpOrInt(v, def int32) int32 {
	if v == 0 {
		return def
	}

	return v
}

func normalizeMaintenance(m *Maintenance) *Maintenance {
	if m == nil {
		return &Maintenance{Window: defaultWindow, Timezone: defaultZone}
	}

	return &Maintenance{Window: cmp.Or(m.Window, defaultWindow), Timezone: cmp.Or(m.Timezone, defaultZone)}
}

func validateInstanceInput(in *InstanceInput) error {
	checks := []error{
		validateName(in.Name),
		validateCredentials(in.Username, in.Password, in.Organization, in.Bucket),
		validateNetwork(in.SubnetIDs, in.SecurityGroupIDs),
		validateStorage(in.AllocatedStorage),
		validatePort(in.Port),
		oneOf("dbInstanceType", in.DBInstanceType, instanceTypes()),
		oneOf("dbStorageType", in.DBStorageType, storageTypes()),
		oneOf("deploymentType", in.DeploymentType, []string{deploySingleAZ, deployMultiAZ}),
		oneOf("networkType", in.NetworkType, networkTypes()),
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

	if in.Password == "" {
		return invalid("password is required")
	}

	return nil
}

func (in *InstanceInput) placement() Placement {
	return Placement{
		LogDelivery: in.LogDelivery, Maintenance: normalizeMaintenance(in.Maintenance),
		DBInstanceType: in.DBInstanceType, DBStorageType: in.DBStorageType, DeploymentType: in.DeploymentType,
		NetworkType: in.NetworkType, KMSKeyID: in.KMSKeyID, ParameterGroupID: in.ParameterGroupID,
		SubnetIDs: slices.Clone(in.SubnetIDs), SecurityGroupIDs: slices.Clone(in.SecurityGroupIDs),
		BackupConfigs: slices.Clone(in.BackupConfigs), AllocatedStorage: in.AllocatedStorage, Port: in.Port,
		PubliclyAccessible: in.PubliclyAccessible,
	}
}

func (b *InMemoryBackend) instanceLocked(sc scope, id string) (*DBInstance, error) {
	i, ok := b.instances.Get(id)
	if !ok || !sc.owns(i.AccountID, i.Region) || b.gone(i.Pending) {
		return nil, notFound("DB instance", id)
	}

	return i, nil
}

func (b *InMemoryBackend) instanceNameTakenLocked(sc scope, name string) bool {
	taken := false

	b.instances.Range(func(i *DBInstance) bool {
		if sc.owns(i.AccountID, i.Region) && lowerEq(i.Name, name) && !b.gone(i.Pending) {
			taken = true
		}

		return !taken
	})

	return taken
}

func (b *InMemoryBackend) groupExistsLocked(sc scope, id string) error {
	if id == "" {
		return nil
	}

	g, ok := b.groups.Get(id)
	if !ok || !sc.owns(g.AccountID, g.Region) {
		return notFound("DB parameter group", id)
	}

	return nil
}

func (b *InMemoryBackend) buildInstanceLocked(sc scope, in *InstanceInput, id string) *DBInstance {
	az := sc.region + "a"
	standby := ""

	if in.DeploymentType == deployMultiAZ {
		standby = sc.region + "b"
	}

	return &DBInstance{
		ID: id, Name: in.Name, ARN: sc.arn("db-instance/" + id), AccountID: sc.account, Region: sc.region,
		Username: in.Username, Organization: in.Organization, Bucket: in.Bucket, Password: in.Password,
		AZ: az, StandbyAZ: standby, EngineType: engineV2, Tags: maps.Clone(in.Tags), CreatedAt: b.now(),
		Placement: in.placement(), Pending: b.pend(statusCreating, false), Mode: modePrimary,
	}
}

// CreateDBInstance creates an instance in CREATING state.
func (b *InMemoryBackend) CreateDBInstance(ctx context.Context, in InstanceInput) (wInstance, error) {
	in.defaults()

	if err := validateInstanceInput(&in); err != nil {
		return wInstance{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateDbInstance")
	defer b.mu.Unlock()

	b.purgeLocked()

	if err := b.checkSubnets(sc, in.SubnetIDs); err != nil {
		return wInstance{}, err
	}

	if err := b.groupExistsLocked(sc, in.ParameterGroupID); err != nil {
		return wInstance{}, err
	}

	if b.instanceNameTakenLocked(sc, in.Name) {
		return wInstance{}, conflict("DB instance %s already exists", in.Name)
	}

	if b.countLocked(sc) >= maxInstances {
		return wInstance{}, quota("DB instances per account and region", maxInstances)
	}

	inst := b.buildInstanceLocked(sc, &in, newID())
	b.instances.Put(inst)
	b.startEngineLocked(inst, statusCreating)

	return b.wireInstance(sc, inst), nil
}

func (b *InMemoryBackend) countLocked(sc scope) int {
	n := 0

	b.instances.Range(func(i *DBInstance) bool {
		if sc.owns(i.AccountID, i.Region) && !b.gone(i.Pending) {
			n++
		}

		return true
	})

	return n
}

// GetDBInstance returns an instance.
func (b *InMemoryBackend) GetDBInstance(ctx context.Context, id string) (wInstance, error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("GetDbInstance")
	defer b.mu.RUnlock()

	i, err := b.instanceLocked(sc, id)
	if err != nil {
		return wInstance{}, err
	}

	return b.wireInstance(sc, i), nil
}

// ListDBInstances lists instances ordered by name.
func (b *InMemoryBackend) ListDBInstances(
	ctx context.Context, token string, maxResults int32,
) (page.Page[wInstanceSummary], error) {
	return b.listInstances(ctx, token, maxResults, "", false)
}

// ListDBInstancesForCluster lists a cluster's member instances.
func (b *InMemoryBackend) ListDBInstancesForCluster(
	ctx context.Context, clusterID, token string, maxResults int32,
) (page.Page[wInstanceSummary], error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("ListDbInstancesForCluster")

	_, err := b.clusterLocked(sc, clusterID)

	b.mu.RUnlock()

	if err != nil {
		return page.Page[wInstanceSummary]{}, err
	}

	return b.listInstances(ctx, token, maxResults, clusterID, true)
}

func (b *InMemoryBackend) listInstances(
	ctx context.Context, token string, maxResults int32, clusterID string, forCluster bool,
) (page.Page[wInstanceSummary], error) {
	if err := validateListArgs(maxResults); err != nil {
		return page.Page[wInstanceSummary]{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("ListDbInstances")
	defer b.mu.RUnlock()

	var items []*DBInstance

	b.instances.Range(func(i *DBInstance) bool {
		if sc.owns(i.AccountID, i.Region) && !b.gone(i.Pending) && (!forCluster || i.ClusterID == clusterID) {
			items = append(items, i)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *DBInstance) int {
		return cmp.Or(cmp.Compare(x.Name, y.Name), cmp.Compare(x.ID, y.ID))
	})

	pg, err := paginate(items, token, maxResults)
	if err != nil {
		return page.Page[wInstanceSummary]{}, err
	}

	out := page.Page[wInstanceSummary]{Next: pg.Next, Data: make([]wInstanceSummary, 0, len(pg.Data))}
	for _, i := range pg.Data {
		out.Data = append(out.Data, b.summarizeInstance(sc, i))
	}

	return out, nil
}

func paginate[T any](items []T, token string, maxResults int32) (page.Page[T], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, invalid("nextToken is not valid")
	}

	return page.New(items, token, int(maxResults), defaultMaxList), nil
}

// InstanceUpdate holds UpdateDbInstance parameters; nil or empty fields are unchanged.
type InstanceUpdate struct {
	LogDelivery      *LogDelivery
	Maintenance      *Maintenance
	AllocatedStorage *int32
	Port             *int32
	DBInstanceType   string
	DBStorageType    string
	DeploymentType   string
	ParameterGroupID string
	BackupConfigs    []BackupConfig
	HasBackupConfigs bool
}

func validateInstanceUpdate(u InstanceUpdate) error {
	for _, err := range []error{
		validateLogDelivery(u.LogDelivery), validateMaintenance(u.Maintenance), validateBackupConfigs(u.BackupConfigs),
	} {
		if err != nil {
			return err
		}
	}

	if u.AllocatedStorage != nil {
		if err := validateStorage(*u.AllocatedStorage); err != nil {
			return err
		}
	}

	if u.Port != nil {
		if err := validatePort(*u.Port); err != nil {
			return err
		}
	}

	if u.DBInstanceType != "" {
		if err := oneOf("dbInstanceType", u.DBInstanceType, instanceTypes()); err != nil {
			return err
		}
	}

	if u.DBStorageType != "" {
		if err := oneOf("dbStorageType", u.DBStorageType, storageTypes()); err != nil {
			return err
		}
	}

	if u.DeploymentType != "" {
		return oneOf("deploymentType", u.DeploymentType, []string{deploySingleAZ, deployMultiAZ})
	}

	return nil
}

// UpdateDBInstance changes an instance and moves it to an updating status.
func (b *InMemoryBackend) UpdateDBInstance(ctx context.Context, id string, u InstanceUpdate) (wInstance, error) {
	if err := validateInstanceUpdate(u); err != nil {
		return wInstance{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("UpdateDbInstance")
	defer b.mu.Unlock()

	b.purgeLocked()

	i, err := b.instanceLocked(sc, id)
	if err != nil {
		return wInstance{}, err
	}

	if st := b.state(i.Pending); st != statusAvailable {
		return wInstance{}, conflict("DB instance %s is %s", id, st)
	}

	if i.ClusterID != "" {
		return wInstance{}, conflict("DB instance %s belongs to a cluster; update the cluster instead", id)
	}

	if u.AllocatedStorage != nil && *u.AllocatedStorage < i.Placement.AllocatedStorage {
		return wInstance{}, invalid("allocatedStorage cannot be decreased")
	}

	if ierr := b.groupExistsLocked(sc, u.ParameterGroupID); ierr != nil {
		return wInstance{}, ierr
	}

	kind := applyInstanceUpdate(i, u)
	i.Pending = b.pend(kind, false)

	return b.wireInstance(sc, i), nil
}

func applyInstanceUpdate(i *DBInstance, u InstanceUpdate) string {
	kind := statusUpdating
	p := &i.Placement

	if u.DBInstanceType != "" && u.DBInstanceType != p.DBInstanceType {
		p.DBInstanceType = u.DBInstanceType
		kind = statusUpdatingInstanceType
	}

	if u.DeploymentType != "" && u.DeploymentType != p.DeploymentType {
		p.DeploymentType = u.DeploymentType
		kind = statusUpdatingDeploymentType

		i.StandbyAZ = ""
		if u.DeploymentType == deployMultiAZ {
			i.StandbyAZ = i.Region + "b"
		}
	}

	if u.AllocatedStorage != nil {
		p.AllocatedStorage = *u.AllocatedStorage
	}

	if u.Port != nil {
		p.Port = *u.Port
	}

	if u.DBStorageType != "" {
		p.DBStorageType = u.DBStorageType
	}

	if u.ParameterGroupID != "" {
		p.ParameterGroupID = u.ParameterGroupID
	}

	if u.LogDelivery != nil {
		p.LogDelivery = u.LogDelivery
	}

	if u.Maintenance != nil {
		p.Maintenance = normalizeMaintenance(u.Maintenance)
	}

	if u.HasBackupConfigs {
		p.BackupConfigs = slices.Clone(u.BackupConfigs)
	}

	return kind
}

// DeleteDBInstance starts instance deletion.
func (b *InMemoryBackend) DeleteDBInstance(ctx context.Context, id string) (wInstance, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteDbInstance")

	b.purgeLocked()

	i, err := b.instanceLocked(sc, id)
	if err != nil {
		b.mu.Unlock()

		return wInstance{}, err
	}

	if i.ClusterID != "" {
		b.mu.Unlock()

		return wInstance{}, conflict("DB instance %s belongs to a cluster; delete the cluster instead", id)
	}

	if st := b.state(i.Pending); st != statusAvailable && st != statusFailed {
		b.mu.Unlock()

		return wInstance{}, conflict("DB instance %s is %s", id, st)
	}

	out := b.wireInstance(sc, i)
	i.Pending = b.pend(statusDeleting, false)
	out.Status = statusDeleting
	handles := b.detachEnginesLocked([]*DBInstance{i})
	b.mu.Unlock()

	b.stopEngines(handles)

	return out, nil
}

// RebootDBInstance moves an instance through REBOOTING back to AVAILABLE.
func (b *InMemoryBackend) RebootDBInstance(ctx context.Context, id string) (wInstance, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("RebootDbInstance")
	defer b.mu.Unlock()

	b.purgeLocked()

	i, err := b.instanceLocked(sc, id)
	if err != nil {
		return wInstance{}, err
	}

	if st := b.state(i.Pending); st != statusAvailable {
		return wInstance{}, conflict("DB instance %s is %s", id, st)
	}

	i.Pending = b.pend(statusRebooting, false)

	return b.wireInstance(sc, i), nil
}
