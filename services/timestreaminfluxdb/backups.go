package timestreaminfluxdb

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultRetention = 7
	maxBackups       = 100
	restoreNew       = "NEW_RESOURCE"
	restoreReplace   = "REPLACE_EXISTING"
)

// BackupInput holds CreateDbBackup parameters.
type BackupInput struct {
	RetentionDays *int32
	Tags          map[string]string
	ResourceID    string
	Name          string
}

func (b *InMemoryBackend) backupLocked(sc scope, id string) (*DBBackup, error) {
	bk, ok := b.backups.Get(id)
	if !ok || !sc.owns(bk.AccountID, bk.Region) || b.gone(bk.Pending) {
		return nil, notFound("DB backup", id)
	}

	return bk, nil
}

// CreateDBBackup takes an on-demand backup of an instance or cluster.
func (b *InMemoryBackend) CreateDBBackup(ctx context.Context, in BackupInput) (wBackup, error) {
	if err := validateName(in.Name); err != nil {
		return wBackup{}, err
	}

	if err := validateTags(in.Tags); err != nil {
		return wBackup{}, err
	}

	days := int32(defaultRetention)

	if in.RetentionDays != nil {
		days = *in.RetentionDays
		if days < 1 || days > maxRetention {
			return wBackup{}, invalid("retentionDays must be between 1 and %d", maxRetention)
		}
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateDbBackup")
	defer b.mu.Unlock()

	b.purgeLocked()

	bk, err := b.snapshotLocked(sc, in.ResourceID)
	if err != nil {
		return wBackup{}, err
	}

	if b.backupNameTakenLocked(sc, in.Name) {
		return wBackup{}, conflict("DB backup %s already exists", in.Name)
	}

	if b.backupCountLocked(sc) >= maxBackups {
		return wBackup{}, quota("DB backups per account and region", maxBackups)
	}

	id := newID()
	bk.ID, bk.Name, bk.ARN = id, in.Name, sc.arn("db-backup/"+id)
	bk.AccountID, bk.Region, bk.Type = sc.account, sc.region, backupOnDemand
	bk.RetentionDays, bk.Tags, bk.CreatedAt = days, cloneTags(in.Tags), b.now()
	bk.Pending = b.pend(statusCreating, false)
	b.backups.Put(bk)

	return b.wireBackup(bk), nil
}

// snapshotLocked copies the settings of an AVAILABLE instance or cluster into a backup shell.
func (b *InMemoryBackend) snapshotLocked(sc scope, resourceID string) (*DBBackup, error) {
	if i, err := b.instanceLocked(sc, resourceID); err == nil {
		if st := b.state(i.Pending); st != statusAvailable {
			return nil, conflict("DB instance %s is %s", resourceID, st)
		}

		return &DBBackup{
			ResourceID: i.ID, ResourceType: resInstance, EngineType: i.EngineType, Username: i.Username,
			Organization: i.Organization, Bucket: i.Bucket, Placement: clonePlacement(i.Placement),
		}, nil
	}

	c, err := b.clusterLocked(sc, resourceID)
	if err != nil {
		return nil, notFound("DB instance or cluster", resourceID)
	}

	if st := b.state(c.Pending); st != statusAvailable {
		return nil, conflict("DB cluster %s is %s", resourceID, st)
	}

	cfg := *c.Config

	return &DBBackup{
		ResourceID: c.ID, ResourceType: resCluster, EngineType: c.EngineType, FailoverMode: c.FailoverMode,
		Username: c.Username, Organization: c.Organization, Bucket: c.Bucket, ClusterConfig: &cfg,
		Placement: clonePlacement(c.Placement),
	}, nil
}

func clonePlacement(p Placement) Placement {
	p.SubnetIDs = slices.Clone(p.SubnetIDs)
	p.SecurityGroupIDs = slices.Clone(p.SecurityGroupIDs)
	p.BackupConfigs = slices.Clone(p.BackupConfigs)

	if p.LogDelivery != nil {
		l := *p.LogDelivery
		p.LogDelivery = &l
	}

	if p.Maintenance != nil {
		m := *p.Maintenance
		p.Maintenance = &m
	}

	return p
}

func (b *InMemoryBackend) backupNameTakenLocked(sc scope, name string) bool {
	taken := false

	b.backups.Range(func(bk *DBBackup) bool {
		if sc.owns(bk.AccountID, bk.Region) && lowerEq(bk.Name, name) && !b.gone(bk.Pending) {
			taken = true
		}

		return !taken
	})

	return taken
}

func (b *InMemoryBackend) backupCountLocked(sc scope) int {
	n := 0

	b.backups.Range(func(bk *DBBackup) bool {
		if sc.owns(bk.AccountID, bk.Region) && !b.gone(bk.Pending) {
			n++
		}

		return true
	})

	return n
}

// GetDBBackup returns a backup.
func (b *InMemoryBackend) GetDBBackup(ctx context.Context, id string) (wBackup, error) {
	b.mu.RLock("GetDbBackup")
	defer b.mu.RUnlock()

	bk, err := b.backupLocked(b.scopeOf(ctx), id)
	if err != nil {
		return wBackup{}, err
	}

	return b.wireBackup(bk), nil
}

// ListDBBackups lists backups, optionally for one resource, newest first.
func (b *InMemoryBackend) ListDBBackups(
	ctx context.Context, resourceID, token string, maxResults int32,
) (page.Page[wBackupSummary], error) {
	if err := validateListArgs(maxResults); err != nil {
		return page.Page[wBackupSummary]{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("ListDbBackups")
	defer b.mu.RUnlock()

	var items []*DBBackup

	b.backups.Range(func(bk *DBBackup) bool {
		if sc.owns(bk.AccountID, bk.Region) && !b.gone(bk.Pending) &&
			(resourceID == "" || bk.ResourceID == resourceID) {
			items = append(items, bk)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *DBBackup) int {
		return cmp.Or(y.CreatedAt.Compare(x.CreatedAt), cmp.Compare(x.ID, y.ID))
	})

	pg, err := paginate(items, token, maxResults)
	if err != nil {
		return page.Page[wBackupSummary]{}, err
	}

	out := page.Page[wBackupSummary]{Next: pg.Next, Data: make([]wBackupSummary, 0, len(pg.Data))}
	for _, bk := range pg.Data {
		out.Data = append(out.Data, b.summarizeBackup(bk))
	}

	return out, nil
}

// DeleteDBBackup starts backup deletion.
func (b *InMemoryBackend) DeleteDBBackup(ctx context.Context, id string) (wBackup, error) {
	b.mu.Lock("DeleteDbBackup")
	defer b.mu.Unlock()

	b.purgeLocked()

	bk, err := b.backupLocked(b.scopeOf(ctx), id)
	if err != nil {
		return wBackup{}, err
	}

	if st := b.backupState(bk); st == backupInProgress {
		return wBackup{}, conflict("DB backup %s is %s", id, st)
	}

	bk.Pending = b.pend(statusDeleting, false)

	return b.wireBackup(bk), nil
}

// RestoreInput holds RestoreFromDbBackup parameters.
type RestoreInput struct {
	RestoreToTime    *time.Time
	Port             *int32
	Publicly         *bool
	LogDelivery      *LogDelivery
	Maintenance      *Maintenance
	Tags             map[string]string
	BackupID         string
	Name             string
	DeploymentType   string
	KMSKeyID         string
	NetworkType      string
	RestoreMode      string
	SubnetIDs        []string
	SecurityGroupIDs []string
	BackupConfigs    []BackupConfig
	HasBackupConfigs bool
}

// RestoreResult describes the resource a restore produced.
type RestoreResult struct {
	DeploymentType string
	EngineType     string
	ResourceType   string
	ResourceID     string
}

func validateRestore(in RestoreInput) error {
	for _, err := range []error{
		validateLogDelivery(in.LogDelivery), validateMaintenance(in.Maintenance),
		validateBackupConfigs(in.BackupConfigs), validateTags(in.Tags),
	} {
		if err != nil {
			return err
		}
	}

	if in.RestoreMode != "" {
		if err := oneOf("restoreMode", in.RestoreMode, []string{restoreNew, restoreReplace}); err != nil {
			return err
		}
	}

	if in.NetworkType != "" {
		if err := oneOf("networkType", in.NetworkType, networkTypes()); err != nil {
			return err
		}
	}

	if in.Port != nil {
		if err := validatePort(*in.Port); err != nil {
			return err
		}
	}

	if len(in.SubnetIDs) > 0 || len(in.SecurityGroupIDs) > 0 {
		return validateNetwork(in.SubnetIDs, in.SecurityGroupIDs)
	}

	return nil
}

// RestoreFromDBBackup creates a resource from a backup, or resets the original one.
func (b *InMemoryBackend) RestoreFromDBBackup(ctx context.Context, in RestoreInput) (RestoreResult, error) {
	if err := validateRestore(in); err != nil {
		return RestoreResult{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("RestoreFromDbBackup")
	defer b.mu.Unlock()

	b.purgeLocked()

	bk, err := b.backupLocked(sc, in.BackupID)
	if err != nil {
		return RestoreResult{}, err
	}

	if st := b.backupState(bk); st != backupCompleted {
		return RestoreResult{}, conflict("DB backup %s is %s", bk.ID, st)
	}

	if in.RestoreToTime != nil &&
		(bk.Type != "CONTINUOUS" || in.RestoreToTime.Before(bk.CreatedAt) || in.RestoreToTime.After(b.now())) {
		return RestoreResult{}, invalid(
			"restoreToTime is only valid for a continuous backup, within its recovery window",
		)
	}

	if in.RestoreMode == restoreReplace {
		return b.restoreInPlaceLocked(sc, bk)
	}

	return b.restoreNewLocked(sc, bk, in)
}

func (b *InMemoryBackend) restoreInPlaceLocked(sc scope, bk *DBBackup) (RestoreResult, error) {
	res := RestoreResult{
		DeploymentType: bk.Placement.DeploymentType, EngineType: bk.EngineType, ResourceType: bk.ResourceType,
		ResourceID: bk.ResourceID,
	}

	if bk.ResourceType == resCluster {
		c, err := b.clusterLocked(sc, bk.ResourceID)
		if err != nil {
			return RestoreResult{}, err
		}

		if st := b.state(c.Pending); st != statusAvailable {
			return RestoreResult{}, conflict("DB cluster %s is %s", c.ID, st)
		}

		c.Placement = clonePlacement(bk.Placement)
		c.Pending = b.pend(statusRestoring, false)
		b.setMembersLocked(c.ID, c.Pending, nil)

		return res, nil
	}

	i, err := b.instanceLocked(sc, bk.ResourceID)
	if err != nil {
		return RestoreResult{}, err
	}

	if st := b.state(i.Pending); st != statusAvailable {
		return RestoreResult{}, conflict("DB instance %s is %s", i.ID, st)
	}

	i.Placement = clonePlacement(bk.Placement)
	i.Pending = b.pend(statusRestoring, false)

	return res, nil
}

func (b *InMemoryBackend) applyRestoreOverrides(p *Placement, in RestoreInput) {
	if in.DeploymentType != "" {
		p.DeploymentType = in.DeploymentType
	}

	if in.KMSKeyID != "" {
		p.KMSKeyID = in.KMSKeyID
	}

	if in.NetworkType != "" {
		p.NetworkType = in.NetworkType
	}

	if in.Port != nil {
		p.Port = *in.Port
	}

	if in.Publicly != nil {
		p.PubliclyAccessible = *in.Publicly
	}

	if in.LogDelivery != nil {
		p.LogDelivery = in.LogDelivery
	}

	if in.Maintenance != nil {
		p.Maintenance = normalizeMaintenance(in.Maintenance)
	}

	if in.HasBackupConfigs {
		p.BackupConfigs = slices.Clone(in.BackupConfigs)
	}

	if len(in.SubnetIDs) > 0 {
		p.SubnetIDs, p.SecurityGroupIDs = slices.Clone(in.SubnetIDs), slices.Clone(in.SecurityGroupIDs)
	}
}

func (b *InMemoryBackend) restoreNewLocked(sc scope, bk *DBBackup, in RestoreInput) (RestoreResult, error) {
	if err := validateName(in.Name); err != nil {
		return RestoreResult{}, err
	}

	p := clonePlacement(bk.Placement)
	b.applyRestoreOverrides(&p, in)

	if in.DeploymentType != "" {
		allowed := []string{deploySingleAZ, deployMultiAZ}
		if bk.ResourceType == resCluster {
			allowed = []string{deployReadReplicas}
		}

		if err := oneOf("deploymentType", in.DeploymentType, allowed); err != nil {
			return RestoreResult{}, err
		}
	}

	if err := b.checkSubnets(sc, p.SubnetIDs); err != nil {
		return RestoreResult{}, err
	}

	res := RestoreResult{DeploymentType: p.DeploymentType, EngineType: bk.EngineType, ResourceType: bk.ResourceType}

	if bk.ResourceType == resCluster {
		if b.clusterNameTakenLocked(sc, in.Name) {
			return RestoreResult{}, conflict("DB cluster %s already exists", in.Name)
		}

		c := b.restoredClusterLocked(sc, bk, p, in)
		b.clusters.Put(c)

		for n := 1; n <= clusterNodes; n++ {
			b.instances.Put(b.buildMemberLocked(sc, c, n))
		}

		res.ResourceID = c.ID

		return res, nil
	}

	if b.instanceNameTakenLocked(sc, in.Name) {
		return RestoreResult{}, conflict("DB instance %s already exists", in.Name)
	}

	i := b.restoredInstanceLocked(sc, bk, p, in)
	b.instances.Put(i)
	res.ResourceID = i.ID

	return res, nil
}

func (b *InMemoryBackend) restoredInstanceLocked(sc scope, bk *DBBackup, p Placement, in RestoreInput) *DBInstance {
	id := newID()
	standby := ""

	if p.DeploymentType == deployMultiAZ {
		standby = sc.region + "b"
	}

	return &DBInstance{
		ID:           id,
		Name:         in.Name,
		ARN:          sc.arn("db-instance/" + id),
		AccountID:    sc.account,
		Region:       sc.region,
		Username:     bk.Username,
		Organization: bk.Organization,
		Bucket:       bk.Bucket,
		AZ:           sc.region + "a",
		StandbyAZ:    standby,
		EngineType:   bk.EngineType,
		Tags:         maps.Clone(in.Tags),
		CreatedAt:    b.now(),
		Placement:    p,
		Mode:         modePrimary,
		Pending:      b.pend(statusRestoring, false),
	}
}

func (b *InMemoryBackend) restoredClusterLocked(sc scope, bk *DBBackup, p Placement, in RestoreInput) *DBCluster {
	id := newID()

	var cfg *ClusterConfig

	if bk.ClusterConfig != nil {
		c := *bk.ClusterConfig
		cfg = &c
	}

	return &DBCluster{
		ID: id, Name: in.Name, ARN: sc.arn("db-cluster/" + id), AccountID: sc.account, Region: sc.region,
		Username: bk.Username, Organization: bk.Organization, Bucket: bk.Bucket, EngineType: bk.EngineType,
		FailoverMode: bk.FailoverMode, Tags: maps.Clone(in.Tags), CreatedAt: b.now(), Config: cfg, Placement: p,
		Pending: b.pend(statusRestoring, false),
	}
}
