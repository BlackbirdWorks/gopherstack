package cloudhsmv2

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateClusterInput holds CreateCluster parameters.
type CreateClusterInput struct {
	Retention      *wireRetention
	HsmType        string
	Mode           string
	NetworkType    string
	SourceBackupID string
	SubnetIDs      []string
	Tags           []wireTag
}

func (c *Cluster) ownedBy(sc scope) bool { return c.Account == sc.account && c.Region == sc.region }

func (b *InMemoryBackend) activeHsmsLocked(clusterID string, now time.Time) int {
	n := 0

	for _, h := range b.hsmsByCluster.Get(clusterID) {
		if h.DeletedAt.IsZero() && !now.Before(h.ReadyAt) {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) liveHsmsLocked(clusterID string, now time.Time) []*Hsm {
	var out []*Hsm

	for _, h := range b.hsmsByCluster.Get(clusterID) {
		if h.DeletedAt.IsZero() || now.Before(h.DeleteUntil) {
			out = append(out, h)
		}
	}

	slices.SortFunc(out, func(a, z *Hsm) int {
		return cmp.Or(a.Created.Compare(z.Created), cmp.Compare(a.ID, z.ID))
	})

	return out
}

func (b *InMemoryBackend) clusterStateLocked(c *Cluster, now time.Time) (string, string) {
	switch {
	case !c.DeletedAt.IsZero() && now.Before(c.DeleteUntil):
		return stateDeleteInProgress, "The cluster is being deleted."
	case !c.DeletedAt.IsZero():
		return stateDeleted, "The cluster has been deleted."
	case now.Before(c.ReadyAt):
		return stateCreateInProgress, "The cluster is being created."
	case !c.Initialized:
		return stateUninitialized, "The cluster has not been initialized."
	case now.Before(c.InitUntil):
		return stateInitializeInProgress, "The cluster is being initialized."
	case now.Before(c.ModifyUntil):
		return stateModifyInProgress, "The cluster is being modified."
	case b.activeHsmsLocked(c.ID, now) > 0:
		return stateActive, ""
	default:
		return stateInitialized, "The cluster is initialized and has no active HSM."
	}
}

func (b *InMemoryBackend) wireClusterLocked(c *Cluster) wireCluster {
	now := b.now()
	state, msg := b.clusterStateLocked(c, now)

	out := wireCluster{
		BackupPolicy:              "DEFAULT",
		BackupRetentionPolicy:     &wireRetention{Type: retentionDays, Value: strconv.Itoa(c.RetentionDays)},
		ClusterID:                 c.ID,
		CreateTimestamp:           awstime.Epoch(c.Created),
		HsmType:                   c.HsmType,
		HsmTypeRollbackExpiration: awstime.Epoch(c.RollbackExpiry),
		Mode:                      c.Mode,
		NetworkType:               c.NetworkType,
		SecurityGroup:             c.SecurityGroup,
		SourceBackupID:            c.SourceBackupID,
		State:                     state,
		StateMessage:              msg,
		SubnetMapping:             c.SubnetMapping,
		VpcID:                     c.VpcID,
		Hsms:                      []wireHsm{},
		TagList:                   wireTags(c.Tags),
	}

	if now.After(c.RollbackExpiry) {
		out.HsmTypeRollbackExpiration = 0
	}

	certs := &wireCertificates{ClusterCertificate: c.ClusterCert}
	if !c.Initialized && !now.Before(c.ReadyAt) {
		certs.ClusterCsr = c.CSR
	}

	if certs.ClusterCertificate != "" || certs.ClusterCsr != "" {
		out.Certificates = certs
	}

	for _, h := range b.liveHsmsLocked(c.ID, now) {
		out.Hsms = append(out.Hsms, b.wireHsmLocked(h))
	}

	return out
}

func wireTags(m map[string]string) []wireTag {
	out := make([]wireTag, 0, len(m))
	for k, v := range m {
		out = append(out, wireTag{Key: k, Value: v})
	}

	slices.SortFunc(out, func(a, z wireTag) int { return cmp.Compare(a.Key, z.Key) })

	return out
}

func (b *InMemoryBackend) clusterLocked(sc scope, id string) (*Cluster, error) {
	c, ok := b.clusters.Get(id)
	if !ok || !c.ownedBy(sc) {
		return nil, fail(ErrResourceNotFound, "cluster %s not found", id)
	}

	if state, _ := b.clusterStateLocked(c, b.now()); state == stateDeleted {
		return nil, fail(ErrResourceNotFound, "cluster %s not found", id)
	}

	return c, nil
}

func (b *InMemoryBackend) liveClusterCountLocked(sc scope) int {
	n := 0

	b.clusters.Range(func(c *Cluster) bool {
		if c.ownedBy(sc) && c.DeletedAt.IsZero() {
			n++
		}

		return true
	})

	return n
}

// reapLocked removes resources whose deletion window has elapsed and releases their EC2 objects.
func (b *InMemoryBackend) reapLocked() {
	now := b.now()

	for _, h := range b.hsms.All() {
		if !h.DeletedAt.IsZero() && !now.Before(h.DeleteUntil) {
			if b.network != nil {
				b.network.DeleteENI(h.Region, h.EniID)
			}

			b.hsms.Delete(h.ID)
		}
	}

	for _, c := range b.clusters.All() {
		if !c.DeletedAt.IsZero() && !now.Before(c.DeleteUntil) {
			if b.network != nil {
				b.network.DeleteSecurityGroup(c.Region, c.SecurityGroup)
			}

			b.clusters.Delete(c.ID)
		}
	}

	for _, bk := range b.backups.All() {
		if state := backupState(bk, now); state == stateDeleted {
			b.backups.Delete(bk.ID)
		}
	}
}

func (b *InMemoryBackend) resolveSubnetsLocked(sc scope, ids []string) (string, map[string]string, error) {
	if len(ids) == 0 || len(ids) > maxSubnets {
		return "", nil, fail(ErrInvalidRequest, "SubnetIds must contain between 1 and %d subnets", maxSubnets)
	}

	if b.network == nil {
		return "", nil, fail(ErrInvalidRequest, "subnets cannot be validated because EC2 is not available")
	}

	info := b.network.SubnetInfo(sc.region, ids)
	mapping := make(map[string]string, len(ids))
	vpc := ""

	for _, id := range ids {
		sn, ok := info[id]
		if !ok {
			return "", nil, fail(ErrInvalidRequest, "subnet %s does not exist", id)
		}

		if vpc != "" && sn.VpcID != vpc {
			return "", nil, fail(ErrInvalidRequest, "all subnets must be in the same VPC")
		}

		if _, dup := mapping[sn.Zone]; dup {
			return "", nil, fail(ErrInvalidRequest, "only one subnet per Availability Zone is allowed")
		}

		vpc = sn.VpcID
		mapping[sn.Zone] = id
	}

	return vpc, mapping, nil
}

// CreateCluster creates a cluster, optionally restored from a backup.
func (b *InMemoryBackend) CreateCluster(ctx context.Context, in CreateClusterInput) (wireCluster, error) {
	if err := validateHsmType(in.HsmType); err != nil {
		return wireCluster{}, err
	}

	mode, err := resolveMode(in.HsmType, in.Mode)
	if err != nil {
		return wireCluster{}, err
	}

	netType, err := resolveNetworkType(in.NetworkType)
	if err != nil {
		return wireCluster{}, err
	}

	days, err := retentionOf(in.Retention)
	if err != nil {
		return wireCluster{}, err
	}

	tags, err := validateTags(in.Tags)
	if err != nil {
		return wireCluster{}, err
	}

	id := newClusterID()

	csr, err := newClusterCSR(id)
	if err != nil {
		return wireCluster{}, fail(ErrInternalFailure, "%v", err)
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	b.reapLocked()

	if b.liveClusterCountLocked(sc) >= maxClusters {
		return wireCluster{}, fail(ErrResourceLimitExceeded, "at most %d clusters are allowed per region", maxClusters)
	}

	vpc, mapping, err := b.resolveSubnetsLocked(sc, in.SubnetIDs)
	if err != nil {
		return wireCluster{}, err
	}

	now := b.now()
	c := &Cluster{
		ID: id, Account: sc.account, Region: sc.region, HsmType: in.HsmType, Mode: mode, NetworkType: netType,
		VpcID: vpc, SubnetMapping: mapping, Tags: tags, RetentionDays: days, CSR: csr,
		Created: now, ReadyAt: now.Add(b.dwell()),
	}

	if in.SourceBackupID != "" {
		if err = b.restoreFromBackupLocked(sc, c, in.SourceBackupID, in.Mode != ""); err != nil {
			return wireCluster{}, err
		}
	}

	if b.network != nil {
		sg, sgErr := b.network.CreateSecurityGroup(sc.region, "cloudhsm-cluster-"+id+"-sg",
			"Allows traffic to the CloudHSM cluster "+id, vpc)
		if sgErr != nil {
			return wireCluster{}, fail(ErrInternalFailure, "create security group: %v", sgErr)
		}

		c.SecurityGroup = sg
	}

	b.clusters.Put(c)

	return b.wireClusterLocked(c), nil
}

func (b *InMemoryBackend) restoreFromBackupLocked(sc scope, c *Cluster, ref string, modeGiven bool) error {
	bk, err := b.backupForUseLocked(sc, ref)
	if err != nil {
		return err
	}

	if state := backupState(bk, b.now()); state != stateReady {
		return fail(ErrInvalidRequest, "backup %s is %s and cannot be restored from", bk.ID, state)
	}

	if bk.HsmType != c.HsmType || (modeGiven && bk.Mode != c.Mode) {
		return fail(ErrInvalidRequest, "the cluster HsmType and Mode must match backup %s", bk.ID)
	}

	c.Mode = bk.Mode
	c.SourceBackupID = bk.ID
	c.ClusterCert = bk.ClusterCert
	c.Initialized = bk.ClusterCert != ""

	return nil
}

// DescribeClusters lists the caller's clusters.
func (b *InMemoryBackend) DescribeClusters(
	ctx context.Context, filters map[string][]string, token string, maxResults *int32,
) (page.Page[wireCluster], error) {
	if err := checkFilterKeys(filters, "clusterIds", "vpcIds", "states"); err != nil {
		return page.Page[wireCluster]{}, err
	}

	limit, err := validatePaging(token, maxResults)
	if err != nil {
		return page.Page[wireCluster]{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	var matched []*Cluster

	b.clusters.Range(func(c *Cluster) bool {
		state, _ := b.clusterStateLocked(c, b.now())
		if c.ownedBy(sc) && state != stateDeleted && matchesFilter(filters, "clusterIds", c.ID) &&
			matchesFilter(filters, "vpcIds", c.VpcID) && matchesFilter(filters, "states", state) {
			matched = append(matched, c)
		}

		return true
	})

	slices.SortFunc(matched, func(a, z *Cluster) int {
		return cmp.Or(a.Created.Compare(z.Created), cmp.Compare(a.ID, z.ID))
	})

	views := make([]wireCluster, len(matched))
	for i, c := range matched {
		views[i] = b.wireClusterLocked(c)
	}

	return page.New(views, token, limit, defaultPage), nil
}

// DeleteCluster starts deleting a cluster that has no HSMs.
func (b *InMemoryBackend) DeleteCluster(ctx context.Context, id string) (wireCluster, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	b.reapLocked()

	c, err := b.clusterLocked(sc, id)
	if err != nil {
		return wireCluster{}, err
	}

	now := b.now()
	if c.DeletedAt.IsZero() {
		if len(b.liveHsmsLocked(c.ID, now)) > 0 {
			return wireCluster{}, fail(ErrInvalidRequest, "cluster %s still has HSMs; delete them first", id)
		}

		c.DeletedAt = now
		c.DeleteUntil = now.Add(b.dwell())
	}

	return b.wireClusterLocked(c), nil
}

// ModifyClusterInput holds ModifyCluster parameters.
type ModifyClusterInput struct {
	Retention *wireRetention
	HsmType   string
	ClusterID string
}

// validate returns the requested retention days (0 when unchanged).
func (in ModifyClusterInput) validate() (int, error) {
	if in.Retention == nil && in.HsmType == "" {
		return 0, fail(ErrInvalidRequest, "specify BackupRetentionPolicy or HsmType")
	}

	if in.HsmType != "" {
		if err := validateHsmType(in.HsmType); err != nil {
			return 0, err
		}
	}

	if in.Retention == nil {
		return 0, nil
	}

	return retentionOf(in.Retention)
}

// ModifyCluster changes the backup retention policy and/or HSM type.
func (b *InMemoryBackend) ModifyCluster(ctx context.Context, in ModifyClusterInput) (wireCluster, error) {
	days, err := in.validate()
	if err != nil {
		return wireCluster{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("ModifyCluster")
	defer b.mu.Unlock()

	b.reapLocked()

	c, err := b.clusterLocked(sc, in.ClusterID)
	if err != nil {
		return wireCluster{}, err
	}

	now := b.now()
	if state, _ := b.clusterStateLocked(c, now); state == stateCreateInProgress || state == stateDeleteInProgress ||
		state == stateModifyInProgress || state == stateInitializeInProgress {
		return wireCluster{}, fail(ErrInvalidRequest, "cluster %s is %s and cannot be modified", c.ID, state)
	}

	if in.HsmType != "" && in.HsmType != c.HsmType {
		if err = b.changeHsmTypeLocked(c, in.HsmType, now); err != nil {
			return wireCluster{}, err
		}
	}

	if days != 0 {
		c.RetentionDays = days
	}

	return b.wireClusterLocked(c), nil
}

func (b *InMemoryBackend) changeHsmTypeLocked(c *Cluster, to string, now time.Time) error {
	switch {
	case c.HsmType == hsmTypeV1 && to == hsmTypeV2:
		c.RollbackExpiry = now.Add(rollbackWindow)
	case c.HsmType == hsmTypeV2 && to == hsmTypeV1 && now.Before(c.RollbackExpiry) && c.Mode == modeFIPS:
		c.RollbackExpiry = time.Time{}
	default:
		return fail(ErrInvalidRequest, "cluster HsmType cannot be changed from %s to %s", c.HsmType, to)
	}

	c.HsmType = to
	c.ModifyUntil = now.Add(b.dwell())

	for _, h := range b.hsmsByCluster.Get(c.ID) {
		h.HsmType = to
	}

	return nil
}

// InitializeCluster claims a cluster with a signed certificate and trust anchor.
func (b *InMemoryBackend) InitializeCluster(
	ctx context.Context, id, signedCert, trustAnchor string,
) (string, string, error) {
	if id == "" || signedCert == "" || trustAnchor == "" {
		return "", "", fail(ErrInvalidRequest, "ClusterId, SignedCert and TrustAnchor are required")
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("InitializeCluster")
	defer b.mu.Unlock()

	b.reapLocked()

	c, err := b.clusterLocked(sc, id)
	if err != nil {
		return "", "", err
	}

	now := b.now()
	if state, _ := b.clusterStateLocked(c, now); state != stateUninitialized {
		return "", "", fail(
			ErrInvalidRequest,
			"cluster %s is %s; only an UNINITIALIZED cluster can be initialized",
			id,
			state,
		)
	}

	if err = verifyClusterCert(c.CSR, signedCert, trustAnchor, now); err != nil {
		return "", "", err
	}

	c.ClusterCert = strings.TrimSpace(signedCert) + "\n"
	c.Initialized = true
	c.InitUntil = now.Add(b.dwell())

	if b.activeHsmsLocked(c.ID, now) > 0 {
		b.takeBackupLocked(c, now)
	}

	state, msg := b.clusterStateLocked(c, now)

	return state, msg, nil
}
