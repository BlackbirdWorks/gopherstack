package cloudhsmv2

import (
	"maps"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func backupState(bk *Backup, now time.Time) string {
	switch exp := bk.expiresAt(); {
	case !bk.DeleteAt.IsZero() && now.Before(bk.DeleteAt):
		return statePendingDeletion
	case !bk.DeleteAt.IsZero():
		return stateDeleted
	case !exp.IsZero() && !now.Before(exp):
		return stateDeleted
	case now.Before(bk.ReadyAt):
		return stateCreateInProgress
	default:
		return stateReady
	}
}

func (b *InMemoryBackend) wireBackupLocked(bk *Backup) wireBackup {
	return wireBackup{
		BackupARN:       backupARN(bk.Account, bk.Region, bk.ID),
		BackupID:        bk.ID,
		BackupState:     backupState(bk, b.now()),
		ClusterID:       bk.ClusterID,
		CopyTimestamp:   awstime.Epoch(bk.CopyTime),
		CreateTimestamp: awstime.Epoch(bk.Created),
		DeleteTimestamp: awstime.Epoch(bk.DeleteAt),
		HsmType:         bk.HsmType,
		Mode:            bk.Mode,
		NeverExpires:    bk.NeverExpires,
		SourceBackup:    bk.SourceBackup,
		SourceCluster:   bk.SourceCluster,
		SourceRegion:    bk.SourceRegion,
		TagList:         wireTags(bk.Tags),
	}
}

// takeBackupLocked records the backup the service takes of an initialized cluster.
func (b *InMemoryBackend) takeBackupLocked(c *Cluster, now time.Time) {
	b.backups.Put(&Backup{
		ID: newBackupID(), Account: c.Account, Region: c.Region, ClusterID: c.ID, HsmType: c.HsmType, Mode: c.Mode,
		ClusterCert: c.ClusterCert, RetentionDays: c.RetentionDays, Created: now, ReadyAt: now.Add(b.dwell()),
		Tags: maps.Clone(c.Tags),
	})
}

// splitRef returns the resource ID of an ARN-or-ID reference and, for ARNs, the owner account and region.
func splitRef(ref string) (string, string, string) {
	parsed, err := awsarn.Parse(ref)
	if err != nil {
		return ref, "", ""
	}

	_, id, _ := strings.Cut(parsed.Resource, "/")

	return id, parsed.AccountID, parsed.Region
}

// backupLocked finds a backup the caller owns in its region.
func (b *InMemoryBackend) backupLocked(sc scope, ref string) (*Backup, error) {
	id, account, region := splitRef(ref)

	bk, ok := b.backups.Get(id)
	if !ok || bk.Account != sc.account || bk.Region != sc.region ||
		(account != "" && account != bk.Account) || (region != "" && region != bk.Region) ||
		backupState(bk, b.now()) == stateDeleted {
		return nil, fail(ErrResourceNotFound, "backup %s not found", ref)
	}

	return bk, nil
}

// backupForUseLocked finds a backup the caller may restore from: its own or one shared by resource policy.
func (b *InMemoryBackend) backupForUseLocked(sc scope, ref string) (*Backup, error) {
	id, _, _ := splitRef(ref)

	bk, ok := b.backups.Get(id)
	if !ok || bk.Region != sc.region || backupState(bk, b.now()) == stateDeleted ||
		(bk.Account != sc.account && !policyGrants(bk.Policy, sc.account)) {
		return nil, fail(ErrResourceNotFound, "backup %s not found", ref)
	}

	return bk, nil
}

// policyGrants reports whether an Allow statement names the account (or everyone) as principal.
func policyGrants(policy, account string) bool {
	if policy == "" {
		return false
	}

	var doc struct {
		Statement []struct {
			Principal any    `json:"Principal"`
			Effect    string `json:"Effect"`
		} `json:"Statement"`
	}
	if json.Unmarshal([]byte(policy), &doc) != nil {
		return false
	}

	for _, st := range doc.Statement {
		if st.Effect == "Allow" && principalMatches(st.Principal, account) {
			return true
		}
	}

	return false
}

func principalMatches(p any, account string) bool {
	switch v := p.(type) {
	case string:
		return v == "*" || v == account || strings.Contains(v, ":"+account+":")
	case []any:
		return slices.ContainsFunc(v, func(e any) bool { return principalMatches(e, account) })
	case map[string]any:
		return principalMatches(v["AWS"], account)
	default:
		return false
	}
}

// DescribeBackupsInput holds DescribeBackups parameters.
type DescribeBackupsInput struct {
	Filters       map[string][]string
	MaxResults    *int32
	NextToken     string
	Shared        bool
	SortAscending bool
}

func (in DescribeBackupsInput) visible(bk *Backup, sc scope, state string) bool {
	if bk.Region != sc.region || state == stateDeleted {
		return false
	}

	if in.Shared {
		return bk.Account != sc.account && policyGrants(bk.Policy, sc.account)
	}

	return bk.Account == sc.account
}

func (in DescribeBackupsInput) matches(bk *Backup, state string) bool {
	return matchesFilter(in.Filters, "backupIds", bk.ID) &&
		matchesFilter(in.Filters, "sourceBackupIds", bk.SourceBackup) &&
		matchesFilter(in.Filters, "clusterIds", bk.ClusterID) && matchesFilter(in.Filters, "states", state) &&
		matchesFilter(in.Filters, "neverExpires", boolString(bk.NeverExpires)) &&
		matchesFilter(in.Filters, "modes", bk.Mode)
}

// DescribeBackups lists the caller's backups, or those shared with it.
func (b *InMemoryBackend) DescribeBackups(ctx context.Context, in DescribeBackupsInput) (page.Page[wireBackup], error) {
	err := checkFilterKeys(in.Filters, "backupIds", "sourceBackupIds", "clusterIds", "states", "neverExpires", "modes")
	if err != nil {
		return page.Page[wireBackup]{}, err
	}

	limit, err := validatePaging(in.NextToken, in.MaxResults)
	if err != nil {
		return page.Page[wireBackup]{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("DescribeBackups")
	defer b.mu.RUnlock()

	now := b.now()

	var matched []*Backup

	b.backups.Range(func(bk *Backup) bool {
		state := backupState(bk, now)
		if in.visible(bk, sc, state) && in.matches(bk, state) {
			matched = append(matched, bk)
		}

		return true
	})

	slices.SortFunc(matched, func(a, z *Backup) int {
		c := cmp.Or(a.Created.Compare(z.Created), cmp.Compare(a.ID, z.ID))
		if in.SortAscending {
			return c
		}

		return -c
	})

	views := make([]wireBackup, len(matched))
	for i, bk := range matched {
		views[i] = b.wireBackupLocked(bk)
	}

	return page.New(views, in.NextToken, limit, defaultPage), nil
}

func boolString(v bool) string {
	if v {
		return "true"
	}

	return "false"
}

// DeleteBackup schedules a backup for deletion; it can be restored for seven days.
func (b *InMemoryBackend) DeleteBackup(ctx context.Context, id string) (wireBackup, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	b.reapLocked()

	bk, err := b.backupLocked(sc, id)
	if err != nil {
		return wireBackup{}, err
	}

	if bk.DeleteAt.IsZero() {
		if state := backupState(bk, b.now()); state == stateCreateInProgress {
			return wireBackup{}, fail(ErrInvalidRequest, "backup %s is still being created", bk.ID)
		}

		bk.DeleteAt = b.now().Add(backupRecoveryWindow)
	}

	return b.wireBackupLocked(bk), nil
}

// RestoreBackup cancels a pending backup deletion.
func (b *InMemoryBackend) RestoreBackup(ctx context.Context, id string) (wireBackup, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("RestoreBackup")
	defer b.mu.Unlock()

	b.reapLocked()

	bk, err := b.backupLocked(sc, id)
	if err != nil {
		return wireBackup{}, err
	}

	bk.DeleteAt = time.Time{}

	return b.wireBackupLocked(bk), nil
}

// ModifyBackupAttributes exempts a backup from, or returns it to, the cluster retention policy.
func (b *InMemoryBackend) ModifyBackupAttributes(
	ctx context.Context,
	id string,
	neverExpires bool,
) (wireBackup, error) {
	sc := b.scopeOf(ctx)

	b.mu.Lock("ModifyBackupAttributes")
	defer b.mu.Unlock()

	b.reapLocked()

	bk, err := b.backupLocked(sc, id)
	if err != nil {
		return wireBackup{}, err
	}

	bk.NeverExpires = neverExpires

	return b.wireBackupLocked(bk), nil
}

// CopyBackupToRegion copies a ready backup into another region.
func (b *InMemoryBackend) CopyBackupToRegion(
	ctx context.Context, id, destRegion string, tagList []wireTag,
) (wireDestinationBackup, error) {
	if id == "" || destRegion == "" {
		return wireDestinationBackup{}, fail(ErrInvalidRequest, "BackupId and DestinationRegion are required")
	}

	if err := validateRegion(destRegion); err != nil {
		return wireDestinationBackup{}, err
	}

	tags, err := validateTags(tagList)
	if err != nil {
		return wireDestinationBackup{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CopyBackupToRegion")
	defer b.mu.Unlock()

	b.reapLocked()

	src, err := b.backupLocked(sc, id)
	if err != nil {
		return wireDestinationBackup{}, err
	}

	if destRegion == src.Region {
		return wireDestinationBackup{}, fail(
			ErrInvalidRequest,
			"DestinationRegion must differ from the backup's region",
		)
	}

	if state := backupState(src, b.now()); state != stateReady {
		return wireDestinationBackup{}, fail(
			ErrInvalidRequest,
			"backup %s is %s; only a READY backup can be copied",
			src.ID,
			state,
		)
	}

	if len(tagList) == 0 {
		tags = maps.Clone(src.Tags)
	}

	now := b.now()
	b.backups.Put(&Backup{
		ID: newBackupID(), Account: src.Account, Region: destRegion, ClusterID: src.ClusterID, HsmType: src.HsmType,
		Mode: src.Mode, ClusterCert: src.ClusterCert, RetentionDays: src.RetentionDays, NeverExpires: src.NeverExpires,
		Created: src.Created, ReadyAt: now.Add(b.dwell()), CopyTime: now, SourceBackup: src.ID,
		SourceCluster: src.ClusterID, SourceRegion: src.Region, Tags: tags,
	})

	return wireDestinationBackup{
		SourceBackup: src.ID, SourceCluster: src.ClusterID, SourceRegion: src.Region,
		CreateTimestamp: awstime.Epoch(src.Created),
	}, nil
}
