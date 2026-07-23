package backup

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// vaultNameRe matches valid vault names: 2–50 alphanumeric, hyphen, or underscore chars.
var vaultNameRe = regexp.MustCompile(`^[a-zA-Z0-9_\-]{2,50}$`)

// isValidVaultName reports whether name is an acceptable AWS Backup vault name:
// 2–50 alphanumeric or hyphen characters.
func isValidVaultName(name string) bool {
	return vaultNameRe.MatchString(name)
}

// CreateBackupVault creates a new backup vault.
func (b *InMemoryBackend) CreateBackupVault(
	name, encryptionKeyArn, creatorRequestID string,
	kv map[string]string,
) (*Vault, error) {
	b.mu.Lock("CreateBackupVault")
	defer b.mu.Unlock()

	if !isValidVaultName(name) {
		return nil, fmt.Errorf(
			"%w: BackupVaultName must be 2-50 alphanumeric or hyphen characters",
			ErrValidation,
		)
	}

	if existing, ok := b.vaults.Get(name); ok {
		// Idempotent create: same CreatorRequestId returns existing vault.
		if creatorRequestID != "" && existing.CreatorRequestID == creatorRequestID {
			cp := *existing

			return &cp, nil
		}

		return nil, fmt.Errorf("%w: vault %s already exists", ErrAlreadyExists, name)
	}

	vaultARN := arn.Build("backup", b.region, b.accountID, "backup-vault:"+name)
	t := tags.New("backup.vault." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	v := &Vault{
		BackupVaultName:  name,
		BackupVaultArn:   vaultARN,
		EncryptionKeyArn: encryptionKeyArn,
		CreatorRequestID: creatorRequestID,
		VaultType:        VaultTypeBackupVault,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.vaults.Put(v)
	b.vaultARNIndex[vaultARN] = name
	cp := *v

	return &cp, nil
}

// DescribeBackupVault returns a vault by name.
func (b *InMemoryBackend) DescribeBackupVault(name string) (*Vault, error) {
	b.mu.RLock("DescribeBackupVault")
	defer b.mu.RUnlock()

	v, ok := b.vaults.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, name)
	}
	cp := *v

	return &cp, nil
}

// ListBackupVaults returns all backup vaults sorted by name.
func (b *InMemoryBackend) ListBackupVaults() []*Vault {
	b.mu.RLock("ListBackupVaults")
	defer b.mu.RUnlock()

	all := b.vaults.All()
	list := make([]*Vault, 0, len(all))
	for _, v := range all {
		cp := *v
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Vault) int {
		if a.BackupVaultName < b.BackupVaultName {
			return -1
		}
		if a.BackupVaultName > b.BackupVaultName {
			return 1
		}

		return 0
	})

	return list
}

// DeleteBackupVault deletes a vault by name.
func (b *InMemoryBackend) DeleteBackupVault(name string) error {
	b.mu.Lock("DeleteBackupVault")
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(name)
	if !ok {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, name)
	}

	if v.NumberOfRecoveryPoints > 0 {
		return fmt.Errorf("%w: vault %s has %d recovery points; delete them first",
			ErrInvalidRequest, name, v.NumberOfRecoveryPoints)
	}

	delete(b.vaultARNIndex, v.BackupVaultArn)
	b.vaults.Delete(name)
	v.Tags.Close()

	return nil
}

// AssociateBackupVaultMpaApprovalTeam associates an MPA approval team with a backup vault.
func (b *InMemoryBackend) AssociateBackupVaultMpaApprovalTeam(
	vaultName, mpaApprovalTeamArn string,
) error {
	b.mu.Lock("AssociateBackupVaultMpaApprovalTeam")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if mpaApprovalTeamArn == "" {
		return fmt.Errorf("%w: MpaApprovalTeamArn is required", ErrValidation)
	}

	b.mpaApprovals[vaultName] = mpaApprovalTeamArn

	return nil
}

// GetVaultMpaApprovalTeamArn returns the MPA approval team ARN associated
// with vaultName (via AssociateBackupVaultMpaApprovalTeam), and false if none
// is associated. Surfaced by DescribeBackupVault's MpaApprovalTeamArn field.
func (b *InMemoryBackend) GetVaultMpaApprovalTeamArn(vaultName string) (string, bool) {
	b.mu.RLock("GetVaultMpaApprovalTeamArn")
	defer b.mu.RUnlock()

	teamArn, ok := b.mpaApprovals[vaultName]

	return teamArn, ok
}

// CreateLogicallyAirGappedBackupVault creates a logically air-gapped backup vault.
func (b *InMemoryBackend) CreateLogicallyAirGappedBackupVault(
	name, creatorRequestID string,
	minRetentionDays, maxRetentionDays int64,
	kv map[string]string,
) (*Vault, error) {
	b.mu.Lock("CreateLogicallyAirGappedBackupVault")
	defer b.mu.Unlock()

	if !isValidVaultName(name) {
		return nil, fmt.Errorf(
			"%w: BackupVaultName must be 2-50 alphanumeric or hyphen characters",
			ErrValidation,
		)
	}

	if minRetentionDays <= 0 {
		return nil, fmt.Errorf("%w: MinRetentionDays must be greater than 0", ErrValidation)
	}

	if maxRetentionDays < minRetentionDays {
		return nil, fmt.Errorf("%w: MaxRetentionDays must be >= MinRetentionDays", ErrValidation)
	}

	if b.vaults.Has(name) {
		return nil, fmt.Errorf("%w: vault %s already exists", ErrAlreadyExists, name)
	}

	vaultARN := arn.Build("backup", b.region, b.accountID, "backup-vault:"+name)
	t := tags.New("backup.vault." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	v := &Vault{
		BackupVaultName:  name,
		BackupVaultArn:   vaultARN,
		CreatorRequestID: creatorRequestID,
		VaultType:        VaultTypeAirGapped,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		MinRetentionDays: minRetentionDays,
		MaxRetentionDays: maxRetentionDays,
		Tags:             t,
	}
	b.vaults.Put(v)
	b.vaultARNIndex[vaultARN] = name
	cp := *v

	return &cp, nil
}

// CreateRestoreAccessBackupVault creates a restore access backup vault.
// sourceVaultArn must resolve to an existing backup vault (real AWS: the
// SOURCE of a restore access vault is always a logically air-gapped vault) --
// ListRestoreAccessBackupVaults and RevokeRestoreAccessBackupVault both key
// off that source vault's NAME (see their real nested
// /logically-air-gapped-backup-vaults/{BackupVaultName}/... paths), so the
// name is resolved and stored here rather than re-derived from the ARN later.
func (b *InMemoryBackend) CreateRestoreAccessBackupVault(
	sourceVaultArn, vaultName string,
	_ /* creatorRequestID */ string,
	_ /* kv */ map[string]string,
) (*RestoreAccessVault, error) {
	b.mu.Lock("CreateRestoreAccessBackupVault")
	defer b.mu.Unlock()

	if sourceVaultArn == "" {
		return nil, fmt.Errorf("%w: SourceBackupVaultArn is required", ErrValidation)
	}

	sourceName, ok := b.vaultARNIndex[sourceVaultArn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: source backup vault %s not found", ErrNotFound, sourceVaultArn,
		)
	}

	if vaultName == "" {
		vaultName = uuid.NewString()
	}

	if b.restoreAccessVaults.Has(vaultName) {
		return nil, fmt.Errorf(
			"%w: restore access vault %s already exists",
			ErrAlreadyExists,
			vaultName,
		)
	}

	vaultARN := arn.Build("backup", b.region, b.accountID, "restore-access-backup-vault:"+vaultName)
	rav := &RestoreAccessVault{
		RestoreAccessBackupVaultName: vaultName,
		RestoreAccessBackupVaultArn:  vaultARN,
		SourceBackupVaultArn:         sourceVaultArn,
		SourceBackupVaultName:        sourceName,
		VaultState:                   statusCreating,
		CreationDate:                 time.Now().UTC(),
	}
	b.restoreAccessVaults.Put(rav)
	cp := *rav

	return &cp, nil
}

// ListRestoreAccessBackupVaults returns the restore access vaults created
// from the given source vault name. Real AWS addresses this op as
// GET /logically-air-gapped-backup-vaults/{BackupVaultName}/restore-access-backup-vaults,
// i.e. always scoped to one source vault -- there is no "list all" op.
func (b *InMemoryBackend) ListRestoreAccessBackupVaults(vaultName string) ([]*RestoreAccessVault, error) {
	b.mu.RLock("ListRestoreAccessBackupVaults")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	all := b.restoreAccessVaults.All()
	out := make([]*RestoreAccessVault, 0, len(all))
	for _, v := range all {
		if v.SourceBackupVaultName != vaultName {
			continue
		}
		cp := *v
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RestoreAccessBackupVaultName < out[j].RestoreAccessBackupVaultName
	})

	return out, nil
}

// RevokeRestoreAccessBackupVault removes a restore access vault, scoped to
// the given source vault name (real AWS: DELETE
// /logically-air-gapped-backup-vaults/{BackupVaultName}/restore-access-backup-vaults/{RestoreAccessBackupVaultArn} --
// a restore access vault not sourced from vaultName is reported not-found,
// matching AWS's per-source-vault addressing).
func (b *InMemoryBackend) RevokeRestoreAccessBackupVault(vaultName, restoreAccessVaultArn string) error {
	b.mu.Lock("RevokeRestoreAccessBackupVault")
	defer b.mu.Unlock()

	for _, v := range b.restoreAccessVaults.All() {
		if v.RestoreAccessBackupVaultArn != restoreAccessVaultArn {
			continue
		}
		if v.SourceBackupVaultName != vaultName {
			break
		}
		b.restoreAccessVaults.Delete(v.RestoreAccessBackupVaultName)

		return nil
	}

	return fmt.Errorf(
		"%w: restore access vault %s not found", ErrNotFound, restoreAccessVaultArn,
	)
}

// ---- MPA Approvals ----

// DisassociateBackupVaultMpaApprovalTeam removes the MPA approval team for a vault.
func (b *InMemoryBackend) DisassociateBackupVaultMpaApprovalTeam(vaultName string) error {
	b.mu.Lock("DisassociateBackupVaultMpaApprovalTeam")
	defer b.mu.Unlock()

	delete(b.mpaApprovals, vaultName)

	return nil
}

// ListVaultsFilter contains optional filter parameters for listing backup vaults.
type ListVaultsFilter struct {
	VaultType  string
	NextToken  string
	MaxResults int
}

// ListBackupVaultsFiltered returns vaults with optional type filter and pagination.
func (b *InMemoryBackend) ListBackupVaultsFiltered(f ListVaultsFilter) ([]*Vault, string) {
	b.mu.RLock("ListBackupVaultsFiltered")
	defer b.mu.RUnlock()

	all := b.vaults.All()
	list := make([]*Vault, 0, len(all))
	for _, v := range all {
		// Filter by vault type: logically air-gapped vaults have MinRetentionDays > 0.
		if f.VaultType == VaultTypeAirGapped && v.MinRetentionDays == 0 {
			continue
		}
		if f.VaultType == VaultTypeBackupVault && v.MinRetentionDays > 0 {
			continue
		}
		cp := *v
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Vault) int {
		return strings.Compare(a.BackupVaultName, b.BackupVaultName)
	})

	return paginateByID(
		list,
		func(v *Vault) string { return v.BackupVaultName },
		f.MaxResults,
		f.NextToken,
	)
}

// ---- ListBackupPlans pagination ----

// IsVaultLocked reports whether the vault's lock date has passed (vault is now immutable).
func (b *InMemoryBackend) IsVaultLocked(vaultName string) bool {
	b.mu.RLock("IsVaultLocked")
	defer b.mu.RUnlock()

	cfg, ok := b.vaultLockConfigs.Get(vaultName)
	if !ok {
		return false
	}

	return cfg.LockDate != nil && time.Now().UTC().After(*cfg.LockDate)
}

// DeleteBackupVaultChecked deletes a vault, enforcing lock and recovery point constraints.
func (b *InMemoryBackend) DeleteBackupVaultChecked(name string) error {
	b.mu.Lock("DeleteBackupVaultChecked")
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(name)
	if !ok {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, name)
	}

	if v.NumberOfRecoveryPoints > 0 {
		return fmt.Errorf(
			"%w: vault %s has %d recovery points; delete them first",
			ErrInvalidRequest, name, v.NumberOfRecoveryPoints,
		)
	}

	// Locked vaults cannot be deleted.
	if cfg, ok2 := b.vaultLockConfigs.Get(name); ok2 {
		if cfg.LockDate != nil && time.Now().UTC().After(*cfg.LockDate) {
			return fmt.Errorf(
				"%w: vault %s is locked and cannot be deleted",
				ErrInvalidRequest, name,
			)
		}
	}

	delete(b.vaultARNIndex, v.BackupVaultArn)
	b.vaults.Delete(name)
	b.vaultLockConfigs.Delete(name)
	b.vaultAccessPolicies.Delete(name)
	b.vaultNotifications.Delete(name)
	v.Tags.Close()

	return nil
}

// ---- CompleteBackupJob ----
