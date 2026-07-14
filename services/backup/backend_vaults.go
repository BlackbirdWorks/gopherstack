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
			ErrValidation, name, v.NumberOfRecoveryPoints)
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
func (b *InMemoryBackend) CreateRestoreAccessBackupVault(
	sourceVaultArn, vaultName string,
	_ /* creatorRequestID */ string,
	_ /* kv */ map[string]string,
) (*RestoreAccessVault, error) {
	b.mu.Lock("CreateRestoreAccessBackupVault")
	defer b.mu.Unlock()

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
		VaultState:                   statusCreating,
		CreationDate:                 time.Now().UTC(),
	}
	b.restoreAccessVaults.Put(rav)
	cp := *rav

	return &cp, nil
}

// ListRestoreAccessBackupVaults returns all restore access vaults.
func (b *InMemoryBackend) ListRestoreAccessBackupVaults() []*RestoreAccessVault {
	b.mu.RLock("ListRestoreAccessBackupVaults")
	defer b.mu.RUnlock()

	all := b.restoreAccessVaults.All()
	out := make([]*RestoreAccessVault, 0, len(all))
	for _, v := range all {
		cp := *v
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RestoreAccessBackupVaultName < out[j].RestoreAccessBackupVaultName
	})

	return out
}

// RevokeRestoreAccessBackupVault removes a restore access vault.
func (b *InMemoryBackend) RevokeRestoreAccessBackupVault(vaultName string) error {
	b.mu.Lock("RevokeRestoreAccessBackupVault")
	defer b.mu.Unlock()

	b.restoreAccessVaults.Delete(vaultName)

	return nil
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
			ErrValidation, name, v.NumberOfRecoveryPoints,
		)
	}

	// Locked vaults cannot be deleted.
	if cfg, ok2 := b.vaultLockConfigs.Get(name); ok2 {
		if cfg.LockDate != nil && time.Now().UTC().After(*cfg.LockDate) {
			return fmt.Errorf(
				"%w: vault %s is locked and cannot be deleted",
				ErrValidation, name,
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
