package glacier

import (
	"fmt"
	"time"
)

const (
	lockStateInProgress = "InProgress"
	lockStateLocked     = "Locked"
	lockStateUnlocked   = "Unlocked"

	// vaultLockExpirationHours is the number of hours before an InProgress vault lock expires.
	vaultLockExpirationHours = 24

	// hoursPerDay converts an archive's age from hours to days for the
	// glacier:ArchiveAgeInDays vault lock condition key.
	hoursPerDay = 24
)

// expireLockIfStale removes an InProgress vault lock that has passed its 24-hour window.
// Caller must hold b.mu.
func (b *InMemoryBackend) expireLockIfStale(vArn string) {
	lock, ok := b.vaultLocks.Get(vArn)
	if !ok || lock.State != lockStateInProgress {
		return
	}

	exp, err := time.Parse("2006-01-02T15:04:05.000Z", lock.ExpirationDate)
	if err == nil && time.Now().UTC().After(exp) {
		b.vaultLocks.Delete(vArn)
	}
}

// GetVaultLock returns the vault lock state.  If no lock has been initiated,
// the returned VaultLock has State "Unlocked".
func (b *InMemoryBackend) GetVaultLock(accountID, region, vaultName string) (*VaultLock, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return nil, ErrVaultNotFound
	}

	b.expireLockIfStale(vArn)

	lock, ok := b.vaultLocks.Get(vArn)
	if !ok {
		return &VaultLock{State: lockStateUnlocked}, nil
	}

	cp := *lock

	return &cp, nil
}

// SetVaultLock stores a vault lock policy (used by InitiateVaultLock). The
// policy is validated as a well-formed vault lock policy document (see
// vault_lock_policy_eval.go), so a malformed policy is rejected here rather
// than silently never being enforced by DeleteArchive/DeleteVault.
func (b *InMemoryBackend) SetVaultLock(accountID, region, vaultName, policy, lockID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return ErrVaultNotFound
	}

	if policy != "" {
		if _, err := parseVaultLockPolicyDocument(policy); err != nil {
			return fmt.Errorf("%w: %w", ErrValidation, err)
		}
	}

	// Expire stale InProgress lock before checking state.
	b.expireLockIfStale(vArn)

	if existing, ok := b.vaultLocks.Get(vArn); ok {
		if existing.State == lockStateInProgress {
			return ErrLockConflict
		}

		if existing.State == lockStateLocked {
			return ErrLockAlreadyLocked
		}
	}

	now := time.Now().UTC()
	b.vaultLocks.Put(&VaultLock{
		VaultARN:       vArn,
		Policy:         policy,
		LockID:         lockID,
		State:          lockStateInProgress,
		CreationDate:   formatDate(now),
		ExpirationDate: formatDate(now.Add(vaultLockExpirationHours * time.Hour)),
	})

	return nil
}

// checkVaultLockDelete enforces the vault's lock policy against a
// delete-shaped action. The policy is consulted while the lock is
// InProgress as well as Locked -- see vault_lock_policy_eval.go for why.
// archiveCreationDate is the Archive.CreationDate of the archive being
// deleted, or "" when the action has no archive in play (DeleteVault).
// Must be called with b.mu already held.
func (b *InMemoryBackend) checkVaultLockDelete(vArn, action, archiveCreationDate string) error {
	b.expireLockIfStale(vArn)

	lock, ok := b.vaultLocks.Get(vArn)
	if !ok || lock.State == lockStateUnlocked || lock.Policy == "" {
		return nil
	}

	archiveAgeDays := -1
	if archiveCreationDate != "" {
		if created, err := time.Parse("2006-01-02T15:04:05.000Z", archiveCreationDate); err == nil {
			archiveAgeDays = int(time.Since(created).Hours() / hoursPerDay)
		}
	}

	// The error return is discarded, not swallowed-and-ignored: the policy
	// was already validated as well-formed JSON in SetVaultLock, so parsing
	// cannot fail here except from corrupted persisted state, in which case
	// failing open (matching every other "can't evaluate this" case in
	// vault_lock_policy_eval.go) is the deliberate choice.
	denied, _ := evaluateVaultLockPolicy(lock.Policy, vArn, action, archiveAgeDays)
	if !denied {
		return nil
	}

	return fmt.Errorf("%w: %s", ErrVaultLockDenied, action)
}

// AbortVaultLock removes an in-progress vault lock.
func (b *InMemoryBackend) AbortVaultLock(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return ErrVaultNotFound
	}

	b.vaultLocks.Delete(vArn)

	return nil
}

// CompleteVaultLock completes and seals a vault lock.
func (b *InMemoryBackend) CompleteVaultLock(accountID, region, vaultName, lockID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return ErrVaultNotFound
	}

	b.expireLockIfStale(vArn)

	lock, ok := b.vaultLocks.Get(vArn)
	if !ok || lock.State != lockStateInProgress {
		return ErrValidation
	}

	if lock.LockID != lockID {
		return ErrValidation
	}

	lock.State = lockStateLocked
	lock.ExpirationDate = ""

	return nil
}
