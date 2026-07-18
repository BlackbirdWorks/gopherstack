package glacier

import "time"

const (
	lockStateInProgress = "InProgress"
	lockStateLocked     = "Locked"
	lockStateUnlocked   = "Unlocked"

	// vaultLockExpirationHours is the number of hours before an InProgress vault lock expires.
	vaultLockExpirationHours = 24
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

// SetVaultLock stores a vault lock policy (used by InitiateVaultLock).
func (b *InMemoryBackend) SetVaultLock(accountID, region, vaultName, policy, lockID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return ErrVaultNotFound
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
