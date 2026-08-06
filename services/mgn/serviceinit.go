package mgn

// This file backs family K (2 ops): InitializeService, ListManagedAccounts.
//
// InitializeService is the account-level "opt in" call every other legacy
// op's UninitializedAccountException implicitly depends on (PARITY.md: 69 of
// 95 ops return it). This backend gates every legacy op behind
// requireInitializedLocked, called first thing inside each such op's own
// backend method (see every other family file).

// InitializeService marks the calling account as initialized. Idempotent:
// calling it again is a no-op, not an error (the real SDK's own Input has no
// fields at all to reject on a second call, and InitializeService's error
// set -- AccessDeniedException, ValidationException only -- has no
// ConflictException to signal "already initialized").
func (b *InMemoryBackend) InitializeService() {
	b.mu.Lock("InitializeService")
	defer b.mu.Unlock()

	b.serviceInitialized = true
}

// requireInitializedLocked returns an UninitializedAccountException-shaped error
// if InitializeService has never been called for this account. Callers must hold
// b.mu (either lock). The tagging trio, /network-migration/ ops, and
// InitializeService itself must NOT call this -- see PARITY.md's per-op tables.
func (b *InMemoryBackend) requireInitializedLocked() error {
	if !b.serviceInitialized {
		return uninitializedAccountError("account has not been initialized; call InitializeService first")
	}

	return nil
}

// ManagedAccount mirrors types.ManagedAccount.
type ManagedAccount struct {
	AccountID string
}

// ListManagedAccounts returns the accounts this caller manages: every real
// account in this account's AWS Organizations organization
// (resolveManagedAccountsLocked, cross_service.go) when this account is that
// organization's management account or a registered MGN delegated
// administrator, else just the calling account itself -- never fabricated
// data for another account.
func (b *InMemoryBackend) ListManagedAccounts() ([]ManagedAccount, error) {
	b.mu.RLock("ListManagedAccounts")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	ids := b.resolveManagedAccountsLocked()
	out := make([]ManagedAccount, len(ids))

	for i, id := range ids {
		out[i] = ManagedAccount{AccountID: id}
	}

	return out, nil
}
