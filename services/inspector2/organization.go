package inspector2

import "fmt"

// EnableDelegatedAdminAccount enables a delegated admin account.
func (b *InMemoryBackend) EnableDelegatedAdminAccount(accountID string) error {
	b.mu.Lock("EnableDelegatedAdminAccount")
	defer b.mu.Unlock()

	if accountID == "" {
		return fmt.Errorf("%w: accountId is required", ErrValidation)
	}

	if existing, ok := b.delegatedAdmins.Get(accountID); ok && existing.Status == statusEnabled {
		return ErrDelegatedAdminAlreadyExists
	}

	b.delegatedAdmins.Put(&DelegatedAdminAccount{
		AccountID: accountID,
		Status:    statusEnabled,
	})

	return nil
}

// DisableDelegatedAdminAccount disables a delegated admin account.
func (b *InMemoryBackend) DisableDelegatedAdminAccount(accountID string) error {
	b.mu.Lock("DisableDelegatedAdminAccount")
	defer b.mu.Unlock()

	if !b.delegatedAdmins.Delete(accountID) {
		return ErrDelegatedAdminNotFound
	}

	return nil
}

// GetDelegatedAdminAccount returns the delegated admin account.
func (b *InMemoryBackend) GetDelegatedAdminAccount() (*DelegatedAdminAccount, error) {
	b.mu.RLock("GetDelegatedAdminAccount")
	defer b.mu.RUnlock()

	for _, d := range b.delegatedAdmins.Snapshot() {
		cp := *d

		return &cp, nil
	}

	return nil, ErrDelegatedAdminNotFound
}

// ListDelegatedAdminAccounts returns all delegated admin accounts.
func (b *InMemoryBackend) ListDelegatedAdminAccounts() ([]*DelegatedAdminAccount, error) {
	b.mu.RLock("ListDelegatedAdminAccounts")
	defer b.mu.RUnlock()

	result := make([]*DelegatedAdminAccount, 0, b.delegatedAdmins.Len())

	for _, d := range b.delegatedAdmins.Snapshot() {
		cp := *d
		result = append(result, &cp)
	}

	return result, nil
}

// DescribeOrganizationConfiguration returns org-level Inspector2 configuration.
func (b *InMemoryBackend) DescribeOrganizationConfiguration() OrgConfiguration {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	return b.orgConfig
}

// UpdateOrganizationConfiguration updates org-level Inspector2 configuration.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(cfg OrgConfiguration) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	b.orgConfig = cfg

	return nil
}
