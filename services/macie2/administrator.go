package macie2

// GetAdministratorAccount returns the administrator account relationship.
func (b *InMemoryBackend) GetAdministratorAccount() (*AdministratorAccount, error) {
	b.mu.RLock("GetAdministratorAccount")
	defer b.mu.RUnlock()

	if b.administrator == nil {
		return nil, nil //nolint:nilnil // existing issue.
	}

	cp := *b.administrator

	return &cp, nil
}

// GetMasterAccount is the legacy alias for GetAdministratorAccount.
func (b *InMemoryBackend) GetMasterAccount() (*AdministratorAccount, error) {
	return b.GetAdministratorAccount()
}

// DisassociateFromAdministratorAccount removes the administrator relationship.
func (b *InMemoryBackend) DisassociateFromAdministratorAccount() error {
	b.mu.Lock("DisassociateFromAdministratorAccount")
	defer b.mu.Unlock()

	b.administrator = nil

	return nil
}

// DisassociateFromMasterAccount is the legacy alias.
func (b *InMemoryBackend) DisassociateFromMasterAccount() error {
	return b.DisassociateFromAdministratorAccount()
}
