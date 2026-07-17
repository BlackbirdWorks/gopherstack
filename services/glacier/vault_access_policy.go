package glacier

// SetVaultAccessPolicy sets the access policy for a vault.
func (b *InMemoryBackend) SetVaultAccessPolicy(accountID, region, vaultName, policy string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return ErrVaultNotFound
	}

	v.AccessPolicy = policy

	return nil
}

// GetVaultAccessPolicy returns the access policy for a vault.
func (b *InMemoryBackend) GetVaultAccessPolicy(accountID, region, vaultName string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return "", ErrVaultNotFound
	}

	return v.AccessPolicy, nil
}

// DeleteVaultAccessPolicy deletes the access policy for a vault.
func (b *InMemoryBackend) DeleteVaultAccessPolicy(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return ErrVaultNotFound
	}

	v.AccessPolicy = ""

	return nil
}
