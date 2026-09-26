package ecrpublic

import "fmt"

// GetRepositoryCatalogData returns the Gallery-visible catalog metadata for a repository.
func (b *InMemoryBackend) GetRepositoryCatalogData(registryID, name string) (*CatalogData, error) {
	b.mu.RLock("GetRepositoryCatalogData")
	defer b.mu.RUnlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, err
	}

	repo, ok := b.repos.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	cd := repo.CatalogData

	return &cd, nil
}

// PutRepositoryCatalogData replaces the Gallery-visible catalog metadata for a repository.
func (b *InMemoryBackend) PutRepositoryCatalogData(
	registryID, name string, catalogData *CatalogData,
) (*CatalogData, error) {
	b.mu.Lock("PutRepositoryCatalogData")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, err
	}

	repo, ok := b.repos.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	if catalogData != nil {
		repo.CatalogData = *catalogData
	}

	cd := repo.CatalogData

	return &cd, nil
}

// DescribeRegistries returns the public registries visible to the caller.
// This is a single-tenant emulator: it always returns exactly the caller's
// own registry, never other accounts' registries (there is no cross-account
// Gallery directory modeled here -- see PARITY.md).
func (b *InMemoryBackend) DescribeRegistries() ([]RegistryInfo, error) {
	b.mu.RLock("DescribeRegistries")
	defer b.mu.RUnlock()

	info := RegistryInfo{
		RegistryArn: registryARN(b.region, b.accountID),
		RegistryID:  b.accountID,
		RegistryURI: repositoryURIHost + "/" + b.registryAlias,
		Verified:    false,
		Aliases: []RegistryAliasInfo{
			{
				Name:                 b.registryAlias,
				DefaultRegistryAlias: true,
				PrimaryRegistryAlias: true,
				Status:               "ACTIVE",
			},
		},
	}

	return []RegistryInfo{info}, nil
}

// GetRegistryCatalogData returns the account-wide Gallery display metadata.
func (b *InMemoryBackend) GetRegistryCatalogData() (RegistryCatalogData, error) {
	b.mu.RLock("GetRegistryCatalogData")
	defer b.mu.RUnlock()

	return b.registryCatalogData, nil
}

// PutRegistryCatalogData replaces the account-wide Gallery display metadata.
func (b *InMemoryBackend) PutRegistryCatalogData(displayName string) (RegistryCatalogData, error) {
	b.mu.Lock("PutRegistryCatalogData")
	defer b.mu.Unlock()

	b.registryCatalogData = RegistryCatalogData{DisplayName: displayName}

	return b.registryCatalogData, nil
}
