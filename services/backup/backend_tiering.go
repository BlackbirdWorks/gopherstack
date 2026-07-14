package backup

import (
	"fmt"
	"sort"
)

// CreateTieringConfiguration creates a tiering configuration for a vault.
func (b *InMemoryBackend) CreateTieringConfiguration(vaultName string) error {
	b.mu.Lock("CreateTieringConfiguration")
	defer b.mu.Unlock()

	vault, ok := b.vaults.Get(vaultName)
	if !ok {
		return fmt.Errorf("%w: %s", errVaultNotFoundB1, vaultName)
	}
	b.tieringConfigs.Put(&TieringConfiguration{
		BackupVaultName: vaultName,
		BackupVaultArn:  vault.BackupVaultArn,
	})

	return nil
}

// DeleteTieringConfiguration removes a tiering configuration.
func (b *InMemoryBackend) DeleteTieringConfiguration(vaultName string) error {
	b.mu.Lock("DeleteTieringConfiguration")
	defer b.mu.Unlock()

	b.tieringConfigs.Delete(vaultName)

	return nil
}

// GetTieringConfiguration returns the tiering configuration for a vault.
func (b *InMemoryBackend) GetTieringConfiguration(vaultName string) (*TieringConfiguration, error) {
	b.mu.RLock("GetTieringConfiguration")
	defer b.mu.RUnlock()

	tc, ok := b.tieringConfigs.Get(vaultName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errTieringConfigNotFound, vaultName)
	}

	return tc, nil
}

// ListTieringConfigurations returns all tiering configurations.
func (b *InMemoryBackend) ListTieringConfigurations() []*TieringConfiguration {
	b.mu.RLock("ListTieringConfigurations")
	defer b.mu.RUnlock()

	all := b.tieringConfigs.All()
	out := make([]*TieringConfiguration, 0, len(all))
	for _, tc := range all {
		cp := *tc
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BackupVaultName < out[j].BackupVaultName })

	return out
}

// UpdateTieringConfiguration updates a tiering configuration (no-op: config has no mutable fields in this mock).
func (b *InMemoryBackend) UpdateTieringConfiguration(vaultName string) error {
	b.mu.Lock("UpdateTieringConfiguration")
	defer b.mu.Unlock()

	if !b.tieringConfigs.Has(vaultName) {
		return fmt.Errorf("%w: %s", errTieringConfigNotFound, vaultName)
	}

	return nil
}

// ---- Restore Access Vaults ----
