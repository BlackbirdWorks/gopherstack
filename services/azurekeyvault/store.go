package azurekeyvault

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// InMemoryBackend holds every vault's state under one coarse lock.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	vaults     map[string]*vault
	now        func() time.Time
	autoCreate bool
}

// NewInMemoryBackend creates an empty backend. autoCreate makes vaults spring
// into existence on first data-plane write.
func NewInMemoryBackend(autoCreate bool) *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("azurekeyvault"),
		vaults:     make(map[string]*vault),
		now:        time.Now,
		autoCreate: autoCreate,
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.vaults = make(map[string]*vault)
}

// VaultOptions are the ARM-controlled vault properties.
type VaultOptions struct {
	RetentionDays   int
	PurgeProtection bool
}

// CreateVault registers (or updates) a vault, as the ARM resource provider does.
func (b *InMemoryBackend) CreateVault(name string, opts VaultOptions) {
	b.mu.Lock("CreateVault")
	defer b.mu.Unlock()

	key := strs.Fold(name)

	v, ok := b.vaults[key]
	if !ok {
		v = newVault(name)
		b.vaults[key] = v
	}

	if opts.RetentionDays > 0 {
		v.RetentionDays = opts.RetentionDays
	}

	v.PurgeProtection = v.PurgeProtection || opts.PurgeProtection
}

// DeleteVault removes a vault and everything in it.
func (b *InMemoryBackend) DeleteVault(name string) {
	b.mu.Lock("DeleteVault")
	defer b.mu.Unlock()

	delete(b.vaults, strs.Fold(name))
}

// ListVaults returns the registered vault names in sorted order.
func (b *InMemoryBackend) ListVaults() []string {
	b.mu.RLock("ListVaults")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.vaults))
	for _, v := range b.vaults {
		names = append(names, v.Name)
	}

	slices.Sort(names)

	return names
}

// VaultExists reports whether the vault is registered.
func (b *InMemoryBackend) VaultExists(name string) bool {
	b.mu.RLock("VaultExists")
	defer b.mu.RUnlock()

	_, ok := b.vaults[strs.Fold(name)]

	return ok
}

// do runs fn against the named vault under the write lock, after sweeping
// expired soft-deleted entities.
func (b *InMemoryBackend) do(name string, fn func(v *vault, now int64) error) error {
	b.mu.Lock("do")
	defer b.mu.Unlock()

	key := strs.Fold(name)

	v, ok := b.vaults[key]
	if !ok {
		if !b.autoCreate {
			return errVaultNotFound(name)
		}

		v = newVault(name)
		b.vaults[key] = v
	}

	now := b.now().Unix()
	v.sweep(now)

	return fn(v, now)
}
