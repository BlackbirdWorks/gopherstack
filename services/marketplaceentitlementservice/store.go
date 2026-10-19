package marketplaceentitlementservice

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName     = "marketplaceentitlementservice"
	defaultPageSize = 25
	maxPageSize     = 100
)

// InMemoryBackend holds the entitlement table behind one coarse lock.
type InMemoryBackend struct {
	entitlements *store.Table[Entitlement]
	registry     *store.Registry
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		accountID: accountID,
		region:    region,
	}

	b.entitlements = store.Register(b.registry, "entitlements", store.New(entitlementKey))

	return b
}

// Region returns the home region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}
