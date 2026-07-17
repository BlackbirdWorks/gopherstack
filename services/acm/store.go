package acm

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend is the in-memory store for ACM certificates.
// InMemoryBackend stores ACM state. certs is a single flat store.Table keyed
// by the composite "region|arn" string (see regionKey) with a companion
// byRegion Index, replacing the previous map[region]map[arn]*Certificate
// nesting -- see store_setup.go. The remaining maps are non-*T value maps
// (their values are plain structs, not pointers) and stay nested by region
// as plain maps.
type InMemoryBackend struct {
	timers map[string]map[string]*time.Timer
	certs  *store.Table[Certificate]
	// certsByRegion groups certs by region, replacing the per-region scans
	// the old outer map nesting answered directly.
	certsByRegion *store.Index[Certificate]
	// idempotencyMap maps RequestCertificate idempotency tokens to cert info (per region).
	idempotencyMap map[string]map[string]certIdempotencyEntry
	// accountIdempotency maps PutAccountConfiguration tokens to their applied settings (per region).
	accountIdempotency map[string]map[string]accountIdempotencyEntry
	// accountConfig holds the account-level configuration per region.
	accountConfig map[string]AccountConfig
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		timers:             make(map[string]map[string]*time.Timer),
		idempotencyMap:     make(map[string]map[string]certIdempotencyEntry),
		accountIdempotency: make(map[string]map[string]accountIdempotencyEntry),
		accountConfig:      make(map[string]AccountConfig),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("acm"),
		registry:           store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") used
// by certs -- see store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) timersStore(region string) map[string]*time.Timer {
	if b.timers[region] == nil {
		b.timers[region] = make(map[string]*time.Timer)
	}

	return b.timers[region]
}

func (b *InMemoryBackend) idempotencyStore(region string) map[string]certIdempotencyEntry {
	if b.idempotencyMap[region] == nil {
		b.idempotencyMap[region] = make(map[string]certIdempotencyEntry)
	}

	return b.idempotencyMap[region]
}

func (b *InMemoryBackend) accountIdempotencyStore(region string) map[string]accountIdempotencyEntry {
	if b.accountIdempotency[region] == nil {
		b.accountIdempotency[region] = make(map[string]accountIdempotencyEntry)
	}

	return b.accountIdempotency[region]
}

// Reset clears all certificate state and stops any pending auto-validate timers.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, regionTimers := range b.timers {
		for _, t := range regionTimers {
			t.Stop()
		}
	}

	b.registry.ResetAll()
	// certs is a "dirty" table (hidden region field) deliberately NOT on
	// b.registry -- see store_setup.go's registerAllTables doc -- so it
	// needs its own Reset() call here.
	b.certs.Reset()
	b.timers = make(map[string]map[string]*time.Timer)
	b.idempotencyMap = make(map[string]map[string]certIdempotencyEntry)
	b.accountIdempotency = make(map[string]map[string]accountIdempotencyEntry)
	b.accountConfig = make(map[string]AccountConfig)
}

// Close stops all in-flight certificate auto-validation timers so their
// goroutines do not outlive the backend, without otherwise clearing state. It is
// safe to call multiple times.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	defer b.mu.Unlock()

	for _, regionTimers := range b.timers {
		for _, t := range regionTimers {
			t.Stop()
		}
	}
	b.timers = make(map[string]map[string]*time.Timer)
}
