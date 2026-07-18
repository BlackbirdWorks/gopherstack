// Package cloudcontrol provides an in-memory implementation of the AWS CloudControl API service.
package cloudcontrol

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is a thread-safe in-memory store for CloudControl resources.
type InMemoryBackend struct {
	registry     *store.Registry
	resources    *store.Table[Resource]      // key: typeName+"/"+identifier
	requests     *store.Table[ProgressEvent] // key: requestToken
	clientTokens map[string]string           // clientToken → requestToken (idempotency)
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		clientTokens: make(map[string]string),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("cloudcontrol"),
		registry:     store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state from the backend, returning it to a clean initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.clientTokens = make(map[string]string)
}
