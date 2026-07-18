// Package mq provides an in-memory stub of Amazon MQ.
package mq

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend stores Amazon MQ state in memory.
type InMemoryBackend struct {
	brokers        *store.Table[Broker]
	configurations *store.Table[Configuration]
	tags           map[string]map[string]string
	mu             *lockmetrics.RWMutex
	registry       *store.Registry
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new in-memory Amazon MQ backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		tags:      make(map[string]map[string]string),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("mq"),
		registry:  store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, preserving only the account ID and region.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

// isAlphanumeric reports whether c is an ASCII letter or digit. Shared by the
// broker-name and username validators.
func isAlphanumeric(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
