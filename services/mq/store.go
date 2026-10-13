// Package mq provides an in-memory Amazon MQ with optional docker-backed brokers.
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
	engine         *brokerEngine
	shares         ResourceShareResolver
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

// ResourceShareResolver looks up RAM resource shares by ARN.
type ResourceShareResolver interface {
	// ResourceShareResources returns the resource ARNs associated with the share; found is false
	// when the share does not exist.
	ResourceShareResources(shareARN string) (resourceARNs []string, found bool)
}

// SetResourceShareResolver wires DescribeSharedResources to RAM.
func (b *InMemoryBackend) SetResourceShareResolver(r ResourceShareResolver) {
	b.mu.Lock("SetResourceShareResolver")
	defer b.mu.Unlock()

	b.shares = r
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, preserving only the account ID and region.
func (b *InMemoryBackend) Reset() {
	var lbs []*liveBroker

	defer func() { b.reapDetached(lbs...) }()

	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	lbs = b.detachAllBrokersLocked()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

// isAlphanumeric reports whether c is an ASCII letter or digit. Shared by the
// broker-name and username validators.
func isAlphanumeric(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
