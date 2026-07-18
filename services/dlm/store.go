package dlm

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	registry  *store.Registry
	policies  *store.Table[storedPolicy] // policyID → policy
	accountID string
	region    string
	counter   int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("dlm"),
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// findPolicyByARNLocked returns the policy matching the given ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findPolicyByARNLocked(policyARN string) (*storedPolicy, bool) {
	var found *storedPolicy

	b.policies.Range(func(p *storedPolicy) bool {
		if p.PolicyArn == policyARN {
			found = p

			return false
		}

		return true
	})

	return found, found != nil
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.counter = 0
}
