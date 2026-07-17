package shield

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// protectionIDBytes is the number of random bytes used for protection/group IDs.
const protectionIDBytes = 16

// newShieldID generates a new random hex ID (16 bytes = 32 hex chars).
// Must be called without holding the lock.
func newShieldID() string {
	b := make([]byte, protectionIDBytes)

	if _, err := rand.Read(b); err != nil {
		// Fallback: deterministic counter - practically unreachable.
		return hex.EncodeToString([]byte("fallback-shield-id"))
	}

	return hex.EncodeToString(b)
}

// InMemoryBackend is an in-memory store for Shield Advanced resources.
//
// protections, protectionGroups, and attacks are *store.Table[T] (see
// store_setup.go), with the former one-to-one ARN/name reverse-lookup maps
// replaced by companion *store.Index values on protections. alarConfigs is
// also a *store.Table[ALARConfig] but is not registered on registry -- see
// store_setup.go's file doc comment.
type InMemoryBackend struct {
	protections               *store.Table[Protection]
	protectionsByResourceARN  *store.Index[Protection]
	protectionsByName         *store.Index[Protection]
	protectionGroups          *store.Table[ProtectionGroup]
	attacks                   *store.Table[Attack]
	alarConfigs               *store.Table[ALARConfig]
	registry                  *store.Registry
	subscription              *Subscription
	drtAccess                 *DRTAccess
	mu                        *lockmetrics.RWMutex
	accountID                 string
	region                    string
	proactiveEngagementStatus string
	emergencyContacts         []EmergencyContact
}

// NewInMemoryBackend creates a new in-memory Shield backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("shield"),
	}

	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all Shield protections and subscription state atomically.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.registry.ResetAll()
	b.alarConfigs.Reset()
	b.subscription = nil
	b.drtAccess = nil
	b.emergencyContacts = nil
	b.proactiveEngagementStatus = ""
	b.mu.Unlock()
}
