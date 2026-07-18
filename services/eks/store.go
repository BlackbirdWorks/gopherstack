package eks

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// InMemoryBackend is the in-memory store for EKS resources.
//
// Phase 3.3 (pkgs/store conversion): every map[string]*T (and nested
// map[string]map[string]*T) resource field is a *store.Table[T] registered
// once on registry -- see store_setup.go. Two fields are deliberately left as
// plain maps because their values are not *T (accessPolicies holds
// []*AccessPolicyAssociation slices; encryptionConfigs holds
// []EncryptionConfig value slices), which does not fit store.Table's
// keyed-single-value shape -- see the comment above registerAllTables.
type InMemoryBackend struct {
	clusters                         *store.Table[Cluster]
	nodegroups                       *store.Table[Nodegroup]
	nodegroupsByCluster              *store.Index[Nodegroup]
	accessEntries                    *store.Table[AccessEntry]
	accessEntriesByCluster           *store.Index[AccessEntry]
	accessPolicies                   map[string]map[string][]*AccessPolicyAssociation
	encryptionConfigs                map[string][]EncryptionConfig
	identityProviderConfigs          *store.Table[IdentityProviderConfig]
	identityProviderConfigsByCluster *store.Index[IdentityProviderConfig]
	addons                           *store.Table[Addon]
	addonsByCluster                  *store.Index[Addon]
	fargateProfiles                  *store.Table[FargateProfile]
	fargateProfilesByCluster         *store.Index[FargateProfile]
	podIdentityAssociations          *store.Table[PodIdentityAssociation]
	podIdentityAssociationsByCluster *store.Index[PodIdentityAssociation]
	capabilities                     *store.Table[Capability]
	capabilitiesByCluster            *store.Index[Capability]
	subscriptions                    *store.Table[AnywhereSubscription]
	updates                          *store.Table[Update]
	updatesByCluster                 *store.Index[Update]
	registry                         *store.Registry
	mu                               *lockmetrics.RWMutex
	work                             *worker.Group
	accountID                        string
	region                           string
}

// NewInMemoryBackend creates a new in-memory EKS backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accessPolicies:    make(map[string]map[string][]*AccessPolicyAssociation),
		encryptionConfigs: make(map[string][]EncryptionConfig),
		accountID:         accountID,
		region:            region,
		registry:          store.NewRegistry(),
		mu:                lockmetrics.New("eks"),
		work:              worker.NewGroup(ctx, "eks"),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// Close stops all scheduled state-transition timers so none outlives the
// backend. It is safe to call multiple times.
func (b *InMemoryBackend) Close() { b.work.Stop() }

// closeClusterTagsLocked closes tag objects for clusters and nodegroups.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeClusterTagsLocked() {
	b.clusters.Range(func(c *Cluster) bool {
		if c.Tags != nil {
			c.Tags.Close()
		}

		return true
	})

	b.nodegroups.Range(func(ng *Nodegroup) bool {
		if ng.Tags != nil {
			ng.Tags.Close()
		}

		return true
	})
}

// closeEntryTagsLocked closes tag objects for access entries and addons.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeEntryTagsLocked() {
	b.accessEntries.Range(func(e *AccessEntry) bool {
		if e.Tags != nil {
			e.Tags.Close()
		}

		return true
	})

	b.addons.Range(func(a *Addon) bool {
		if a.Tags != nil {
			a.Tags.Close()
		}

		return true
	})
}

// closeProfileTagsLocked closes tag objects for fargate profiles, pod identity
// associations, identity provider configs, and subscriptions.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeProfileTagsLocked() {
	b.fargateProfiles.Range(func(p *FargateProfile) bool {
		if p.Tags != nil {
			p.Tags.Close()
		}

		return true
	})

	b.podIdentityAssociations.Range(func(a *PodIdentityAssociation) bool {
		if a.Tags != nil {
			a.Tags.Close()
		}

		return true
	})

	b.closeIDPAndSubscriptionTagsLocked()
}

func (b *InMemoryBackend) closeIDPAndSubscriptionTagsLocked() {
	b.identityProviderConfigs.Range(func(cfg *IdentityProviderConfig) bool {
		if cfg.Tags != nil {
			cfg.Tags.Close()
		}

		return true
	})

	b.capabilities.Range(func(capa *Capability) bool {
		if capa.Tags != nil {
			capa.Tags.Close()
		}

		return true
	})

	b.subscriptions.Range(func(sub *AnywhereSubscription) bool {
		if sub.Tags != nil {
			sub.Tags.Close()
		}

		return true
	})
}

// Reset clears all state, returning the backend to a fresh empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all tag resources to deregister Prometheus labels.
	b.closeClusterTagsLocked()
	b.closeEntryTagsLocked()
	b.closeProfileTagsLocked()

	// Reset every store.Table-backed resource in one call instead of the
	// per-map make() calls this used to be (Phase 3.3 pkgs/store conversion).
	// See registerAllTables in store_setup.go for the full list of tables.
	b.registry.ResetAll()

	b.accessPolicies = make(map[string]map[string][]*AccessPolicyAssociation)
	b.encryptionConfigs = make(map[string][]EncryptionConfig)
}
