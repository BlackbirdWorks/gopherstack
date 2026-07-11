package eks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// eksSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 and services/ses Phase 3.3 conversions.
const eksSnapshotVersion = 1

type backendSnapshot struct {
	Tables            map[string]json.RawMessage                       `json:"tables"`
	AccessPolicies    map[string]map[string][]*AccessPolicyAssociation `json:"accessPolicies,omitempty"`
	EncryptionConfigs map[string][]EncryptionConfig                    `json:"encryptionConfigs,omitempty"`
	AccountID         string                                           `json:"accountId"`
	Region            string                                           `json:"region"`
	Version           int                                              `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "eks: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:           eksSnapshotVersion,
		Tables:            tables,
		AccessPolicies:    b.accessPolicies,
		EncryptionConfigs: b.encryptionConfigs,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "eks: Snapshot marshal failed", "error", err)

		return nil
	}

	return data
}

// restoreClusterAndNodegroupTagsLocked rebuilds Prometheus-named tag
// instances for clusters and nodegroups after a JSON round-trip, which
// deserializes every tags.Tags with the generic "json.tags" name.
// The caller MUST hold b.mu for writing.
func restoreClusterAndNodegroupTagsLocked(b *InMemoryBackend) {
	for _, c := range b.clusters.All() {
		rawTags := c.Tags.Clone()
		c.Tags.Close()
		c.Tags = tags.FromMap("eks.cluster."+c.Name+".tags", rawTags)
	}

	for _, ng := range b.nodegroups.All() {
		rawTags := ng.Tags.Clone()
		ng.Tags.Close()
		ng.Tags = tags.FromMap("eks.nodegroup."+ng.ClusterName+"."+ng.NodegroupName+".tags", rawTags)
	}
}

// restoreAccessEntryAndAddonTagsLocked rebuilds Prometheus-named tag
// instances for access entries and addons. The caller MUST hold b.mu for
// writing.
func restoreAccessEntryAndAddonTagsLocked(b *InMemoryBackend) {
	for _, e := range b.accessEntries.All() {
		rawTags := e.Tags.Clone()
		e.Tags.Close()
		e.Tags = tags.FromMap("eks.access-entry."+e.ClusterName+"."+stableID(e.PrincipalARN)+".tags", rawTags)
	}

	for _, a := range b.addons.All() {
		rawTags := a.Tags.Clone()
		a.Tags.Close()
		a.Tags = tags.FromMap("eks.addon."+a.ClusterName+"."+a.AddonName+".tags", rawTags)
	}
}

// restoreProfileAndAssocTagsLocked rebuilds Prometheus-named tag instances
// for fargate profiles, pod identity associations, identity provider
// configs, and subscriptions. The caller MUST hold b.mu for writing.
func restoreProfileAndAssocTagsLocked(b *InMemoryBackend) {
	for _, p := range b.fargateProfiles.All() {
		rawTags := p.Tags.Clone()
		p.Tags.Close()
		p.Tags = tags.FromMap("eks.fargate."+p.ClusterName+"."+p.FargateProfileName+".tags", rawTags)
	}

	for _, a := range b.podIdentityAssociations.All() {
		rawTags := a.Tags.Clone()
		a.Tags.Close()
		a.Tags = tags.FromMap("eks.podidentity."+a.ClusterName+"."+a.AssociationID+".tags", rawTags)
	}

	b.restoreIDPAndSubscriptionTagsLocked()
}

// restoreIDPAndSubscriptionTagsLocked rebuilds Prometheus-named tag
// instances for identity provider configs and subscriptions. The caller MUST
// hold b.mu for writing.
func (b *InMemoryBackend) restoreIDPAndSubscriptionTagsLocked() {
	for _, cfg := range b.identityProviderConfigs.All() {
		rawTags := cfg.Tags.Clone()
		cfg.Tags.Close()
		cfg.Tags = tags.FromMap("eks.idp."+cfg.ClusterName+"."+cfg.Name+".tags", rawTags)
	}

	for _, sub := range b.subscriptions.All() {
		rawTags := sub.Tags.Clone()
		sub.Tags.Close()
		sub.Tags = tags.FromMap("eks.subscription."+sub.ID+".tags", rawTags)
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "eks", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != eksSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"eks: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", eksSnapshotVersion)

		b.registry.ResetAll()
		b.accessPolicies = make(map[string]map[string][]*AccessPolicyAssociation)
		b.encryptionConfigs = make(map[string][]EncryptionConfig)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("eks: restore snapshot tables: %w", err)
	}

	// Rebuild tags with proper Prometheus lock names. Tags deserialized from
	// JSON use the generic "json.tags" name; we reassign to named instances.
	restoreClusterAndNodegroupTagsLocked(b)
	restoreAccessEntryAndAddonTagsLocked(b)
	restoreProfileAndAssocTagsLocked(b)

	if snap.AccessPolicies != nil {
		b.accessPolicies = snap.AccessPolicies
	} else {
		b.accessPolicies = make(map[string]map[string][]*AccessPolicyAssociation)
	}

	if snap.EncryptionConfigs != nil {
		b.encryptionConfigs = snap.EncryptionConfigs
	} else {
		b.encryptionConfigs = make(map[string][]EncryptionConfig)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
