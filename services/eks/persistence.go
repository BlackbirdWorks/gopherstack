package eks

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Clusters                map[string]*Cluster                              `json:"clusters"`
	Nodegroups              map[string]map[string]*Nodegroup                 `json:"nodegroups"`
	AccessEntries           map[string]map[string]*AccessEntry               `json:"accessEntries,omitempty"`
	AccessPolicies          map[string]map[string][]*AccessPolicyAssociation `json:"accessPolicies,omitempty"`
	EncryptionConfigs       map[string][]EncryptionConfig                    `json:"encryptionConfigs,omitempty"`
	IdentityProviderConfigs map[string]map[string]*IdentityProviderConfig    `json:"identityProviderConfigs,omitempty"`
	Addons                  map[string]map[string]*Addon                     `json:"addons,omitempty"`
	FargateProfiles         map[string]map[string]*FargateProfile            `json:"fargateProfiles,omitempty"`
	PodIdentityAssociations map[string]map[string]*PodIdentityAssociation    `json:"podIdentityAssociations,omitempty"`
	Capabilities            map[string]*Capability                           `json:"capabilities,omitempty"`
	Subscriptions           map[string]*AnywhereSubscription                 `json:"subscriptions,omitempty"`
	AccountID               string                                           `json:"accountId"`
	Region                  string                                           `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:                b.clusters,
		Nodegroups:              b.nodegroups,
		AccessEntries:           b.accessEntries,
		AccessPolicies:          b.accessPolicies,
		EncryptionConfigs:       b.encryptionConfigs,
		IdentityProviderConfigs: b.identityProviderConfigs,
		Addons:                  b.addons,
		FargateProfiles:         b.fargateProfiles,
		PodIdentityAssociations: b.podIdentityAssociations,
		Capabilities:            b.capabilities,
		Subscriptions:           b.subscriptions,
		AccountID:               b.accountID,
		Region:                  b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "eks: Snapshot marshal failed", "error", err)

		return nil
	}

	return data
}

// ensureNonNilSnap initialises any nil map fields in the snapshot to empty maps
// so that subsequent range iterations are safe.
func (snap *backendSnapshot) ensureNonNilSnap() {
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*Cluster)
	}

	if snap.Nodegroups == nil {
		snap.Nodegroups = make(map[string]map[string]*Nodegroup)
	}

	if snap.AccessEntries == nil {
		snap.AccessEntries = make(map[string]map[string]*AccessEntry)
	}

	if snap.AccessPolicies == nil {
		snap.AccessPolicies = make(map[string]map[string][]*AccessPolicyAssociation)
	}

	if snap.EncryptionConfigs == nil {
		snap.EncryptionConfigs = make(map[string][]EncryptionConfig)
	}

	if snap.IdentityProviderConfigs == nil {
		snap.IdentityProviderConfigs = make(map[string]map[string]*IdentityProviderConfig)
	}

	if snap.Addons == nil {
		snap.Addons = make(map[string]map[string]*Addon)
	}

	if snap.FargateProfiles == nil {
		snap.FargateProfiles = make(map[string]map[string]*FargateProfile)
	}

	if snap.PodIdentityAssociations == nil {
		snap.PodIdentityAssociations = make(map[string]map[string]*PodIdentityAssociation)
	}

	if snap.Capabilities == nil {
		snap.Capabilities = make(map[string]*Capability)
	}

	if snap.Subscriptions == nil {
		snap.Subscriptions = make(map[string]*AnywhereSubscription)
	}
}

// restorePrimaryTagsLocked rebuilds Prometheus-named tag instances for clusters,
// nodegroups, access entries, and addons.
func restorePrimaryTagsLocked(snap *backendSnapshot) {
	for name, c := range snap.Clusters {
		rawTags := c.Tags.Clone()
		c.Tags.Close()
		c.Tags = tags.FromMap("eks.cluster."+name+".tags", rawTags)
	}

	for clusterName, ngs := range snap.Nodegroups {
		if ngs == nil {
			snap.Nodegroups[clusterName] = make(map[string]*Nodegroup)

			continue
		}

		for ngName, ng := range ngs {
			rawTags := ng.Tags.Clone()
			ng.Tags.Close()
			ng.Tags = tags.FromMap("eks.nodegroup."+clusterName+"."+ngName+".tags", rawTags)
		}
	}

	for clusterName, entries := range snap.AccessEntries {
		if entries == nil {
			snap.AccessEntries[clusterName] = make(map[string]*AccessEntry)

			continue
		}

		for _, e := range entries {
			rawTags := e.Tags.Clone()
			e.Tags.Close()
			e.Tags = tags.FromMap("eks.access-entry."+clusterName+"."+stableID(e.PrincipalARN)+".tags", rawTags)
		}
	}

	for clusterName, clusterAddons := range snap.Addons {
		if clusterAddons == nil {
			snap.Addons[clusterName] = make(map[string]*Addon)

			continue
		}

		for addonName, a := range clusterAddons {
			rawTags := a.Tags.Clone()
			a.Tags.Close()
			a.Tags = tags.FromMap("eks.addon."+clusterName+"."+addonName+".tags", rawTags)
		}
	}
}

// restoreNewOpsTagsLocked rebuilds Prometheus-named tag instances for fargate
// profiles, pod identity associations, identity provider configs, and subscriptions.
func restoreNewOpsTagsLocked(snap *backendSnapshot) {
	for clusterName, profiles := range snap.FargateProfiles {
		if profiles == nil {
			snap.FargateProfiles[clusterName] = make(map[string]*FargateProfile)

			continue
		}

		for profileName, p := range profiles {
			rawTags := p.Tags.Clone()
			p.Tags.Close()
			p.Tags = tags.FromMap("eks.fargate."+clusterName+"."+profileName+".tags", rawTags)
		}
	}

	for clusterName, assocs := range snap.PodIdentityAssociations {
		if assocs == nil {
			snap.PodIdentityAssociations[clusterName] = make(map[string]*PodIdentityAssociation)

			continue
		}

		for assocID, a := range assocs {
			rawTags := a.Tags.Clone()
			a.Tags.Close()
			a.Tags = tags.FromMap("eks.podidentity."+clusterName+"."+assocID+".tags", rawTags)
		}
	}

	for clusterName, idpCfgs := range snap.IdentityProviderConfigs {
		if idpCfgs == nil {
			snap.IdentityProviderConfigs[clusterName] = make(map[string]*IdentityProviderConfig)

			continue
		}

		for name, cfg := range idpCfgs {
			rawTags := cfg.Tags.Clone()
			cfg.Tags.Close()
			cfg.Tags = tags.FromMap("eks.idp."+clusterName+"."+name+".tags", rawTags)
		}
	}

	for id, sub := range snap.Subscriptions {
		rawTags := sub.Tags.Clone()
		sub.Tags.Close()
		sub.Tags = tags.FromMap("eks.subscription."+id+".tags", rawTags)
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	snap.ensureNonNilSnap()

	// Rebuild tags with proper Prometheus lock names. Tags deserialized from
	// JSON use the generic "json.tags" name; we reassign to named instances.
	restorePrimaryTagsLocked(&snap)
	restoreNewOpsTagsLocked(&snap)

	b.clusters = snap.Clusters
	b.nodegroups = snap.Nodegroups
	b.accessEntries = snap.AccessEntries
	b.accessPolicies = snap.AccessPolicies
	b.encryptionConfigs = snap.EncryptionConfigs
	b.identityProviderConfigs = snap.IdentityProviderConfigs
	b.addons = snap.Addons
	b.fargateProfiles = snap.FargateProfiles
	b.podIdentityAssociations = snap.PodIdentityAssociations
	b.capabilities = snap.Capabilities
	b.subscriptions = snap.Subscriptions
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
