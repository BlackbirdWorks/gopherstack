package eks

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (or nested map[string]map[string]*T) resource field on
// InMemoryBackend is replaced with a *store.Table[T], following the pattern
// established by services/ec2 (commit 12e611a4, data-driven registration) and
// services/sqs (commit 0f09d77c, DTO-registry for identity-less types). See
// pkgs/store's package doc for the underlying primitive.
//
// Every EKS resource value type already carries its own identity as a real
// JSON field (Cluster.Name, Nodegroup.ClusterName+NodegroupName,
// AccessEntry.ClusterName+PrincipalARN, Addon.ClusterName+AddonName,
// FargateProfile.ClusterName+FargateProfileName,
// PodIdentityAssociation.ClusterName+AssociationID,
// IdentityProviderConfig.ClusterName+Name, Update.ClusterName+ID,
// Capability.ClusterName+CapabilityName, AnywhereSubscription.ID) -- unlike
// the services/ses pilot, no value type here needs a json:"-" identity field
// or a DTO wrapper: every table below is "clean" and registered on
// b.registry directly.
//
// Resources that used to live in a nested map[string]map[string]*T (keyed by
// clusterName then by a local name) are now a single flat store.Table[T]
// keyed by a composite "clusterName\x00localName" string (see e.g.
// nodegroupKey) plus a secondary store.Index grouping by ClusterName, which
// replaces the "all X of cluster Y" lookup the outer map used to offer. The
// NUL byte separator is deliberate: EKS cluster names, ARNs, and UUIDs never
// contain it, so the composite key can never collide across a cluster-name /
// local-name boundary the way a printable separator (":", "/", "#") could for
// a value like a principal ARN.
//
// Two fields are deliberately NOT registered here and remain plain maps:
//   - accessPolicies (map[string]map[string][]*AccessPolicyAssociation) --
//     its values are []*AccessPolicyAssociation slices, not a single *T, so
//     it does not fit store.Table's keyed-single-value shape.
//   - encryptionConfigs (map[string][]EncryptionConfig) -- its values are
//     []EncryptionConfig slices of a non-pointer type, for the same reason.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func clusterKeyFn(v *Cluster) string { return v.Name }

// nodegroupKey builds the composite key shared by every nodegroup nested
// under a cluster. Access sites use this directly so the exact same key is
// used for Put/Get/Delete.
func nodegroupKey(clusterName, nodegroupName string) string {
	return clusterName + "\x00" + nodegroupName
}

func nodegroupKeyFn(v *Nodegroup) string { return nodegroupKey(v.ClusterName, v.NodegroupName) }

func nodegroupClusterKeyFn(v *Nodegroup) string { return v.ClusterName }

// accessEntryKey builds the composite key shared by every access entry
// nested under a cluster.
func accessEntryKey(clusterName, principalARN string) string {
	return clusterName + "\x00" + principalARN
}

func accessEntryKeyFn(v *AccessEntry) string { return accessEntryKey(v.ClusterName, v.PrincipalARN) }

func accessEntryClusterKeyFn(v *AccessEntry) string { return v.ClusterName }

// identityProviderConfigKey builds the composite key shared by every identity
// provider config nested under a cluster.
func identityProviderConfigKey(clusterName, name string) string {
	return clusterName + "\x00" + name
}

func identityProviderConfigKeyFn(v *IdentityProviderConfig) string {
	return identityProviderConfigKey(v.ClusterName, v.Name)
}

func identityProviderConfigClusterKeyFn(v *IdentityProviderConfig) string { return v.ClusterName }

// addonKey builds the composite key shared by every addon nested under a cluster.
func addonKey(clusterName, addonName string) string {
	return clusterName + "\x00" + addonName
}

func addonKeyFn(v *Addon) string { return addonKey(v.ClusterName, v.AddonName) }

func addonClusterKeyFn(v *Addon) string { return v.ClusterName }

// fargateProfileKey builds the composite key shared by every Fargate profile
// nested under a cluster.
func fargateProfileKey(clusterName, profileName string) string {
	return clusterName + "\x00" + profileName
}

func fargateProfileKeyFn(v *FargateProfile) string {
	return fargateProfileKey(v.ClusterName, v.FargateProfileName)
}

func fargateProfileClusterKeyFn(v *FargateProfile) string { return v.ClusterName }

// podIdentityAssociationKey builds the composite key shared by every pod
// identity association nested under a cluster.
func podIdentityAssociationKey(clusterName, associationID string) string {
	return clusterName + "\x00" + associationID
}

func podIdentityAssociationKeyFn(v *PodIdentityAssociation) string {
	return podIdentityAssociationKey(v.ClusterName, v.AssociationID)
}

func podIdentityAssociationClusterKeyFn(v *PodIdentityAssociation) string { return v.ClusterName }

// capabilityKey builds the composite key shared by every capability nested
// under a cluster (CapabilityName is unique per cluster, not globally).
func capabilityKey(clusterName, capabilityName string) string {
	return clusterName + "\x00" + capabilityName
}

func capabilityKeyFn(v *Capability) string { return capabilityKey(v.ClusterName, v.CapabilityName) }

func capabilityClusterKeyFn(v *Capability) string { return v.ClusterName }

func subscriptionKeyFn(v *AnywhereSubscription) string { return v.ID }

// updateKey builds the composite key shared by every update record nested
// under a cluster.
func updateKey(clusterName, id string) string {
	return clusterName + "\x00" + id
}

func updateKeyFn(v *Update) string { return updateKey(v.ClusterName, v.ID) }

func updateClusterKeyFn(v *Update) string { return v.ClusterName }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. It must be called during construction
// only, never on every Reset(): store.Register panics on a duplicate name, so
// runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.clusters = store.Register(b.registry, "clusters", store.New(clusterKeyFn))

	b.nodegroups = store.Register(b.registry, "nodegroups", store.New(nodegroupKeyFn))
	b.nodegroupsByCluster = b.nodegroups.AddIndex("byCluster", nodegroupClusterKeyFn)

	b.accessEntries = store.Register(b.registry, "accessEntries", store.New(accessEntryKeyFn))
	b.accessEntriesByCluster = b.accessEntries.AddIndex("byCluster", accessEntryClusterKeyFn)

	b.identityProviderConfigs = store.Register(
		b.registry,
		"identityProviderConfigs",
		store.New(identityProviderConfigKeyFn),
	)
	b.identityProviderConfigsByCluster = b.identityProviderConfigs.AddIndex(
		"byCluster",
		identityProviderConfigClusterKeyFn,
	)

	b.addons = store.Register(b.registry, "addons", store.New(addonKeyFn))
	b.addonsByCluster = b.addons.AddIndex("byCluster", addonClusterKeyFn)

	b.fargateProfiles = store.Register(b.registry, "fargateProfiles", store.New(fargateProfileKeyFn))
	b.fargateProfilesByCluster = b.fargateProfiles.AddIndex("byCluster", fargateProfileClusterKeyFn)

	b.podIdentityAssociations = store.Register(
		b.registry,
		"podIdentityAssociations",
		store.New(podIdentityAssociationKeyFn),
	)
	b.podIdentityAssociationsByCluster = b.podIdentityAssociations.AddIndex(
		"byCluster",
		podIdentityAssociationClusterKeyFn,
	)

	b.capabilities = store.Register(b.registry, "capabilities", store.New(capabilityKeyFn))
	b.capabilitiesByCluster = b.capabilities.AddIndex("byCluster", capabilityClusterKeyFn)

	b.subscriptions = store.Register(b.registry, "subscriptions", store.New(subscriptionKeyFn))

	b.updates = store.Register(b.registry, "updates", store.New(updateKeyFn))
	b.updatesByCluster = b.updates.AddIndex("byCluster", updateClusterKeyFn)
}
