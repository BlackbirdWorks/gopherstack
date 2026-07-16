package eks

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// addonTransitionDelay is the async delay before a CREATING addon reaches ACTIVE.
const addonTransitionDelay = 100 * time.Millisecond

const addonVPCCNI = "vpc-cni"

// defaultAddonVersion returns a realistic default addon version for well-known addons.
func defaultAddonVersion(addonName string) string {
	switch addonName {
	case addonVPCCNI:
		return "v1.18.5-eksbuild.1"
	case "coredns":
		return "v1.11.4-eksbuild.2"
	case "kube-proxy":
		return "v1.32.0-eksbuild.1"
	case "aws-ebs-csi-driver":
		return "v1.37.0-eksbuild.1"
	case "aws-efs-csi-driver":
		return "v2.1.0-eksbuild.1"
	default:
		return "v1.16.1-eksbuild.1"
	}
}

const (
	resolveConflictsOverwrite = "OVERWRITE"
	resolveConflictsNone      = "NONE"
	resolveConflictsPreserve  = "PRESERVE"
)

// isValidResolveConflicts reports whether s is an accepted resolveConflicts value.
func isValidResolveConflicts(s string) bool {
	return s == resolveConflictsOverwrite || s == resolveConflictsNone || s == resolveConflictsPreserve
}

// CreateAddon creates a new managed add-on in a cluster.
func (b *InMemoryBackend) CreateAddon(
	clusterName, addonName, addonVersion, serviceAccountRoleARN, configuration, resolveConflicts string,
	kv map[string]string,
) (*Addon, error) {
	b.mu.Lock("CreateAddon")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.addons.Get(addonKey(clusterName, addonName)); ok {
		return nil, fmt.Errorf("%w: addon %s already exists in cluster %s", ErrAlreadyExists, addonName, clusterName)
	}

	if resolveConflicts != "" && !isValidResolveConflicts(resolveConflicts) {
		return nil, fmt.Errorf(
			"%w: resolveConflicts %q must be one of OVERWRITE, NONE, PRESERVE",
			ErrValidation, resolveConflicts,
		)
	}

	addonARN := arn.Build(
		"eks",
		b.region,
		b.accountID,
		"addon/"+clusterName+"/"+addonName+"/"+stableID(clusterName+"/"+addonName),
	)

	t := tags.New("eks.addon." + clusterName + "." + addonName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if addonVersion == "" {
		addonVersion = defaultAddonVersion(addonName)
	}

	addon := &Addon{
		ClusterName:        clusterName,
		AddonName:          addonName,
		ARN:                addonARN,
		AddonVersion:       addonVersion,
		MarketplaceVersion: addonVersion,
		Health: &AddonHealth{
			Issues: []map[string]string{},
		},
		ServiceAccountRoleARN: serviceAccountRoleARN,
		Status:                statusCreating,
		CreatedAt:             time.Now().UTC(),
		Tags:                  t,
		Configuration:         configuration,
		ResolveConflicts:      resolveConflicts,
	}
	b.addons.Put(addon)

	b.work.After("AddonTransition", addonTransitionDelay, func() {
		b.mu.Lock("CreateAddon-async")
		defer b.mu.Unlock()

		if a, ok := b.addons.Get(addonKey(clusterName, addonName)); ok && a.Status == statusCreating {
			a.Status = statusActive
		}
	})

	cp := *addon

	return &cp, nil
}

// DeleteAddon removes an add-on from a cluster.
func (b *InMemoryBackend) DeleteAddon(clusterName, addonName string) (*Addon, error) {
	b.mu.Lock("DeleteAddon")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	addon, ok := b.addons.Get(addonKey(clusterName, addonName))
	if !ok {
		return nil, fmt.Errorf("%w: addon %s not found in cluster %s", ErrNotFound, addonName, clusterName)
	}

	cp := *addon
	if addon.Tags != nil {
		addon.Tags.Close()
	}

	b.addons.Delete(addonKey(clusterName, addonName))

	cp.Status = statusDeleting

	return &cp, nil
}

// DescribeAddon returns an add-on by cluster and add-on name.
func (b *InMemoryBackend) DescribeAddon(clusterName, addonName string) (*Addon, error) {
	b.mu.RLock("DescribeAddon")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	addon, ok := b.addons.Get(addonKey(clusterName, addonName))
	if !ok {
		return nil, fmt.Errorf("%w: addon %s not found in cluster %s", ErrNotFound, addonName, clusterName)
	}

	cp := *addon

	return &cp, nil
}

// ListAddons returns all add-on names in a cluster sorted alphabetically.
func (b *InMemoryBackend) ListAddons(clusterName string) ([]string, error) {
	b.mu.RLock("ListAddons")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	items := b.addonsByCluster.Get(clusterName)
	names := make([]string, len(items))

	for i, a := range items {
		names[i] = a.AddonName
	}

	slices.Sort(names)

	return names, nil
}

// UpdateAddon updates an existing add-on.
func (b *InMemoryBackend) UpdateAddon(
	clusterName, addonName, addonVersion, serviceAccountRoleARN, configuration, resolveConflicts string,
) (*Addon, error) {
	b.mu.Lock("UpdateAddon")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	addon, ok := b.addons.Get(addonKey(clusterName, addonName))
	if !ok {
		return nil, fmt.Errorf("%w: addon %s not found in cluster %s", ErrNotFound, addonName, clusterName)
	}

	if resolveConflicts != "" && !isValidResolveConflicts(resolveConflicts) {
		return nil, fmt.Errorf(
			"%w: resolveConflicts %q must be one of OVERWRITE, NONE, PRESERVE",
			ErrValidation, resolveConflicts,
		)
	}

	if addonVersion != "" {
		addon.AddonVersion = addonVersion
	}

	if serviceAccountRoleARN != "" {
		addon.ServiceAccountRoleARN = serviceAccountRoleARN
	}

	if configuration != "" {
		addon.Configuration = configuration
	}

	if resolveConflicts != "" {
		addon.ResolveConflicts = resolveConflicts
	}

	cp := *addon

	return &cp, nil
}

// DescribeAddonVersions returns static addon version metadata.
func (b *InMemoryBackend) DescribeAddonVersions() []map[string]any {
	return []map[string]any{
		{
			keyAddonName: addonVPCCNI,
			keyType:      keyNetworking,
			keyAddonVersions: []map[string]any{
				{
					keyAddonVersion: "v1.18.5-eksbuild.1",
					keyCompatibilities: []map[string]string{
						{keyClusterVersion: defaultK8sVersion},
						{keyClusterVersion: priorK8sVersion},
					},
				},
				{
					keyAddonVersion:    "v1.17.1-eksbuild.1",
					keyCompatibilities: []map[string]string{{keyClusterVersion: "1.30"}, {keyClusterVersion: "1.29"}},
				},
			},
		},
		{
			keyAddonName: "coredns",
			keyType:      keyNetworking,
			keyAddonVersions: []map[string]any{
				{
					keyAddonVersion: "v1.11.4-eksbuild.2",
					keyCompatibilities: []map[string]string{
						{keyClusterVersion: defaultK8sVersion},
						{keyClusterVersion: priorK8sVersion},
					},
				},
			},
		},
		{
			keyAddonName: "kube-proxy",
			keyType:      keyNetworking,
			keyAddonVersions: []map[string]any{
				{
					keyAddonVersion:    "v1.32.0-eksbuild.1",
					keyCompatibilities: []map[string]string{{keyClusterVersion: defaultK8sVersion}},
				},
				{
					keyAddonVersion:    "v1.31.3-eksbuild.1",
					keyCompatibilities: []map[string]string{{keyClusterVersion: priorK8sVersion}},
				},
			},
		},
		{
			keyAddonName: "aws-ebs-csi-driver",
			keyType:      "storage",
			keyAddonVersions: []map[string]any{
				{
					keyAddonVersion: "v1.37.0-eksbuild.1",
					keyCompatibilities: []map[string]string{
						{keyClusterVersion: defaultK8sVersion},
						{keyClusterVersion: priorK8sVersion},
					},
				},
			},
		},
		{
			keyAddonName: "aws-efs-csi-driver",
			keyType:      "storage",
			keyAddonVersions: []map[string]any{
				{
					keyAddonVersion: "v2.1.0-eksbuild.1",
					keyCompatibilities: []map[string]string{
						{keyClusterVersion: defaultK8sVersion},
						{keyClusterVersion: priorK8sVersion},
					},
				},
			},
		},
	}
}

// DescribeAddonConfiguration returns static addon configuration schema.
func (b *InMemoryBackend) DescribeAddonConfiguration(addonName, addonVersion string) map[string]any {
	return map[string]any{
		keyAddonName:    addonName,
		keyAddonVersion: addonVersion,
		"configurationSchema": map[string]any{
			keyType:      "object",
			"properties": map[string]any{},
		},
	}
}

// AddAddonInternal inserts a pre-built add-on into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddAddonInternal(a *Addon) {
	b.mu.Lock("AddAddonInternal")
	defer b.mu.Unlock()

	if a.Tags == nil {
		a.Tags = tags.New("eks.addon." + a.ClusterName + "." + a.AddonName + ".tags")
	}

	b.addons.Put(a)
}

// ListAllAddons returns all addons across all clusters.
func (b *InMemoryBackend) ListAllAddons() []*Addon {
	b.mu.RLock("ListAllAddons")
	defer b.mu.RUnlock()

	items := b.addons.All()
	list := make([]*Addon, 0, len(items))

	for _, a := range items {
		cp := *a
		list = append(list, &cp)
	}

	return list
}
