package eks

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	keyNetworking               = "networking"
	keyCompatibilities          = "compatibilities"
	keyClusterVersion           = "clusterVersion"
	keyDefaultVersion           = "defaultVersion"
	keyEndOfStandardSupportDate = "endOfStandardSupportDate"
	keyEndOfExtendedSupportDate = "endOfExtendedSupportDate"
	strFalse                    = "false"
)

const (
	keyAddonName         = "addonName"
	keyAddonVersions     = "addonVersions"
	keyAddonVersion      = "addonVersion"
	typeUpgradeReadiness = "UPGRADE_READINESS"
	typeVersionUpdate    = "VersionUpdate"
)

const (
	statusPassing    = "PASSING"
	statusCreating   = "CREATING"
	statusFailed     = "FAILED"
	statusDegraded   = "DEGRADED"
	statusDeleting   = "DELETING"
	statusSuccessful = "Successful"
)

// Insight represents an EKS cluster insight.
type Insight struct {
	LastRefreshTime time.Time         `json:"lastRefreshTime"`
	LastTransition  time.Time         `json:"lastTransitionTime"`
	AdditionalInfo  map[string]string `json:"additionalInfo,omitempty"`
	ID              string            `json:"id"`
	ClusterName     string            `json:"clusterName"`
	Category        string            `json:"category"`
	Status          string            `json:"status"`
	Description     string            `json:"description,omitempty"`
	Recommendation  string            `json:"recommendation,omitempty"`
}

// InsightsRefresh represents an EKS insights refresh request.
type InsightsRefresh struct {
	StartedAt   time.Time `json:"startedAt"`
	ID          string    `json:"id"`
	ClusterName string    `json:"clusterName"`
	Status      string    `json:"status"`
}

// UpdateParam represents a single parameter changed by an EKS update operation.
type UpdateParam struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// UpdateError represents an error encountered during an EKS update.
type UpdateError struct {
	ErrorCode    string   `json:"errorCode"`
	ErrorMessage string   `json:"errorMessage"`
	ResourceIDs  []string `json:"resourceIds,omitempty"`
}

// Update represents an EKS update record.
type Update struct {
	CreatedAt   time.Time     `json:"createdAt"`
	ID          string        `json:"id"`
	ClusterName string        `json:"clusterName"`
	Status      string        `json:"status"`
	Type        string        `json:"type"`
	Params      []UpdateParam `json:"params,omitempty"`
	Errors      []UpdateError `json:"errors,omitempty"`
}

// --- Addon CRUD ---

// DeleteAddon removes an add-on from a cluster.
func (b *InMemoryBackend) DeleteAddon(clusterName, addonName string) (*Addon, error) {
	b.mu.Lock("DeleteAddon")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	addons := b.addons[clusterName]
	if addons == nil {
		return nil, fmt.Errorf("%w: addon %s not found in cluster %s", ErrNotFound, addonName, clusterName)
	}

	addon, ok := addons[addonName]
	if !ok {
		return nil, fmt.Errorf("%w: addon %s not found in cluster %s", ErrNotFound, addonName, clusterName)
	}

	cp := *addon
	if addon.Tags != nil {
		addon.Tags.Close()
	}

	delete(addons, addonName)

	cp.Status = statusDeleting

	return &cp, nil
}

// DescribeAddon returns an add-on by cluster and add-on name.
func (b *InMemoryBackend) DescribeAddon(clusterName, addonName string) (*Addon, error) {
	b.mu.RLock("DescribeAddon")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	addons := b.addons[clusterName]
	if addons == nil {
		return nil, fmt.Errorf("%w: addon %s not found in cluster %s", ErrNotFound, addonName, clusterName)
	}

	addon, ok := addons[addonName]
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

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	addons := b.addons[clusterName]
	names := make([]string, 0, len(addons))

	for name := range addons {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// UpdateAddon updates an existing add-on.
func (b *InMemoryBackend) UpdateAddon(
	clusterName, addonName, addonVersion, serviceAccountRoleARN, configuration, resolveConflicts string,
) (*Addon, error) {
	b.mu.Lock("UpdateAddon")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	addons := b.addons[clusterName]
	if addons == nil {
		return nil, fmt.Errorf("%w: addon %s not found in cluster %s", ErrNotFound, addonName, clusterName)
	}

	addon, ok := addons[addonName]
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

// --- Capability CRUD ---

// DeleteCapability removes a capability by name.
func (b *InMemoryBackend) DeleteCapability(name string) (*Capability, error) {
	b.mu.Lock("DeleteCapability")
	defer b.mu.Unlock()

	capa, ok := b.capabilities[name]
	if !ok {
		return nil, fmt.Errorf("%w: capability %s not found", ErrNotFound, name)
	}

	cp := *capa
	delete(b.capabilities, name)

	cp.Status = statusDeleting

	return &cp, nil
}

// DescribeCapability returns a capability by name.
func (b *InMemoryBackend) DescribeCapability(name string) (*Capability, error) {
	b.mu.RLock("DescribeCapability")
	defer b.mu.RUnlock()

	capa, ok := b.capabilities[name]
	if !ok {
		return nil, fmt.Errorf("%w: capability %s not found", ErrNotFound, name)
	}

	cp := *capa

	return &cp, nil
}

// ListCapabilities returns all capability names sorted alphabetically.
func (b *InMemoryBackend) ListCapabilities() []string {
	b.mu.RLock("ListCapabilities")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.capabilities))
	for name := range b.capabilities {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// UpdateCapability updates an existing capability.
func (b *InMemoryBackend) UpdateCapability(name, version string) (*Capability, error) {
	b.mu.Lock("UpdateCapability")
	defer b.mu.Unlock()

	capa, ok := b.capabilities[name]
	if !ok {
		return nil, fmt.Errorf("%w: capability %s not found", ErrNotFound, name)
	}

	if version != "" {
		capa.Version = version
	}

	cp := *capa

	return &cp, nil
}

// --- EKS Anywhere Subscription CRUD ---

// DeleteEksAnywhereSubscription removes a subscription by ID.
func (b *InMemoryBackend) DeleteEksAnywhereSubscription(id string) (*AnywhereSubscription, error) {
	b.mu.Lock("DeleteEksAnywhereSubscription")
	defer b.mu.Unlock()

	sub, ok := b.subscriptions[id]
	if !ok {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrNotFound, id)
	}

	cp := *sub

	if sub.Tags != nil {
		sub.Tags.Close()
	}

	delete(b.subscriptions, id)

	cp.Status = statusDeleting

	return &cp, nil
}

// DescribeEksAnywhereSubscription returns a subscription by ID.
func (b *InMemoryBackend) DescribeEksAnywhereSubscription(id string) (*AnywhereSubscription, error) {
	b.mu.RLock("DescribeEksAnywhereSubscription")
	defer b.mu.RUnlock()

	sub, ok := b.subscriptions[id]
	if !ok {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrNotFound, id)
	}

	cp := *sub

	return &cp, nil
}

// ListEksAnywhereSubscriptions returns all subscription IDs sorted.
func (b *InMemoryBackend) ListEksAnywhereSubscriptions() []*AnywhereSubscription {
	b.mu.RLock("ListEksAnywhereSubscriptions")
	defer b.mu.RUnlock()

	list := make([]*AnywhereSubscription, 0, len(b.subscriptions))
	for _, sub := range b.subscriptions {
		cp := *sub
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateEksAnywhereSubscription updates a subscription.
func (b *InMemoryBackend) UpdateEksAnywhereSubscription(
	id string, licenseQuantity *int32, licenseType string,
) (*AnywhereSubscription, error) {
	b.mu.Lock("UpdateEksAnywhereSubscription")
	defer b.mu.Unlock()

	sub, ok := b.subscriptions[id]
	if !ok {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrNotFound, id)
	}

	if licenseQuantity != nil {
		sub.LicenseQuantity = *licenseQuantity
	}

	if licenseType != "" {
		sub.LicenseType = licenseType
	}

	cp := *sub

	return &cp, nil
}

// --- Pod Identity Association CRUD ---

// DeletePodIdentityAssociation removes a pod identity association from a cluster.
func (b *InMemoryBackend) DeletePodIdentityAssociation(
	clusterName, associationID string,
) (*PodIdentityAssociation, error) {
	b.mu.Lock("DeletePodIdentityAssociation")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assocs := b.podIdentityAssociations[clusterName]
	if assocs == nil {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	assoc, ok := assocs[associationID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	cp := *assoc

	if assoc.Tags != nil {
		assoc.Tags.Close()
	}

	delete(assocs, associationID)

	return &cp, nil
}

// DescribePodIdentityAssociation returns a pod identity association by ID.
func (b *InMemoryBackend) DescribePodIdentityAssociation(
	clusterName, associationID string,
) (*PodIdentityAssociation, error) {
	b.mu.RLock("DescribePodIdentityAssociation")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assocs := b.podIdentityAssociations[clusterName]
	if assocs == nil {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	assoc, ok := assocs[associationID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	cp := *assoc

	return &cp, nil
}

// ListPodIdentityAssociations returns all pod identity association summaries for a cluster.
func (b *InMemoryBackend) ListPodIdentityAssociations(clusterName string) ([]*PodIdentityAssociation, error) {
	b.mu.RLock("ListPodIdentityAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assocs := b.podIdentityAssociations[clusterName]
	list := make([]*PodIdentityAssociation, 0, len(assocs))

	for _, a := range assocs {
		cp := *a
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].AssociationID < list[j].AssociationID })

	return list, nil
}

// UpdatePodIdentityAssociation updates a pod identity association.
func (b *InMemoryBackend) UpdatePodIdentityAssociation(
	clusterName, associationID, roleARN string,
) (*PodIdentityAssociation, error) {
	b.mu.Lock("UpdatePodIdentityAssociation")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assocs := b.podIdentityAssociations[clusterName]
	if assocs == nil {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	assoc, ok := assocs[associationID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	if roleARN != "" {
		assoc.RoleARN = roleARN
	}

	cp := *assoc

	return &cp, nil
}

// --- Fargate Profile CRUD ---

// deepCopyFargateProfile returns a deep copy of a FargateProfile.
func deepCopyFargateProfile(p *FargateProfile) *FargateProfile {
	cp := *p
	cp.Selectors = make([]FargateProfileSelector, len(p.Selectors))
	copy(cp.Selectors, p.Selectors)
	cp.Subnets = cloneStrings(p.Subnets)

	return &cp
}

// DeleteFargateProfile removes a Fargate profile from a cluster.
func (b *InMemoryBackend) DeleteFargateProfile(clusterName, profileName string) (*FargateProfile, error) {
	b.mu.Lock("DeleteFargateProfile")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	profiles := b.fargateProfiles[clusterName]
	if profiles == nil {
		return nil, fmt.Errorf("%w: fargate profile %s not found in cluster %s", ErrNotFound, profileName, clusterName)
	}

	profile, ok := profiles[profileName]
	if !ok {
		return nil, fmt.Errorf("%w: fargate profile %s not found in cluster %s", ErrNotFound, profileName, clusterName)
	}

	cp := deepCopyFargateProfile(profile)

	if profile.Tags != nil {
		profile.Tags.Close()
	}

	delete(profiles, profileName)

	cp.Status = statusDeleting

	return cp, nil
}

// DescribeFargateProfile returns a Fargate profile by name.
func (b *InMemoryBackend) DescribeFargateProfile(clusterName, profileName string) (*FargateProfile, error) {
	b.mu.RLock("DescribeFargateProfile")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	profiles := b.fargateProfiles[clusterName]
	if profiles == nil {
		return nil, fmt.Errorf("%w: fargate profile %s not found in cluster %s", ErrNotFound, profileName, clusterName)
	}

	profile, ok := profiles[profileName]
	if !ok {
		return nil, fmt.Errorf("%w: fargate profile %s not found in cluster %s", ErrNotFound, profileName, clusterName)
	}

	return deepCopyFargateProfile(profile), nil
}

// ListFargateProfiles returns all Fargate profile names in a cluster sorted alphabetically.
func (b *InMemoryBackend) ListFargateProfiles(clusterName string) ([]string, error) {
	b.mu.RLock("ListFargateProfiles")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	profiles := b.fargateProfiles[clusterName]
	names := make([]string, 0, len(profiles))

	for name := range profiles {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// --- Access Entry CRUD ---

// DescribeAccessEntry returns an access entry by principal ARN.
func (b *InMemoryBackend) DescribeAccessEntry(clusterName, principalARN string) (*AccessEntry, error) {
	b.mu.RLock("DescribeAccessEntry")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entries := b.accessEntries[clusterName]
	if entries == nil {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	entry, ok := entries[principalARN]
	if !ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	cp := *entry

	return &cp, nil
}

// ListAccessEntries returns all principal ARNs for access entries in a cluster.
func (b *InMemoryBackend) ListAccessEntries(clusterName string) ([]string, error) {
	b.mu.RLock("ListAccessEntries")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entries := b.accessEntries[clusterName]
	arns := make([]string, 0, len(entries))

	for principalARN := range entries {
		arns = append(arns, principalARN)
	}

	sort.Strings(arns)

	return arns, nil
}

// AccessEntryUpdate holds the mutable fields for UpdateAccessEntry.
type AccessEntryUpdate struct {
	Username         string
	KubernetesGroups []string
}

// UpdateAccessEntry updates an access entry's username and kubernetes groups.
func (b *InMemoryBackend) UpdateAccessEntry(
	clusterName, principalARN string,
	upd AccessEntryUpdate,
) (*AccessEntry, error) {
	b.mu.Lock("UpdateAccessEntry")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entries := b.accessEntries[clusterName]
	if entries == nil {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	entry, ok := entries[principalARN]
	if !ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	if upd.Username != "" {
		entry.Username = upd.Username
	}

	if upd.KubernetesGroups != nil {
		entry.KubernetesGroups = cloneStrings(upd.KubernetesGroups)
	}

	cp := *entry

	return &cp, nil
}

// ListAccessPolicies returns a static list of AWS-managed EKS access policies.
func (b *InMemoryBackend) ListAccessPolicies() []map[string]string {
	return []map[string]string{
		{
			keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy",
			keyName:      "AmazonEKSClusterAdminPolicy",
		},
		{keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminPolicy", keyName: "AmazonEKSAdminPolicy"},
		{keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy", keyName: "AmazonEKSEditPolicy"},
		{keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy", keyName: "AmazonEKSViewPolicy"},
		{
			keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminViewPolicy",
			keyName:      "AmazonEKSAdminViewPolicy",
		},
	}
}

// ListAssociatedAccessPolicies returns all access policies associated with an entry.
func (b *InMemoryBackend) ListAssociatedAccessPolicies(
	clusterName, principalARN string,
) ([]*AccessPolicyAssociation, error) {
	b.mu.RLock("ListAssociatedAccessPolicies")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entries := b.accessEntries[clusterName]
	if entries == nil || entries[principalARN] == nil {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	policies := b.accessPolicies[clusterName][principalARN]
	result := make([]*AccessPolicyAssociation, len(policies))

	for i, p := range policies {
		cp := *p
		result[i] = &cp
	}

	return result, nil
}

// DisassociateAccessPolicy removes an access policy from an access entry.
func (b *InMemoryBackend) DisassociateAccessPolicy(clusterName, principalARN, policyARN string) error {
	b.mu.Lock("DisassociateAccessPolicy")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entries := b.accessEntries[clusterName]
	if entries == nil || entries[principalARN] == nil {
		return fmt.Errorf("%w: access entry for %s not found in cluster %s", ErrNotFound, principalARN, clusterName)
	}

	policies := b.accessPolicies[clusterName][principalARN]
	found := false

	for i, p := range policies {
		if p.PolicyARN == policyARN {
			b.accessPolicies[clusterName][principalARN] = append(policies[:i], policies[i+1:]...)
			found = true

			break
		}
	}

	if !found {
		return fmt.Errorf(
			"%w: policy %s not found for %s in cluster %s",
			ErrNotFound,
			policyARN,
			principalARN,
			clusterName,
		)
	}

	return nil
}

// --- Identity Provider Config CRUD ---

// DescribeIdentityProviderConfig returns an identity provider config by name.
func (b *InMemoryBackend) DescribeIdentityProviderConfig(clusterName, name string) (*IdentityProviderConfig, error) {
	b.mu.RLock("DescribeIdentityProviderConfig")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	configs := b.identityProviderConfigs[clusterName]
	if configs == nil {
		return nil, fmt.Errorf(
			"%w: identity provider config %s not found in cluster %s",
			ErrNotFound,
			name,
			clusterName,
		)
	}

	cfg, ok := configs[name]
	if !ok {
		return nil, fmt.Errorf(
			"%w: identity provider config %s not found in cluster %s",
			ErrNotFound,
			name,
			clusterName,
		)
	}

	cp := *cfg

	return &cp, nil
}

// ListIdentityProviderConfigs returns all identity provider config summaries for a cluster.
func (b *InMemoryBackend) ListIdentityProviderConfigs(clusterName string) ([]map[string]string, error) {
	b.mu.RLock("ListIdentityProviderConfigs")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	configs := b.identityProviderConfigs[clusterName]
	result := make([]map[string]string, 0, len(configs))

	for _, cfg := range configs {
		result = append(result, map[string]string{
			keyName: cfg.Name,
			keyType: cfg.Type,
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i]["name"] < result[j]["name"] })

	return result, nil
}

// DisassociateIdentityProviderConfig removes an identity provider config from a cluster.
func (b *InMemoryBackend) DisassociateIdentityProviderConfig(clusterName, name string) error {
	b.mu.Lock("DisassociateIdentityProviderConfig")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	configs := b.identityProviderConfigs[clusterName]
	if configs == nil {
		return fmt.Errorf("%w: identity provider config %s not found in cluster %s", ErrNotFound, name, clusterName)
	}

	cfg, ok := configs[name]
	if !ok {
		return fmt.Errorf("%w: identity provider config %s not found in cluster %s", ErrNotFound, name, clusterName)
	}

	if cfg.Tags != nil {
		cfg.Tags.Close()
	}

	delete(configs, name)

	return nil
}

// --- Insights ---

// DescribeInsight returns a synthetic insight for a cluster.
func (b *InMemoryBackend) DescribeInsight(clusterName, insightID string) (*Insight, error) {
	b.mu.RLock("DescribeInsight")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return &Insight{
		ID:              insightID,
		ClusterName:     clusterName,
		Category:        typeUpgradeReadiness,
		Status:          statusPassing,
		Description:     "Cluster is ready for upgrade",
		Recommendation:  "No action needed",
		LastRefreshTime: now,
		LastTransition:  now,
	}, nil
}

// ListInsights returns synthetic insights for a cluster.
func (b *InMemoryBackend) ListInsights(clusterName string) ([]*Insight, error) {
	b.mu.RLock("ListInsights")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return []*Insight{
		{
			ID:              stableID(clusterName + "/upgrade-readiness"),
			ClusterName:     clusterName,
			Category:        typeUpgradeReadiness,
			Status:          statusPassing,
			Description:     "Cluster is ready for upgrade",
			LastRefreshTime: now,
			LastTransition:  now,
		},
		{
			ID:              stableID(clusterName + "/deprecated-apis"),
			ClusterName:     clusterName,
			Category:        typeUpgradeReadiness,
			Status:          statusPassing,
			Description:     "No deprecated APIs in use",
			LastRefreshTime: now,
			LastTransition:  now,
		},
	}, nil
}

// StartInsightsRefresh starts an insights refresh for a cluster.
func (b *InMemoryBackend) StartInsightsRefresh(clusterName string) (*InsightsRefresh, error) {
	b.mu.RLock("StartInsightsRefresh")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	return &InsightsRefresh{
		ID:          stableID(clusterName + "/refresh/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      "COMPLETED",
		StartedAt:   time.Now().UTC(),
	}, nil
}

// DescribeInsightsRefresh returns status of an insights refresh.
func (b *InMemoryBackend) DescribeInsightsRefresh(clusterName, refreshID string) (*InsightsRefresh, error) {
	b.mu.RLock("DescribeInsightsRefresh")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	return &InsightsRefresh{
		ID:          refreshID,
		ClusterName: clusterName,
		Status:      "COMPLETED",
		StartedAt:   time.Now().UTC(),
	}, nil
}

// --- Cluster updates ---

// ClusterConfigUpdate holds mutable cluster config fields for UpdateClusterConfig.
type ClusterConfigUpdate struct {
	LogEntries    []ClusterLogEntry
	SubnetIDs     []string
	AccessConfig  *AccessConfig
	ComputeConfig *ComputeConfig
	StorageConfig *StorageConfig
}

// UpdateClusterConfig updates the cluster configuration including logging, subnets, and access settings.
func (b *InMemoryBackend) UpdateClusterConfig(clusterName string, upd ClusterConfigUpdate) (*Update, error) {
	b.mu.Lock("UpdateClusterConfig")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if upd.LogEntries != nil {
		c.ClusterLogging = mergeClusterLogEntries(c.ClusterLogging, upd.LogEntries)
	}

	if len(upd.SubnetIDs) > 0 {
		if c.VpcConfig == nil {
			c.VpcConfig = &VpcConfig{}
		}

		c.VpcConfig.SubnetIDs = cloneStrings(upd.SubnetIDs)
	}

	if upd.AccessConfig != nil {
		if c.AccessConfig == nil {
			c.AccessConfig = &AccessConfig{}
		}

		if upd.AccessConfig.AuthenticationMode != "" {
			c.AccessConfig.AuthenticationMode = upd.AccessConfig.AuthenticationMode
		}

		c.AccessConfig.BootstrapClusterCreatorAdminPermissions = upd.AccessConfig.BootstrapClusterCreatorAdminPermissions
	}

	if upd.ComputeConfig != nil {
		c.ComputeConfig = upd.ComputeConfig
	}

	if upd.StorageConfig != nil {
		c.StorageConfig = upd.StorageConfig
	}

	return &Update{
		ID:          stableID(clusterName + "/config-update/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      statusInProgress,
		Type:        "ConfigUpdate",
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// VpcEndpointUpdate carries optional VPC endpoint access changes for UpdateClusterVpcEndpoint.
type VpcEndpointUpdate struct {
	EndpointPublicAccess  *bool
	EndpointPrivateAccess *bool
	PublicAccessCIDRs     []string
}

// UpdateClusterVpcEndpoint updates the VPC endpoint access settings on a cluster.
// Fields with nil pointer values are left unchanged.
func (b *InMemoryBackend) UpdateClusterVpcEndpoint(clusterName string, upd VpcEndpointUpdate) (*Update, error) {
	b.mu.Lock("UpdateClusterVpcEndpoint")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if c.VpcConfig == nil {
		c.VpcConfig = &VpcConfig{
			EndpointPublicAccess: true,
		}
	}

	var params []UpdateParam
	if upd.EndpointPublicAccess != nil {
		c.VpcConfig.EndpointPublicAccess = *upd.EndpointPublicAccess
		params = append(
			params,
			UpdateParam{Type: "EndpointPublicAccess", Value: strconv.FormatBool(*upd.EndpointPublicAccess)},
		)
	}
	if upd.EndpointPrivateAccess != nil {
		c.VpcConfig.EndpointPrivateAccess = *upd.EndpointPrivateAccess
		params = append(
			params,
			UpdateParam{Type: "EndpointPrivateAccess", Value: strconv.FormatBool(*upd.EndpointPrivateAccess)},
		)
	}
	if upd.PublicAccessCIDRs != nil {
		c.VpcConfig.PublicAccessCIDRs = cloneStrings(upd.PublicAccessCIDRs)
		params = append(params, UpdateParam{Type: "PublicAccessCidrs", Value: fmt.Sprintf("%v", upd.PublicAccessCIDRs)})
	}

	return &Update{
		ID:          stableID(clusterName + "/vpc-update/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      statusSuccessful,
		Type:        "EndpointAccessUpdate",
		Params:      params,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// mergeClusterLogEntries applies logEntries on top of existing, enabling or disabling
// individual log types, and returns the merged structured result.
func mergeClusterLogEntries(existing []ClusterLogEntry, updates []ClusterLogEntry) []ClusterLogEntry {
	merged := make(map[string]bool)

	for _, e := range existing {
		if e.Enabled {
			for _, t := range e.Types {
				merged[t] = true
			}
		}
	}

	for _, entry := range updates {
		for _, t := range entry.Types {
			if entry.Enabled {
				merged[t] = true
			} else {
				delete(merged, t)
			}
		}
	}

	if len(merged) == 0 {
		return nil
	}

	enabled := make([]string, 0, len(merged))
	for t := range merged {
		enabled = append(enabled, t)
	}

	sort.Strings(enabled)

	return []ClusterLogEntry{{Types: enabled, Enabled: true}}
}

// UpdateClusterVersion updates the cluster Kubernetes version.
func (b *InMemoryBackend) UpdateClusterVersion(clusterName, version string) (*Update, error) {
	b.mu.Lock("UpdateClusterVersion")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if version != "" {
		c.Version = version
	}

	return &Update{
		ID:          stableID(clusterName + "/version-update/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      statusSuccessful,
		Type:        typeVersionUpdate,
		Params:      []UpdateParam{{Type: "Version", Value: version}},
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// UpdateNodegroupVersion updates the node group Kubernetes version.
func (b *InMemoryBackend) UpdateNodegroupVersion(
	clusterName, nodegroupName, version string,
) (*Update, error) {
	b.mu.Lock("UpdateNodegroupVersion")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ng, ok := b.nodegroups[clusterName][nodegroupName]
	if !ok {
		return nil, fmt.Errorf("%w: nodegroup %s not found in cluster %s", ErrNotFound, nodegroupName, clusterName)
	}

	if version != "" {
		ng.Version = version
	}

	return &Update{
		ID:          stableID(clusterName + "/" + nodegroupName + "/version-update/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      statusInProgress,
		Type:        typeVersionUpdate,
		Params:      []UpdateParam{{Type: "Version", Value: version}},
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// DescribeUpdate returns an update record.
func (b *InMemoryBackend) DescribeUpdate(clusterName, updateID string) (*Update, error) {
	b.mu.RLock("DescribeUpdate")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	return &Update{
		ID:          updateID,
		ClusterName: clusterName,
		Status:      statusSuccessful,
		Type:        typeVersionUpdate,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// ListUpdates returns update IDs for a cluster.
func (b *InMemoryBackend) ListUpdates(clusterName string) ([]string, error) {
	b.mu.RLock("ListUpdates")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	return []string{}, nil
}

// --- Register / Deregister Cluster ---

// RegisterCluster registers an external cluster.
func (b *InMemoryBackend) RegisterCluster(
	name, _, _ string,
	kv map[string]string,
) (*Cluster, error) {
	b.mu.Lock("RegisterCluster")
	defer b.mu.Unlock()

	if _, ok := b.clusters[name]; ok {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrAlreadyExists, name)
	}

	clusterARN := arn.Build("eks", b.region, b.accountID, "cluster/"+name)
	t := tags.New("eks.cluster." + name + ".tags")

	if len(kv) > 0 {
		t.Merge(kv)
	}

	c := &Cluster{
		Name:            name,
		ARN:             clusterARN,
		Version:         defaultK8sVersion,
		Status:          statusActive,
		Endpoint:        fmt.Sprintf("https://%s.%s.eks.amazonaws.com", stableID(name), b.region),
		PlatformVersion: "eks.1",
		AccountID:       b.accountID,
		Region:          b.region,
		CreatedAt:       time.Now().UTC(),
		Tags:            t,
	}
	b.clusters[name] = c
	b.nodegroups[name] = make(map[string]*Nodegroup)
	b.accessEntries[name] = make(map[string]*AccessEntry)
	b.accessPolicies[name] = make(map[string][]*AccessPolicyAssociation)
	b.encryptionConfigs[name] = nil
	b.identityProviderConfigs[name] = make(map[string]*IdentityProviderConfig)
	b.addons[name] = make(map[string]*Addon)
	b.fargateProfiles[name] = make(map[string]*FargateProfile)
	b.podIdentityAssociations[name] = make(map[string]*PodIdentityAssociation)

	cp := *c

	return &cp, nil
}

// DeregisterCluster removes a registered external cluster.
func (b *InMemoryBackend) DeregisterCluster(name string) (*Cluster, error) {
	return b.DeleteCluster(name)
}

// DescribeClusterVersions returns supported cluster versions.
func (b *InMemoryBackend) DescribeClusterVersions() []map[string]string {
	return []map[string]string{
		{
			keyClusterVersion:           defaultK8sVersion,
			keyDefaultVersion:           "true",
			keyEndOfStandardSupportDate: "2027-04-01",
			keyEndOfExtendedSupportDate: "2028-04-01",
		},
		{
			keyClusterVersion:           "1.31",
			keyDefaultVersion:           strFalse,
			keyEndOfStandardSupportDate: "2026-11-01",
			keyEndOfExtendedSupportDate: "2027-11-01",
		},
		{
			keyClusterVersion:           "1.30",
			keyDefaultVersion:           strFalse,
			keyEndOfStandardSupportDate: "2026-07-01",
			keyEndOfExtendedSupportDate: "2027-07-01",
		},
		{
			keyClusterVersion:           "1.29",
			keyDefaultVersion:           strFalse,
			keyEndOfStandardSupportDate: "2026-03-01",
			keyEndOfExtendedSupportDate: "2027-03-01",
		},
	}
}

// --- Dashboard helpers ---

// ListAllAddons returns all addons across all clusters.
func (b *InMemoryBackend) ListAllAddons() []*Addon {
	b.mu.RLock("ListAllAddons")
	defer b.mu.RUnlock()

	var list []*Addon

	for _, addons := range b.addons {
		for _, a := range addons {
			cp := *a
			list = append(list, &cp)
		}
	}

	return list
}

// ListAllFargateProfiles returns all Fargate profiles across all clusters.
func (b *InMemoryBackend) ListAllFargateProfiles() []*FargateProfile {
	b.mu.RLock("ListAllFargateProfiles")
	defer b.mu.RUnlock()

	var list []*FargateProfile

	for _, profiles := range b.fargateProfiles {
		for _, p := range profiles {
			cp := *p
			cp.Selectors = make([]FargateProfileSelector, len(p.Selectors))
			copy(cp.Selectors, p.Selectors)
			list = append(list, &cp)
		}
	}

	return list
}

// ListAllPodIdentityAssociations returns all pod identity associations across all clusters.
func (b *InMemoryBackend) ListAllPodIdentityAssociations() []*PodIdentityAssociation {
	b.mu.RLock("ListAllPodIdentityAssociations")
	defer b.mu.RUnlock()

	var list []*PodIdentityAssociation

	for _, assocs := range b.podIdentityAssociations {
		for _, a := range assocs {
			cp := *a
			list = append(list, &cp)
		}
	}

	return list
}

// ListAllAccessEntries returns all access entries across all clusters.
func (b *InMemoryBackend) ListAllAccessEntries() []*AccessEntry {
	b.mu.RLock("ListAllAccessEntries")
	defer b.mu.RUnlock()

	var list []*AccessEntry

	for _, entries := range b.accessEntries {
		for _, e := range entries {
			cp := *e
			list = append(list, &cp)
		}
	}

	return list
}

// ListAllCapabilities returns all capabilities.
func (b *InMemoryBackend) ListAllCapabilities() []*Capability {
	b.mu.RLock("ListAllCapabilities")
	defer b.mu.RUnlock()

	list := make([]*Capability, 0, len(b.capabilities))

	for _, c := range b.capabilities {
		cp := *c
		list = append(list, &cp)
	}

	return list
}

// ListAllSubscriptions returns all EKS Anywhere subscriptions.
func (b *InMemoryBackend) ListAllSubscriptions() []*AnywhereSubscription {
	b.mu.RLock("ListAllSubscriptions")
	defer b.mu.RUnlock()

	list := make([]*AnywhereSubscription, 0, len(b.subscriptions))

	for _, s := range b.subscriptions {
		cp := *s
		list = append(list, &cp)
	}

	return list
}

// AddPodIdentityAssociationInternal inserts a pre-built pod identity association.
// Intended only for test seeding.
func (b *InMemoryBackend) AddPodIdentityAssociationInternal(a *PodIdentityAssociation) {
	b.mu.Lock("AddPodIdentityAssociationInternal")
	defer b.mu.Unlock()

	if a.Tags == nil {
		a.Tags = tags.New("eks.podidentity." + a.ClusterName + "." + a.AssociationID + ".tags")
	}

	if b.podIdentityAssociations[a.ClusterName] == nil {
		b.podIdentityAssociations[a.ClusterName] = make(map[string]*PodIdentityAssociation)
	}

	b.podIdentityAssociations[a.ClusterName][a.AssociationID] = a
}
