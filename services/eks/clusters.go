package eks

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// clusterTransitionDelay is the async delay before a CREATING cluster reaches ACTIVE.
const clusterTransitionDelay = 100 * time.Millisecond

// ClusterOptionalConfig groups optional cluster configuration for CreateCluster.
type ClusterOptionalConfig struct {
	AccessConfig  *AccessConfig
	ComputeConfig *ComputeConfig
	StorageConfig *StorageConfig
}

// resolveClusterOptionalConfig deep-copies the (at most one) supplied
// ClusterOptionalConfig into independent, nil-safe fields for CreateCluster.
func resolveClusterOptionalConfig(
	opts ...ClusterOptionalConfig,
) (*AccessConfig, *ComputeConfig, *StorageConfig) {
	var opt ClusterOptionalConfig
	if len(opts) > 0 {
		opt = opts[0]
	}

	var accessCfg *AccessConfig
	if opt.AccessConfig != nil {
		cp := *opt.AccessConfig
		accessCfg = &cp
	}

	var computeCfg *ComputeConfig
	if opt.ComputeConfig != nil {
		cp := *opt.ComputeConfig
		cp.NodePools = cloneStrings(opt.ComputeConfig.NodePools)
		computeCfg = &cp
	}

	var storageCfg *StorageConfig
	if opt.StorageConfig != nil {
		cp := *opt.StorageConfig
		storageCfg = &cp
	}

	return accessCfg, computeCfg, storageCfg
}

// cloneKubernetesNetworkConfig deep-copies a KubernetesNetworkConfig,
// including its nested ElasticLoadBalancing pointer, so the stored Cluster
// never aliases the caller's config.
func cloneKubernetesNetworkConfig(cfg *KubernetesNetworkConfig) *KubernetesNetworkConfig {
	if cfg == nil {
		return nil
	}

	cp := *cfg

	if cfg.ElasticLoadBalancing != nil {
		elbCp := *cfg.ElasticLoadBalancing
		cp.ElasticLoadBalancing = &elbCp
	}

	return &cp
}

// newClusterLocked builds a new Cluster value for CreateCluster. Must be
// called with b.mu held.
func (b *InMemoryBackend) newClusterLocked(
	name, version, roleARN string,
	vpcConfig *VpcConfig,
	networkConfig *KubernetesNetworkConfig,
	kv map[string]string,
	opts ...ClusterOptionalConfig,
) *Cluster {
	clusterARN := arn.Build("eks", b.region, b.accountID, "cluster/"+name)
	t := tags.New("eks.cluster." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	var vpcCopy *VpcConfig
	if vpcConfig != nil {
		vpcCopy = cloneVpcConfig(vpcConfig, name)
	}

	netCopy := cloneKubernetesNetworkConfig(networkConfig)

	accessCfg, computeCfg, storageCfg := resolveClusterOptionalConfig(opts...)

	return &Cluster{
		Name:                    name,
		ARN:                     clusterARN,
		Version:                 version,
		RoleARN:                 roleARN,
		Status:                  statusCreating,
		Endpoint:                fmt.Sprintf("https://%s.%s.eks.amazonaws.com", stableID(name), b.region),
		OIDCIssuer:              fmt.Sprintf("https://oidc.eks.%s.amazonaws.com/id/%s", b.region, randomHex16()),
		PlatformVersion:         "eks.1",
		AccountID:               b.accountID,
		Region:                  b.region,
		CreatedAt:               time.Now().UTC(),
		Tags:                    t,
		VpcConfig:               vpcCopy,
		KubernetesNetworkConfig: netCopy,
		AccessConfig:            accessCfg,
		ComputeConfig:           computeCfg,
		StorageConfig:           storageCfg,
		CertificateAuthority:    stableID(name + "/ca"),
	}
}

// scheduleClusterActivation schedules the async CREATING -> ACTIVE transition
// for the named cluster.
func (b *InMemoryBackend) scheduleClusterActivation(name string) {
	b.work.After("ClusterTransition", clusterTransitionDelay, func() {
		b.mu.Lock("CreateCluster-async")
		defer b.mu.Unlock()

		if cl, found := b.clusters.Get(name); found && cl.Status == statusCreating {
			cl.Status = statusActive
		}
	})
}

// CreateCluster creates a new EKS cluster.
func (b *InMemoryBackend) CreateCluster(
	name, version, roleARN string,
	vpcConfig *VpcConfig,
	networkConfig *KubernetesNetworkConfig,
	kv map[string]string,
	opts ...ClusterOptionalConfig,
) (*Cluster, error) {
	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(name); ok {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrAlreadyExists, name)
	}

	if version == "" {
		version = defaultK8sVersion
	}

	c := b.newClusterLocked(name, version, roleARN, vpcConfig, networkConfig, kv, opts...)
	b.clusters.Put(c)
	b.accessPolicies[name] = make(map[string][]*AccessPolicyAssociation)
	b.encryptionConfigs[name] = nil

	b.scheduleClusterActivation(name)

	cp := *c

	return &cp, nil
}

func cloneVpcConfig(src *VpcConfig, clusterName string) *VpcConfig {
	cp := *src
	cp.SubnetIDs = cloneStrings(src.SubnetIDs)
	cp.SecurityGroupIDs = cloneStrings(src.SecurityGroupIDs)
	cp.PublicAccessCIDRs = cloneStrings(src.PublicAccessCIDRs)
	if cp.ClusterSecurityGroupID == "" {
		cp.ClusterSecurityGroupID = "sg-" + stableID(clusterName)
	}
	if cp.VpcID == "" {
		cp.VpcID = "vpc-" + stableID(clusterName)
	}

	return &cp
}

// DescribeCluster returns a cluster by name.
func (b *InMemoryBackend) DescribeCluster(name string) (*Cluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, name)
	}
	cp := *c

	return &cp, nil
}

// ListClusters returns all cluster names sorted alphabetically.
func (b *InMemoryBackend) ListClusters() []string {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	items := b.clusters.Snapshot()
	names := make([]string, len(items))

	for i, c := range items {
		names[i] = c.Name
	}

	return names
}

// closeAccessEntryTagsForCluster closes tag objects for all access entries in
// a cluster and removes them from the maps. Must be called with b.mu held.
func (b *InMemoryBackend) closeAccessEntryTagsForCluster(clusterName string) {
	entries := slices.Clone(b.accessEntriesByCluster.Get(clusterName))
	for _, e := range entries {
		if e.Tags != nil {
			e.Tags.Close()
		}

		b.accessEntries.Delete(accessEntryKey(e.ClusterName, e.PrincipalARN))
	}

	delete(b.accessPolicies, clusterName)
}

// closeIDPAndPodTagsForCluster closes tag objects for identity provider configs
// and pod identity associations in a cluster, then removes them from the maps.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeIDPAndPodTagsForCluster(clusterName string) {
	cfgs := slices.Clone(b.identityProviderConfigsByCluster.Get(clusterName))
	for _, cfg := range cfgs {
		if cfg.Tags != nil {
			cfg.Tags.Close()
		}

		b.identityProviderConfigs.Delete(identityProviderConfigKey(cfg.ClusterName, cfg.Name))
	}

	assocs := slices.Clone(b.podIdentityAssociationsByCluster.Get(clusterName))
	for _, a := range assocs {
		if a.Tags != nil {
			a.Tags.Close()
		}

		b.podIdentityAssociations.Delete(podIdentityAssociationKey(a.ClusterName, a.AssociationID))
	}
}

// DeleteCluster deletes a cluster by name. For test convenience this fake cascades
// and removes nodegroups as well (real AWS requires manual nodegroup deletion first).
func (b *InMemoryBackend) DeleteCluster(name string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, name)
	}

	cp := *c
	cp.Status = statusDeleting

	// Collect nodegroups for cleanup before removal.
	ngs := slices.Clone(b.nodegroupsByCluster.Get(name))
	b.clusters.Delete(name)

	for _, ng := range ngs {
		b.nodegroups.Delete(nodegroupKey(ng.ClusterName, ng.NodegroupName))
	}

	delete(b.encryptionConfigs, name)

	// Matches the pre-conversion behavior exactly: addons and fargate
	// profiles for the cluster are bulk-removed WITHOUT closing their Tags
	// (a pre-existing quirk of the map-based implementation this preserves
	// byte-for-byte rather than fixes).
	for _, a := range slices.Clone(b.addonsByCluster.Get(name)) {
		b.addons.Delete(addonKey(a.ClusterName, a.AddonName))
	}

	for _, fp := range slices.Clone(b.fargateProfilesByCluster.Get(name)) {
		b.fargateProfiles.Delete(fargateProfileKey(fp.ClusterName, fp.FargateProfileName))
	}

	for _, capa := range slices.Clone(b.capabilitiesByCluster.Get(name)) {
		if capa.Tags != nil {
			capa.Tags.Close()
		}

		b.capabilities.Delete(capabilityKey(capa.ClusterName, capa.CapabilityName))
	}

	b.closeAccessEntryTagsForCluster(name)
	b.closeIDPAndPodTagsForCluster(name)

	// Release cluster and nodegroup tag resources.
	if c.Tags != nil {
		c.Tags.Close()
	}

	for _, ng := range ngs {
		if ng.Tags != nil {
			ng.Tags.Close()
		}
	}

	return &cp, nil
}

// RegisterCluster registers an external cluster.
func (b *InMemoryBackend) RegisterCluster(
	name, provider, roleARN string,
	kv map[string]string,
) (*Cluster, error) {
	b.mu.Lock("RegisterCluster")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(name); ok {
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
		ConnectorConfig: &ConnectorConfig{
			Provider:         provider,
			RoleARN:          roleARN,
			ActivationID:     stableID(name + "/activation-id"),
			ActivationCode:   stableID(name + "/activation-code"),
			ActivationExpiry: time.Now().Add(connectorActivationWindow).UTC().Format(time.RFC3339),
		},
	}
	b.clusters.Put(c)
	b.accessPolicies[name] = make(map[string][]*AccessPolicyAssociation)
	b.encryptionConfigs[name] = nil

	cp := *c

	return &cp, nil
}

// DeregisterCluster removes a registered external cluster.
func (b *InMemoryBackend) DeregisterCluster(name string) (*Cluster, error) {
	return b.DeleteCluster(name)
}

// supportDate parses a static "YYYY-MM-DD" date into the epoch-seconds
// number awsjson1.1/restjson1 expects on the wire -- confirmed against
// aws-sdk-go-v2/service/eks@v1.90.4's deserializers.go (case
// "endOfStandardSupportDate"/"endOfExtendedSupportDate": json.Number via
// smithytime.ParseEpochSeconds). Emitting the literal date string instead
// failed DescribeClusterVersions outright for every real client.
func supportDate(date string) float64 {
	t, err := time.Parse(time.DateOnly, date)
	if err != nil {
		panic("eks: invalid static support date " + date)
	}

	return awstime.Epoch(t)
}

// DescribeClusterVersions returns supported cluster versions.
func (b *InMemoryBackend) DescribeClusterVersions() []map[string]any {
	return []map[string]any{
		{
			keyClusterVersion:           defaultK8sVersion,
			keyDefaultVersion:           true,
			keyEndOfStandardSupportDate: supportDate("2027-04-01"),
			keyEndOfExtendedSupportDate: supportDate("2028-04-01"),
		},
		{
			keyClusterVersion:           "1.31",
			keyDefaultVersion:           false,
			keyEndOfStandardSupportDate: supportDate("2026-11-01"),
			keyEndOfExtendedSupportDate: supportDate("2027-11-01"),
		},
		{
			keyClusterVersion:           "1.30",
			keyDefaultVersion:           false,
			keyEndOfStandardSupportDate: supportDate("2026-07-01"),
			keyEndOfExtendedSupportDate: supportDate("2027-07-01"),
		},
		{
			keyClusterVersion:           "1.29",
			keyDefaultVersion:           false,
			keyEndOfStandardSupportDate: supportDate("2026-03-01"),
			keyEndOfExtendedSupportDate: supportDate("2027-03-01"),
		},
	}
}

// AddClusterInternal inserts a pre-built cluster directly into the backend.
// Intended only for test seeding; not safe for production use.
func (b *InMemoryBackend) AddClusterInternal(c *Cluster) {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	if c.Tags == nil {
		c.Tags = tags.New("eks.cluster." + c.Name + ".tags")
	}

	b.clusters.Put(c)

	if b.accessPolicies[c.Name] == nil {
		b.accessPolicies[c.Name] = make(map[string][]*AccessPolicyAssociation)
	}
}

// ListAllClusters returns all clusters across the backend.
func (b *InMemoryBackend) ListAllClusters() []*Cluster {
	b.mu.RLock("ListAllClusters")
	defer b.mu.RUnlock()

	items := b.clusters.All()
	list := make([]*Cluster, 0, len(items))

	for _, c := range items {
		cp := *c
		list = append(list, &cp)
	}

	return list
}
