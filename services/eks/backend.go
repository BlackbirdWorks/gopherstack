package eks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	statusActive      = "ACTIVE"
	statusInProgress  = "InProgress"
	defaultK8sVersion = "1.32"
	priorK8sVersion   = "1.31"
)

// clusterTransitionDelay is the async delay before a CREATING cluster reaches ACTIVE.
const clusterTransitionDelay = 100 * time.Millisecond

// nodegroupTransitionDelay is the async delay before a CREATING nodegroup reaches ACTIVE.
const nodegroupTransitionDelay = 100 * time.Millisecond

// fargateProfileTransitionDelay is the async delay before a CREATING Fargate profile reaches ACTIVE.
const fargateProfileTransitionDelay = 100 * time.Millisecond

// addonTransitionDelay is the async delay before a CREATING addon reaches ACTIVE.
const addonTransitionDelay = 100 * time.Millisecond

var (
	// ErrNotFound is returned when an EKS resource is not found.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when an EKS resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrConflict)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
)

// VpcConfig captures the cluster VPC configuration returned by AWS.
type VpcConfig struct {
	ClusterSecurityGroupID string   `json:"clusterSecurityGroupId,omitempty"`
	VpcID                  string   `json:"vpcId,omitempty"`
	SubnetIDs              []string `json:"subnetIds,omitempty"`
	SecurityGroupIDs       []string `json:"securityGroupIds,omitempty"`
	PublicAccessCIDRs      []string `json:"publicAccessCidrs,omitempty"`
	EndpointPrivateAccess  bool     `json:"endpointPrivateAccess"`
	EndpointPublicAccess   bool     `json:"endpointPublicAccess"`
}

// AccessConfig holds the cluster authentication mode configuration.
type AccessConfig struct {
	AuthenticationMode                      string `json:"authenticationMode,omitempty"`
	BootstrapClusterCreatorAdminPermissions bool   `json:"bootstrapClusterCreatorAdminPermissions"`
}

// ComputeConfig holds the EKS Auto Mode compute configuration.
type ComputeConfig struct {
	NodeRoleARN string   `json:"nodeRoleArn,omitempty"`
	NodePools   []string `json:"nodePools,omitempty"`
	Enabled     bool     `json:"enabled"`
}

// BlockStorageConfig holds EKS Auto Mode block storage settings.
type BlockStorageConfig struct {
	Enabled bool `json:"enabled"`
}

// StorageConfig holds the EKS Auto Mode storage configuration.
type StorageConfig struct {
	BlockStorage *BlockStorageConfig `json:"blockStorage,omitempty"`
}

// ElasticLoadBalancingConfig holds EKS Auto Mode load balancer settings.
type ElasticLoadBalancingConfig struct {
	Enabled bool `json:"enabled"`
}

// NetworkingConfig holds the EKS Auto Mode networking configuration.
type NetworkingConfig struct {
	ElasticLoadBalancing *ElasticLoadBalancingConfig `json:"elasticLoadBalancing,omitempty"`
}

// KubernetesNetworkConfig captures cluster networking parameters.
type KubernetesNetworkConfig struct {
	IPFamily        string `json:"ipFamily,omitempty"`
	ServiceIPv4CIDR string `json:"serviceIpv4Cidr,omitempty"`
	ServiceIPv6CIDR string `json:"serviceIpv6Cidr,omitempty"`
}

// ClusterLogEntry represents one log-type group in the structured logging config.
type ClusterLogEntry struct {
	Types   []string `json:"types"`
	Enabled bool     `json:"enabled"`
}

// ConnectorConfig holds metadata for externally-registered clusters.
type ConnectorConfig struct {
	ActivationCode   string `json:"activationCode,omitempty"`
	ActivationExpiry string `json:"activationExpiry,omitempty"`
	ActivationID     string `json:"activationId,omitempty"`
	Provider         string `json:"provider,omitempty"`
	RoleARN          string `json:"roleArn,omitempty"`
}

// Cluster represents an EKS cluster.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateCluster.
type Cluster struct {
	CreatedAt               time.Time                `json:"createdAt"`
	Tags                    *tags.Tags               `json:"tags,omitempty"`
	VpcConfig               *VpcConfig               `json:"resourcesVpcConfig,omitempty"`
	KubernetesNetworkConfig *KubernetesNetworkConfig `json:"kubernetesNetworkConfig,omitempty"`
	AccessConfig            *AccessConfig            `json:"accessConfig,omitempty"`
	ComputeConfig           *ComputeConfig           `json:"computeConfig,omitempty"`
	StorageConfig           *StorageConfig           `json:"storageConfig,omitempty"`
	NetworkingConfig        *NetworkingConfig        `json:"networkingConfig,omitempty"`
	ConnectorConfig         *ConnectorConfig         `json:"connectorConfig,omitempty"`
	ARN                     string                   `json:"arn"`
	Name                    string                   `json:"name"`
	Endpoint                string                   `json:"endpoint,omitempty"`
	OIDCIssuer              string                   `json:"oidcIssuer,omitempty"`
	Version                 string                   `json:"version"`
	Status                  string                   `json:"status"`
	RoleARN                 string                   `json:"roleArn,omitempty"`
	AccountID               string                   `json:"accountId"`
	Region                  string                   `json:"region"`
	PlatformVersion         string                   `json:"platformVersion,omitempty"`
	CertificateAuthority    string                   `json:"certificateAuthority,omitempty"`
	ClusterLogging          []ClusterLogEntry        `json:"clusterLogging,omitempty"`
	EncryptionConfig        []EncryptionConfig       `json:"encryptionConfig,omitempty"`
}

// NodegroupTaint represents a Kubernetes taint applied to managed nodes.
type NodegroupTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

// RemoteAccess captures SSH remote-access configuration for a node group.
type RemoteAccess struct {
	EC2SSHKey            string   `json:"ec2SshKey,omitempty"`
	SourceSecurityGroups []string `json:"sourceSecurityGroups,omitempty"`
}

// LaunchTemplate captures the launch-template reference for a node group.
type LaunchTemplate struct {
	ID      string `json:"id,omitempty"`
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// AutoScalingGroup holds the name of an ASG backing the node group.
type AutoScalingGroup struct {
	Name string `json:"name"`
}

// NodegroupResources captures AWS resources backing the node group.
type NodegroupResources struct {
	AutoScalingGroups []AutoScalingGroup `json:"autoScalingGroups,omitempty"`
}

// NodegroupUpdateConfig holds the nodegroup update strategy settings.
type NodegroupUpdateConfig struct {
	MaxUnavailable           *int32 `json:"maxUnavailable,omitempty"`
	MaxUnavailablePercentage *int32 `json:"maxUnavailablePercentage,omitempty"`
}

// Nodegroup represents an EKS managed node group.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateNodegroup.
type Nodegroup struct {
	CreatedAt      time.Time              `json:"createdAt"`
	Tags           *tags.Tags             `json:"tags,omitempty"`
	Labels         map[string]string      `json:"labels,omitempty"`
	RemoteAccess   *RemoteAccess          `json:"remoteAccess,omitempty"`
	LaunchTemplate *LaunchTemplate        `json:"launchTemplate,omitempty"`
	Resources      *NodegroupResources    `json:"resources,omitempty"`
	UpdateConfig   *NodegroupUpdateConfig `json:"updateConfig,omitempty"`
	CapacityType   string                 `json:"capacityType,omitempty"`
	Region         string                 `json:"region"`
	ARN            string                 `json:"nodegroupArn"`
	NodeRole       string                 `json:"nodeRole,omitempty"`
	Status         string                 `json:"status"`
	AMIType        string                 `json:"amiType,omitempty"`
	NodegroupName  string                 `json:"nodegroupName"`
	ClusterName    string                 `json:"clusterName"`
	Version        string                 `json:"version,omitempty"`
	ReleaseVersion string                 `json:"releaseVersion,omitempty"`
	AccountID      string                 `json:"accountId"`
	Taints         []NodegroupTaint       `json:"taints,omitempty"`
	InstanceTypes  []string               `json:"instanceTypes,omitempty"`
	Subnets        []string               `json:"subnets,omitempty"`
	DesiredSize    int32                  `json:"desiredSize"`
	MinSize        int32                  `json:"minSize"`
	MaxSize        int32                  `json:"maxSize"`
	DiskSize       int32                  `json:"diskSize,omitempty"`
}

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

// Reset clears all state, returning the backend to a fresh empty state.
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

// ClusterOptionalConfig groups optional cluster configuration for CreateCluster.
type ClusterOptionalConfig struct {
	AccessConfig     *AccessConfig
	ComputeConfig    *ComputeConfig
	StorageConfig    *StorageConfig
	NetworkingConfig *NetworkingConfig
}

// CreateCluster creates a new EKS cluster.
func (b *InMemoryBackend) CreateCluster( //nolint:funlen // existing issue.
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

	clusterARN := arn.Build("eks", b.region, b.accountID, "cluster/"+name)
	t := tags.New("eks.cluster." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if version == "" {
		version = defaultK8sVersion
	}

	var vpcCopy *VpcConfig
	if vpcConfig != nil {
		vpcCopy = cloneVpcConfig(vpcConfig, name)
	}

	var netCopy *KubernetesNetworkConfig
	if networkConfig != nil {
		cp := *networkConfig
		netCopy = &cp
	}

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

	var networkingCfg *NetworkingConfig
	if opt.NetworkingConfig != nil {
		cp := *opt.NetworkingConfig
		networkingCfg = &cp
	}

	c := &Cluster{
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
		NetworkingConfig:        networkingCfg,
		CertificateAuthority:    stableID(name + "/ca"),
	}
	b.clusters.Put(c)
	b.accessPolicies[name] = make(map[string][]*AccessPolicyAssociation)
	b.encryptionConfigs[name] = nil

	// Schedule async transition CREATING -> ACTIVE.
	b.work.After("ClusterTransition", clusterTransitionDelay, func() {
		b.mu.Lock("CreateCluster-async")
		defer b.mu.Unlock()

		if cl, found := b.clusters.Get(name); found && cl.Status == statusCreating {
			cl.Status = statusActive
		}
	})

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

// NodegroupInput holds optional fields for CreateNodegroup beyond positional params.
type NodegroupInput struct {
	Labels         map[string]string
	RemoteAccess   *RemoteAccess
	LaunchTemplate *LaunchTemplate
	UpdateConfig   *NodegroupUpdateConfig
	Subnets        []string
	Taints         []NodegroupTaint
	DiskSize       int32
}

const (
	nodegroupDiskSizeMin = 20
	nodegroupDiskSizeMax = 16384
)

// CreateNodegroup creates a new node group in a cluster.
func (b *InMemoryBackend) CreateNodegroup(
	clusterName, nodegroupName, nodeRole, amiType, capacityType, version, releaseVersion string,
	instanceTypes []string,
	desiredSize, minSize, maxSize int32,
	input NodegroupInput,
	kv map[string]string,
) (*Nodegroup, error) {
	b.mu.Lock("CreateNodegroup")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.nodegroups.Get(nodegroupKey(clusterName, nodegroupName)); ok {
		return nil, fmt.Errorf(
			"%w: nodegroup %s already exists in cluster %s",
			ErrAlreadyExists,
			nodegroupName,
			clusterName,
		)
	}

	if input.DiskSize != 0 && (input.DiskSize < nodegroupDiskSizeMin || input.DiskSize > nodegroupDiskSizeMax) {
		return nil, fmt.Errorf(
			"%w: diskSize %d is out of range [%d, %d]",
			ErrValidation, input.DiskSize, nodegroupDiskSizeMin, nodegroupDiskSizeMax,
		)
	}

	ngARN := arn.Build(
		"eks",
		b.region,
		b.accountID,
		"nodegroup/"+clusterName+"/"+nodegroupName+"/"+stableID(clusterName+"/"+nodegroupName),
	)
	t := tags.New("eks.nodegroup." + clusterName + "." + nodegroupName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if amiType == "" {
		amiType = "AL2_x86_64"
	}
	if capacityType == "" {
		capacityType = "ON_DEMAND"
	}

	asgName := "eks-" + nodegroupName + "-" + stableID(clusterName+"/"+nodegroupName)

	var updateCfg *NodegroupUpdateConfig
	if input.UpdateConfig != nil {
		uc := *input.UpdateConfig
		updateCfg = &uc
	}

	ng := &Nodegroup{
		NodegroupName:  nodegroupName,
		ClusterName:    clusterName,
		ARN:            ngARN,
		NodeRole:       nodeRole,
		Status:         statusCreating,
		AMIType:        amiType,
		CapacityType:   capacityType,
		InstanceTypes:  cloneStrings(instanceTypes),
		Version:        version,
		ReleaseVersion: releaseVersion,
		DesiredSize:    desiredSize,
		MinSize:        minSize,
		MaxSize:        maxSize,
		DiskSize:       input.DiskSize,
		Subnets:        cloneStrings(input.Subnets),
		Labels:         cloneStringMap(input.Labels),
		Taints:         cloneTaints(input.Taints),
		RemoteAccess:   cloneRemoteAccess(input.RemoteAccess),
		LaunchTemplate: cloneLaunchTemplate(input.LaunchTemplate),
		UpdateConfig:   updateCfg,
		Resources: &NodegroupResources{
			AutoScalingGroups: []AutoScalingGroup{{Name: asgName}},
		},
		AccountID: b.accountID,
		Region:    b.region,
		CreatedAt: time.Now().UTC(),
		Tags:      t,
	}
	b.nodegroups.Put(ng)

	// Schedule async transition CREATING -> ACTIVE.
	b.work.After("NodegroupTransition", nodegroupTransitionDelay, func() {
		b.mu.Lock("CreateNodegroup-async")
		defer b.mu.Unlock()

		if n, found := b.nodegroups.Get(nodegroupKey(clusterName, nodegroupName)); found && n.Status == statusCreating {
			n.Status = statusActive
		}
	})

	cp := deepCopyNodegroup(ng)

	return cp, nil
}

// DescribeNodegroup returns a node group by cluster and nodegroup name.
func (b *InMemoryBackend) DescribeNodegroup(clusterName, nodegroupName string) (*Nodegroup, error) {
	b.mu.RLock("DescribeNodegroup")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ng, ok := b.nodegroups.Get(nodegroupKey(clusterName, nodegroupName))
	if !ok {
		return nil, fmt.Errorf("%w: nodegroup %s not found in cluster %s", ErrNotFound, nodegroupName, clusterName)
	}

	return deepCopyNodegroup(ng), nil
}

// ListNodegroups returns all node group names in a cluster sorted alphabetically.
func (b *InMemoryBackend) ListNodegroups(clusterName string) ([]string, error) {
	b.mu.RLock("ListNodegroups")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	items := b.nodegroupsByCluster.Get(clusterName)
	names := make([]string, len(items))

	for i, ng := range items {
		names[i] = ng.NodegroupName
	}

	slices.Sort(names)

	return names, nil
}

// DeleteNodegroup deletes a node group from a cluster.
func (b *InMemoryBackend) DeleteNodegroup(clusterName, nodegroupName string) (*Nodegroup, error) {
	b.mu.Lock("DeleteNodegroup")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ng, ok := b.nodegroups.Get(nodegroupKey(clusterName, nodegroupName))
	if !ok {
		return nil, fmt.Errorf("%w: nodegroup %s not found in cluster %s", ErrNotFound, nodegroupName, clusterName)
	}
	cp := deepCopyNodegroup(ng)
	cp.Status = statusDeleting
	b.nodegroups.Delete(nodegroupKey(clusterName, nodegroupName))

	if ng.Tags != nil {
		ng.Tags.Close()
	}

	return cp, nil
}

// NodegroupConfigUpdate holds the mutable fields for UpdateNodegroupConfig.
type NodegroupConfigUpdate struct {
	AddOrUpdateLabels map[string]string
	UpdateConfig      *NodegroupUpdateConfig
	DesiredSize       *int32
	MinSize           *int32
	MaxSize           *int32
	RemoveLabels      []string
	AddOrUpdateTaints []NodegroupTaint
	RemoveTaints      []NodegroupTaint
}

// UpdateNodegroupConfig updates the configuration of a node group including scaling,
// labels, taints, and update strategy.
func (b *InMemoryBackend) UpdateNodegroupConfig(
	clusterName, nodegroupName string,
	upd NodegroupConfigUpdate,
) (*Nodegroup, error) {
	b.mu.Lock("UpdateNodegroupConfig")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ng, ok := b.nodegroups.Get(nodegroupKey(clusterName, nodegroupName))
	if !ok {
		return nil, fmt.Errorf("%w: nodegroup %s not found in cluster %s", ErrNotFound, nodegroupName, clusterName)
	}

	if upd.DesiredSize != nil {
		ng.DesiredSize = *upd.DesiredSize
	}

	if upd.MinSize != nil {
		ng.MinSize = *upd.MinSize
	}

	if upd.MaxSize != nil {
		ng.MaxSize = *upd.MaxSize
	}

	if len(upd.AddOrUpdateLabels) > 0 {
		if ng.Labels == nil {
			ng.Labels = make(map[string]string)
		}

		maps.Copy(ng.Labels, upd.AddOrUpdateLabels)
	}

	for _, k := range upd.RemoveLabels {
		delete(ng.Labels, k)
	}

	if len(upd.AddOrUpdateTaints) > 0 {
		ng.Taints = mergeTaints(ng.Taints, upd.AddOrUpdateTaints)
	}

	if len(upd.RemoveTaints) > 0 {
		ng.Taints = removeTaints(ng.Taints, upd.RemoveTaints)
	}

	if upd.UpdateConfig != nil {
		uc := *upd.UpdateConfig
		ng.UpdateConfig = &uc
	}

	return deepCopyNodegroup(ng), nil
}

// mergeTaints adds or updates taints in the existing slice.
func mergeTaints(existing []NodegroupTaint, updates []NodegroupTaint) []NodegroupTaint {
	result := make([]NodegroupTaint, 0, len(existing)+len(updates))
	result = append(result, existing...)

	for _, upd := range updates {
		found := false

		for i, t := range result {
			if t.Key == upd.Key {
				result[i] = upd
				found = true

				break
			}
		}

		if !found {
			result = append(result, upd)
		}
	}

	return result
}

// removeTaints removes taints matching by key+effect from the slice.
func removeTaints(existing []NodegroupTaint, toRemove []NodegroupTaint) []NodegroupTaint {
	result := existing[:0:len(existing)]

	for _, t := range existing {
		removed := false

		for _, r := range toRemove {
			if t.Key == r.Key && t.Effect == r.Effect {
				removed = true

				break
			}
		}

		if !removed {
			result = append(result, t)
		}
	}

	return result
}

// findTagInNodegroupsLocked searches nodegroups for a resource with the given ARN.
func (b *InMemoryBackend) findTagInNodegroupsLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.nodegroups.Range(func(ng *Nodegroup) bool {
		if ng.ARN == resourceARN {
			found = ng.Tags

			return false
		}

		return true
	})

	return found
}

// findTagInAccessEntriesAndAddonsLocked searches access entries and addons.
func (b *InMemoryBackend) findTagInAccessEntriesAndAddonsLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.accessEntries.Range(func(e *AccessEntry) bool {
		if e.ARN == resourceARN {
			found = e.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.addons.Range(func(a *Addon) bool {
		if a.ARN == resourceARN {
			found = a.Tags

			return false
		}

		return true
	})

	return found
}

// findTagInProfilesAndAssocLocked searches fargate profiles, pod identity
// associations, and subscriptions.
func (b *InMemoryBackend) findTagInProfilesAndAssocLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.fargateProfiles.Range(func(p *FargateProfile) bool {
		if p.ARN == resourceARN {
			found = p.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.podIdentityAssociations.Range(func(a *PodIdentityAssociation) bool {
		if a.ARN == resourceARN {
			found = a.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.capabilities.Range(func(capa *Capability) bool {
		if capa.ARN == resourceARN {
			found = capa.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	b.subscriptions.Range(func(sub *AnywhereSubscription) bool {
		if sub.ARN == resourceARN {
			found = sub.Tags

			return false
		}

		return true
	})

	return found
}

// findTagsForARNLocked returns a pointer to the tags.Tags for the resource
// identified by resourceARN. Must be called with b.mu held (read or write).
// Returns nil if the resource is not found.
func (b *InMemoryBackend) findTagsForARNLocked(resourceARN string) *tags.Tags {
	var found *tags.Tags

	b.clusters.Range(func(c *Cluster) bool {
		if c.ARN == resourceARN {
			found = c.Tags

			return false
		}

		return true
	})

	if found != nil {
		return found
	}

	if t := b.findTagInNodegroupsLocked(resourceARN); t != nil {
		return t
	}

	if t := b.findTagInAccessEntriesAndAddonsLocked(resourceARN); t != nil {
		return t
	}

	return b.findTagInProfilesAndAssocLocked(resourceARN)
}

// TagResource adds tags to a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t := b.findTagsForARNLocked(resourceARN)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t.Merge(kv)

	return nil
}

// UntagResource removes specific tag keys from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t := b.findTagsForARNLocked(resourceARN)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.findTagsForARNLocked(resourceARN)
	if t == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return t.Clone(), nil
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

// AddNodegroupInternal inserts a pre-built node group into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddNodegroupInternal(ng *Nodegroup) {
	b.mu.Lock("AddNodegroupInternal")
	defer b.mu.Unlock()

	if ng.Tags == nil {
		ng.Tags = tags.New("eks.nodegroup." + ng.ClusterName + "." + ng.NodegroupName + ".tags")
	}

	b.nodegroups.Put(ng)
}

// AddAccessEntryInternal inserts a pre-built access entry into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddAccessEntryInternal(e *AccessEntry) {
	b.mu.Lock("AddAccessEntryInternal")
	defer b.mu.Unlock()

	if e.Tags == nil {
		e.Tags = tags.New("eks.access-entry." + e.ClusterName + "." + stableID(e.PrincipalARN) + ".tags")
	}

	b.accessEntries.Put(e)
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

// AddFargateProfileInternal inserts a pre-built Fargate profile into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddFargateProfileInternal(p *FargateProfile) {
	b.mu.Lock("AddFargateProfileInternal")
	defer b.mu.Unlock()

	if p.Tags == nil {
		p.Tags = tags.New("eks.fargate." + p.ClusterName + "." + p.FargateProfileName + ".tags")
	}

	b.fargateProfiles.Put(p)
}

// AddCapabilityInternal inserts a pre-built capability into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddCapabilityInternal(capa *Capability) {
	b.mu.Lock("AddCapabilityInternal")
	defer b.mu.Unlock()

	if capa.Tags == nil {
		capa.Tags = tags.New("eks.capability." + capa.ClusterName + "." + capa.CapabilityName + ".tags")
	}

	b.capabilities.Put(capa)
}

// AddSubscriptionInternal inserts a pre-built EKS Anywhere subscription into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddSubscriptionInternal(sub *AnywhereSubscription) {
	b.mu.Lock("AddSubscriptionInternal")
	defer b.mu.Unlock()

	if sub.Tags == nil {
		sub.Tags = tags.New("eks.subscription." + sub.ID + ".tags")
	}

	b.subscriptions.Put(sub)
}

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

// cloneStrings returns a deep copy of a string slice (nil-safe).
func cloneStrings(ss []string) []string {
	if ss == nil {
		return nil
	}

	cp := make([]string, len(ss))
	copy(cp, ss)

	return cp
}

// cloneStringMap returns a deep copy of a string map (nil-safe).
func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}

// cloneTaints returns a deep copy of a NodegroupTaint slice (nil-safe).
func cloneTaints(ts []NodegroupTaint) []NodegroupTaint {
	if ts == nil {
		return nil
	}

	cp := make([]NodegroupTaint, len(ts))
	copy(cp, ts)

	return cp
}

// cloneRemoteAccess returns a deep copy of a RemoteAccess struct (nil-safe).
func cloneRemoteAccess(ra *RemoteAccess) *RemoteAccess {
	if ra == nil {
		return nil
	}

	cp := *ra
	cp.SourceSecurityGroups = cloneStrings(ra.SourceSecurityGroups)

	return &cp
}

// cloneLaunchTemplate returns a deep copy of a LaunchTemplate struct (nil-safe).
func cloneLaunchTemplate(lt *LaunchTemplate) *LaunchTemplate {
	if lt == nil {
		return nil
	}

	cp := *lt

	return &cp
}

// deepCopyNodegroup returns a deep copy of a Nodegroup with all slice/map fields duplicated.
func deepCopyNodegroup(ng *Nodegroup) *Nodegroup {
	cp := *ng
	cp.InstanceTypes = cloneStrings(ng.InstanceTypes)
	cp.Subnets = cloneStrings(ng.Subnets)
	cp.Labels = cloneStringMap(ng.Labels)
	cp.Taints = cloneTaints(ng.Taints)
	cp.RemoteAccess = cloneRemoteAccess(ng.RemoteAccess)
	cp.LaunchTemplate = cloneLaunchTemplate(ng.LaunchTemplate)

	if ng.Resources != nil {
		resCp := *ng.Resources
		resCp.AutoScalingGroups = make([]AutoScalingGroup, len(ng.Resources.AutoScalingGroups))
		copy(resCp.AutoScalingGroups, ng.Resources.AutoScalingGroups)
		cp.Resources = &resCp
	}

	if ng.UpdateConfig != nil {
		uc := *ng.UpdateConfig
		cp.UpdateConfig = &uc
	}

	return &cp
}

// stableID returns a deterministic 8-character hex identifier derived from the
// input string using FNV-1a (a non-cryptographic hash). The identifier is
// stable across calls but only 32 bits long, so collisions are possible at
// scale; it should be used only for non-critical IDs such as test ARN suffixes
// and endpoint URL components, not for strong uniqueness or cryptographic
// guarantees.
func stableID(input string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(input))

	return fmt.Sprintf("%08x", h.Sum32())
}

// oidcIDBytes is the number of random bytes needed to produce a 16-char hex OIDC ID.
const oidcIDBytes = 8

// randomHex16 returns a random 16-character lowercase hex string for OIDC IDs.
func randomHex16() string {
	buf := make([]byte, oidcIDBytes)
	if _, err := rand.Read(buf); err != nil {
		// Fallback: use time-based value (safe for non-cryptographic use).
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}
