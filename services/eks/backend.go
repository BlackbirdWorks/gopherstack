package eks

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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
	// EncryptionConfig holds the current cluster encryption config, kept in sync
	// with b.encryptionConfigs. Populated by AssociateEncryptionConfig.
	EncryptionConfig []EncryptionConfig `json:"encryptionConfig,omitempty"`
	Name             string             `json:"name"`
	ARN              string             `json:"arn"`
	Endpoint         string             `json:"endpoint,omitempty"`
	OIDCIssuer       string             `json:"oidcIssuer,omitempty"`
	Version          string             `json:"version"`
	Status           string             `json:"status"`
	RoleARN          string             `json:"roleArn,omitempty"`
	AccountID        string             `json:"accountId"`
	Region           string             `json:"region"`
	PlatformVersion  string             `json:"platformVersion,omitempty"`
	ClusterLogging   []ClusterLogEntry  `json:"clusterLogging,omitempty"`
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
type InMemoryBackend struct {
	clusters                map[string]*Cluster
	nodegroups              map[string]map[string]*Nodegroup // clusterName -> nodegroupName -> nodegroup
	accessEntries           map[string]map[string]*AccessEntry
	accessPolicies          map[string]map[string][]*AccessPolicyAssociation
	encryptionConfigs       map[string][]EncryptionConfig
	identityProviderConfigs map[string]map[string]*IdentityProviderConfig
	addons                  map[string]map[string]*Addon
	fargateProfiles         map[string]map[string]*FargateProfile
	podIdentityAssociations map[string]map[string]*PodIdentityAssociation
	capabilities            map[string]*Capability
	subscriptions           map[string]*AnywhereSubscription
	mu                      *lockmetrics.RWMutex
	accountID               string
	region                  string
}

// NewInMemoryBackend creates a new in-memory EKS backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:                make(map[string]*Cluster),
		nodegroups:              make(map[string]map[string]*Nodegroup),
		accessEntries:           make(map[string]map[string]*AccessEntry),
		accessPolicies:          make(map[string]map[string][]*AccessPolicyAssociation),
		encryptionConfigs:       make(map[string][]EncryptionConfig),
		identityProviderConfigs: make(map[string]map[string]*IdentityProviderConfig),
		addons:                  make(map[string]map[string]*Addon),
		fargateProfiles:         make(map[string]map[string]*FargateProfile),
		podIdentityAssociations: make(map[string]map[string]*PodIdentityAssociation),
		capabilities:            make(map[string]*Capability),
		subscriptions:           make(map[string]*AnywhereSubscription),
		accountID:               accountID,
		region:                  region,
		mu:                      lockmetrics.New("eks"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state, returning the backend to a fresh empty state.
// closeClusterTagsLocked closes tag objects for clusters and nodegroups.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeClusterTagsLocked() {
	for _, c := range b.clusters {
		if c.Tags != nil {
			c.Tags.Close()
		}
	}

	for _, ngs := range b.nodegroups {
		for _, ng := range ngs {
			if ng.Tags != nil {
				ng.Tags.Close()
			}
		}
	}
}

// closeEntryTagsLocked closes tag objects for access entries and addons.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeEntryTagsLocked() {
	for _, entries := range b.accessEntries {
		for _, e := range entries {
			if e.Tags != nil {
				e.Tags.Close()
			}
		}
	}

	for _, clusterAddons := range b.addons {
		for _, a := range clusterAddons {
			if a.Tags != nil {
				a.Tags.Close()
			}
		}
	}
}

// closeProfileTagsLocked closes tag objects for fargate profiles, pod identity
// associations, identity provider configs, and subscriptions.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeProfileTagsLocked() {
	for _, profiles := range b.fargateProfiles {
		for _, p := range profiles {
			if p.Tags != nil {
				p.Tags.Close()
			}
		}
	}

	for _, assocs := range b.podIdentityAssociations {
		for _, a := range assocs {
			if a.Tags != nil {
				a.Tags.Close()
			}
		}
	}

	b.closeIDPAndSubscriptionTagsLocked()
}

func (b *InMemoryBackend) closeIDPAndSubscriptionTagsLocked() {
	for _, idpCfgs := range b.identityProviderConfigs {
		for _, cfg := range idpCfgs {
			if cfg.Tags != nil {
				cfg.Tags.Close()
			}
		}
	}

	for _, sub := range b.subscriptions {
		if sub.Tags != nil {
			sub.Tags.Close()
		}
	}
}

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all tag resources to deregister Prometheus labels.
	b.closeClusterTagsLocked()
	b.closeEntryTagsLocked()
	b.closeProfileTagsLocked()

	b.clusters = make(map[string]*Cluster)
	b.nodegroups = make(map[string]map[string]*Nodegroup)
	b.accessEntries = make(map[string]map[string]*AccessEntry)
	b.accessPolicies = make(map[string]map[string][]*AccessPolicyAssociation)
	b.encryptionConfigs = make(map[string][]EncryptionConfig)
	b.identityProviderConfigs = make(map[string]map[string]*IdentityProviderConfig)
	b.addons = make(map[string]map[string]*Addon)
	b.fargateProfiles = make(map[string]map[string]*FargateProfile)
	b.podIdentityAssociations = make(map[string]map[string]*PodIdentityAssociation)
	b.capabilities = make(map[string]*Capability)
	b.subscriptions = make(map[string]*AnywhereSubscription)
}

// ClusterOptionalConfig groups optional cluster configuration for CreateCluster.
type ClusterOptionalConfig struct {
	AccessConfig     *AccessConfig
	ComputeConfig    *ComputeConfig
	StorageConfig    *StorageConfig
	NetworkingConfig *NetworkingConfig
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

	if _, ok := b.clusters[name]; ok {
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

	// Schedule async transition CREATING -> ACTIVE.
	time.AfterFunc(clusterTransitionDelay, func() {
		b.mu.Lock("CreateCluster-async")
		defer b.mu.Unlock()

		if cl, found := b.clusters[name]; found && cl.Status == statusCreating {
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

	c, ok := b.clusters[name]
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

	names := make([]string, 0, len(b.clusters))
	for name := range b.clusters {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// closeAccessEntryTagsForCluster closes tag objects for all access entries in
// a cluster and removes them from the maps. Must be called with b.mu held.
func (b *InMemoryBackend) closeAccessEntryTagsForCluster(clusterName string) {
	for _, e := range b.accessEntries[clusterName] {
		if e.Tags != nil {
			e.Tags.Close()
		}
	}

	delete(b.accessEntries, clusterName)
	delete(b.accessPolicies, clusterName)
}

// closeIDPAndPodTagsForCluster closes tag objects for identity provider configs
// and pod identity associations in a cluster, then removes them from the maps.
// Must be called with b.mu held.
func (b *InMemoryBackend) closeIDPAndPodTagsForCluster(clusterName string) {
	for _, cfg := range b.identityProviderConfigs[clusterName] {
		if cfg.Tags != nil {
			cfg.Tags.Close()
		}
	}

	delete(b.identityProviderConfigs, clusterName)

	for _, a := range b.podIdentityAssociations[clusterName] {
		if a.Tags != nil {
			a.Tags.Close()
		}
	}

	delete(b.podIdentityAssociations, clusterName)
}

// DeleteCluster deletes a cluster by name. For test convenience this fake cascades
// and removes nodegroups as well (real AWS requires manual nodegroup deletion first).
func (b *InMemoryBackend) DeleteCluster(name string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters[name]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, name)
	}

	cp := *c
	cp.Status = statusDeleting

	// Collect nodegroups for cleanup before removal.
	ngs := b.nodegroups[name]
	delete(b.clusters, name)
	delete(b.nodegroups, name)
	delete(b.encryptionConfigs, name)
	delete(b.addons, name)
	delete(b.fargateProfiles, name)

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

const nodegroupDiskSizeMin = 20
const nodegroupDiskSizeMax = 16384

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

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	// Defensive: ensure the inner map is always initialised.
	// Under normal operation CreateCluster always initialises it, but this
	// guard prevents a panic if state is inconsistent (e.g. restored from a
	// partial snapshot).
	if b.nodegroups[clusterName] == nil {
		b.nodegroups[clusterName] = make(map[string]*Nodegroup)
	}

	if _, ok := b.nodegroups[clusterName][nodegroupName]; ok {
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
	b.nodegroups[clusterName][nodegroupName] = ng

	// Schedule async transition CREATING -> ACTIVE.
	time.AfterFunc(nodegroupTransitionDelay, func() {
		b.mu.Lock("CreateNodegroup-async")
		defer b.mu.Unlock()

		if ngs, ok := b.nodegroups[clusterName]; ok {
			if n, found := ngs[nodegroupName]; found && n.Status == statusCreating {
				n.Status = statusActive
			}
		}
	})

	cp := deepCopyNodegroup(ng)

	return cp, nil
}

// DescribeNodegroup returns a node group by cluster and nodegroup name.
func (b *InMemoryBackend) DescribeNodegroup(clusterName, nodegroupName string) (*Nodegroup, error) {
	b.mu.RLock("DescribeNodegroup")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ng, ok := b.nodegroups[clusterName][nodegroupName]
	if !ok {
		return nil, fmt.Errorf("%w: nodegroup %s not found in cluster %s", ErrNotFound, nodegroupName, clusterName)
	}

	return deepCopyNodegroup(ng), nil
}

// ListNodegroups returns all node group names in a cluster sorted alphabetically.
func (b *InMemoryBackend) ListNodegroups(clusterName string) ([]string, error) {
	b.mu.RLock("ListNodegroups")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	names := make([]string, 0, len(b.nodegroups[clusterName]))
	for name := range b.nodegroups[clusterName] {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// DeleteNodegroup deletes a node group from a cluster.
func (b *InMemoryBackend) DeleteNodegroup(clusterName, nodegroupName string) (*Nodegroup, error) {
	b.mu.Lock("DeleteNodegroup")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ng, ok := b.nodegroups[clusterName][nodegroupName]
	if !ok {
		return nil, fmt.Errorf("%w: nodegroup %s not found in cluster %s", ErrNotFound, nodegroupName, clusterName)
	}
	cp := deepCopyNodegroup(ng)
	cp.Status = statusDeleting
	delete(b.nodegroups[clusterName], nodegroupName)

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

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ngs, ok := b.nodegroups[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s has no nodegroups", ErrNotFound, clusterName)
	}

	ng, ok := ngs[nodegroupName]
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

// findTagInNodegroupsLocked searches nodegroupsfor a resource with the given ARN.
func (b *InMemoryBackend) findTagInNodegroupsLocked(resourceARN string) *tags.Tags {
	for _, ngs := range b.nodegroups {
		for _, ng := range ngs {
			if ng.ARN == resourceARN {
				return ng.Tags
			}
		}
	}

	return nil
}

// findTagInAccessEntriesAndAddonsLocked searches access entries and addons.
func (b *InMemoryBackend) findTagInAccessEntriesAndAddonsLocked(resourceARN string) *tags.Tags {
	for _, entries := range b.accessEntries {
		for _, e := range entries {
			if e.ARN == resourceARN {
				return e.Tags
			}
		}
	}

	for _, clusterAddons := range b.addons {
		for _, a := range clusterAddons {
			if a.ARN == resourceARN {
				return a.Tags
			}
		}
	}

	return nil
}

// findTagInProfilesAndAssocLocked searches fargate profiles, pod identity
// associations, and subscriptions.
func (b *InMemoryBackend) findTagInProfilesAndAssocLocked(resourceARN string) *tags.Tags {
	for _, profiles := range b.fargateProfiles {
		for _, p := range profiles {
			if p.ARN == resourceARN {
				return p.Tags
			}
		}
	}

	for _, assocs := range b.podIdentityAssociations {
		for _, a := range assocs {
			if a.ARN == resourceARN {
				return a.Tags
			}
		}
	}

	for _, sub := range b.subscriptions {
		if sub.ARN == resourceARN {
			return sub.Tags
		}
	}

	return nil
}

// findTagsForARNLocked returns a pointer to the tags.Tags for the resource
// identified by resourceARN. Must be called with b.mu held (read or write).
// Returns nil if the resource is not found.
func (b *InMemoryBackend) findTagsForARNLocked(resourceARN string) *tags.Tags {
	for _, c := range b.clusters {
		if c.ARN == resourceARN {
			return c.Tags
		}
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

	b.clusters[c.Name] = c

	if b.nodegroups[c.Name] == nil {
		b.nodegroups[c.Name] = make(map[string]*Nodegroup)
	}

	if b.accessEntries[c.Name] == nil {
		b.accessEntries[c.Name] = make(map[string]*AccessEntry)
	}

	if b.accessPolicies[c.Name] == nil {
		b.accessPolicies[c.Name] = make(map[string][]*AccessPolicyAssociation)
	}

	if b.identityProviderConfigs[c.Name] == nil {
		b.identityProviderConfigs[c.Name] = make(map[string]*IdentityProviderConfig)
	}

	if b.addons[c.Name] == nil {
		b.addons[c.Name] = make(map[string]*Addon)
	}

	if b.fargateProfiles[c.Name] == nil {
		b.fargateProfiles[c.Name] = make(map[string]*FargateProfile)
	}

	if b.podIdentityAssociations[c.Name] == nil {
		b.podIdentityAssociations[c.Name] = make(map[string]*PodIdentityAssociation)
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

	if b.nodegroups[ng.ClusterName] == nil {
		b.nodegroups[ng.ClusterName] = make(map[string]*Nodegroup)
	}

	b.nodegroups[ng.ClusterName][ng.NodegroupName] = ng
}

// AddAccessEntryInternal inserts a pre-built access entry into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddAccessEntryInternal(e *AccessEntry) {
	b.mu.Lock("AddAccessEntryInternal")
	defer b.mu.Unlock()

	if e.Tags == nil {
		e.Tags = tags.New("eks.access-entry." + e.ClusterName + "." + stableID(e.PrincipalARN) + ".tags")
	}

	if b.accessEntries[e.ClusterName] == nil {
		b.accessEntries[e.ClusterName] = make(map[string]*AccessEntry)
	}

	b.accessEntries[e.ClusterName][e.PrincipalARN] = e
}

// AddAddonInternal inserts a pre-built add-on into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddAddonInternal(a *Addon) {
	b.mu.Lock("AddAddonInternal")
	defer b.mu.Unlock()

	if a.Tags == nil {
		a.Tags = tags.New("eks.addon." + a.ClusterName + "." + a.AddonName + ".tags")
	}

	if b.addons[a.ClusterName] == nil {
		b.addons[a.ClusterName] = make(map[string]*Addon)
	}

	b.addons[a.ClusterName][a.AddonName] = a
}

// AddFargateProfileInternal inserts a pre-built Fargate profile into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddFargateProfileInternal(p *FargateProfile) {
	b.mu.Lock("AddFargateProfileInternal")
	defer b.mu.Unlock()

	if p.Tags == nil {
		p.Tags = tags.New("eks.fargate." + p.ClusterName + "." + p.FargateProfileName + ".tags")
	}

	if b.fargateProfiles[p.ClusterName] == nil {
		b.fargateProfiles[p.ClusterName] = make(map[string]*FargateProfile)
	}

	b.fargateProfiles[p.ClusterName][p.FargateProfileName] = p
}

// AddCapabilityInternal inserts a pre-built capability into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddCapabilityInternal(capa *Capability) {
	b.mu.Lock("AddCapabilityInternal")
	defer b.mu.Unlock()

	b.capabilities[capa.Name] = capa
}

// AddSubscriptionInternal inserts a pre-built EKS Anywhere subscription into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddSubscriptionInternal(sub *AnywhereSubscription) {
	b.mu.Lock("AddSubscriptionInternal")
	defer b.mu.Unlock()

	if sub.Tags == nil {
		sub.Tags = tags.New("eks.subscription." + sub.ID + ".tags")
	}

	b.subscriptions[sub.ID] = sub
}

func (b *InMemoryBackend) ListAllClusters() []*Cluster {
	b.mu.RLock("ListAllClusters")
	defer b.mu.RUnlock()

	list := make([]*Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
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
// input string using SHA-256. The identifier is stable across calls but only
// 32 bits long, so collisions are possible at scale; it should be used only
// for non-critical IDs such as test ARN suffixes and endpoint URL components,
// not for strong uniqueness or cryptographic guarantees.
func stableID(input string) string {
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])[:8]
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
