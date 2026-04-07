package eks

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when an EKS resource is not found.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when an EKS resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrConflict)
)

// Cluster represents an EKS cluster.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateCluster.
type Cluster struct {
	CreatedAt       time.Time  `json:"createdAt"`
	Tags            *tags.Tags `json:"tags,omitempty"`
	Name            string     `json:"name"`
	ARN             string     `json:"arn"`
	Endpoint        string     `json:"endpoint,omitempty"`
	Version         string     `json:"version"`
	Status          string     `json:"status"`
	RoleARN         string     `json:"roleArn,omitempty"`
	AccountID       string     `json:"accountId"`
	Region          string     `json:"region"`
	PlatformVersion string     `json:"platformVersion,omitempty"`
}

// Nodegroup represents an EKS managed node group.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateNodegroup.
type Nodegroup struct {
	CreatedAt     time.Time  `json:"createdAt"`
	Tags          *tags.Tags `json:"tags,omitempty"`
	CapacityType  string     `json:"capacityType,omitempty"`
	Region        string     `json:"region"`
	ARN           string     `json:"nodegroupArn"`
	NodeRole      string     `json:"nodeRole,omitempty"`
	Status        string     `json:"status"`
	AMIType       string     `json:"amiType,omitempty"`
	NodegroupName string     `json:"nodegroupName"`
	ClusterName   string     `json:"clusterName"`
	Version       string     `json:"version,omitempty"`
	AccountID     string     `json:"accountId"`
	InstanceTypes []string   `json:"instanceTypes,omitempty"`
	DesiredSize   int32      `json:"desiredSize"`
	MinSize       int32      `json:"minSize"`
	MaxSize       int32      `json:"maxSize"`
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
	subscriptions           map[string]*EksAnywhereSubscription
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
		subscriptions:           make(map[string]*EksAnywhereSubscription),
		accountID:               accountID,
		region:                  region,
		mu:                      lockmetrics.New("eks"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateCluster creates a new EKS cluster.
func (b *InMemoryBackend) CreateCluster(name, version, roleARN string, kv map[string]string) (*Cluster, error) {
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
		version = "1.32"
	}

	c := &Cluster{
		Name:            name,
		ARN:             clusterARN,
		Version:         version,
		RoleARN:         roleARN,
		Status:          "ACTIVE",
		Endpoint:        fmt.Sprintf("https://%s.%s.eks.amazonaws.com", stableID(name), b.region),
		PlatformVersion: "eks.1",
		AccountID:       b.accountID,
		Region:          b.region,
		CreatedAt:       time.Now().UTC(),
		Tags:            t,
	}
	b.clusters[name] = c
	b.nodegroups[name] = make(map[string]*Nodegroup)
	cp := *c

	return &cp, nil
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

// ListClusters returns all cluster names.
func (b *InMemoryBackend) ListClusters() []string {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.clusters))
	for name := range b.clusters {
		names = append(names, name)
	}

	return names
}

// DeleteCluster deletes a cluster by name.
func (b *InMemoryBackend) DeleteCluster(name string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, ok := b.clusters[name]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, name)
	}
	cp := *c

	// Collect nodegroups for cleanup before removal.
	ngs := b.nodegroups[name]
	delete(b.clusters, name)
	delete(b.nodegroups, name)
	delete(b.encryptionConfigs, name)
	delete(b.addons, name)
	delete(b.fargateProfiles, name)

	// Close access entry tags.
	if entries := b.accessEntries[name]; entries != nil {
		for _, e := range entries {
			if e.Tags != nil {
				e.Tags.Close()
			}
		}
	}
	delete(b.accessEntries, name)
	delete(b.accessPolicies, name)

	// Close identity provider config tags.
	if idpCfgs := b.identityProviderConfigs[name]; idpCfgs != nil {
		for _, cfg := range idpCfgs {
			if cfg.Tags != nil {
				cfg.Tags.Close()
			}
		}
	}
	delete(b.identityProviderConfigs, name)

	// Close pod identity association tags.
	if assocs := b.podIdentityAssociations[name]; assocs != nil {
		for _, a := range assocs {
			if a.Tags != nil {
				a.Tags.Close()
			}
		}
	}
	delete(b.podIdentityAssociations, name)

	// Release tag resources outside the map but still inside the lock since
	// Tags.Close only unregisters Prometheus labels (cheap and non-blocking).
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

// CreateNodegroup creates a new node group in a cluster.
func (b *InMemoryBackend) CreateNodegroup(
	clusterName, nodegroupName, nodeRole, amiType, capacityType, version string,
	instanceTypes []string,
	desiredSize, minSize, maxSize int32,
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

	ng := &Nodegroup{
		NodegroupName: nodegroupName,
		ClusterName:   clusterName,
		ARN:           ngARN,
		NodeRole:      nodeRole,
		Status:        "ACTIVE",
		AMIType:       amiType,
		CapacityType:  capacityType,
		InstanceTypes: instanceTypes,
		Version:       version,
		DesiredSize:   desiredSize,
		MinSize:       minSize,
		MaxSize:       maxSize,
		AccountID:     b.accountID,
		Region:        b.region,
		CreatedAt:     time.Now().UTC(),
		Tags:          t,
	}
	b.nodegroups[clusterName][nodegroupName] = ng
	cp := *ng
	cp.InstanceTypes = make([]string, len(ng.InstanceTypes))
	copy(cp.InstanceTypes, ng.InstanceTypes)

	return &cp, nil
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
	cp := *ng
	cp.InstanceTypes = make([]string, len(ng.InstanceTypes))
	copy(cp.InstanceTypes, ng.InstanceTypes)

	return &cp, nil
}

// ListNodegroups returns all node group names in a cluster.
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
	cp := *ng
	cp.InstanceTypes = make([]string, len(ng.InstanceTypes))
	copy(cp.InstanceTypes, ng.InstanceTypes)
	delete(b.nodegroups[clusterName], nodegroupName)

	if ng.Tags != nil {
		ng.Tags.Close()
	}

	return &cp, nil
}

// UpdateNodegroupConfig updates the scaling configuration of a node group.
func (b *InMemoryBackend) UpdateNodegroupConfig(
	clusterName, nodegroupName string,
	desiredSize, minSize, maxSize *int32,
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

	if desiredSize != nil {
		ng.DesiredSize = *desiredSize
	}

	if minSize != nil {
		ng.MinSize = *minSize
	}

	if maxSize != nil {
		ng.MaxSize = *maxSize
	}

	cp := *ng
	cp.InstanceTypes = make([]string, len(ng.InstanceTypes))
	copy(cp.InstanceTypes, ng.InstanceTypes)

	return &cp, nil
}

// TagResource adds tags to a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, c := range b.clusters {
		if c.ARN == resourceARN {
			c.Tags.Merge(kv)

			return nil
		}
	}

	for _, ngs := range b.nodegroups {
		for _, ng := range ngs {
			if ng.ARN == resourceARN {
				ng.Tags.Merge(kv)

				return nil
			}
		}
	}

	for _, entries := range b.accessEntries {
		for _, e := range entries {
			if e.ARN == resourceARN {
				e.Tags.Merge(kv)

				return nil
			}
		}
	}

	for _, clusterAddons := range b.addons {
		for _, a := range clusterAddons {
			if a.ARN == resourceARN {
				a.Tags.Merge(kv)

				return nil
			}
		}
	}

	for _, profiles := range b.fargateProfiles {
		for _, p := range profiles {
			if p.ARN == resourceARN {
				p.Tags.Merge(kv)

				return nil
			}
		}
	}

	for _, assocs := range b.podIdentityAssociations {
		for _, a := range assocs {
			if a.ARN == resourceARN {
				a.Tags.Merge(kv)

				return nil
			}
		}
	}

	for _, sub := range b.subscriptions {
		if sub.ARN == resourceARN {
			sub.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes specific tag keys from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, c := range b.clusters {
		if c.ARN == resourceARN {
			c.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}

	for _, ngs := range b.nodegroups {
		for _, ng := range ngs {
			if ng.ARN == resourceARN {
				ng.Tags.DeleteKeys(tagKeys)

				return nil
			}
		}
	}

	for _, entries := range b.accessEntries {
		for _, e := range entries {
			if e.ARN == resourceARN {
				e.Tags.DeleteKeys(tagKeys)

				return nil
			}
		}
	}

	for _, clusterAddons := range b.addons {
		for _, a := range clusterAddons {
			if a.ARN == resourceARN {
				a.Tags.DeleteKeys(tagKeys)

				return nil
			}
		}
	}

	for _, profiles := range b.fargateProfiles {
		for _, p := range profiles {
			if p.ARN == resourceARN {
				p.Tags.DeleteKeys(tagKeys)

				return nil
			}
		}
	}

	for _, assocs := range b.podIdentityAssociations {
		for _, a := range assocs {
			if a.ARN == resourceARN {
				a.Tags.DeleteKeys(tagKeys)

				return nil
			}
		}
	}

	for _, sub := range b.subscriptions {
		if sub.ARN == resourceARN {
			sub.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, c := range b.clusters {
		if c.ARN == resourceARN {
			return c.Tags.Clone(), nil
		}
	}

	for _, ngs := range b.nodegroups {
		for _, ng := range ngs {
			if ng.ARN == resourceARN {
				return ng.Tags.Clone(), nil
			}
		}
	}

	for _, entries := range b.accessEntries {
		for _, e := range entries {
			if e.ARN == resourceARN {
				return e.Tags.Clone(), nil
			}
		}
	}

	for _, clusterAddons := range b.addons {
		for _, a := range clusterAddons {
			if a.ARN == resourceARN {
				return a.Tags.Clone(), nil
			}
		}
	}

	for _, profiles := range b.fargateProfiles {
		for _, p := range profiles {
			if p.ARN == resourceARN {
				return p.Tags.Clone(), nil
			}
		}
	}

	for _, assocs := range b.podIdentityAssociations {
		for _, a := range assocs {
			if a.ARN == resourceARN {
				return a.Tags.Clone(), nil
			}
		}
	}

	for _, sub := range b.subscriptions {
		if sub.ARN == resourceARN {
			return sub.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListAllClusters returns all clusters as a slice (for dashboard use).
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

// stableID returns a deterministic 8-character hex identifier derived from the
// input string using SHA-256. The identifier is stable across calls but only
// 32 bits long, so collisions are possible at scale; it should be used only
// for non-critical IDs such as test ARN suffixes and endpoint URL components,
// not for strong uniqueness or cryptographic guarantees.
func stableID(input string) string {
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])[:8]
}
