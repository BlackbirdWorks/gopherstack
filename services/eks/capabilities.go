package eks

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateCapability creates a new EKS capability scoped to a cluster.
// CapabilityName is unique per cluster, not globally.
func (b *InMemoryBackend) CreateCapability(
	clusterName, capabilityName, capType, roleARN, deletePropagationPolicy string,
	kv map[string]string,
) (*Capability, error) {
	b.mu.Lock("CreateCapability")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	key := capabilityKey(clusterName, capabilityName)
	if _, ok := b.capabilities.Get(key); ok {
		return nil, fmt.Errorf(
			"%w: capability %s already exists in cluster %s", ErrAlreadyExists, capabilityName, clusterName,
		)
	}

	capaARN := arn.Build("eks", b.region, b.accountID, "capability/"+clusterName+"/"+capabilityName)
	t := tags.New("eks.capability." + clusterName + "." + capabilityName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	capa := &Capability{
		ClusterName:             clusterName,
		CapabilityName:          capabilityName,
		ARN:                     capaARN,
		Type:                    capType,
		RoleARN:                 roleARN,
		DeletePropagationPolicy: deletePropagationPolicy,
		Status:                  statusActive,
		CreatedAt:               time.Now().UTC(),
		Tags:                    t,
	}
	b.capabilities.Put(capa)
	cp := *capa

	return &cp, nil
}

// DeleteCapability removes a capability by cluster and capability name.
func (b *InMemoryBackend) DeleteCapability(clusterName, capabilityName string) (*Capability, error) {
	b.mu.Lock("DeleteCapability")
	defer b.mu.Unlock()

	key := capabilityKey(clusterName, capabilityName)

	capa, ok := b.capabilities.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: capability %s not found in cluster %s", ErrNotFound, capabilityName, clusterName)
	}

	cp := *capa
	b.capabilities.Delete(key)

	if capa.Tags != nil {
		capa.Tags.Close()
	}

	cp.Status = statusDeleting

	return &cp, nil
}

// DescribeCapability returns a capability by cluster and capability name.
func (b *InMemoryBackend) DescribeCapability(clusterName, capabilityName string) (*Capability, error) {
	b.mu.RLock("DescribeCapability")
	defer b.mu.RUnlock()

	capa, ok := b.capabilities.Get(capabilityKey(clusterName, capabilityName))
	if !ok {
		return nil, fmt.Errorf("%w: capability %s not found in cluster %s", ErrNotFound, capabilityName, clusterName)
	}

	cp := *capa

	return &cp, nil
}

// ListCapabilities returns all capability names in a cluster sorted alphabetically.
func (b *InMemoryBackend) ListCapabilities(clusterName string) []string {
	b.mu.RLock("ListCapabilities")
	defer b.mu.RUnlock()

	items := b.capabilitiesByCluster.Get(clusterName)
	names := make([]string, len(items))

	for i, c := range items {
		names[i] = c.CapabilityName
	}

	sort.Strings(names)

	return names
}

// UpdateCapability updates an existing capability's role ARN and/or delete
// propagation policy.
func (b *InMemoryBackend) UpdateCapability(
	clusterName, capabilityName, roleARN, deletePropagationPolicy string,
) (*Capability, error) {
	b.mu.Lock("UpdateCapability")
	defer b.mu.Unlock()

	key := capabilityKey(clusterName, capabilityName)

	capa, ok := b.capabilities.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: capability %s not found in cluster %s", ErrNotFound, capabilityName, clusterName)
	}

	if roleARN != "" {
		capa.RoleARN = roleARN
	}

	if deletePropagationPolicy != "" {
		capa.DeletePropagationPolicy = deletePropagationPolicy
	}

	cp := *capa

	return &cp, nil
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

// ListAllCapabilities returns all capabilities.
func (b *InMemoryBackend) ListAllCapabilities() []*Capability {
	b.mu.RLock("ListAllCapabilities")
	defer b.mu.RUnlock()

	items := b.capabilities.All()
	list := make([]*Capability, 0, len(items))

	for _, c := range items {
		cp := *c
		list = append(list, &cp)
	}

	return list
}
