package eks

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// capabilityTypeArgoCd is the only CapabilityType (verified against
// aws-sdk-go-v2/service/eks/types.CapabilityType, enums.go) whose
// Configuration this pinned SDK version models -- see CapabilityConfiguration's
// doc comment in models.go.
const capabilityTypeArgoCd = "ARGOCD"

// CreateCapability creates a new EKS capability scoped to a cluster.
// CapabilityName is unique per cluster, not globally. config may be nil.
func (b *InMemoryBackend) CreateCapability(
	clusterName, capabilityName, capType, roleARN, deletePropagationPolicy string,
	config *CapabilityConfiguration,
	kv map[string]string,
) (*Capability, error) {
	b.mu.Lock("CreateCapability")
	defer b.mu.Unlock()

	// CreateCapability's own deserializer (eks@v1.90.4 deserializers.go) has
	// no ResourceNotFoundException case -- an unknown cluster here is
	// ErrValidation (InvalidParameterException), not ErrNotFound.
	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrValidation, clusterName)
	}

	key := capabilityKey(clusterName, capabilityName)
	if _, ok := b.capabilities.Get(key); ok {
		return nil, fmt.Errorf(
			"%w: capability %s already exists in cluster %s", ErrAlreadyExists, capabilityName, clusterName,
		)
	}

	if config != nil && config.ArgoCd != nil && capType != capabilityTypeArgoCd {
		return nil, fmt.Errorf(
			"%w: configuration.argoCd is only valid for type %s",
			ErrValidation,
			capabilityTypeArgoCd,
		)
	}

	// https://docs.aws.amazon.com/eks/latest/userguide/capabilities.html
	// (WebFetch'd 2026-09-11): "You can create one capability resource of
	// each type ... for a given cluster. You cannot create multiple
	// capability resources of the same type on the same cluster." A fixed
	// structural rule (always 1), not an AWS-adjustable Service Quota, so it
	// is not part of resourceLimits/limits.go.
	for _, existing := range b.capabilitiesByCluster.Get(clusterName) {
		if existing.Type == capType {
			return nil, resourceLimitExceededErr(
				"capability of type "+capType+" per cluster (one per cluster)", 1,
			)
		}
	}

	capaARN := arn.Build("eks", b.region, b.accountID, "capability/"+clusterName+"/"+capabilityName)
	t := tags.New("eks.capability." + clusterName + "." + capabilityName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	now := time.Now().UTC()
	capa := &Capability{
		ClusterName:             clusterName,
		CapabilityName:          capabilityName,
		ARN:                     capaARN,
		Type:                    capType,
		RoleARN:                 roleARN,
		DeletePropagationPolicy: deletePropagationPolicy,
		Status:                  statusActive,
		CreatedAt:               now,
		ModifiedAt:              now,
		Tags:                    t,
		Health:                  &CapabilityHealth{Issues: []CapabilityIssue{}},
		Configuration:           config,
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

// ListCapabilities returns all capabilities in a cluster (deep-copied,
// sorted by name). The real API's ListCapabilities returns
// CapabilitySummary objects (name/arn/status/type/version/timestamps), not
// bare names -- callers needing the summary shape should project the
// returned Capability values themselves (see capabilitySummaryToJSON in
// handler_capabilities.go).
func (b *InMemoryBackend) ListCapabilities(clusterName string) []*Capability {
	b.mu.RLock("ListCapabilities")
	defer b.mu.RUnlock()

	items := b.capabilitiesByCluster.Get(clusterName)
	list := make([]*Capability, len(items))

	for i, c := range items {
		cp := *c
		list[i] = &cp
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CapabilityName < list[j].CapabilityName })

	return list
}

// UpdateCapability updates an existing capability's role ARN, delete
// propagation policy, and/or Configuration (merged per
// applyUpdateCapabilityConfiguration -- configUpdate may be nil).
func (b *InMemoryBackend) UpdateCapability(
	clusterName, capabilityName, roleARN, deletePropagationPolicy string,
	configUpdate *updateCapabilityConfigurationBody,
) (*Capability, error) {
	b.mu.Lock("UpdateCapability")
	defer b.mu.Unlock()

	key := capabilityKey(clusterName, capabilityName)

	capa, ok := b.capabilities.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: capability %s not found in cluster %s", ErrNotFound, capabilityName, clusterName)
	}

	if configUpdate != nil && configUpdate.ArgoCd != nil && capa.Type != capabilityTypeArgoCd {
		return nil, fmt.Errorf(
			"%w: configuration.argoCd is only valid for type %s",
			ErrValidation,
			capabilityTypeArgoCd,
		)
	}

	if configUpdate != nil {
		capa.Configuration = applyUpdateCapabilityConfiguration(capa.Configuration, configUpdate)
	}

	if roleARN != "" {
		capa.RoleARN = roleARN
	}

	if deletePropagationPolicy != "" {
		capa.DeletePropagationPolicy = deletePropagationPolicy
	}

	capa.ModifiedAt = time.Now().UTC()

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

	if capa.Health == nil {
		capa.Health = &CapabilityHealth{Issues: []CapabilityIssue{}}
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
