package servicediscovery

import (
	"fmt"
	"sort"
)

// DiscoverInstances returns discovered instances with full per-instance metadata.
// Also returns the per-service revision counter.
func (b *InMemoryBackend) DiscoverInstances(
	namespaceName, serviceName, healthStatus string,
	queryParams map[string]string,
) ([]DiscoveredInstance, int64, error) {
	b.mu.RLock("DiscoverInstances")
	defer b.mu.RUnlock()

	nsMatches := b.namespacesByName.Get(namespaceName)
	if len(nsMatches) == 0 {
		return []DiscoveredInstance{}, 0, nil
	}

	nsID := nsMatches[0].ID

	svcMatches := b.servicesByNsName.Get(nsID + ":" + serviceName)
	if len(svcMatches) == 0 {
		return []DiscoveredInstance{}, 0, nil
	}

	// Mirror the pre-conversion svcByNsAndName map's last-write-wins
	// semantics: CreateService never enforced (namespaceID, name)
	// uniqueness, so the most recently created match is the one an
	// overwriting map assignment would have kept.
	svcID := svcMatches[len(svcMatches)-1].ID

	revision := b.instanceRevision
	insts := b.instancesByService.Get(svcID)

	// Query-parameter filtering narrows the candidate set first; health
	// filtering (including the HEALTHY_OR_ELSE_ALL fail-open case, which
	// needs to see the whole matched set to decide whether to fall back to
	// "all") is applied on top of it.
	candidates := make([]*Instance, 0, len(insts))

	for _, inst := range insts {
		if instanceMatchesQueryParams(inst, queryParams) {
			candidates = append(candidates, inst)
		}
	}

	result := b.filterInstancesByHealth(svcID, namespaceName, serviceName, candidates, healthStatus)

	sort.Slice(result, func(i, j int) bool {
		return result[i].InstanceID < result[j].InstanceID
	})

	return result, revision, nil
}

// discoveredInstance builds the DiscoverInstances response entry for inst,
// resolving its stored health status (defaulting to HEALTHY when unset).
func (b *InMemoryBackend) discoveredInstance(
	svcID, namespaceName, serviceName string,
	inst *Instance,
) DiscoveredInstance {
	hs := b.instanceHealthStatuses[instanceKey(svcID, inst.ID)]
	if hs == "" {
		hs = instanceHealthStatusHealthy
	}

	return DiscoveredInstance{
		InstanceID:    inst.ID,
		NamespaceName: namespaceName,
		ServiceName:   serviceName,
		HealthStatus:  hs,
		Attributes:    copyAttrs(inst.Attributes),
	}
}

// filterInstancesByHealth applies the DiscoverInstances HealthStatus filter to
// candidates. An empty value or "ALL" returns every candidate. HEALTHY_OR_ELSE_ALL
// returns only healthy instances unless none are healthy, in which case it "fails
// open" and returns every candidate -- matching real Cloud Map semantics. Any
// other value (HEALTHY, UNHEALTHY) is matched exactly against the stored status.
func (b *InMemoryBackend) filterInstancesByHealth(
	svcID, namespaceName, serviceName string,
	candidates []*Instance,
	healthStatus string,
) []DiscoveredInstance {
	all := func() []DiscoveredInstance {
		result := make([]DiscoveredInstance, 0, len(candidates))
		for _, inst := range candidates {
			result = append(result, b.discoveredInstance(svcID, namespaceName, serviceName, inst))
		}

		return result
	}

	if healthStatus == "" || healthStatus == healthStatusFilterAll {
		return all()
	}

	if healthStatus == healthStatusFilterHealthyOrElseAll {
		healthy := make([]DiscoveredInstance, 0, len(candidates))

		for _, inst := range candidates {
			d := b.discoveredInstance(svcID, namespaceName, serviceName, inst)
			if d.HealthStatus == instanceHealthStatusHealthy {
				healthy = append(healthy, d)
			}
		}

		if len(healthy) > 0 {
			return healthy
		}

		return all()
	}

	result := make([]DiscoveredInstance, 0, len(candidates))

	for _, inst := range candidates {
		d := b.discoveredInstance(svcID, namespaceName, serviceName, inst)
		if d.HealthStatus == healthStatus {
			result = append(result, d)
		}
	}

	return result
}

// instanceMatchesQueryParams returns true when the instance attributes satisfy
// every key-value pair in queryParams.
func instanceMatchesQueryParams(inst *Instance, queryParams map[string]string) bool {
	for k, v := range queryParams {
		if inst.Attributes[k] != v {
			return false
		}
	}

	return true
}

// DiscoverInstancesRevision returns the current revision for the specified service.
// Revision is per-service, incremented on each RegisterInstance/DeregisterInstance.
func (b *InMemoryBackend) DiscoverInstancesRevision(namespaceName, serviceName string) (int64, error) {
	b.mu.RLock("DiscoverInstancesRevision")
	defer b.mu.RUnlock()

	nsMatches := b.namespacesByName.Get(namespaceName)
	if len(nsMatches) == 0 {
		return 0, fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, namespaceName)
	}

	nsID := nsMatches[0].ID

	if svcMatches := b.servicesByNsName.Get(nsID + ":" + serviceName); len(svcMatches) == 0 {
		return 0, fmt.Errorf("%w: service %s not found in namespace %s", ErrServiceNotFound, serviceName, namespaceName)
	}

	return b.instanceRevision, nil
}
