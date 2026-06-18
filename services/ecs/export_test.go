package ecs

// ClusterFromTaskARNForTest exposes clusterFromTaskARN for unit tests.
func ClusterFromTaskARNForTest(taskARN string) string {
	return clusterFromTaskARN(taskARN)
}

// CapacityProviderCount returns the number of capacity providers (test helper).
func (b *InMemoryBackend) CapacityProviderCount() int {
	b.mu.RLock("CapacityProviderCount")
	defer b.mu.RUnlock()

	return len(b.capacityProviders)
}

// ServiceDeploymentCount returns the number of service deployments (test helper).
func (b *InMemoryBackend) ServiceDeploymentCount() int {
	b.mu.RLock("ServiceDeploymentCount")
	defer b.mu.RUnlock()

	return len(b.serviceDeployments)
}

// AccountSettingCount returns the number of account settings (test helper).
func (b *InMemoryBackend) AccountSettingCount() int {
	b.mu.RLock("AccountSettingCount")
	defer b.mu.RUnlock()

	return len(b.accountSettings)
}

// AttributeCount returns the number of attributes for a cluster (test helper).
func (b *InMemoryBackend) AttributeCount(cluster string) int {
	b.mu.RLock("AttributeCount")
	defer b.mu.RUnlock()

	return len(b.attributes[cluster])
}

// ExpressGatewayServiceCount returns the number of express gateway services (test helper).
func (b *InMemoryBackend) ExpressGatewayServiceCount() int {
	b.mu.RLock("ExpressGatewayServiceCount")
	defer b.mu.RUnlock()

	return len(b.expressGatewayServices)
}

// GetServicesForReconcilerForTest exposes getServicesForReconciler for tests.
// Returns a slice of all service snapshots (same as the reconciler sees each tick).
func (b *InMemoryBackend) GetServicesForReconcilerForTest() []Service {
	snaps := b.getServicesForReconciler()
	out := make([]Service, len(snaps))
	for i, s := range snaps {
		out[i] = s.service
	}

	return out
}
