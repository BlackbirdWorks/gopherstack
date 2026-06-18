package dms

// sumRegions returns the total number of entries across all per-region inner maps.
func sumRegions[T any](m map[string]map[string]*T) int {
	total := 0
	for _, regionMap := range m {
		total += len(regionMap)
	}

	return total
}

// ReplicationInstanceCount returns the number of replication instances. Used only in tests.
func (b *InMemoryBackend) ReplicationInstanceCount() int {
	b.mu.RLock("ReplicationInstanceCount")
	defer b.mu.RUnlock()

	return sumRegions(b.replicationInstances)
}

// EndpointCount returns the number of endpoints. Used only in tests.
func (b *InMemoryBackend) EndpointCount() int {
	b.mu.RLock("EndpointCount")
	defer b.mu.RUnlock()

	return sumRegions(b.endpoints)
}

// ReplicationTaskCount returns the number of replication tasks. Used only in tests.
func (b *InMemoryBackend) ReplicationTaskCount() int {
	b.mu.RLock("ReplicationTaskCount")
	defer b.mu.RUnlock()

	return sumRegions(b.replicationTasks)
}

// DataMigrationCount returns the number of data migrations. Used only in tests.
func (b *InMemoryBackend) DataMigrationCount() int {
	b.mu.RLock("DataMigrationCount")
	defer b.mu.RUnlock()

	return sumRegions(b.dataMigrations)
}

// DataProviderCount returns the number of data providers. Used only in tests.
func (b *InMemoryBackend) DataProviderCount() int {
	b.mu.RLock("DataProviderCount")
	defer b.mu.RUnlock()

	return sumRegions(b.dataProviders)
}

// EventSubscriptionCount returns the number of event subscriptions. Used only in tests.
func (b *InMemoryBackend) EventSubscriptionCount() int {
	b.mu.RLock("EventSubscriptionCount")
	defer b.mu.RUnlock()

	return sumRegions(b.eventSubscriptions)
}

// FleetAdvisorCollectorCount returns the number of Fleet Advisor collectors. Used only in tests.
func (b *InMemoryBackend) FleetAdvisorCollectorCount() int {
	b.mu.RLock("FleetAdvisorCollectorCount")
	defer b.mu.RUnlock()

	return sumRegions(b.fleetAdvisorCollectors)
}

// InstanceProfileCount returns the number of instance profiles. Used only in tests.
func (b *InMemoryBackend) InstanceProfileCount() int {
	b.mu.RLock("InstanceProfileCount")
	defer b.mu.RUnlock()

	return sumRegions(b.instanceProfiles)
}

// ConnectionCount returns the number of stored connections. Used only in tests.
func (b *InMemoryBackend) ConnectionCount() int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return sumRegions(b.connections)
}
