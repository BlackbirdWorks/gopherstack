package cloudformation

// TopoSortResources exposes topoSortResources for white-box testing.
func TopoSortResources(resources map[string]TemplateResource) []string {
	return topoSortResources(resources)
}

// ParseDependsOn exposes parseDependsOn for white-box testing.
func ParseDependsOn(v any) []string {
	return parseDependsOn(v)
}

// ForceStackStatus sets the status of a stack by name for test purposes.
func (b *InMemoryBackend) ForceStackStatus(stackName, status string) {
	b.mu.Lock("ForceStackStatus")
	defer b.mu.Unlock()

	if s, ok := b.stacks[stackName]; ok {
		s.StackStatus = status
	}
}

// InjectCreateHook installs a persistent hook on the ResourceCreator that is
// called before any real creation logic. The hook remains active for all
// subsequent Create calls until replaced or cleared (set to nil). If the hook
// returns a non-nil error the Create call fails with that error. Used only for
// testing error and rollback paths.
func (rc *ResourceCreator) InjectCreateHook(fn func(resourceType string) error) {
	rc.createHook = fn
}

// GetCreator returns the backend's ResourceCreator for test-only hook injection.
func (b *InMemoryBackend) GetCreator() *ResourceCreator {
	return b.creator
}

// ResourcesEntryExists reports whether b.resources has an entry for stackID.
func (b *InMemoryBackend) ResourcesEntryExists(stackID string) bool {
	b.mu.RLock("ResourcesEntryExists")
	defer b.mu.RUnlock()

	_, ok := b.resources[stackID]

	return ok
}

// ChangeSetsEntryExists reports whether b.changeSets has an entry for stackName.
func (b *InMemoryBackend) ChangeSetsEntryExists(stackName string) bool {
	b.mu.RLock("ChangeSetsEntryExists")
	defer b.mu.RUnlock()

	_, ok := b.changeSets[stackName]

	return ok
}

// DriftDetectionCount returns the number of drift detection entries for stackID.
func (b *InMemoryBackend) DriftDetectionCount(stackID string) int {
	b.mu.RLock("DriftDetectionCount")
	defer b.mu.RUnlock()

	count := 0

	for _, status := range b.driftDetections {
		if status.StackID == stackID {
			count++
		}
	}

	return count
}

// ResourceCountForStack returns the number of resources tracked for stackID.
func (b *InMemoryBackend) ResourceCountForStack(stackID string) int {
	b.mu.RLock("ResourceCountForStack")
	defer b.mu.RUnlock()

	return len(b.resources[stackID])
}
