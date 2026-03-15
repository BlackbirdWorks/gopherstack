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
