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
