package appstream

// StackCount returns the number of stored stacks.
func StackCount(b *InMemoryBackend) int {
	b.mu.RLock("StackCount")
	defer b.mu.RUnlock()

	return len(b.stacks)
}

// FleetCount returns the number of stored fleets.
func FleetCount(b *InMemoryBackend) int {
	b.mu.RLock("FleetCount")
	defer b.mu.RUnlock()

	return len(b.fleets)
}
