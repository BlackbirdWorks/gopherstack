package appstream

// StackCount returns the number of stored stacks.
func StackCount(b *InMemoryBackend) int {
	b.mu.RLock("StackCount")
	defer b.mu.RUnlock()

	return b.stacks.Len()
}

// FleetCount returns the number of stored fleets.
func FleetCount(b *InMemoryBackend) int {
	b.mu.RLock("FleetCount")
	defer b.mu.RUnlock()

	return b.fleets.Len()
}

// AppBlockCount returns the number of stored app blocks.
func AppBlockCount(b *InMemoryBackend) int {
	b.mu.RLock("AppBlockCount")
	defer b.mu.RUnlock()

	return b.appBlocks.Len()
}

// AppBlockBuilderCount returns the number of stored app block builders.
func AppBlockBuilderCount(b *InMemoryBackend) int {
	b.mu.RLock("AppBlockBuilderCount")
	defer b.mu.RUnlock()

	return b.appBlockBuilders.Len()
}

// ApplicationCount returns the number of stored applications.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return b.applications.Len()
}

// UserCount returns the number of stored users.
func UserCount(b *InMemoryBackend) int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	return b.users.Len()
}

// ImageCount returns the number of stored images.
func ImageCount(b *InMemoryBackend) int {
	b.mu.RLock("ImageCount")
	defer b.mu.RUnlock()

	return b.images.Len()
}

// ImageBuilderCount returns the number of stored image builders.
func ImageBuilderCount(b *InMemoryBackend) int {
	b.mu.RLock("ImageBuilderCount")
	defer b.mu.RUnlock()

	return b.imageBuilders.Len()
}
