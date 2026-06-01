package rekognition

// CollectionCount returns the number of stored collections.
func CollectionCount(b *InMemoryBackend) int {
	b.mu.RLock("CollectionCount")
	defer b.mu.RUnlock()

	return len(b.collections)
}

// FaceCount returns the number of indexed faces in a collection.
func FaceCount(b *InMemoryBackend, collectionID string) int {
	b.mu.RLock("FaceCount")
	defer b.mu.RUnlock()

	return len(b.faces[collectionID])
}

// StreamProcessorCount returns the number of stored stream processors.
func StreamProcessorCount(b *InMemoryBackend) int {
	b.mu.RLock("StreamProcessorCount")
	defer b.mu.RUnlock()

	return len(b.streamProcessors)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
