package rekognition

// CollectionCount returns the number of stored collections.
func CollectionCount(b *InMemoryBackend) int {
	b.mu.RLock("CollectionCount")
	defer b.mu.RUnlock()

	return b.collections.Len()
}

// FaceCount returns the number of indexed faces in a collection.
func FaceCount(b *InMemoryBackend, collectionID string) int {
	b.mu.RLock("FaceCount")
	defer b.mu.RUnlock()

	return len(b.facesByCollection.Get(collectionID))
}

// StreamProcessorCount returns the number of stored stream processors.
func StreamProcessorCount(b *InMemoryBackend) int {
	b.mu.RLock("StreamProcessorCount")
	defer b.mu.RUnlock()

	return b.streamProcessors.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ResolveDetectLabelsMinConfidence exposes resolveMinConfidence for testing
// the DetectLabels default-MinConfidence value.
func ResolveDetectLabelsMinConfidence(v float64) float64 {
	return resolveMinConfidence(v)
}
