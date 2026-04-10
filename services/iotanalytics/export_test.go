package iotanalytics

// ExportedCreateChannelRequest is an alias for the unexported createChannelRequest type,
// made available for use in external (_test) test packages.
type ExportedCreateChannelRequest = createChannelRequest

// ExportedCreateDatastoreRequest is an alias for the unexported createDatastoreRequest type,
// made available for use in external (_test) test packages.
type ExportedCreateDatastoreRequest = createDatastoreRequest

// ExportedCreateDatasetRequest is an alias for the unexported createDatasetRequest type,
// made available for use in external (_test) test packages.
type ExportedCreateDatasetRequest = createDatasetRequest

// ExportedCreatePipelineRequest is an alias for the unexported createPipelineRequest type,
// made available for use in external (_test) test packages.
type ExportedCreatePipelineRequest = createPipelineRequest

// ExportedTagDTO is an alias for the TagDTO type,
// made available for use in external (_test) test packages.
type ExportedTagDTO = TagDTO

// ChannelCount returns the number of channels in the backend (for white-box testing).
func ChannelCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.channels)
}

// DatastoreCount returns the number of datastores in the backend (for white-box testing).
func DatastoreCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.datastores)
}

// DatasetCount returns the number of datasets in the backend (for white-box testing).
func DatasetCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.datasets)
}

// PipelineCount returns the number of pipelines in the backend (for white-box testing).
func PipelineCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.pipelines)
}

// HandlerOpsLen returns the number of pre-built dispatch operations.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
