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
	b.mu.RLock("ChannelCount")
	defer b.mu.RUnlock()

	return b.channels.Len()
}

// DatastoreCount returns the number of datastores in the backend (for white-box testing).
func DatastoreCount(b *InMemoryBackend) int {
	b.mu.RLock("DatastoreCount")
	defer b.mu.RUnlock()

	return b.datastores.Len()
}

// DatasetCount returns the number of datasets in the backend (for white-box testing).
func DatasetCount(b *InMemoryBackend) int {
	b.mu.RLock("DatasetCount")
	defer b.mu.RUnlock()

	return b.datasets.Len()
}

// PipelineCount returns the number of pipelines in the backend (for white-box testing).
func PipelineCount(b *InMemoryBackend) int {
	b.mu.RLock("PipelineCount")
	defer b.mu.RUnlock()

	return b.pipelines.Len()
}

// HandlerOpsLen returns the number of pre-built dispatch operations.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
