package mediaconvert

// QueueCount returns the number of queues in the backend.
func QueueCount(b *InMemoryBackend) int {
	b.mu.RLock("QueueCount")
	defer b.mu.RUnlock()

	return len(b.queues)
}

// JobTemplateCount returns the number of job templates in the backend.
func JobTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("JobTemplateCount")
	defer b.mu.RUnlock()

	return len(b.jobTemplates)
}

// JobCount returns the number of jobs in the backend.
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return len(b.jobs)
}

// PresetCount returns the number of presets in the backend.
func PresetCount(b *InMemoryBackend) int {
	b.mu.RLock("PresetCount")
	defer b.mu.RUnlock()

	return len(b.presets)
}

// CertificateCount returns the number of associated certificates in the backend.
func CertificateCount(b *InMemoryBackend) int {
	b.mu.RLock("CertificateCount")
	defer b.mu.RUnlock()

	return len(b.certificates)
}

// HandlerOpsLen returns the number of supported operations for the given handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
