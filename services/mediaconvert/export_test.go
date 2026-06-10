package mediaconvert

// DeepCloneMapForTest exposes deepCloneMap for testing that deeply-nested settings
// documents are cloned without silent truncation.
func DeepCloneMapForTest(m map[string]any) map[string]any {
	return deepCloneMap(m)
}

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

// JobStatusOf returns the current status of the job with the given ID.
// Returns empty string if not found.
func JobStatusOf(b *InMemoryBackend, id string) string {
	b.mu.RLock("JobStatusOf")
	defer b.mu.RUnlock()

	if j, ok := b.jobs[id]; ok {
		return j.Status
	}

	return ""
}

// JobPhaseOf returns the current phase of the job with the given ID.
// Returns empty string if not found.
func JobPhaseOf(b *InMemoryBackend, id string) string {
	b.mu.RLock("JobPhaseOf")
	defer b.mu.RUnlock()

	if j, ok := b.jobs[id]; ok {
		return j.CurrentPhase
	}

	return ""
}

// QueueCounterSubmitted returns the submitted counter for the given queue ARN.
func QueueCounterSubmitted(b *InMemoryBackend, queueArn string) int {
	b.mu.RLock("QueueCounterSubmitted")
	defer b.mu.RUnlock()

	if c, ok := b.queueCounters[queueArn]; ok {
		return c.submitted
	}

	return 0
}

// QueueCounterProgressing returns the progressing counter for the given queue ARN.
func QueueCounterProgressing(b *InMemoryBackend, queueArn string) int {
	b.mu.RLock("QueueCounterProgressing")
	defer b.mu.RUnlock()

	if c, ok := b.queueCounters[queueArn]; ok {
		return c.progressing
	}

	return 0
}

// TokenIndexSize returns the current number of entries in the token dedup index.
func TokenIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("TokenIndexSize")
	defer b.mu.RUnlock()

	return len(b.tokenIndex)
}
