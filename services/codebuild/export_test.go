package codebuild

import "time"

// BuildCount returns the number of builds stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) BuildCount() int {
	b.mu.RLock("BuildCount")
	defer b.mu.RUnlock()

	return len(b.builds)
}

// BuildARNIndexSize returns the number of entries in the build ARN index.
// Used only in tests.
func (b *InMemoryBackend) BuildARNIndexSize() int {
	b.mu.RLock("BuildARNIndexSize")
	defer b.mu.RUnlock()

	return len(b.buildARNIndex)
}

// SetBuildEndTime overrides the EndTime and BuildStatus of a build.
// If endTime is zero, EndTime is set to 0 (meaning "not yet completed").
// Used only in tests to simulate a completed build at a specific time.
func (b *InMemoryBackend) SetBuildEndTime(id string, status string, endTime time.Time) {
	b.mu.Lock("SetBuildEndTime")
	defer b.mu.Unlock()

	if build, ok := b.builds[id]; ok {
		build.BuildStatus = status
		build.CurrentPhase = "COMPLETED"

		if endTime.IsZero() {
			build.EndTime = 0
		} else {
			build.EndTime = float64(endTime.Unix())
		}
	}
}
