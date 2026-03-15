package codebuild

import "time"

// BuildCount returns the number of builds stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) BuildCount() int {
	b.mu.RLock("BuildCount")
	defer b.mu.RUnlock()

	return len(b.builds)
}

// SetBuildEndTime overrides the EndTime and BuildStatus of a build.
// Used only in tests to simulate a completed build at a specific time.
func (b *InMemoryBackend) SetBuildEndTime(id string, status string, endTime time.Time) {
	b.mu.Lock("SetBuildEndTime")
	defer b.mu.Unlock()

	if build, ok := b.builds[id]; ok {
		build.BuildStatus = status
		build.EndTime = float64(endTime.Unix())
		build.CurrentPhase = "COMPLETED"
	}
}
