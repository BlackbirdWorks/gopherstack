package codebuild

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultCodeBuildJanitorInterval

// DefaultBuildTTL exposes the package default build TTL for testing.
const DefaultBuildTTL = defaultCodeBuildBuildTTL

// BuildCount returns the number of builds stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) BuildCount() int {
	b.mu.RLock("BuildCount")
	defer b.mu.RUnlock()

	return b.builds.Len()
}

// BuildARNIndexSize returns the number of entries in the build ARN index.
// Used only in tests.
func (b *InMemoryBackend) BuildARNIndexSize() int {
	b.mu.RLock("BuildARNIndexSize")
	defer b.mu.RUnlock()

	return b.buildsByARN.Len()
}

// BuildsByProjectSize returns the number of build IDs tracked for projectName.
// Used only in tests.
func (b *InMemoryBackend) BuildsByProjectSize(projectName string) int {
	b.mu.RLock("BuildsByProjectSize")
	defer b.mu.RUnlock()

	return len(b.buildsByProject.Get(projectName))
}

// SetBuildEndTime overrides the EndTime and BuildStatus of a build.
// If endTime is zero, EndTime is set to 0 (meaning "not yet completed").
// Used only in tests to simulate a completed build at a specific time.
func (b *InMemoryBackend) SetBuildEndTime(id string, status string, endTime time.Time) {
	b.mu.Lock("SetBuildEndTime")
	defer b.mu.Unlock()

	if build, ok := b.builds.Get(id); ok {
		build.BuildStatus = status
		build.CurrentPhase = "COMPLETED"

		if endTime.IsZero() {
			build.EndTime = 0
		} else {
			build.EndTime = float64(endTime.Unix())
		}
	}
}

// ProjectCount returns the number of projects stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) ProjectCount() int {
	b.mu.RLock("ProjectCount")
	defer b.mu.RUnlock()

	return b.projects.Len()
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// GetJanitorBuildTTL returns the BuildTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorBuildTTL() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.BuildTTL
}
