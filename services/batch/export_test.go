package batch

import (
	"fmt"
	"time"
)

// JobDefinitionCount returns the number of job definitions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) JobDefinitionCount() int {
	b.mu.RLock("JobDefinitionCount")
	defer b.mu.RUnlock()

	return len(b.jobDefinitions)
}

// RevisionFor returns the current revision counter for the given job definition name.
// Returns 0 if the name has no counter (deleted or never registered).
// Used only in tests.
func (b *InMemoryBackend) RevisionFor(name string) int32 {
	b.mu.RLock("RevisionFor")
	defer b.mu.RUnlock()

	return b.jobDefRevisions[name]
}

// HasRevisionCounter reports whether a revision counter exists for name.
// Used only in tests to verify cleanup.
func (b *InMemoryBackend) HasRevisionCounter(name string) bool {
	b.mu.RLock("HasRevisionCounter")
	defer b.mu.RUnlock()

	_, ok := b.jobDefRevisions[name]

	return ok
}

// SetJobDefinitionDeregisteredAt overrides the DeregisteredAt timestamp for a job definition.
// Used only in tests to simulate TTL expiry.
func (b *InMemoryBackend) SetJobDefinitionDeregisteredAt(arnOrNameRev string, timestamp time.Time) {
	b.mu.Lock("SetJobDefinitionDeregisteredAt")
	defer b.mu.Unlock()

	if jd, ok := b.jobDefinitions[arnOrNameRev]; ok {
		jd.DeregisteredAt = &timestamp

		return
	}

	for _, jd := range b.jobDefinitions {
		nameRev := fmt.Sprintf("%s:%d", jd.JobDefinitionName, jd.Revision)
		if nameRev == arnOrNameRev {
			jd.DeregisteredAt = &timestamp

			return
		}
	}
}
