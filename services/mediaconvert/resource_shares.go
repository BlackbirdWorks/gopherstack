package mediaconvert

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateResourceShare records a resource-share request for the given job ID.
// Sets ShareStatus = "SHARED" on the job and populates LastShareDetails.
func (b *InMemoryBackend) CreateResourceShare(jobID string) (string, error) {
	b.mu.Lock("CreateResourceShare")
	defer b.mu.Unlock()

	if jobID == "" {
		return "", fmt.Errorf("%w: jobId is required", ErrValidation)
	}

	j, ok := b.jobs.Get(jobID)
	if !ok {
		return "", fmt.Errorf("%w: job %s not found", ErrNotFound, jobID)
	}

	token := uuid.NewString()
	j.ShareStatus = "SHARED"
	j.LastShareDetails = &ShareDetails{
		ShareToken: token,
		SharedAt:   epochSeconds(time.Now()),
	}

	return jobID, nil
}
