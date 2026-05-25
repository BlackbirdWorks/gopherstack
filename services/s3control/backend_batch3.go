package s3control

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// errAccessPointNotFound is returned when an access point is not found.
var errAccessPointNotFound = awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)

// errAPPABNotFound is returned when no per-AP public access block configuration exists.
var errAPPABNotFound = awserr.New("NoSuchPublicAccessBlockConfiguration", awserr.ErrNotFound)

// ---- Per-AccessPoint PublicAccessBlock ----

// GetAccessPointPublicAccessBlock returns the public access block configuration for an access point.
func (b *InMemoryBackend) GetAccessPointPublicAccessBlock(accountID, name string) (*PublicAccessBlock, error) {
	b.mu.RLock("GetAccessPointPublicAccessBlock")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return nil, fmt.Errorf("%w: %s", errAccessPointNotFound, name)
	}

	pab, ok := b.accessPointPABs[key]
	if !ok {
		return nil, errAPPABNotFound
	}

	cp := *pab

	return &cp, nil
}

// PutAccessPointPublicAccessBlock sets the public access block configuration for an access point.
func (b *InMemoryBackend) PutAccessPointPublicAccessBlock(accountID, name string, cfg PublicAccessBlock) error {
	b.mu.Lock("PutAccessPointPublicAccessBlock")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return fmt.Errorf("%w: %s", errAccessPointNotFound, name)
	}

	cp := cfg
	b.accessPointPABs[key] = &cp

	return nil
}

// DeleteAccessPointPublicAccessBlock removes the public access block configuration for an access point.
func (b *InMemoryBackend) DeleteAccessPointPublicAccessBlock(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointPublicAccessBlock")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return fmt.Errorf("%w: %s", errAccessPointNotFound, name)
	}

	delete(b.accessPointPABs, key)

	return nil
}

// ---- Job status validation ----

// validJobStatusTransitions maps a current status to the set of allowed RequestedJobStatus values.
// AWS only allows transitioning to "Cancelled" or "Ready" via UpdateJobStatus.
var validJobRequestedStatuses = map[string]bool{
	"Cancelled": true,
	"Ready":     true,
}

// UpdateJobStatusValidated changes the status of a batch job after validating the requested transition.
func (b *InMemoryBackend) UpdateJobStatusValidated(accountID, jobID, requestedStatus, statusUpdateReason string) (*BatchJob, error) {
	if !validJobRequestedStatuses[requestedStatus] {
		return nil, fmt.Errorf(
			"requestedJobStatus must be Cancelled or Ready, got %q: %w",
			requestedStatus,
			ErrValidation,
		)
	}

	b.mu.Lock("UpdateJobStatusValidated")
	defer b.mu.Unlock()

	job, ok := b.batchJobs[accountID+":"+jobID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errJobNotFound, jobID)
	}

	job.Status = requestedStatus
	if statusUpdateReason != "" {
		job.StatusUpdateReason = statusUpdateReason
	}

	cp := *job

	return &cp, nil
}
