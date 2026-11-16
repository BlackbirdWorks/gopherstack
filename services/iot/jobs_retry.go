package iot

import "fmt"

const (
	retryFailureFailed   = "FAILED"
	retryFailureTimedOut = "TIMED_OUT"
	retryFailureAll      = "ALL"
)

// retryAllowed reports whether a finished execution has retries left for its failure type; a
// FAILED or TIMED_OUT criterion counts only that failure type, an ALL criterion counts every retry.
func retryAllowed(cfg *JobExecutionsRetryConfig, e *JobExecution) bool {
	if cfg == nil || (e.Status != JobExecFailed && e.Status != JobExecTimedOut) {
		return false
	}

	failure, used := retryFailureFailed, e.RetriedFailed
	if e.Status == JobExecTimedOut {
		failure, used = retryFailureTimedOut, e.RetriedTimedOut
	}

	for _, c := range cfg.CriteriaList {
		switch c.FailureType {
		case failure:
			if used < c.NumberOfRetries {
				return true
			}
		case retryFailureAll:
			if e.RetryAttempt < c.NumberOfRetries {
				return true
			}
		}
	}

	return false
}

// retryJobExecutionLocked replaces a FAILED or TIMED_OUT execution of a running job with a new QUEUED
// one when the job's retry configuration allows it
// (docs.aws.amazon.com/iot/latest/developerguide/jobs-configurations-details.html#job-retry-configuration).
func (b *InMemoryBackend) retryJobExecutionLocked(job *Job, e *JobExecution, now float64) {
	if job == nil || job.Status != JobStatusInProgress || !retryAllowed(job.JobExecutionsRetryConfig, e) {
		return
	}

	next := &JobExecution{
		JobID:           e.JobID,
		ThingName:       e.ThingName,
		Status:          JobExecQueued,
		ExecutionNumber: e.ExecutionNumber + 1,
		VersionNumber:   1,
		QueuedAt:        now,
		LastUpdatedAt:   now,
		RetryAttempt:    e.RetryAttempt + 1,
		RetriedFailed:   e.RetriedFailed,
		RetriedTimedOut: e.RetriedTimedOut,
	}

	if e.Status == JobExecTimedOut {
		next.RetriedTimedOut++
	} else {
		next.RetriedFailed++
	}

	b.jobExecutions.Put(next)
	b.notifyJob(e.ThingName)
}

const (
	maxRetryCriteria   = 2
	maxNumberOfRetries = 10
)

// validateRetryConfig enforces the documented RetryCriteria/JobExecutionsRetryConfig constraints
// (docs.aws.amazon.com/iot/latest/apireference/API_RetryCriteria.html: failureType FAILED|TIMED_OUT|ALL,
// numberOfRetries 0-10; API_JobExecutionsRetryConfig.html: 1-2 criteria).
func validateRetryConfig(cfg *JobExecutionsRetryConfig) error {
	if cfg == nil {
		return nil
	}

	if n := len(cfg.CriteriaList); n < 1 || n > maxRetryCriteria {
		return fmt.Errorf("%w: retry criteriaList must contain 1 to %d items", ErrValidation, maxRetryCriteria)
	}

	for _, c := range cfg.CriteriaList {
		switch c.FailureType {
		case retryFailureFailed, retryFailureTimedOut, retryFailureAll:
		default:
			return fmt.Errorf("%w: invalid retry failureType %q", ErrValidation, c.FailureType)
		}

		if c.NumberOfRetries < 0 || c.NumberOfRetries > maxNumberOfRetries {
			return fmt.Errorf("%w: numberOfRetries must be between 0 and %d", ErrValidation, maxNumberOfRetries)
		}
	}

	return nil
}
