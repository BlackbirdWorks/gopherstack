package comprehend

import "time"

const lifecycleStagesToTerminal = 2

// SetLifecycleDelay sets the dwell per stage for async jobs (SUBMITTED, then IN_PROGRESS,
// then terminal) and classifier/recognizer training (SUBMITTED, TRAINING, TRAINED).
// Zero (the default) advances one stage per Describe and trains instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func jobStage(status string) int {
	switch status {
	case statusSubmitted:
		return 0
	case statusInProgress:
		return 1
	default:
		return lifecycleStagesToTerminal
	}
}

func resourceStage(status string) int {
	switch status {
	case statusSubmitted:
		return 0
	case statusModelTraining:
		return 1
	default:
		return lifecycleStagesToTerminal
	}
}

// settleJobLocked advances job; the caller holds the write lock.
func (b *InMemoryBackend) settleJobLocked(job *Job) {
	if b.lifecycleDelay <= 0 {
		advanceJob(job)

		return
	}

	if job.JobStatus == statusStopRequested {
		if time.Since(job.stopRequestedAt) >= b.lifecycleDelay {
			advanceJob(job)
		}

		return
	}

	desired := min(int(time.Since(job.SubmitTime)/b.lifecycleDelay), lifecycleStagesToTerminal)
	for jobStage(job.JobStatus) < desired {
		advanceJob(job)
	}
}

func (b *InMemoryBackend) settleResourceLocked(resource *Resource) {
	if b.lifecycleDelay <= 0 {
		advanceTrainingResource(resource)

		return
	}

	desired := min(int(time.Since(resource.CreatedAt)/b.lifecycleDelay), lifecycleStagesToTerminal)
	for isTrainingResourceType(resource.Type) && resourceStage(resource.Status) < desired {
		advanceTrainingResource(resource)
	}
}
