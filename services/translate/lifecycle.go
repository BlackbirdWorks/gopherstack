package translate

import "time"

const jobStagesToTerminal = 2

// SetLifecycleDelay sets the dwell per stage for batch translation jobs
// (SUBMITTED, then IN_PROGRESS, then terminal) and parallel data (CREATING or UPDATING).
// Zero (the default) keeps the advance-per-poll behavior.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func jobStage(status string) int {
	switch status {
	case jobStatusSubmitted:
		return 0
	case jobStatusInProgress:
		return 1
	default:
		return jobStagesToTerminal
	}
}

// settleJobLocked advances job by elapsed time; the caller holds the write lock.
func (b *InMemoryBackend) settleJobLocked(job *TranslationJob) {
	if job.JobStatus == jobStatusStopRequested {
		if time.Since(job.EndAt) >= b.lifecycleDelay {
			b.advanceJob(job)
		}

		return
	}

	desired := min(int(time.Since(job.SubmittedAt)/b.lifecycleDelay), jobStagesToTerminal)
	for jobStage(job.JobStatus) < desired {
		b.advanceJob(job)
	}
}

func (b *InMemoryBackend) settleParallelDataLocked(pd *ParallelData) {
	if time.Since(pd.LastUpdatedAt) >= b.lifecycleDelay {
		b.advanceParallelData(pd)
	}
}
