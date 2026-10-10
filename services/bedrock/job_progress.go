package bedrock

import "time"

const (
	invocationStatusSubmitted  = "Submitted"
	invocationStatusValidating = "Validating"
	invocationStatusScheduled  = "Scheduled"
	statusStopping             = "Stopping"
)

const invocationPhaseCount = 4

// AdvanceEvaluationAndInvocationJobStatuses moves evaluation jobs InProgress →
// Completed and invocation jobs Submitted → Validating → Scheduled → InProgress
// → Completed, each phase taking a quarter of minAge since creation.
func (b *InMemoryBackend) AdvanceEvaluationAndInvocationJobStatuses(minAge time.Duration) int {
	b.mu.Lock("AdvanceEvaluationAndInvocationJobStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, j := range b.evaluationJobs.All() {
		if j.Status == statusInProgress && now.Sub(j.CreationTime) >= minAge {
			j.Status = statusCompleted
			j.LastModifiedTime = now
			advanced++
		}
	}

	phases := []string{
		invocationStatusSubmitted, invocationStatusValidating, invocationStatusScheduled, statusInProgress,
	}

	for _, j := range b.modelInvocationJobs.All() {
		cur := -1

		for i, p := range phases {
			if j.Status == p {
				cur = i
			}
		}

		if cur < 0 {
			continue
		}

		reached := int(now.Sub(j.CreationTime) * invocationPhaseCount / max(minAge, 1))
		if reached <= cur {
			continue
		}

		j.LastModifiedTime = now
		advanced++

		if reached >= invocationPhaseCount {
			j.Status = statusCompleted
			j.EndTime = &now

			continue
		}

		j.Status = phases[reached]
	}

	return advanced
}

// SettleStoppingJobs moves every job left in Stopping to Stopped.
func (b *InMemoryBackend) SettleStoppingJobs() int {
	b.mu.Lock("SettleStoppingJobs")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	settled := 0

	for _, j := range b.modelCustomizationJobs.All() {
		if j.Status == statusStopping {
			j.Status = statusStopped
			j.LastModifiedTime = now
			j.EndTime = now
			settled++
		}
	}

	for _, j := range b.evaluationJobs.All() {
		if j.Status == statusStopping {
			j.Status = statusStopped
			j.LastModifiedTime = now
			settled++
		}
	}

	for _, j := range b.modelInvocationJobs.All() {
		if j.Status == statusStopping {
			j.Status = statusStopped
			j.LastModifiedTime = now
			j.EndTime = &now
			settled++
		}
	}

	for _, j := range b.advancedPromptOptimizationJobs.All() {
		if j.JobStatus == statusStopping {
			j.JobStatus = statusStopped
			j.LastModifiedTime = now
			settled++
		}
	}

	return settled
}
