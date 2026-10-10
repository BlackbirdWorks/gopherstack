package polly

import "time"

const taskStagesToTerminal = 2

// SetLifecycleDelay sets the dwell per stage for synthesis tasks (scheduled, then
// inProgress, then terminal). Zero (the default) advances one stage per poll.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func taskStage(status string) int {
	switch status {
	case taskStatusScheduled:
		return 0
	case taskStatusProgress:
		return 1
	default:
		return taskStagesToTerminal
	}
}

// settleTaskLocked advances task; the caller holds the write lock.
func (b *InMemoryBackend) settleTaskLocked(task *SpeechSynthesisTask) {
	if b.lifecycleDelay <= 0 {
		advanceTask(task)

		return
	}

	desired := min(int(time.Since(task.CreationTime)/b.lifecycleDelay), taskStagesToTerminal)
	for taskStage(task.TaskStatus) < desired {
		advanceTask(task)
	}
}
