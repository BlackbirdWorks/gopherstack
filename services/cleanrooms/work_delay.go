package cleanrooms

import "time"

const statusStarted = "STARTED"

type workStage int

const (
	workQueued workStage = iota
	workRunning
	workDone
)

// SetProtectedWorkDelay keeps new protected queries and jobs in SUBMITTED, then
// STARTED, for d before they reach SUCCESS (default 0: SUCCESS on the next read).
func (b *InMemoryBackend) SetProtectedWorkDelay(d time.Duration) {
	b.mu.Lock("SetProtectedWorkDelay")
	defer b.mu.Unlock()

	b.workDelay = d
}

func (b *InMemoryBackend) workStageLocked(created float64) workStage {
	if b.workDelay <= 0 {
		return workDone
	}

	age := time.Duration((b.now() - created) * float64(time.Second))

	switch {
	case age >= b.workDelay:
		return workDone
	case age >= b.workDelay/2:
		return workRunning
	default:
		return workQueued
	}
}
