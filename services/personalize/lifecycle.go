package personalize

import "time"

const (
	statusCompleted       = "COMPLETED"
	statusJobPending      = "PENDING"
	statusJobProgress     = "IN PROGRESS"
	statusDeletionPending = "PENDING"
	statusDeletionRunning = "IN_PROGRESS"
	phaseCount            = 3
)

// lifecycle carries the creation-time delay so status is derived from age without mutating shared records.
type lifecycle struct{ delay time.Duration }

// SetLifecycleDelay sets how long new resources and jobs stay pending/in progress; zero settles them immediately.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

func (b *InMemoryBackend) lifecycle() lifecycle { return lifecycle{delay: b.lifecycleDelay} }

// phase returns 0 (pending), 1 (in progress) or 2 (settled) for a record created at created.
func (l lifecycle) phase(created time.Time) int {
	if l.delay <= 0 {
		return phaseCount - 1
	}

	elapsed := time.Since(created)

	switch {
	case elapsed >= l.delay:
		return phaseCount - 1
	case elapsed >= l.delay/phaseCount:
		return 1
	default:
		return 0
	}
}

func (l lifecycle) resourceStatus(status string, created time.Time) string {
	if status != statusActive {
		return status
	}

	switch l.phase(created) {
	case 0:
		return statusCreatePending
	case 1:
		return statusCreateProgress
	default:
		return status
	}
}

func (l lifecycle) jobStatus(status string, created time.Time) string {
	if status != statusActive {
		return status
	}

	switch l.phase(created) {
	case 0:
		return statusJobPending
	case 1:
		return statusJobProgress
	default:
		return status
	}
}

func (l lifecycle) deletionStatus(status string, created time.Time) string {
	if status != statusCompleted {
		return status
	}

	switch l.phase(created) {
	case 0:
		return statusDeletionPending
	case 1:
		return statusDeletionRunning
	default:
		return status
	}
}
