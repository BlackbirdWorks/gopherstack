package outposts

import "time"

// RenewalIdempotencyLenForTest returns the number of cached CreateRenewal
// idempotency entries, for tests verifying DeleteOutpost's pruning.
func (b *InMemoryBackend) RenewalIdempotencyLenForTest() int {
	b.mu.RLock("RenewalIdempotencyLenForTest")
	defer b.mu.RUnlock()

	return len(b.renewalIdempotency)
}

// ArmProbeTimerForTest arms a trivial timer directly on the backend's
// worker.Group and reports on the returned channel when it fires. Used by
// shutdown_leak_test.go to determine whether Close() (and so work.Stop())
// already ran: per pkgs/worker's own TestGroupAfterIsNoOpAfterStop, After
// silently no-ops once Stop has been called, so the channel never receiving
// a value proves the Group was already stopped.
func (b *InMemoryBackend) ArmProbeTimerForTest(d time.Duration) <-chan struct{} {
	ch := make(chan struct{}, 1)
	b.work.After("shutdown-leak-probe", d, func() { ch <- struct{}{} })

	return ch
}
