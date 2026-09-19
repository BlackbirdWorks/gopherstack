package mgn

import "time"

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

// PutSourceServerForTest inserts a SourceServer directly into the backend's
// store, bypassing every real op. Used to reach fields no op ever sets --
// e.g. DataReplicationInfo.ReplicatorID, which this backend's own replication
// simulation never populates (sourceservers.go's scheduleReplication) -- so
// a wire-shape regression on such a field can still be caught by a real
// aws-sdk-go-v2 client round trip.
func (b *InMemoryBackend) PutSourceServerForTest(s *SourceServer) {
	b.sourceServers.Put(s)
}
