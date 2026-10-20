package persistence

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// SetPolicy installs the per-service persistence policy. Call before RestoreAll.
func (m *Manager) SetPolicy(p Policy) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.policy = p
}

// FlushDirty saves every hybrid-mode service whose state changed since its last
// flush and returns how many were written. Unchanged services are skipped.
func (m *Manager) FlushDirty(ctx context.Context) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dirty := make([]*entry, 0, len(m.entries))

	for name, e := range m.entries {
		if mode, on := m.policy.ModeFor(name); on && mode == ModeHybrid && e.isDirty() {
			dirty = append(dirty, e)
		}
	}

	written := 0

	for _, e := range dirty {
		if m.flushEntry(ctx, e) {
			written++
		}
	}

	return written
}

func (e *entry) isDirty() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.generation != e.flushed
}

// flushEntry marks the generation flushed before snapshotting so a mutation
// racing the save re-dirties the entry; it is restored on failure.
func (m *Manager) flushEntry(ctx context.Context, e *entry) bool {
	e.mu.Lock()
	gen, prev := e.generation, e.flushed
	e.flushed = gen
	e.mu.Unlock()

	ectx := entryCtx(ctx, e.name)
	if err := m.save(ectx, e); err != nil {
		logger.Load(ectx).WarnContext(ectx, "persistence: flush failed", "service", e.name, "error", err)

		e.mu.Lock()
		e.flushed = prev
		e.mu.Unlock()

		return false
	}

	return true
}

// StartFlusher runs FlushDirty every interval until the returned stop function
// is called; stop waits for the loop.
func (m *Manager) StartFlusher(ctx context.Context, interval time.Duration) func() {
	return runPeriodic(ctx, interval, func(c context.Context) { m.FlushDirty(c) })
}

func runPeriodic(ctx context.Context, interval time.Duration, fn func(context.Context)) func() {
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		t := time.NewTicker(interval)
		defer t.Stop()

		for {
			select {
			case <-loopCtx.Done():
				return
			case <-t.C:
				fn(loopCtx)
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}
