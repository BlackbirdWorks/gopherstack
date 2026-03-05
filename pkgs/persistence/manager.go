package persistence

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const (
	// snapshotKey is the Store key used to save a service snapshot.
	snapshotKey = "snapshot"

	// debounceDuration is the minimum quiet period between the last mutation
	// notification and the next disk write.
	debounceDuration = 500 * time.Millisecond
)

// entry holds the Persistable backend and debounce state for a single service.
type entry struct {
	persistable Persistable
	timer       *time.Timer
	name        string
	generation  uint64 // incremented on each Notify; callback checks it hasn't changed
	mu          sync.Mutex
}

// Manager coordinates persistence for a set of named service backends.
// It restores state on startup and performs debounced async saves on mutation.
type Manager struct {
	store   Store
	entries map[string]*entry
	mu      sync.RWMutex
}

// NewManager creates a Manager backed by the given Store.
func NewManager(store Store) *Manager {
	return &Manager{
		store:   store,
		entries: make(map[string]*entry),
	}
}

// Register associates a named Persistable with the manager.
// It must be called before RestoreAll or Notify.
func (m *Manager) Register(name string, p Persistable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries[name] = &entry{name: name, persistable: p}
}

// RestoreAll loads and restores snapshots for all registered Persistable backends.
// Errors for individual services are logged but do not abort restoration of others.
func (m *Manager) RestoreAll(ctx context.Context) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, e := range m.entries {
		data, err := m.store.Load(name, snapshotKey)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				slog.Default().DebugContext(ctx, "persistence: no snapshot found", "service", name)
			} else {
				slog.Default().WarnContext(ctx, "persistence: load failed", "service", name, "error", err)
			}

			continue
		}

		if restoreErr := e.persistable.Restore(data); restoreErr != nil {
			slog.Default().WarnContext(ctx, "persistence: restore failed", "service", name, "error", restoreErr)
		} else {
			slog.Default().InfoContext(ctx, "persistence: restored", "service", name)
		}
	}
}

// Notify schedules a debounced save for the named service.
// If no service with that name is registered the call is a no-op.
// A generation counter ensures that only the most-recent callback fires a save;
// any earlier callback that was still running is skipped, preventing concurrent
// writes to the same snapshot file.
func (m *Manager) Notify(name string) {
	m.mu.RLock()
	e, ok := m.entries[name]
	m.mu.RUnlock()

	if !ok {
		return
	}

	e.mu.Lock()
	e.generation++
	gen := e.generation

	if e.timer != nil {
		e.timer.Reset(debounceDuration)
		e.mu.Unlock()

		return
	}

	e.timer = time.AfterFunc(debounceDuration, func() {
		m.saveIfCurrent(e, gen)
	})
	e.mu.Unlock()
}

// SaveAll immediately persists all registered backends.
// Pending debounce timers are stopped to prevent concurrent writes on shutdown.
// It is intended for graceful shutdown.
func (m *Manager) SaveAll(ctx context.Context) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, e := range m.entries {
		// Stop any pending debounce timer so it doesn't fire concurrently.
		e.mu.Lock()
		if e.timer != nil {
			e.timer.Stop()
			e.timer = nil
		}
		e.mu.Unlock()

		if saveErr := m.save(e); saveErr != nil {
			slog.Default().WarnContext(ctx, "persistence: save failed on shutdown", "service", e.name, "error", saveErr)
		}
	}
}

// saveIfCurrent fires a save only if the generation still matches the one
// captured at Notify time. This check is performed after acquiring the entry
// lock, which prevents a stale callback (from a superseded Notify call) from
// running concurrently with the save that was already dispatched by a newer
// Notify.
func (m *Manager) saveIfCurrent(e *entry, gen uint64) {
	e.mu.Lock()
	if e.generation != gen {
		// A newer Notify arrived; skip this stale callback.
		e.mu.Unlock()

		return
	}

	e.timer = nil
	e.mu.Unlock()

	if err := m.save(e); err != nil {
		slog.Default().Warn("persistence: save failed", "service", e.name, "error", err)
	}
}

// save snapshots and stores a single entry.
func (m *Manager) save(e *entry) error {
	data := e.persistable.Snapshot()
	if len(data) == 0 {
		return nil
	}

	return m.store.Save(e.name, snapshotKey, data)
}
