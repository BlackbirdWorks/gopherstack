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
	mu          sync.Mutex
}

// Manager coordinates persistence for a set of named service backends.
// It restores state on startup and performs debounced async saves on mutation.
type Manager struct {
	store   Store
	log     *slog.Logger
	entries map[string]*entry
	mu      sync.RWMutex
}

// NewManager creates a Manager backed by the given Store.
func NewManager(store Store, log *slog.Logger) *Manager {
	return &Manager{
		store:   store,
		log:     log,
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
				m.log.DebugContext(ctx, "persistence: no snapshot found", "service", name)
			} else {
				m.log.WarnContext(ctx, "persistence: load failed", "service", name, "error", err)
			}

			continue
		}

		if restoreErr := e.persistable.Restore(data); restoreErr != nil {
			m.log.WarnContext(ctx, "persistence: restore failed", "service", name, "error", restoreErr)
		} else {
			m.log.InfoContext(ctx, "persistence: restored", "service", name)
		}
	}
}

// Notify schedules a debounced save for the named service.
// If no service with that name is registered the call is a no-op.
func (m *Manager) Notify(name string) {
	m.mu.RLock()
	e, ok := m.entries[name]
	m.mu.RUnlock()

	if !ok {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.timer != nil {
		e.timer.Reset(debounceDuration)

		return
	}

	e.timer = time.AfterFunc(debounceDuration, func() {
		m.saveOne(e)
	})
}

// SaveAll immediately persists all registered backends.
// It is intended for graceful shutdown.
func (m *Manager) SaveAll(ctx context.Context) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, e := range m.entries {
		if saveErr := m.save(e); saveErr != nil {
			m.log.WarnContext(ctx, "persistence: save failed on shutdown", "service", e.name, "error", saveErr)
		}
	}
}

// saveOne cancels the outstanding timer and persists the entry.
func (m *Manager) saveOne(e *entry) {
	e.mu.Lock()
	e.timer = nil
	e.mu.Unlock()

	if err := m.save(e); err != nil {
		m.log.Warn("persistence: save failed", "service", e.name, "error", err)
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
