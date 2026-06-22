package persistence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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
	baseCtx context.Context //nolint:containedctx // lifecycle ctx for the async (debounce) save path; no request ctx.
	mu      sync.RWMutex
}

// NewManager creates a Manager backed by the given Store. The supplied context
// is the process lifecycle context; it carries the root logger and is used for
// the debounced async save path (Notify -> AfterFunc), which has no per-request
// or per-call context of its own. A nil ctx falls back to context.Background().
func NewManager(ctx context.Context, store Store) *Manager {
	if ctx == nil {
		ctx = context.Background()
	}

	return &Manager{
		store:   store,
		entries: make(map[string]*entry),
		baseCtx: ctx,
	}
}

// entryCtx derives a context whose logger is tagged worker=<service>-persistence
// from the supplied parent. Used so every persistence record is attributable to
// the service whose state is being snapshotted/restored.
func entryCtx(parent context.Context, name string) context.Context {
	return logger.WithWorker(parent, name, "persistence")
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
		ectx := entryCtx(ctx, name)
		log := logger.Load(ectx)

		data, err := m.store.Load(name, snapshotKey)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				log.DebugContext(ectx, "persistence: no snapshot found", "service", name)
			} else {
				log.WarnContext(ectx, "persistence: load failed", "service", name, "error", err)
			}

			continue
		}

		if restoreErr := e.persistable.Restore(ectx, data); restoreErr != nil {
			log.WarnContext(ectx, "persistence: restore failed", "service", name, "error", restoreErr)
		} else {
			log.InfoContext(ectx, "persistence: restored", "service", name)
		}
	}
}

// Notify schedules a debounced save for the named service.
// If no service with that name is registered the call is a no-op.
//
// Each Notify bumps a per-entry generation counter, stops any pending timer,
// and starts a fresh AfterFunc that captures the new generation. The save
// callback only proceeds if the generation it was created with is still the
// current one. This prevents two pathological cases:
//
//  1. A previously-fired AfterFunc whose closure captured an old generation
//     racing with a newer Notify (the old fire is skipped by the gen check).
//  2. Notify being called multiple times during the debounce window: only
//     the most recent timer's closure will pass the generation check and
//     perform the save.
//
// Earlier versions of this function captured the generation only on the
// first Notify and reused the same AfterFunc via Reset, which caused the
// captured generation to fall stale and the eventual save to be skipped
// indefinitely after a second Notify.
func (m *Manager) Notify(name string) {
	m.mu.RLock()
	e, ok := m.entries[name]
	m.mu.RUnlock()

	if !ok {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.generation++
	gen := e.generation

	if e.timer != nil {
		e.timer.Stop()
	}

	e.timer = time.AfterFunc(debounceDuration, func() {
		m.saveIfCurrent(e, gen)
	})
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

		ectx := entryCtx(ctx, e.name)
		if saveErr := m.save(ectx, e); saveErr != nil {
			logger.Load(ectx).
				WarnContext(ectx, "persistence: save failed on shutdown", "service", e.name, "error", saveErr)
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

	// The debounced save fires off-request; use the manager lifecycle context.
	ectx := entryCtx(m.baseCtx, e.name)
	if err := m.save(ectx, e); err != nil {
		logger.Load(ectx).WarnContext(ectx, "persistence: save failed", "service", e.name, "error", err)
	}
}

// save snapshots and stores a single entry. ctx must already be tagged for the
// entry (see entryCtx) so the backend's Snapshot logs under worker=<svc>-persistence.
func (m *Manager) save(ctx context.Context, e *entry) error {
	data := e.persistable.Snapshot(ctx)
	if len(data) == 0 {
		return nil
	}

	return m.store.Save(e.name, snapshotKey, data)
}

// ExportAll captures an in-memory snapshot from every registered service and
// returns the results as a map of service name → raw snapshot bytes. Services
// that return an empty snapshot (nil or zero-length) are omitted from the map.
// The store is not written; this is a pure read of current in-memory state.
func (m *Manager) ExportAll() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]byte, len(m.entries))

	for name, e := range m.entries {
		data := e.persistable.Snapshot(entryCtx(m.baseCtx, name))
		if len(data) > 0 {
			result[name] = data
		}
	}

	return result
}

// ImportAll restores state for every service whose name appears in snapshots.
// Services not registered with the manager are warned and skipped. Restore
// errors are collected; all services in the map are attempted regardless of
// earlier failures. Returns a combined error if any restore failed, nil
// otherwise.
func (m *Manager) ImportAll(ctx context.Context, snapshots map[string][]byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var errs []error

	for name, data := range snapshots {
		ectx := entryCtx(ctx, name)
		log := logger.Load(ectx)

		e, ok := m.entries[name]
		if !ok {
			log.WarnContext(ectx, "persistence: unknown service in snapshot, skipping", "service", name)

			continue
		}

		if err := e.persistable.Restore(ectx, data); err != nil {
			log.WarnContext(ectx, "persistence: import restore failed", "service", name, "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		} else {
			log.InfoContext(ectx, "persistence: imported", "service", name)
		}
	}

	return errors.Join(errs...)
}
