package persistence_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// --- test doubles ---

// mockPersistable tracks calls to Snapshot and Restore for assertion in tests.
type mockPersistable struct {
	restoreErr error
	data       []byte
	snapshots  atomic.Int64
	restores   atomic.Int64
	mu         sync.Mutex
}

func (m *mockPersistable) Snapshot() []byte {
	m.snapshots.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.data
}

func (m *mockPersistable) Restore(data []byte) error {
	m.restores.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.restoreErr != nil {
		return m.restoreErr
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	m.data = cp

	return nil
}

func (m *mockPersistable) Data() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]byte, len(m.data))
	copy(cp, m.data)

	return cp
}

// memStore is a thread-safe in-memory Store used in manager tests.
type memStore struct {
	loadErr   error
	saveErr   error
	data      map[string][]byte
	saveCount int
	mu        sync.Mutex
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (m *memStore) storeKey(service, key string) string {
	return service + "/" + key
}

func (m *memStore) Save(service, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.saveCount++

	if m.saveErr != nil {
		return m.saveErr
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[m.storeKey(service, key)] = cp

	return nil
}

func (m *memStore) Load(service, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.loadErr != nil {
		return nil, m.loadErr
	}

	v, ok := m.data[m.storeKey(service, key)]
	if !ok {
		return nil, persistence.ErrKeyNotFound
	}

	cp := make([]byte, len(v))
	copy(cp, v)

	return cp, nil
}

func (m *memStore) Delete(service, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, m.storeKey(service, key))

	return nil
}

func (m *memStore) ListKeys(_ string) ([]string, error) {
	return nil, nil
}

func (m *memStore) SaveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.saveCount
}

// discardLogger returns a [slog.Logger] that discards all output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- NewManager ---

func TestNewManager(t *testing.T) {
	t.Parallel()

	tests := []struct {
		store persistence.Store
		name  string
	}{
		{name: "null_store", store: persistence.NullStore{}},
		{name: "mem_store", store: newMemStore()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := persistence.NewManager(tt.store, discardLogger())
			require.NotNil(t, mgr)
		})
	}
}

// --- Register + RestoreAll ---

var (
	errLoad     = errors.New("i/o failure")
	errRestore  = errors.New("decode error")
	errDiskFull = errors.New("disk full")
)

func TestManager_Register_RestoreAll(t *testing.T) {
	t.Parallel()

	snapshot := []byte(`{"key":"value"}`)

	tests := []struct {
		setup        func(store *memStore, p *mockPersistable)
		name         string
		wantData     []byte
		wantRestores int64
	}{
		{
			name: "restore_success",
			setup: func(store *memStore, _ *mockPersistable) {
				require.NoError(t, store.Save("svc", "snapshot", snapshot))
			},
			wantRestores: 1,
			wantData:     snapshot,
		},
		{
			name:         "key_not_found_skips_restore",
			setup:        func(_ *memStore, _ *mockPersistable) {},
			wantRestores: 0,
		},
		{
			name: "load_error_skips_restore",
			setup: func(store *memStore, _ *mockPersistable) {
				store.loadErr = errLoad
			},
			wantRestores: 0,
		},
		{
			name: "restore_error_continues",
			setup: func(store *memStore, p *mockPersistable) {
				require.NoError(t, store.Save("svc", "snapshot", snapshot))
				p.restoreErr = errRestore
			},
			wantRestores: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := newMemStore()
			p := &mockPersistable{}
			tt.setup(store, p)

			mgr := persistence.NewManager(store, discardLogger())
			mgr.Register("svc", p)
			mgr.RestoreAll(t.Context())

			assert.Equal(t, tt.wantRestores, p.restores.Load())

			if tt.wantData != nil {
				assert.Equal(t, tt.wantData, p.Data())
			}
		})
	}
}

func TestManager_RestoreAll_NoServices(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(newMemStore(), discardLogger())

	require.NotPanics(t, func() {
		mgr.RestoreAll(t.Context())
	})
}

func TestManager_RestoreAll_MultipleServices(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	snap := []byte(`{"ok":true}`)

	services := []string{"alpha", "beta", "gamma"}
	persistables := make(map[string]*mockPersistable, len(services))

	for _, name := range services {
		require.NoError(t, store.Save(name, "snapshot", snap))
		persistables[name] = &mockPersistable{}
	}

	mgr := persistence.NewManager(store, discardLogger())

	for _, name := range services {
		mgr.Register(name, persistables[name])
	}

	mgr.RestoreAll(t.Context())

	for _, name := range services {
		assert.Equal(t, int64(1), persistables[name].restores.Load(), "service %s", name)
		assert.Equal(t, snap, persistables[name].Data(), "service %s", name)
	}
}

// --- SaveAll ---

func TestManager_SaveAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		saveErr       error
		name          string
		snapshotData  []byte
		wantSaveCount int
	}{
		{
			name:          "saves_registered_backend",
			snapshotData:  []byte(`{"data":"value"}`),
			wantSaveCount: 1,
		},
		{
			name:          "empty_snapshot_skipped",
			snapshotData:  nil,
			wantSaveCount: 0,
		},
		{
			name:          "save_error_is_logged_and_continues",
			snapshotData:  []byte(`{"data":"value"}`),
			saveErr:       errDiskFull,
			wantSaveCount: 1, // Save is attempted; error is logged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := newMemStore()
			store.saveErr = tt.saveErr

			p := &mockPersistable{data: tt.snapshotData}

			mgr := persistence.NewManager(store, discardLogger())
			mgr.Register("svc", p)
			mgr.SaveAll(t.Context())

			assert.Equal(t, tt.wantSaveCount, store.SaveCount())
		})
	}
}

func TestManager_SaveAll_MultipleServices(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	mgr := persistence.NewManager(store, discardLogger())

	for _, name := range []string{"alpha", "beta", "gamma"} {
		mgr.Register(name, &mockPersistable{data: []byte(`{"ok":true}`)})
	}

	mgr.SaveAll(t.Context())

	assert.Equal(t, 3, store.SaveCount())
}

// --- Notify ---

func TestManager_Notify_UnknownService(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(newMemStore(), discardLogger())

	require.NotPanics(t, func() {
		mgr.Notify("nonexistent")
	})
}

func TestManager_Notify_TriggersDebouncedSave(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"notify":true}`)}

	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)
	mgr.Notify("svc")

	assert.Eventually(t, func() bool {
		return store.SaveCount() > 0
	}, 2*time.Second, 25*time.Millisecond)

	assert.Equal(t, 1, store.SaveCount())
}

func TestManager_Notify_SaveAllCancelsPendingTimer(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"cancel":true}`)}

	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)

	mgr.Notify("svc")
	mgr.SaveAll(t.Context())

	// SaveAll performs the synchronous save; the timer must be stopped.
	assert.Equal(t, 1, store.SaveCount())

	// After SaveAll the debounce timer must not fire a second save.
	time.Sleep(750 * time.Millisecond)
	assert.Equal(t, 1, store.SaveCount())
}

func TestManager_Notify_ResetTimerOnRapidCalls(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"rapid":true}`)}

	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)

	// Rapid calls must not create multiple concurrent saves.
	for range 10 {
		mgr.Notify("svc")
	}

	// Use SaveAll to flush; at most one save should be recorded.
	mgr.SaveAll(t.Context())

	assert.Equal(t, 1, store.SaveCount())
}

// --- Debounce generation mechanism ---

func TestManager_Notify_GenerationSkipsStaleCallback(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"gen":true}`)}

	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)

	// Trigger a Notify so a timer is created (generation=1, closure captures gen=1).
	mgr.Notify("svc")

	// Immediately trigger again; generation advances to 2 but the closure
	// created above still holds gen=1. When saveIfCurrent fires it will see
	// generation(2) != gen(1) and skip the save.
	mgr.Notify("svc")

	// SaveAll stops the timer and does the authoritative synchronous save.
	mgr.SaveAll(t.Context())

	// Exactly one save must have been performed (by SaveAll, not the stale timer).
	assert.Equal(t, 1, store.SaveCount())
}

// TestManager_Notify_StaleGenerationSkipped verifies that when two Notify calls
// arrive before the debounce timer fires, the stale generation check in
// saveIfCurrent prevents a save (covering the early-return branch).
func TestManager_Notify_StaleGenerationSkipped(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"gen":true}`)}

	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)

	// First Notify: generation=1, AfterFunc closure captures gen=1.
	// Second Notify: generation=2, timer.Reset() called — closure still holds gen=1.
	// When the timer fires, saveIfCurrent sees generation(2)!=gen(1) and skips.
	mgr.Notify("svc")
	mgr.Notify("svc")

	// Wait long enough for the debounce timer to fire and execute the stale check.
	time.Sleep(750 * time.Millisecond)

	// The timer fired with a stale generation; no save should have occurred.
	assert.Equal(t, 0, store.SaveCount())
}

// TestManager_Notify_SaveErrorInCallback covers the error-logging branch inside
// saveIfCurrent by injecting a store error and letting the debounce fire.
func TestManager_Notify_SaveErrorInCallback(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	store.saveErr = errDiskFull

	p := &mockPersistable{data: []byte(`{"err":true}`)}

	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)

	// Single Notify so generation remains 1; saveIfCurrent will proceed to save.
	mgr.Notify("svc")

	// Wait for the debounce callback to fire and attempt (and fail) the save.
	assert.Eventually(t, func() bool {
		return store.SaveCount() > 0
	}, 2*time.Second, 25*time.Millisecond)

	// The save was attempted (count > 0) but failed; manager must not panic.
	assert.GreaterOrEqual(t, store.SaveCount(), 1)
}

// TestManager_Notify_ConcurrentNotify verifies that concurrent Notify calls
// do not race or panic under the race detector.
func TestManager_Notify_ConcurrentNotify(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"concurrent":true}`)}

	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)

	const goroutines = 20

	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			mgr.Notify("svc")
		})
	}

	wg.Wait()
	mgr.SaveAll(t.Context())

	assert.GreaterOrEqual(t, store.SaveCount(), 1)
}

// --- Context cancellation ---

func TestManager_RestoreAll_WithCancelledContext(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	require.NoError(t, store.Save("svc", "snapshot", []byte(`{"x":1}`)))

	p := &mockPersistable{}
	mgr := persistence.NewManager(store, discardLogger())
	mgr.Register("svc", p)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// RestoreAll must not panic even with a cancelled context (slog accepts it).
	require.NotPanics(t, func() {
		mgr.RestoreAll(ctx)
	})
}
