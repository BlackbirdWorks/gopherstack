package persistence_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func (m *mockPersistable) Snapshot(ctx context.Context) []byte {
	m.snapshots.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.data
}

func (m *mockPersistable) Restore(ctx context.Context, data []byte) error {
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

			mgr := persistence.NewManager(t.Context(), tt.store)
			require.NotNil(t, mgr)
		})
	}
}

// --- Register + RestoreAll ---

var (
	errLoad     = errors.New("i/o failure")
	errRestore  = errors.New("decode error")
	errDiskFull = errors.New("disk full")
	errAlpha    = errors.New("alpha failed")
	errBeta     = errors.New("beta failed")
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

			mgr := persistence.NewManager(t.Context(), store)
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

	mgr := persistence.NewManager(t.Context(), newMemStore())

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

	mgr := persistence.NewManager(t.Context(), store)

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

			mgr := persistence.NewManager(t.Context(), store)
			mgr.Register("svc", p)
			mgr.SaveAll(t.Context())

			assert.Equal(t, tt.wantSaveCount, store.SaveCount())
		})
	}
}

func TestManager_SaveAll_MultipleServices(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	mgr := persistence.NewManager(t.Context(), store)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		mgr.Register(name, &mockPersistable{data: []byte(`{"ok":true}`)})
	}

	mgr.SaveAll(t.Context())

	assert.Equal(t, 3, store.SaveCount())
}

// --- Notify ---

func TestManager_Notify_UnknownService(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(t.Context(), newMemStore())

	require.NotPanics(t, func() {
		mgr.Notify("nonexistent")
	})
}

func TestManager_Notify_TriggersDebouncedSave(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"notify":true}`)}

	mgr := persistence.NewManager(t.Context(), store)
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

	mgr := persistence.NewManager(t.Context(), store)
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

	mgr := persistence.NewManager(t.Context(), store)
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

// TestManager_Notify_GenerationSkipsStaleCallback verifies that SaveAll
// stops any pending debounce timer and performs exactly one authoritative
// save, even after multiple rapid Notify calls.
func TestManager_Notify_GenerationSkipsStaleCallback(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"gen":true}`)}

	mgr := persistence.NewManager(t.Context(), store)
	mgr.Register("svc", p)

	mgr.Notify("svc")
	mgr.Notify("svc")

	// SaveAll stops the pending timer and does a synchronous save.
	mgr.SaveAll(t.Context())

	assert.Equal(t, 1, store.SaveCount())
}

// TestManager_Notify_StaleGenerationSkipped verifies that when two Notify calls
// arrive before the debounce timer fires, exactly one save is performed. Each
// Notify cancels the previous timer and starts a fresh AfterFunc whose closure
// captures the current generation; the stale generation check guarantees a
// previously-scheduled fire that races past Stop is skipped.
func TestManager_Notify_StaleGenerationSkipped(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	p := &mockPersistable{data: []byte(`{"gen":true}`)}

	mgr := persistence.NewManager(t.Context(), store)
	mgr.Register("svc", p)

	// Two Notify calls within the debounce window: only the second timer's
	// closure carries the current generation, so exactly one save happens.
	mgr.Notify("svc")
	mgr.Notify("svc")

	// Wait long enough for the debounce timer to fire.
	time.Sleep(750 * time.Millisecond)

	assert.Equal(t, 1, store.SaveCount())
}

// TestManager_Notify_SaveErrorInCallback covers the error-logging branch inside
// saveIfCurrent by injecting a store error and letting the debounce fire.
func TestManager_Notify_SaveErrorInCallback(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	store.saveErr = errDiskFull

	p := &mockPersistable{data: []byte(`{"err":true}`)}

	mgr := persistence.NewManager(t.Context(), store)
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

	mgr := persistence.NewManager(t.Context(), store)
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
	mgr := persistence.NewManager(t.Context(), store)
	mgr.Register("svc", p)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// RestoreAll must not panic even with a cancelled context (slog accepts it).
	require.NotPanics(t, func() {
		mgr.RestoreAll(ctx)
	})
}

// --- ExportAll ---

func TestManager_ExportAll_Empty(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(t.Context(), newMemStore())
	result := mgr.ExportAll()

	assert.Empty(t, result, "empty manager must return empty map")
}

func TestManager_ExportAll(t *testing.T) {
	t.Parallel()

	snap := []byte(`{"state":"active"}`)

	tests := []struct {
		name     string
		data     []byte
		wantData []byte
		wantLen  int
	}{
		{
			name:     "non_empty_snapshot_included",
			data:     snap,
			wantLen:  1,
			wantData: snap,
		},
		{
			name:    "nil_snapshot_excluded",
			data:    nil,
			wantLen: 0,
		},
		{
			name:    "empty_snapshot_excluded",
			data:    []byte{},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := persistence.NewManager(t.Context(), newMemStore())
			mgr.Register("svc", &mockPersistable{data: tt.data})

			result := mgr.ExportAll()

			assert.Len(t, result, tt.wantLen)

			if tt.wantData != nil {
				assert.Equal(t, tt.wantData, result["svc"])
			}
		})
	}
}

func TestManager_ExportAll_MultipleServices(t *testing.T) {
	t.Parallel()

	snapshots := map[string][]byte{
		"alpha": []byte(`{"a":1}`),
		"beta":  []byte(`{"b":2}`),
		"gamma": []byte(`{"c":3}`),
	}

	mgr := persistence.NewManager(t.Context(), newMemStore())

	for name, data := range snapshots {
		mgr.Register(name, &mockPersistable{data: data})
	}

	result := mgr.ExportAll()

	assert.Len(t, result, len(snapshots))

	for name, want := range snapshots {
		assert.Equal(t, want, result[name], "service %s", name)
	}
}

func TestManager_ExportAll_MixedEmptyAndNonEmpty(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(t.Context(), newMemStore())
	mgr.Register("present", &mockPersistable{data: []byte(`{"ok":true}`)})
	mgr.Register("absent", &mockPersistable{data: nil})
	mgr.Register("empty", &mockPersistable{data: []byte{}})

	result := mgr.ExportAll()

	assert.Len(t, result, 1, "only services with non-empty snapshots should be included")
	assert.Contains(t, result, "present")
	assert.NotContains(t, result, "absent")
	assert.NotContains(t, result, "empty")
}

func TestManager_ExportAll_DoesNotWriteStore(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	mgr := persistence.NewManager(t.Context(), store)
	mgr.Register("svc", &mockPersistable{data: []byte(`{"x":1}`)})

	_ = mgr.ExportAll()

	assert.Equal(t, 0, store.SaveCount(), "ExportAll must not write to the store")
}

func TestManager_ExportAll_ConcurrentSafety(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(t.Context(), newMemStore())

	for i := range 10 {
		mgr.Register(fmt.Sprintf("svc%d", i), &mockPersistable{data: []byte(`{"ok":true}`)})
	}

	const goroutines = 20

	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			result := mgr.ExportAll()
			assert.Len(t, result, 10)
		})
	}

	wg.Wait()
}

// --- ImportAll ---

func TestManager_ImportAll_Empty(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(t.Context(), newMemStore())

	err := mgr.ImportAll(t.Context(), map[string][]byte{})

	require.NoError(t, err)
}

func TestManager_ImportAll(t *testing.T) {
	t.Parallel()

	snap := []byte(`{"restored":true}`)

	tests := []struct {
		setupErr     error
		snapshots    map[string][]byte
		name         string
		wantRestores int64
		wantErr      bool
	}{
		{
			name:         "single_service_restored",
			snapshots:    map[string][]byte{"svc": snap},
			wantRestores: 1,
		},
		{
			name:         "unknown_service_skipped",
			snapshots:    map[string][]byte{"nonexistent": snap},
			wantRestores: 0,
		},
		{
			name:         "restore_error_returns_error",
			snapshots:    map[string][]byte{"svc": snap},
			setupErr:     errRestore,
			wantErr:      true,
			wantRestores: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &mockPersistable{restoreErr: tt.setupErr}
			mgr := persistence.NewManager(t.Context(), newMemStore())
			mgr.Register("svc", p)

			err := mgr.ImportAll(t.Context(), tt.snapshots)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRestores, p.restores.Load())
		})
	}
}

func TestManager_ImportAll_MultipleServices(t *testing.T) {
	t.Parallel()

	snapshots := map[string][]byte{
		"alpha": []byte(`{"a":1}`),
		"beta":  []byte(`{"b":2}`),
		"gamma": []byte(`{"c":3}`),
	}

	persistables := make(map[string]*mockPersistable, len(snapshots))
	mgr := persistence.NewManager(t.Context(), newMemStore())

	for name := range snapshots {
		p := &mockPersistable{}
		persistables[name] = p
		mgr.Register(name, p)
	}

	err := mgr.ImportAll(t.Context(), snapshots)

	require.NoError(t, err)

	for name, snap := range snapshots {
		assert.Equal(t, int64(1), persistables[name].restores.Load(), "service %s", name)
		assert.Equal(t, snap, persistables[name].Data(), "service %s", name)
	}
}

func TestManager_ImportAll_MultipleErrors(t *testing.T) {
	t.Parallel()

	mgr := persistence.NewManager(t.Context(), newMemStore())
	mgr.Register("alpha", &mockPersistable{restoreErr: errAlpha})
	mgr.Register("beta", &mockPersistable{restoreErr: errBeta})

	err := mgr.ImportAll(t.Context(), map[string][]byte{
		"alpha": []byte(`{"a":1}`),
		"beta":  []byte(`{"b":2}`),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "alpha failed")
	assert.Contains(t, err.Error(), "beta failed")
}

func TestManager_ImportAll_PartialSuccess(t *testing.T) {
	t.Parallel()

	good := &mockPersistable{}
	bad := &mockPersistable{restoreErr: errRestore}

	mgr := persistence.NewManager(t.Context(), newMemStore())
	mgr.Register("good", good)
	mgr.Register("bad", bad)

	err := mgr.ImportAll(t.Context(), map[string][]byte{
		"good": []byte(`{"ok":true}`),
		"bad":  []byte(`{"ok":true}`),
	})

	require.Error(t, err)

	// The good service must still have been restored.
	assert.Equal(t, int64(1), good.restores.Load())
	assert.Equal(t, int64(1), bad.restores.Load())
}

func TestManager_ImportAll_WithCancelledContext(t *testing.T) {
	t.Parallel()

	p := &mockPersistable{}
	mgr := persistence.NewManager(t.Context(), newMemStore())
	mgr.Register("svc", p)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// ImportAll is synchronous; a cancelled context affects only slog output.
	err := mgr.ImportAll(ctx, map[string][]byte{"svc": []byte(`{"x":1}`)})

	require.NoError(t, err)
	assert.Equal(t, int64(1), p.restores.Load())
}

// --- ExportAll / ImportAll round-trip ---

// statefulPersistable is a simple in-memory service that tracks a named state
// value and implements Persistable for round-trip testing.
type statefulPersistable struct {
	state string
	mu    sync.Mutex
}

func (s *statefulPersistable) Snapshot(ctx context.Context) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	return []byte(`{"state":"` + s.state + `"}`)
}

func (s *statefulPersistable) Restore(ctx context.Context, data []byte) error {
	var v struct {
		State string `json:"state"`
	}

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	s.mu.Lock()
	s.state = v.State
	s.mu.Unlock()

	return nil
}

func (s *statefulPersistable) Reset() {
	s.mu.Lock()
	s.state = ""
	s.mu.Unlock()
}

func (s *statefulPersistable) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state
}

func TestManager_ExportImport_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initial    map[string]string
		wantStates map[string]string
		name       string
	}{
		{
			name:       "single_service_state_preserved",
			initial:    map[string]string{"svc": "running"},
			wantStates: map[string]string{"svc": "running"},
		},
		{
			name: "multiple_services_state_preserved",
			initial: map[string]string{
				"alpha": "active",
				"beta":  "idle",
				"gamma": "stopped",
			},
			wantStates: map[string]string{
				"alpha": "active",
				"beta":  "idle",
				"gamma": "stopped",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := persistence.NewManager(t.Context(), newMemStore())
			services := make(map[string]*statefulPersistable, len(tt.initial))

			for name, state := range tt.initial {
				svc := &statefulPersistable{state: state}
				services[name] = svc
				mgr.Register(name, svc)
			}

			// Step 1: export current state.
			exported := mgr.ExportAll()

			// Step 2: reset all services to empty state.
			for _, svc := range services {
				svc.Reset()
			}

			for name, svc := range services {
				assert.Empty(t, svc.State(), "service %s should be empty after reset", name)
			}

			// Step 3: import the exported snapshot to restore state.
			err := mgr.ImportAll(t.Context(), exported)
			require.NoError(t, err)

			// Step 4: verify each service is back to its original state.
			for name, want := range tt.wantStates {
				assert.Equal(t, want, services[name].State(), "service %s state mismatch", name)
			}
		})
	}
}

func TestManager_ExportImport_RoundTrip_PartialLoad(t *testing.T) {
	t.Parallel()

	// Export two services but only load one; the other keeps its reset state.
	alpha := &statefulPersistable{state: "alpha-value"}
	beta := &statefulPersistable{state: "beta-value"}

	mgr := persistence.NewManager(t.Context(), newMemStore())
	mgr.Register("alpha", alpha)
	mgr.Register("beta", beta)

	exported := mgr.ExportAll()

	alpha.Reset()
	beta.Reset()

	// Import only alpha.
	err := mgr.ImportAll(t.Context(), map[string][]byte{"alpha": exported["alpha"]})
	require.NoError(t, err)

	assert.Equal(t, "alpha-value", alpha.State(), "alpha should be restored")
	assert.Empty(t, beta.State(), "beta should remain reset")
}

func TestManager_ExportImport_RoundTrip_MultipleRounds(t *testing.T) {
	t.Parallel()

	// Simulate a workflow where state is mutated between rounds.
	svc := &statefulPersistable{state: "v1"}
	mgr := persistence.NewManager(t.Context(), newMemStore())
	mgr.Register("svc", svc)

	// Round 1: export v1.
	snap1 := mgr.ExportAll()
	assert.JSONEq(t, `{"state":"v1"}`, string(snap1["svc"]))

	// Mutate to v2 and export.
	svc.mu.Lock()
	svc.state = "v2"
	svc.mu.Unlock()

	snap2 := mgr.ExportAll()
	assert.JSONEq(t, `{"state":"v2"}`, string(snap2["svc"]))

	// Reset, load snap1, verify v1 is restored.
	svc.Reset()
	require.NoError(t, mgr.ImportAll(t.Context(), snap1))
	assert.Equal(t, "v1", svc.State())

	// Reset, load snap2, verify v2 is restored.
	svc.Reset()
	require.NoError(t, mgr.ImportAll(t.Context(), snap2))
	assert.Equal(t, "v2", svc.State())
}
