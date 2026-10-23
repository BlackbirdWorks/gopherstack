package persistence_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type counterSvc struct {
	vals map[string]int
	mu   sync.Mutex
}

func newCounter() *counterSvc { return &counterSvc{vals: map[string]int{}} }

func (c *counterSvc) set(k string, v int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.vals[k] = v
}

func (c *counterSvc) get(k string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.vals[k]
}

func (c *counterSvc) Snapshot(context.Context) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, _ := json.Marshal(c.vals)

	return b
}

func (c *counterSvc) Restore(_ context.Context, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return json.Unmarshal(data, &c.vals)
}

func newMgr(
	t *testing.T,
	dir string,
	pol persistence.Policy,
	svcs map[string]*counterSvc,
) *persistence.Manager {
	t.Helper()

	fs, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	m := persistence.NewManager(t.Context(), fs)
	m.SetPolicy(pol)

	for n, s := range svcs {
		m.Register(n, s)
	}

	return m
}

func TestPolicyModeFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svc      string
		wantMode persistence.Mode
		include  []string
		exclude  []string
		override []string
		wantOn   bool
	}{
		{name: "default", svc: "S3", wantMode: persistence.ModeSnapshot, wantOn: true},
		{
			name:     "include hit folded",
			svc:      "S3",
			include:  []string{"s3"},
			wantMode: persistence.ModeSnapshot,
			wantOn:   true,
		},
		{name: "include miss", svc: "SQS", include: []string{"s3"}, wantMode: persistence.ModeSnapshot},
		{
			name:     "exclude folded",
			svc:      "DynamoDB",
			exclude:  []string{"DYNAMODB"},
			wantMode: persistence.ModeSnapshot,
		},
		{
			name:     "exclude wins",
			svc:      "S3",
			include:  []string{"s3"},
			exclude:  []string{"s3"},
			wantMode: persistence.ModeSnapshot,
		},
		{
			name:     "override",
			svc:      "DynamoDB",
			override: []string{"dynamodb=HYBRID"},
			wantMode: persistence.ModeHybrid,
			wantOn:   true,
		},
		{
			name:     "system always on",
			svc:      "system-regions",
			include:  []string{"s3"},
			wantMode: persistence.ModeSnapshot,
			wantOn:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := persistence.NewPolicy(persistence.ModeSnapshot, tt.include, tt.exclude, tt.override)
			require.NoError(t, err)

			mode, on := p.ModeFor(tt.svc)
			assert.Equal(t, tt.wantOn, on)

			if tt.wantOn {
				assert.Equal(t, tt.wantMode, mode)
			}
		})
	}
}

func TestPolicyRejectsBadInput(t *testing.T) {
	t.Parallel()

	_, err := persistence.NewPolicy(persistence.ModeSnapshot, nil, nil, []string{"s3"})
	require.Error(t, err)

	_, err = persistence.NewPolicy(persistence.ModeSnapshot, nil, nil, []string{"s3=bogus"})
	require.Error(t, err)

	_, err = persistence.ParseMode("nope")
	require.Error(t, err)
}

func TestPerServiceFilterPersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pol, err := persistence.NewPolicy(persistence.ModeSnapshot, []string{"KEEP"}, nil, nil)
	require.NoError(t, err)

	keep, skip := newCounter(), newCounter()
	m := newMgr(t, dir, pol, map[string]*counterSvc{"keep": keep, "skip": skip})
	keep.set("a", 1)
	skip.set("a", 1)
	m.SaveAll(t.Context())

	keep2, skip2 := newCounter(), newCounter()
	m2 := newMgr(t, dir, pol, map[string]*counterSvc{"keep": keep2, "skip": skip2})
	m2.RestoreAll(t.Context())

	assert.Equal(t, 1, keep2.get("a"))
	assert.Zero(t, skip2.get("a"))
	assert.NoFileExists(t, filepath.Join(dir, "skip", "snapshot.json"))
}

func TestHybridCrashRecoveryFlushesOnlyDirty(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pol, err := persistence.NewPolicy(persistence.ModeHybrid, nil, nil, nil)
	require.NoError(t, err)

	a, b := newCounter(), newCounter()
	m := newMgr(t, dir, pol, map[string]*counterSvc{"a": a, "b": b})

	a.set("x", 1)
	m.Notify("a")
	assert.Equal(t, 1, m.FlushDirty(t.Context()))
	assert.Zero(t, m.FlushDirty(t.Context()), "clean services are not rewritten")

	b.set("y", 2)
	m.Notify("b")
	assert.Equal(t, 1, m.FlushDirty(t.Context()))

	// crash: no SaveAll
	a2, b2 := newCounter(), newCounter()
	m2 := newMgr(t, dir, pol, map[string]*counterSvc{"a": a2, "b": b2})
	m2.RestoreAll(t.Context())

	assert.Equal(t, 1, a2.get("x"))
	assert.Equal(t, 2, b2.get("y"))
}

func TestHybridPeriodicFlusher(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		pol, err := persistence.NewPolicy(persistence.ModeHybrid, nil, nil, nil)
		require.NoError(t, err)

		a := newCounter()
		m := newMgr(t, dir, pol, map[string]*counterSvc{"a": a})
		stop := m.StartFlusher(t.Context(), time.Second)

		a.set("x", 7)
		m.Notify("a")
		assert.NoFileExists(t, filepath.Join(dir, "a", "snapshot.json"), "not flushed before the interval")

		time.Sleep(time.Second)
		synctest.Wait()
		stop()

		assert.FileExists(t, filepath.Join(dir, "a", "snapshot.json"))
	})
}
