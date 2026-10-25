package persistence_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

func TestSaveModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mode          persistence.Mode
		afterDebounce bool
		afterSaveAll  bool
		afterSaveNow  bool
	}{
		{name: "snapshot", mode: persistence.ModeSnapshot, afterDebounce: true, afterSaveAll: true, afterSaveNow: true},
		{name: "shutdown", mode: persistence.ModeShutdown, afterSaveAll: true, afterSaveNow: true},
		{name: "manual", mode: persistence.ModeManual, afterSaveNow: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				dir := t.TempDir()
				file := filepath.Join(dir, "a", "snapshot.json")
				pol, err := persistence.NewPolicy(tt.mode, nil, nil, nil)
				require.NoError(t, err)

				a := newCounter()
				m := newMgr(t, dir, pol, map[string]*counterSvc{"a": a})

				a.set("x", 1)
				m.Notify("a")
				time.Sleep(2 * time.Second)
				synctest.Wait()
				assert.Equal(t, tt.afterDebounce, fileExists(file), "after debounce window")

				m.SaveAll(t.Context())
				assert.Equal(t, tt.afterSaveAll, fileExists(file), "after SaveAll")

				assert.Equal(t, 1, m.SaveNow(t.Context()))
				assert.Equal(t, tt.afterSaveNow, fileExists(file), "after SaveNow")
			})
		})
	}
}

func TestLoadStrategies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		load         persistence.LoadStrategy
		wantStartup  int
		wantOnAccess int
		wantAfterNow int
	}{
		{name: "startup", load: persistence.LoadStartup, wantStartup: 5, wantOnAccess: 5, wantAfterNow: 5},
		{name: "request", load: persistence.LoadRequest, wantStartup: 0, wantOnAccess: 5, wantAfterNow: 5},
		{name: "manual", load: persistence.LoadManual, wantStartup: 0, wantOnAccess: 0, wantAfterNow: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			pol, err := persistence.NewPolicy(persistence.ModeSnapshot, nil, nil, nil)
			require.NoError(t, err)

			seed := newCounter()
			seed.set("k", 5)
			newMgr(t, dir, pol, map[string]*counterSvc{"a": seed}).SaveAll(t.Context())

			a := newCounter()
			m := newMgr(t, dir, pol, map[string]*counterSvc{"a": a})
			m.SetLoadStrategy(tt.load)
			m.RestoreAll(t.Context())
			assert.Equal(t, tt.wantStartup, a.get("k"), "after startup")

			m.EnsureLoaded(t.Context(), "a")
			assert.Equal(t, tt.wantOnAccess, a.get("k"), "after first access")

			assert.Equal(t, 1, m.LoadNow(t.Context()))
			assert.Equal(t, tt.wantAfterNow, a.get("k"), "after LoadNow")
		})
	}
}

func TestLazyLoadShutdownKeepsUnloadedState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pol, err := persistence.NewPolicy(persistence.ModeSnapshot, nil, nil, nil)
	require.NoError(t, err)

	seed := newCounter()
	seed.set("k", 9)
	newMgr(t, dir, pol, map[string]*counterSvc{"a": seed, "b": newCounter()}).SaveAll(t.Context())

	a, b := newCounter(), newCounter()
	m := newMgr(t, dir, pol, map[string]*counterSvc{"a": a, "b": b})
	m.SetLoadStrategy(persistence.LoadRequest)
	m.RestoreAll(t.Context())
	m.EnsureLoaded(t.Context(), "b")
	m.SaveAll(t.Context())

	a2 := newCounter()
	m2 := newMgr(t, dir, pol, map[string]*counterSvc{"a": a2})
	m2.RestoreAll(t.Context())
	assert.Equal(t, 9, a2.get("k"), "never-touched service keeps its stored snapshot")
}

func TestParseLoadStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    persistence.LoadStrategy
		wantErr bool
	}{
		{in: "", want: persistence.LoadStartup},
		{in: "REQUEST", want: persistence.LoadRequest},
		{in: " manual ", want: persistence.LoadManual},
		{in: "bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run("in_"+tt.in, func(t *testing.T) {
			t.Parallel()

			got, err := persistence.ParseLoadStrategy(tt.in)
			if tt.wantErr {
				require.ErrorIs(t, err, persistence.ErrInvalidMode)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}
