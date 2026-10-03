package persistence_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type recordingPersistable struct {
	got  map[string][]byte
	mu   *sync.Mutex
	name string
	data []byte
}

func (r recordingPersistable) Snapshot(context.Context) []byte { return r.data }

func (r recordingPersistable) Restore(_ context.Context, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.got[r.name] = data

	return nil
}

func TestManager_SaveRestoreAllCompat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		services int
		preWrite bool
	}{
		{name: "round_trip_new_code", services: 20},
		{name: "loads_baseline_layout", services: 20, preWrite: true},
		{name: "single_service", services: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			base := t.TempDir()
			got := map[string][]byte{}
			mu := &sync.Mutex{}

			fs, err := persistence.NewFileStore(base)
			require.NoError(t, err)

			m := persistence.NewManager(ctx, fs)
			want := map[string][]byte{}

			for i := range tt.services {
				name := "svc" + strconv.Itoa(i)
				data := []byte(`{"version":1,"n":` + strconv.Itoa(i) + `}`)
				want[name] = data
				m.Register(name, recordingPersistable{name: name, data: data, got: got, mu: mu})

				if tt.preWrite {
					dir := filepath.Join(base, name)
					require.NoError(t, os.MkdirAll(dir, 0o700))
					require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json"), data, 0o600))
				}
			}

			if !tt.preWrite {
				m.SaveAll(ctx)

				for name, data := range want {
					onDisk, readErr := os.ReadFile(filepath.Join(base, name, "snapshot.json"))
					require.NoError(t, readErr)
					assert.Equal(t, data, onDisk)
				}
			}

			m.RestoreAll(ctx)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, want, got)
		})
	}
}

func TestNewFileStore_RemovesStaleTemps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		file      string
		age       time.Duration
		wantExist bool
	}{
		{name: "stale_temp_removed", file: ".tmp-abc", age: 2 * time.Hour},
		{name: "fresh_temp_kept", file: ".tmp-abc", age: time.Minute, wantExist: true},
		{name: "stale_snapshot_kept", file: "snapshot.json", age: 2 * time.Hour, wantExist: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := t.TempDir()
			dir := filepath.Join(base, "svc")
			require.NoError(t, os.MkdirAll(dir, 0o700))

			p := filepath.Join(dir, tt.file)
			require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))

			mtime := time.Now().Add(-tt.age)
			require.NoError(t, os.Chtimes(p, mtime, mtime))

			_, err := persistence.NewFileStore(base)
			require.NoError(t, err)

			_, statErr := os.Stat(p)
			assert.Equal(t, tt.wantExist, statErr == nil)
		})
	}
}
