package timestreaminfluxdb_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

func newDockerEngine(t *testing.T, rt *fakeRuntime, lo, hi int) *timestreaminfluxdb.DockerEngine {
	t.Helper()

	ports, err := portalloc.New(lo, hi)
	require.NoError(t, err)

	return &timestreaminfluxdb.DockerEngine{
		Runtime: rt, Ports: ports, HTTP: &fakeHealth{}, PollInterval: time.Millisecond,
	}
}

func volumeSpec(t *testing.T) timestreaminfluxdb.EngineSpec {
	t.Helper()

	root := t.TempDir()

	return timestreaminfluxdb.EngineSpec{
		Name: "vol", Username: "u", Password: "Passw0rd", Organization: "o", Bucket: "bk", Token: "tok",
		DataDir: filepath.Join(root, "data"), BackupsDir: filepath.Join(root, "backups"),
		Env: []string{"INFLUXD_LOG_LEVEL=debug"},
	}
}

func TestDockerEngineVolumesAndRestart(t *testing.T) {
	t.Parallel()

	t.Run("start mounts the data and backup directories", func(t *testing.T) {
		t.Parallel()

		rt := &fakeRuntime{hasImage: true}
		spec := volumeSpec(t)

		_, err := newDockerEngine(t, rt, 31000, 31010).Start(t.Context(), spec)
		require.NoError(t, err)

		require.Len(t, rt.specs, 1)
		assert.Equal(t, []string{
			spec.DataDir + ":/var/lib/influxdb2", spec.BackupsDir + ":/backups",
		}, rt.specs[0].Mounts)
		assert.Contains(t, rt.specs[0].Env, "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=tok")
		assert.Contains(t, rt.specs[0].Env, "INFLUXD_LOG_LEVEL=debug")
		assert.NotEmpty(t, rt.specs[0].User, "the server runs as the emulator's user")
	})

	t.Run("restart keeps the container and its port", func(t *testing.T) {
		t.Parallel()

		rt := &fakeRuntime{hasImage: true}
		eng := newDockerEngine(t, rt, 31020, 31030)

		h, err := eng.Start(t.Context(), volumeSpec(t))
		require.NoError(t, err)

		got, err := eng.Restart(t.Context(), h)
		require.NoError(t, err)
		assert.Equal(t, h, got)
		assert.Equal(t, []string{"stop:ctr-1", "start:ctr-1"}, rt.calls)
		assert.Empty(t, rt.removed, "a restart must not remove the container")
		assert.Len(t, rt.specs, 1)
	})

	t.Run("recreate reuses the port and volumes with new settings", func(t *testing.T) {
		t.Parallel()

		rt := &fakeRuntime{hasImage: true}
		eng := newDockerEngine(t, rt, 31040, 31050)
		spec := volumeSpec(t)

		h, err := eng.Start(t.Context(), spec)
		require.NoError(t, err)

		spec.Env = []string{"INFLUXD_LOG_LEVEL=error"}
		got, err := eng.Recreate(t.Context(), h, spec)
		require.NoError(t, err)

		assert.Equal(t, h.Port, got.Port)
		assert.Equal(t, "ctr-2", got.ContainerID)
		assert.Equal(t, []string{"ctr-1"}, rt.removed)
		assert.Equal(t, rt.specs[0].Mounts, rt.specs[1].Mounts)
		assert.Contains(t, rt.specs[1].Env, "INFLUXD_LOG_LEVEL=error")
		assert.Equal(t, rt.specs[0].Ports, rt.specs[1].Ports)
	})

	t.Run("recreate for a restore clears the data and replays the backup", func(t *testing.T) {
		t.Parallel()

		rt := &fakeRuntime{hasImage: true}
		eng := newDockerEngine(t, rt, 31060, 31070)
		spec := volumeSpec(t)

		h, err := eng.Start(t.Context(), spec)
		require.NoError(t, err)

		require.NoError(t, os.MkdirAll(spec.DataDir, 0o750))

		stale := filepath.Join(spec.DataDir, "stale")
		require.NoError(t, os.WriteFile(stale, []byte("x"), 0o600))

		spec.RestoreFrom = "bk-1"
		_, err = eng.Recreate(t.Context(), h, spec)
		require.NoError(t, err)

		assert.NoFileExists(t, stale)
		assert.DirExists(t, spec.DataDir)
		assert.Contains(t, rt.specs[1].Env, "DOCKER_INFLUXDB_INIT_MODE=setup")
		require.Len(t, rt.execs, 1)
		assert.Equal(
			t,
			"ctr-2 influx restore --full --host http://127.0.0.1:8086 --token tok /backups/bk-1",
			rt.execs[0],
		)
	})
}

func TestDockerEngineBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		exit    int
		wantErr bool
	}{
		{name: "runs influx backup in the backups directory"},
		{name: "a failing command is an error", exit: 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeRuntime{hasImage: true, execExit: tt.exit}
			eng := newDockerEngine(t, rt, 31080, 31090)
			h := timestreaminfluxdb.EngineHandle{ContainerID: "ctr-x"}

			err := eng.Backup(t.Context(), h, "tok", "bk-9")
			require.Len(t, rt.execs, 1)
			assert.Equal(t, "ctr-x influx backup --host http://127.0.0.1:8086 --token tok /backups/bk-9", rt.execs[0])

			if tt.wantErr {
				require.ErrorIs(t, err, timestreaminfluxdb.ErrExecFailed)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestParameterEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "scalars and durations",
			in: `{"InfluxDBv2":{"logLevel":"debug","noTasks":true,"queryConcurrency":10,
				"httpIdleTimeout":{"durationType":"seconds","value":30},
				"storageRetentionCheckInterval":{"durationType":"hours","value":1},
				"storageWalMaxWriteDelay":{"durationType":"milliseconds","value":500},
				"storageCacheSnapshotWriteColdDuration":{"durationType":"days","value":1}}}`,
			want: []string{
				"INFLUXD_HTTP_IDLE_TIMEOUT=30s", "INFLUXD_LOG_LEVEL=debug", "INFLUXD_NO_TASKS=true",
				"INFLUXD_QUERY_CONCURRENCY=10", "INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=24h0m0s",
				"INFLUXD_STORAGE_RETENTION_CHECK_INTERVAL=1h0m0s", "INFLUXD_STORAGE_WAL_MAX_WRITE_DELAY=500ms",
			},
		},
		{name: "v3 groups are stored only", in: `{"InfluxDBv3Core":{"logFilter":"x"}}`},
		{name: "empty", in: ``},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, timestreaminfluxdb.ParameterEnv(json.RawMessage(tt.in)))
		})
	}
}
