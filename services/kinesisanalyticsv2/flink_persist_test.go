package kinesisanalyticsv2_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

const (
	persistRole  = "arn:aws:iam::000000000000:role/flink-role"
	persistAKID  = "ASIAFLINKTEST"
	persistStop  = "file:/opt/flink/savepoints/stop-1"
	persistZip   = "zip-jar-bytes"
	persistToken = "tok-xyz"
)

var errFlinkTestDenied = errors.New("denied")

type fakeIssuer struct {
	err   error
	roles []string
}

func (f *fakeIssuer) IssueApplicationCredentials(role, _ string) (kinesisanalyticsv2.RoleCredentials, error) {
	f.roles = append(f.roles, role)

	if f.err != nil {
		return kinesisanalyticsv2.RoleCredentials{}, f.err
	}

	return kinesisanalyticsv2.RoleCredentials{
		AccessKeyID: persistAKID, SecretAccessKey: "secret", SessionToken: persistToken,
		Expires: time.Now().Add(time.Hour),
	}, nil
}

func newPersistHarness(t *testing.T, dataDir string, issuer kinesisanalyticsv2.RoleCredentialIssuer) *flinkHarness {
	t.Helper()

	h := &flinkHarness{
		b:  kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"),
		rt: newFakeFlinkRuntime(),
		fl: newFakeFlinkREST(),
	}

	if issuer != nil {
		h.b.SetRoleCredentialIssuer(issuer)
	}

	h.b.EnableFlink(kinesisanalyticsv2.FlinkConfig{
		Runtime: h.rt, HTTP: h.fl, DataDir: dataDir,
		PollInterval: 2 * time.Millisecond, StartTimeout: 10 * time.Second, StopTimeout: 10 * time.Second,
	})
	t.Cleanup(h.b.Close)

	return h
}

func (h *flinkHarness) createZipApp(t *testing.T, name string) {
	t.Helper()

	_, err := h.b.CreateApplication(context.Background(), name, "FLINK-1_18", persistRole, "", "", nil)
	require.NoError(t, err)

	enabled := true
	require.NoError(t, h.b.SeedApplicationConfiguration(context.Background(), name, kinesisanalyticsv2.SeedConfig{
		SnapshotsEnabled: &enabled,
	}))
	kinesisanalyticsv2.SetZipCodeForTest(h.b, name, []byte(persistZip))
}

func (h *flinkHarness) startRunning(t *testing.T) {
	t.Helper()

	h.fl.setState("RUNNING")

	_, err := h.b.StartApplication(context.Background(), "app", nil, nil)
	require.NoError(t, err)
	h.waitStatus(t, "app", "RUNNING")
}

// stateDir is the host side of the savepoint bind mount of the newest container.
func (h *flinkHarness) stateDir(t *testing.T) string {
	t.Helper()

	specs, _, _ := h.rt.snapshot()
	require.NotEmpty(t, specs)

	m := specs[len(specs)-1].Mounts[1]
	host, ctr, ok := strings.Cut(m, ":")
	require.True(t, ok)
	assert.Equal(t, "/opt/flink/savepoints", ctr)

	return host
}

func (h *flinkHarness) snapshotReady(t *testing.T, app, snap string) {
	t.Helper()

	ctx := context.Background()
	_, err := h.b.CreateApplicationSnapshot(ctx, app, snap)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		s, derr := h.b.DescribeApplicationSnapshot(ctx, app, snap)

		return derr == nil && s.SnapshotStatus == "READY"
	}, 10*time.Second, 2*time.Millisecond)
}

func touchSavepoint(t *testing.T, stateDir, loc string) {
	t.Helper()

	dir := filepath.Join(stateDir, filepath.Base(loc))
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "_metadata"), []byte("m"), 0o600))
}

func TestFlinkPersist_LayoutAndRoleCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		issuer     *fakeIssuer
		name       string
		persist    bool
		wantTokens bool
	}{
		{name: "persist with role creds", persist: true, issuer: &fakeIssuer{}, wantTokens: true},
		{name: "temp dir static creds", persist: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dataDir := ""
			if tt.persist {
				dataDir = filepath.Join(t.TempDir(), "flink")
			}

			var issuer kinesisanalyticsv2.RoleCredentialIssuer
			if tt.issuer != nil {
				issuer = tt.issuer
			}

			h := newPersistHarness(t, dataDir, issuer)
			h.createZipApp(t, "app")
			h.startRunning(t)

			specs, _, _ := h.rt.snapshot()
			require.Len(t, specs, 1)
			env := strings.Join(specs[0].Env, "|")
			state := h.stateDir(t)

			if tt.persist {
				root := filepath.Join(dataDir, "savepoints") + string(filepath.Separator)
				assert.True(t, strings.HasPrefix(state, root))

				info, err := os.Stat(filepath.Join(dataDir, "savepoints"))
				require.NoError(t, err)
				assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
			} else {
				assert.Contains(t, filepath.Base(state), "kda-flink-state-")
			}

			assert.Equal(t, tt.wantTokens, strings.Contains(env, "AWS_ACCESS_KEY_ID="+persistAKID))
			assert.Equal(t, tt.wantTokens, strings.Contains(env, "AWS_SESSION_TOKEN="+persistToken))
			assert.Equal(t, !tt.wantTokens, strings.Contains(env, "AWS_ACCESS_KEY_ID=test"))

			if tt.issuer != nil {
				assert.Equal(t, []string{persistRole}, tt.issuer.roles)
			}

			assert.Equal(t, []byte(persistZip), h.fl.jar)
		})
	}
}

func TestFlinkPersist_RoleCredentialFailureFailsStart(t *testing.T) {
	t.Parallel()

	h := newPersistHarness(t, "", &fakeIssuer{err: errFlinkTestDenied})
	h.createZipApp(t, "app")

	opID, err := h.b.StartApplication(context.Background(), "app", nil, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return h.status(t, "app") == "READY" && h.opStatus(t, "app", opID) == "FAILED"
	}, 10*time.Second, 2*time.Millisecond)

	specs, _, _ := h.rt.snapshot()
	assert.Empty(t, specs)
}

func TestFlinkPersist_RestartRelaunchesFromSavepoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		dropSavepoint bool
		wantRestore   bool
	}{
		{name: "restores from latest snapshot", wantRestore: true},
		{name: "missing savepoint files start fresh", dropSavepoint: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dataDir := filepath.Join(t.TempDir(), "flink")
			h1 := newPersistHarness(t, dataDir, &fakeIssuer{})
			h1.createZipApp(t, "app")
			h1.startRunning(t)
			touchSavepoint(t, h1.stateDir(t), flinkSavepoint)
			h1.snapshotReady(t, "app", "snap1")

			data := h1.b.Snapshot(context.Background())
			require.NotEmpty(t, data)

			codes, err := filepath.Glob(filepath.Join(dataDir, "code", "*.zip"))
			require.NoError(t, err)
			require.Len(t, codes, 1)

			info, err := os.Stat(codes[0])
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

			h1.b.Close()

			if tt.dropSavepoint {
				require.NoError(t, os.RemoveAll(filepath.Join(h1.stateDir(t), filepath.Base(flinkSavepoint))))
			}

			h2 := newPersistHarness(t, dataDir, &fakeIssuer{})
			h2.fl.setState("RUNNING")
			require.NoError(t, h2.b.Restore(context.Background(), data))
			h2.waitStatus(t, "app", "RUNNING")

			runs := h2.fl.runRequests()
			require.Len(t, runs, 1)
			assert.Equal(t, []byte(persistZip), h2.fl.jar)

			if tt.wantRestore {
				assert.Equal(t, flinkSavepoint, runs[0]["savepointPath"])
			} else {
				assert.NotContains(t, runs[0], "savepointPath")
			}
		})
	}
}

func TestFlinkPersist_ReadyAppKeepsSnapshotsAndStopSavepoint(t *testing.T) {
	t.Parallel()

	dataDir := filepath.Join(t.TempDir(), "flink")
	ctx := context.Background()

	h1 := newPersistHarness(t, dataDir, nil)
	h1.createZipApp(t, "app")
	h1.startRunning(t)
	touchSavepoint(t, h1.stateDir(t), flinkSavepoint)
	h1.snapshotReady(t, "app", "snap1")

	h1.fl.savepath = persistStop
	touchSavepoint(t, h1.stateDir(t), persistStop)

	_, err := h1.b.StopApplication(ctx, "app", false)
	require.NoError(t, err)
	h1.waitStatus(t, "app", "READY")

	data := h1.b.Snapshot(ctx)
	h1.b.Close()

	tests := []struct {
		rc   *kinesisanalyticsv2.RunConfigInput
		name string
		want string
	}{
		{name: "default restores the stop savepoint", want: persistStop},
		{
			name: "custom snapshot restores its savepoint", want: flinkSavepoint,
			rc: &kinesisanalyticsv2.RunConfigInput{
				ApplicationRestoreConfiguration: &kinesisanalyticsv2.ApplicationRestoreConfig{
					ApplicationRestoreType: "RESTORE_FROM_CUSTOM_SNAPSHOT", SnapshotName: "snap1",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newPersistHarness(t, dataDir, nil)
			require.NoError(t, h2.b.Restore(ctx, data))
			assert.Equal(t, "READY", h2.status(t, "app"))

			s, derr := h2.b.DescribeApplicationSnapshot(ctx, "app", "snap1")
			require.NoError(t, derr)
			assert.Equal(t, "READY", s.SnapshotStatus)

			h2.fl.setState("RUNNING")

			_, serr := h2.b.StartApplication(ctx, "app", tt.rc, nil)
			require.NoError(t, serr)
			h2.waitStatus(t, "app", "RUNNING")

			assert.Equal(t, tt.want, h2.fl.runRequests()[0]["savepointPath"])
		})
	}
}

func TestFlinkPersist_DeleteRemovesFiles(t *testing.T) {
	t.Parallel()

	dataDir := filepath.Join(t.TempDir(), "flink")
	ctx := context.Background()

	h := newPersistHarness(t, dataDir, nil)
	h.createZipApp(t, "app")
	h.startRunning(t)

	state := h.stateDir(t)
	touchSavepoint(t, state, flinkSavepoint)
	h.snapshotReady(t, "app", "snap1")

	_, err := h.b.StopApplication(ctx, "app", true)
	require.NoError(t, err)
	h.waitStatus(t, "app", "READY")

	require.NotEmpty(t, h.b.Snapshot(ctx))

	savepoint := filepath.Join(state, filepath.Base(flinkSavepoint))
	require.DirExists(t, savepoint)
	require.NoError(t, h.b.DeleteApplicationSnapshot(ctx, "app", "snap1"))
	assert.NoDirExists(t, savepoint)

	require.NoError(t, h.b.DeleteApplication(ctx, "app", nil))
	assert.NoDirExists(t, state)

	codes, err := filepath.Glob(filepath.Join(dataDir, "code", "*.zip"))
	require.NoError(t, err)
	assert.Empty(t, codes)
}
