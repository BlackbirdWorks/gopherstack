package kinesisanalyticsv2_test

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

const (
	dockerFlinkImage = "flink:1.18"
	dockerFlinkJar   = "/opt/flink/examples/streaming/TopSpeedWindowing.jar"
)

func newDockerRuntime(t *testing.T) container.Runtime {
	t.Helper()

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	pctx, pcancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer pcancel()

	if err = rt.Ping(pctx); err != nil {
		t.Skipf("container runtime not reachable: %v", err)
	}

	if ok, herr := rt.HasImage(pctx, dockerFlinkImage); herr != nil || !ok {
		t.Skipf("image %s not present", dockerFlinkImage)
	}

	return rt
}

func dockerFlinkJarBytes(t *testing.T) []byte {
	t.Helper()

	var jar bytes.Buffer

	cmd := exec.CommandContext(t.Context(), "docker", "run", "--rm", "--entrypoint", "cat",
		dockerFlinkImage, dockerFlinkJar)
	cmd.Stdout = &jar

	if err := cmd.Run(); err != nil {
		t.Skipf("cannot read example jar: %v", err)
	}

	return jar.Bytes()
}

func TestFlinkEngine_RealFlinkDocker(t *testing.T) {
	t.Parallel()

	rt := newDockerRuntime(t)
	jar := dockerFlinkJarBytes(t)
	var err error

	b := kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableFlink(kinesisanalyticsv2.FlinkConfig{
		Runtime:      rt,
		PollInterval: time.Second,
		StartTimeout: 4 * time.Minute,
		Code: func(context.Context, string, string, string) ([]byte, error) {
			return jar, nil
		},
	})
	t.Cleanup(b.Close)

	h := &flinkHarness{b: b}
	h.createApp(t, "docker-app", true)

	ctx := context.Background()
	_, err = b.StartApplication(ctx, "docker-app", nil, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return h.status(t, "docker-app") == "RUNNING" },
		5*time.Minute, time.Second)

	_, err = b.CreateApplicationSnapshot(ctx, "docker-app", "real-snap")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		s, derr := b.DescribeApplicationSnapshot(ctx, "docker-app", "real-snap")

		return derr == nil && s.SnapshotStatus == "READY"
	}, 2*time.Minute, time.Second)

	_, err = b.StopApplication(ctx, "docker-app", false)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return h.status(t, "docker-app") == "READY" },
		3*time.Minute, time.Second)

	snaps, _, err := b.ListApplicationSnapshots(ctx, "docker-app", "", 0)
	require.NoError(t, err)
	assert.Len(t, snaps, 1, "the stop savepoint is internal, not listed")

	_, err = b.StartApplication(ctx, "docker-app", nil, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return h.status(t, "docker-app") == "RUNNING" },
		5*time.Minute, time.Second)
}

// chmodStateDir lets the host remove savepoint files written by the container's flink user.
func chmodStateDir(t *testing.T, dir string) {
	t.Helper()

	t.Cleanup(func() {
		cmd := exec.CommandContext(context.Background(), "docker", "run", "--rm", "-v", dir+":/d",
			"--entrypoint", "chmod", dockerFlinkImage, "-R", "a+rwX", "/d")
		_ = cmd.Run()
	})
}

func newDockerPersistBackend(t *testing.T, dataDir string) *kinesisanalyticsv2.InMemoryBackend {
	t.Helper()

	b := kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableFlink(kinesisanalyticsv2.FlinkConfig{
		Runtime: newDockerRuntime(t), DataDir: dataDir,
		PollInterval: time.Second, StartTimeout: 4 * time.Minute,
	})
	t.Cleanup(b.Close)

	return b
}

func TestFlinkEngine_RealFlinkDockerRestartRestores(t *testing.T) {
	t.Parallel()

	jar := dockerFlinkJarBytes(t)
	dataDir := filepath.Join(t.TempDir(), "flink")
	chmodStateDir(t, dataDir)

	ctx := context.Background()
	b1 := newDockerPersistBackend(t, dataDir)
	h1 := &flinkHarness{b: b1}
	h1.createZipApp(t, "restart-app")
	kinesisanalyticsv2.SetZipCodeForTest(b1, "restart-app", jar)

	_, err := b1.StartApplication(ctx, "restart-app", nil, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return h1.status(t, "restart-app") == "RUNNING" },
		5*time.Minute, time.Second)

	h1.snapshotReady(t, "restart-app", "real-snap")

	saved, err := filepath.Glob(filepath.Join(dataDir, "savepoints", "*", "savepoint-*", "_metadata"))
	require.NoError(t, err)
	require.Len(t, saved, 1, "the savepoint lands on the host through the bind mount")

	data := b1.Snapshot(ctx)
	require.NotEmpty(t, data)
	b1.Close()

	b2 := newDockerPersistBackend(t, dataDir)
	h2 := &flinkHarness{b: b2}
	require.NoError(t, b2.Restore(ctx, data))
	require.Eventually(t, func() bool { return h2.status(t, "restart-app") == "RUNNING" },
		5*time.Minute, time.Second, "a RUNNING app relaunches from its savepoint after restart")

	_, err = b2.StopApplication(ctx, "restart-app", true)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return h2.status(t, "restart-app") == "READY" },
		3*time.Minute, time.Second)

	_, err = b2.StartApplication(ctx, "restart-app", &kinesisanalyticsv2.RunConfigInput{
		ApplicationRestoreConfiguration: &kinesisanalyticsv2.ApplicationRestoreConfig{
			ApplicationRestoreType: "RESTORE_FROM_LATEST_SNAPSHOT",
		},
	}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return h2.status(t, "restart-app") == "RUNNING" },
		5*time.Minute, time.Second)
}
