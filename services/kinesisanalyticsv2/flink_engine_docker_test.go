package kinesisanalyticsv2_test

import (
	"bytes"
	"context"
	"os/exec"
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

func TestFlinkEngine_RealFlinkDocker(t *testing.T) {
	t.Parallel()

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

	var jar bytes.Buffer

	cmd := exec.CommandContext(pctx, "docker", "run", "--rm", "--entrypoint", "cat", dockerFlinkImage, dockerFlinkJar)
	cmd.Stdout = &jar

	if err = cmd.Run(); err != nil {
		t.Skipf("cannot read example jar: %v", err)
	}

	b := kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableFlink(kinesisanalyticsv2.FlinkConfig{
		Runtime:      rt,
		PollInterval: time.Second,
		StartTimeout: 4 * time.Minute,
		Code: func(context.Context, string, string, string) ([]byte, error) {
			return jar.Bytes(), nil
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
