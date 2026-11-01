// Package helm_test checks the charts/gopherstack Helm chart: lint and golden rendering, plus an install into a real
// k3s cluster. Both need docker and run only with GOPHERSTACK_TEST_DOCKER=1.
package helm_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	// helmImage is the pinned tool image; helm and kubectl are not installed on the host.
	helmImage    = "alpine/helm@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
	dockerOptIn  = "GOPHERSTACK_TEST_DOCKER"
	updateGolden = "GOPHERSTACK_UPDATE_GOLDEN"
)

func requireDocker(t *testing.T) {
	t.Helper()

	if os.Getenv(dockerOptIn) != "1" {
		t.Skipf("set %s=1 to run helm chart tests", dockerOptIn)
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	if err := exec.CommandContext(t.Context(), "docker", "info").Run(); err != nil {
		t.Skipf("docker daemon unreachable: %v", err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Join(filepath.Dir(file), "..", "..")
}

func chartDir(t *testing.T) string {
	t.Helper()

	return filepath.Join(repoRoot(t), "charts", "gopherstack")
}

// helm runs the pinned helm image with the chart mounted read-only at /chart and extra bind mounts.
func helm(ctx context.Context, t *testing.T, dockerArgs []string, args ...string) (string, string, error) {
	t.Helper()

	cmd := []string{"run", "--rm", "-v", chartDir(t) + ":/chart:ro"}
	cmd = append(cmd, dockerArgs...)
	cmd = append(cmd, helmImage)
	cmd = append(cmd, args...)

	var stdout, stderr bytes.Buffer

	c := exec.CommandContext(ctx, "docker", cmd...)
	c.Stdout, c.Stderr = &stdout, &stderr
	err := c.Run()

	return stdout.String(), stderr.String(), err
}
