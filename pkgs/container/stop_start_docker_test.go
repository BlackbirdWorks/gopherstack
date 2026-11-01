package container_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestDockerRuntime_StopStartKeepsFilesystem(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	const image = "busybox:1.36"
	if ok, hasErr := rt.HasImage(t.Context(), image); hasErr != nil || !ok {
		t.Skipf("image %s not present locally", image)
	}

	id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: image, Cmd: []string{"sleep", "300"}})
	require.NoError(t, err)

	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-fv", id).Run() })

	res, err := rt.Exec(t.Context(), id, []string{"sh", "-c", "echo kept > /state.txt"})
	require.NoError(t, err)
	require.Zero(t, res.ExitCode)

	require.NoError(t, rt.StopContainer(t.Context(), id))

	out, err := exec.CommandContext(t.Context(), "docker", "inspect", "-f", "{{.State.Running}}", id).Output()
	require.NoError(t, err)
	assert.Equal(t, "false", strings.TrimSpace(string(out)))

	require.NoError(t, rt.StartContainer(t.Context(), id))

	res, err = rt.Exec(t.Context(), id, []string{"cat", "/state.txt"})
	require.NoError(t, err)
	assert.Equal(t, "kept", strings.TrimSpace(res.Stdout))
}
