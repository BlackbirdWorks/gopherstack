package ssm_test

import (
	"bytes"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestDockerShellOpener(t *testing.T) {
	t.Parallel()

	const image = "alpine:3.21"

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}

	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		t.Skipf("image %s not present", image)
	}

	out, err := exec.CommandContext(t.Context(), "docker", "run", "-d", image, "sleep", "120").Output()
	require.NoError(t, err)

	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })

	opener, err := ssm.NewDockerShellOpener()
	require.NoError(t, err)

	closer, ok := opener.(io.Closer)
	require.True(t, ok)
	t.Cleanup(func() { _ = closer.Close() })

	sh, err := opener.OpenShell(t.Context(), id, 80, 24)
	require.NoError(t, err)

	defer sh.Close()

	_, err = sh.Write([]byte("echo gs-$((20+22))\n"))
	require.NoError(t, err)
	require.NoError(t, sh.Resize(t.Context(), 120, 40))

	var seen bytes.Buffer

	buf := make([]byte, 256)

	for !strings.Contains(strings.ReplaceAll(seen.String(), "echo gs-$((20+22))", ""), "gs-42") {
		n, rerr := sh.Read(buf)
		seen.Write(buf[:n])
		require.NoError(t, rerr)
	}
}
