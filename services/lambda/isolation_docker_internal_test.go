package lambda

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const gvisorTestImage = "public.ecr.aws/docker/library/alpine:3.20"

func TestGVisorRuntime_RealDaemon(t *testing.T) {
	t.Parallel()

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	name, err := container.ResolveOCIRuntime(t.Context(), rt, "runsc")
	if err != nil {
		t.Skipf("runsc is not registered with the daemon: %v", err)
	}

	id, err := rt.CreateAndStart(t.Context(), container.Spec{
		Image:      gvisorTestImage,
		Cmd:        []string{"sleep", "30"},
		OCIRuntime: name,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.StopAndRemove(context.Background(), id) })

	res, err := rt.Exec(t.Context(), id, []string{"dmesg"})
	require.NoError(t, err)
	require.Contains(t, res.Stdout+res.Stderr, "gVisor", "container is not running under runsc")
}
