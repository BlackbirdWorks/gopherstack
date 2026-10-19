package container_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestProbeUnknownRuntime(t *testing.T) {
	t.Parallel()

	_, err := container.Probe(context.Background(), container.Config{Runtime: "nope"})
	require.ErrorIs(t, err, container.ErrUnknownRuntime)
}
