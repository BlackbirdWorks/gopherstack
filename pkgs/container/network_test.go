package container_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type networkAPI struct {
	created   []string
	connected []string
	removed   []string
	mockAPI
}

func (n *networkAPI) NetworkCreate(_ context.Context, name string) (string, error) {
	n.created = append(n.created, name)

	return "net-" + name, nil
}

func (n *networkAPI) NetworkConnect(_ context.Context, networkID, containerID string) error {
	n.connected = append(n.connected, networkID+"/"+containerID)

	return nil
}

func (n *networkAPI) NetworkRemove(_ context.Context, networkID string) error {
	n.removed = append(n.removed, networkID)

	return nil
}

func TestDockerRuntime_Networks(t *testing.T) {
	t.Parallel()

	api := &networkAPI{}
	rt := newRuntime(api)

	id, err := rt.CreateNetwork(t.Context(), "n1")
	require.NoError(t, err)
	assert.Equal(t, "net-n1", id)
	require.NoError(t, rt.ConnectNetwork(t.Context(), id, "c1"))
	require.NoError(t, rt.RemoveNetwork(t.Context(), id))

	assert.Equal(t, []string{"net-n1/c1"}, api.connected)
	assert.Equal(t, []string{"net-n1"}, api.removed)
}

func TestDockerRuntime_NetworksUnsupported(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})

	_, err := rt.CreateNetwork(t.Context(), "n1")
	require.ErrorIs(t, err, container.ErrNetworkUnsupported)
	require.ErrorIs(t, rt.ConnectNetwork(t.Context(), "n", "c"), container.ErrNetworkUnsupported)
	require.ErrorIs(t, rt.RemoveNetwork(t.Context(), "n"), container.ErrNetworkUnsupported)
}

func TestDockerRuntime_SpecNetworkJoinsNetworkMode(t *testing.T) {
	t.Parallel()

	api := &mockAPI{}
	_, err := newRuntime(api).CreateAndStart(t.Context(), container.Spec{Image: "alpine", Network: "repl-net"})
	require.NoError(t, err)
	assert.Equal(t, "repl-net", string(api.lastHost.NetworkMode))
}
