package container_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestDockerRuntime_SpecStopTimeoutAndReadOnlyRootfs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		grace       *int
		name        string
		wantTimeout int
		readOnly    bool
	}{
		{name: "default", grace: nil, wantTimeout: 10},
		{name: "explicit", grace: new(45), wantTimeout: 45, readOnly: true},
		{name: "zero", grace: new(0), wantTimeout: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &graceAPI{}
			rt := newRuntime(api)

			id, err := rt.CreateAndStart(t.Context(), container.Spec{
				Image: "alpine", StopTimeoutSecs: tt.grace, ReadOnlyRootfs: tt.readOnly,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.readOnly, api.lastHost.ReadonlyRootfs)

			require.NoError(t, rt.StopAndRemove(t.Context(), id))
			require.NotNil(t, api.gotTimeout)
			assert.Equal(t, tt.wantTimeout, *api.gotTimeout)
		})
	}
}

func TestDockerRuntime_StopAndRemoveWithinForgetsGrace(t *testing.T) {
	t.Parallel()

	api := &graceAPI{}
	rt := newRuntime(api)

	id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine", StopTimeoutSecs: new(45)})
	require.NoError(t, err)

	_, err = rt.StopAndRemoveWithin(t.Context(), id, time.Second)
	require.NoError(t, err)

	require.NoError(t, rt.StopContainer(t.Context(), id))
	require.NotNil(t, api.gotTimeout)
	assert.Equal(t, 10, *api.gotTimeout, "the removed container's remembered grace period is dropped")
}
