package container_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errInspectFailed = errors.New("inspect failed")

type digestAPI struct {
	*mockAPI

	err    error
	digest string
}

func (d *digestAPI) ImageDigest(_ context.Context, _ string) (string, error) { return d.digest, d.err }

func TestDockerRuntime_ImageDigest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		api        container.APIClient
		wantErr    error
		name       string
		wantDigest string
	}{
		{name: "digest", api: &digestAPI{mockAPI: &mockAPI{}, digest: "sha256:abc"}, wantDigest: "sha256:abc"},
		{name: "inspect failure", api: &digestAPI{mockAPI: &mockAPI{}, err: errInspectFailed}, wantErr: errInspectFailed},
		{name: "client cannot inspect", api: &mockAPI{}, wantErr: container.ErrPushUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := container.NewDockerRuntimeWithAPI(tt.api, container.Config{PoolSize: 1})

			digest, err := rt.ImageDigest(t.Context(), "img:1")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDigest, digest)
		})
	}
}
