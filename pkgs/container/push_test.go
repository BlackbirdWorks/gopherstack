package container_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var (
	errTagFailed  = errors.New("tag failed")
	errPushFailed = errors.New("push failed")
)

type pushAPI struct {
	tagErr  error
	pushErr error
	digest  string
	tagged  []string
	pushed  []string
	mockAPI
}

func (p *pushAPI) ImageTag(_ context.Context, source, target string) error {
	p.tagged = append(p.tagged, source+">"+target)

	return p.tagErr
}

func (p *pushAPI) ImagePush(_ context.Context, ref string) (string, error) {
	p.pushed = append(p.pushed, ref)

	return p.digest, p.pushErr
}

func TestDockerRuntime_TagAndPush(t *testing.T) {
	t.Parallel()

	tests := []struct {
		api        func() container.APIClient
		wantErr    error
		name       string
		wantDigest string
	}{
		{
			name: "pushes the tagged image",
			api: func() container.APIClient {
				return &pushAPI{digest: "sha256:abc"}
			},
			wantDigest: "sha256:abc",
		},
		{
			name:    "tag failure",
			api:     func() container.APIClient { return &pushAPI{tagErr: errTagFailed} },
			wantErr: errTagFailed,
		},
		{
			name:    "push failure",
			api:     func() container.APIClient { return &pushAPI{pushErr: errPushFailed} },
			wantErr: errPushFailed,
		},
		{
			name:    "client cannot push",
			api:     func() container.APIClient { return &mockAPI{} },
			wantErr: container.ErrPushUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := tt.api()
			rt := container.NewDockerRuntimeWithAPI(api, container.Config{PoolSize: 1})

			digest, err := rt.TagAndPush(t.Context(), "src:1", "localhost:5000/dst:1")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDigest, digest)

			p, ok := api.(*pushAPI)
			require.True(t, ok)
			assert.Equal(t, []string{"src:1>localhost:5000/dst:1"}, p.tagged)
			assert.Equal(t, []string{"localhost:5000/dst:1"}, p.pushed)
		})
	}
}

func TestDockerRuntime_WorkingDir(t *testing.T) {
	t.Parallel()

	api := &mockAPI{images: nil}
	rt := container.NewDockerRuntimeWithAPI(api, container.Config{PoolSize: 1})

	_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "img", WorkingDir: "/workspace/sub"})
	require.NoError(t, err)

	api.mu.Lock()
	defer api.mu.Unlock()

	assert.Equal(t, "/workspace/sub", api.lastCfg.WorkingDir)
}
