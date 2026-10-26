package container_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type authRecordingAPI struct {
	*mockAPI
	auth string
	ref  string
}

func (a *authRecordingAPI) ImagePull(_ context.Context, ref string, opts image.PullOptions) (io.ReadCloser, error) {
	a.auth, a.ref = opts.RegistryAuth, ref

	return io.NopCloser(strings.NewReader("")), nil
}

func TestPullImageWithCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ref        string
		wantServer string
	}{
		{"private-registry", "myreg.example.com:5000/team/app:1", "myreg.example.com:5000"},
		{"hub-image", "library/busybox:1", "docker.io"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			api := &authRecordingAPI{mockAPI: &mockAPI{}}
			rt := newRuntime(api)

			require.NoError(t, rt.PullImageWithCredentials(t.Context(), tc.ref, "puller", "s3cret"))
			assert.Equal(t, tc.ref, api.ref)

			raw, err := base64.URLEncoding.DecodeString(api.auth)
			require.NoError(t, err)

			var got map[string]string

			require.NoError(t, json.Unmarshal(raw, &got))
			assert.Equal(t, "puller", got["username"])
			assert.Equal(t, "s3cret", got["password"])
			assert.Contains(t, got["serveraddress"], strings.TrimPrefix(tc.wantServer, "docker.io"))
		})
	}
}

func TestCreateAndStartAppliesResourceLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     container.Spec
		wantMem  int64
		wantNano int64
	}{
		{"limited", container.Spec{Image: "busybox", MemoryBytes: 512 << 20, NanoCPUs: 1_500_000_000}, 512 << 20, 1_500_000_000},
		{"unlimited", container.Spec{Image: "busybox"}, 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			api := &mockAPI{images: []image.Summary{{}}}
			rt := newRuntime(api)

			_, err := rt.CreateAndStart(t.Context(), tc.spec)
			require.NoError(t, err)

			api.mu.Lock()
			defer api.mu.Unlock()

			assert.Equal(t, tc.wantMem, api.lastHost.Memory)
			assert.Equal(t, tc.wantNano, api.lastHost.NanoCPUs)
		})
	}
}
