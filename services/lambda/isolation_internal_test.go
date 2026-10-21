package lambda

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type isolationAPI struct {
	hosts []*dockercontainer.HostConfig
	rts   []string
	mu    sync.Mutex
}

func (a *isolationAPI) Runtimes(context.Context) ([]string, error) { return a.rts, nil }

func (a *isolationAPI) ImagePull(context.Context, string, image.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (a *isolationAPI) ImageList(context.Context, image.ListOptions) ([]image.Summary, error) {
	return nil, nil
}

func (a *isolationAPI) ContainerCreate(
	_ context.Context, _ *dockercontainer.Config, host *dockercontainer.HostConfig, _, _ any, _ string,
) (dockercontainer.CreateResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.hosts = append(a.hosts, host)

	return dockercontainer.CreateResponse{ID: "c1"}, nil
}

func (a *isolationAPI) ContainerStart(context.Context, string, dockercontainer.StartOptions) error {
	return nil
}

func (a *isolationAPI) ContainerStop(context.Context, string, dockercontainer.StopOptions) error {
	return nil
}

func (a *isolationAPI) ContainerRemove(context.Context, string, dockercontainer.RemoveOptions) error {
	return nil
}

func (a *isolationAPI) Ping(context.Context) (any, error) { return struct{}{}, nil }
func (a *isolationAPI) Close() error                      { return nil }

func TestResolveIsolation(t *testing.T) {
	t.Parallel()

	withRuntimes := func(rts ...string) container.Runtime {
		return container.NewDockerRuntimeWithAPI(&isolationAPI{rts: rts}, container.Config{})
	}

	tests := []struct {
		rt   container.Runtime
		err  error
		name string
		mode string
		want string
	}{
		{name: "container", mode: "container", rt: withRuntimes("runc")},
		{name: "empty", mode: "", rt: nil},
		{name: "gvisor_ok", mode: "gvisor", rt: withRuntimes("runc", "runsc"), want: "runsc"},
		{name: "gvisor_missing", mode: "gvisor", rt: withRuntimes("runc"), err: container.ErrOCIRuntimeMissing},
		{name: "kata_ok", mode: "kata", rt: withRuntimes("kata-runtime"), want: "kata-runtime"},
		{name: "no_runtime", mode: "gvisor", rt: nil, err: ErrIsolationNeedsRuntime},
		{name: "firecracker", mode: "firecracker", rt: withRuntimes("runc"), err: ErrIsolationUnsupported},
		{name: "bogus", mode: "bogus", rt: nil, err: ErrInvalidIsolation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveIsolation(t.Context(), tt.mode, tt.rt)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStartContainer_PassesOCIRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		oci  string
	}{
		{name: "gvisor", oci: "runsc"},
		{name: "default", oci: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &isolationAPI{}
			b := NewInMemoryBackend(
				container.NewDockerRuntimeWithAPI(api, container.Config{}),
				nil, DefaultSettings(), "000000000000", "us-east-1",
			)
			t.Cleanup(func() { b.Close(context.Background()) })
			b.ociRuntime = tt.oci

			fn := &FunctionConfiguration{FunctionName: "f", PackageType: PackageTypeImage, ImageURI: "img:latest"}
			_, _, id, err := b.startContainer(t.Context(), fn, nil)
			require.NoError(t, err)
			require.NotEmpty(t, id)

			api.mu.Lock()
			defer api.mu.Unlock()

			require.Len(t, api.hosts, 1)
			assert.Equal(t, tt.oci, api.hosts[0].Runtime)
		})
	}
}
