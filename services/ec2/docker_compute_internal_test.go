package ec2

import (
	"context"
	"errors"
	"io"
	"iter"
	"net/netip"
	"strings"
	"sync"
	"testing"

	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/jsonstream"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errFakeBoom = errors.New("boom")

// fakeDockerAPI implements the dockerAPI interface used by DockerCompute.
type fakeDockerAPI struct {
	pullErr     error
	inspectErr  error
	createErr   error
	startErr    error
	pingErr     error
	containerIP string
	containerID string
	pulled      []string
	created     []mobyclient.ContainerCreateOptions
	started     []string
	stopped     []string
	removed     []string
	mu          sync.Mutex
}

type fakePullRespAdapter struct {
	rc io.ReadCloser
}

func (f *fakeDockerAPI) ImagePull(
	_ context.Context, ref string, _ mobyclient.ImagePullOptions,
) (mobyclient.ImagePullResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pulled = append(f.pulled, ref)

	if f.pullErr != nil {
		return nil, f.pullErr
	}

	return fakePullRespAdapter{rc: io.NopCloser(strings.NewReader(""))}, nil
}

func (f fakePullRespAdapter) Read(p []byte) (int, error) { return f.rc.Read(p) }
func (f fakePullRespAdapter) Close() error               { return f.rc.Close() }
func (f fakePullRespAdapter) JSONMessages(_ context.Context) iter.Seq2[jsonstream.Message, error] {
	return func(_ func(jsonstream.Message, error) bool) {}
}
func (f fakePullRespAdapter) Wait(_ context.Context) error { return nil }

func (f *fakeDockerAPI) ContainerCreate(
	_ context.Context, opts mobyclient.ContainerCreateOptions,
) (mobyclient.ContainerCreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, opts)

	if f.createErr != nil {
		return mobyclient.ContainerCreateResult{}, f.createErr
	}

	return mobyclient.ContainerCreateResult{ID: f.containerID}, nil
}

func (f *fakeDockerAPI) ContainerStart(
	_ context.Context, id string, _ mobyclient.ContainerStartOptions,
) (mobyclient.ContainerStartResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, id)

	return mobyclient.ContainerStartResult{}, f.startErr
}

func (f *fakeDockerAPI) ContainerStop(
	_ context.Context, id string, _ mobyclient.ContainerStopOptions,
) (mobyclient.ContainerStopResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, id)

	return mobyclient.ContainerStopResult{}, nil
}

func (f *fakeDockerAPI) ContainerRemove(
	_ context.Context, id string, _ mobyclient.ContainerRemoveOptions,
) (mobyclient.ContainerRemoveResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, id)

	return mobyclient.ContainerRemoveResult{}, nil
}

func (f *fakeDockerAPI) ContainerInspect(
	_ context.Context, _ string, _ mobyclient.ContainerInspectOptions,
) (mobyclient.ContainerInspectResult, error) {
	if f.inspectErr != nil {
		return mobyclient.ContainerInspectResult{}, f.inspectErr
	}

	addr, _ := netip.ParseAddr(f.containerIP)

	return mobyclient.ContainerInspectResult{
		Container: mobycontainer.InspectResponse{
			ID: f.containerID,
			NetworkSettings: &mobycontainer.NetworkSettings{
				Networks: map[string]*mobynetwork.EndpointSettings{
					"bridge": {IPAddress: addr},
				},
				Ports: mobynetwork.PortMap{},
			},
		},
	}, nil
}

func (f *fakeDockerAPI) Ping(
	_ context.Context, _ mobyclient.PingOptions,
) (mobyclient.PingResult, error) {
	return mobyclient.PingResult{}, f.pingErr
}

func (f *fakeDockerAPI) Close() error { return nil }

func TestDockerComputeLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupAPI     func(*fakeDockerAPI)
		assertResult func(t *testing.T, res LaunchResult)
		name         string
		wantLaunchOK bool
	}{
		{
			name: "happy_path",
			setupAPI: func(f *fakeDockerAPI) {
				f.containerID = "deadbeef"
				f.containerIP = "172.17.0.5"
			},
			wantLaunchOK: true,
			assertResult: func(t *testing.T, res LaunchResult) {
				t.Helper()
				assert.Equal(t, "deadbeef", res.ProviderID)
				assert.Equal(t, "172.17.0.5", res.PrivateIP)
				assert.Equal(t, "127.0.0.1", res.PublicIPAddress)
			},
		},
		{
			name: "create_error_propagates",
			setupAPI: func(f *fakeDockerAPI) {
				f.createErr = errFakeBoom
			},
			wantLaunchOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &fakeDockerAPI{}
			tt.setupAPI(api)

			dc := newDockerComputeWithAPI(api, DockerComputeConfig{Image: defaultDockerImage})

			res, err := dc.Launch(t.Context(), LaunchRequest{
				InstanceID:    "i-test1",
				ImageID:       "ami-test",
				InstanceType:  "t3.micro",
				KeyName:       "demo",
				AuthorizedKey: "ssh-rsa AAAA demo",
			})

			if !tt.wantLaunchOK {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []string{defaultDockerImage}, api.pulled)
			assert.Len(t, api.created, 1)
			assert.Equal(t, []string{"deadbeef"}, api.started)

			tt.assertResult(t, res)

			require.NoError(t, dc.Terminate(t.Context(), "i-test1", "deadbeef"))
			assert.Contains(t, api.removed, "deadbeef")
		})
	}
}
