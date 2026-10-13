package ec2

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"

	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errLinkLocalAttach = errors.New("attach failed")

type fakeLinkLocalNet struct {
	api        *fakeDockerAPI
	attachErr  error
	owners     map[string]string
	attached   [][2]string
	startedAt  []int
	released   []string
	mu         sync.Mutex
	activeFlag bool
}

func (f *fakeLinkLocalNet) Active(context.Context) bool { return f.activeFlag }

func (f *fakeLinkLocalNet) Attach(_ context.Context, containerID, owner string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.attachErr != nil {
		return f.attachErr
	}

	f.api.mu.Lock()
	f.startedAt = append(f.startedAt, len(f.api.started))
	f.api.mu.Unlock()

	f.attached = append(f.attached, [2]string{containerID, owner})

	return nil
}

func (f *fakeLinkLocalNet) Release(containerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.released = append(f.released, containerID)
}

func (f *fakeLinkLocalNet) OwnerOf(_ context.Context, ip string) (string, bool) {
	owner, ok := f.owners[ip]

	return owner, ok
}

func TestDockerLaunchLinkLocal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attachErr    error
		name         string
		wantEnv      []string
		wantAttached int
		wantStarted  int
		wantRemoved  int
		active       bool
		wantErr      bool
	}{
		{
			name: "active_uses_default_address", active: true, wantAttached: 1, wantStarted: 1,
		},
		{
			name: "inactive_keeps_endpoint_env", wantStarted: 1,
			wantEnv: []string{"AWS_EC2_METADATA_SERVICE_ENDPOINT=http://172.17.0.1:4242"},
		},
		{
			name: "attach_failure_cleans_up", active: true, attachErr: errLinkLocalAttach,
			wantErr: true, wantRemoved: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &fakeDockerAPI{containerID: "c1", containerIP: "172.17.0.7"}
			ll := &fakeLinkLocalNet{api: api, activeFlag: tt.active, attachErr: tt.attachErr}
			dc := newDockerComputeWithAPI(api, DockerComputeConfig{Image: defaultDockerImage})
			dc.SetLinkLocal(ll)

			res, err := dc.Launch(t.Context(), LaunchRequest{
				InstanceID: "i-ll", MetadataEndpoint: "http://172.17.0.1:4242",
			})
			if tt.wantErr {
				require.ErrorIs(t, err, errLinkLocalAttach)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "172.17.0.7", res.PrivateIP)
			}

			api.mu.Lock()
			require.Len(t, api.created, 1)
			assert.Equal(t, tt.wantEnv, api.created[0].Config.Env)
			assert.Len(t, api.started, tt.wantStarted)
			assert.Len(t, api.removed, tt.wantRemoved)
			api.mu.Unlock()

			assert.Len(t, ll.attached, tt.wantAttached)

			if tt.wantAttached > 0 {
				assert.Equal(t, [2]string{"c1", "i-ll"}, ll.attached[0])
				assert.Equal(t, []int{0}, ll.startedAt, "attach must precede start")
			}
		})
	}
}

func TestDockerTerminateReleasesLinkLocal(t *testing.T) {
	t.Parallel()

	api := &fakeDockerAPI{containerID: "c1", containerIP: "172.17.0.7"}
	ll := &fakeLinkLocalNet{api: api, activeFlag: true}
	dc := newDockerComputeWithAPI(api, DockerComputeConfig{Image: defaultDockerImage})
	dc.SetLinkLocal(ll)

	require.NoError(t, dc.Terminate(t.Context(), "i-ll", "c1"))
	assert.Equal(t, []string{"c1"}, ll.released)
}

func TestPickNetworkIPSkipsLinkLocal(t *testing.T) {
	t.Parallel()

	nets := map[string]*mobynetwork.EndpointSettings{
		"gopherstack-linklocal-imds": {IPAddress: netip.MustParseAddr("169.254.169.3")},
		"bridge":                     {IPAddress: netip.MustParseAddr("172.17.0.7")},
	}

	for range 20 {
		assert.Equal(t, "172.17.0.7", pickNetworkIP(nets, ""))
	}
}
