package ec2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const peerRegion = "eu-west-1"

func newDockerRegionHandler(t *testing.T, cfg DockerComputeConfig) (*Handler, *fakeDockerAPI, *DockerCompute) {
	t.Helper()

	api := &fakeDockerAPI{containerID: "ctr-1", containerIP: "172.17.0.9"}
	dc := newDockerComputeWithAPI(api, cfg)

	home := NewInMemoryBackend("000000000000", "us-east-1")
	home.WithCompute(dc)

	h := NewHandler(home)
	h.EnableRegions(t.Context())

	return h, api, dc
}

func runInstance(t *testing.T, h *Handler) *Instance {
	t.Helper()

	_, err := h.handleRunInstances(map[string][]string{
		"ImageId":      {"ami-1"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
	}, "req")
	require.NoError(t, err)

	bk, ok := h.Backend.(*InMemoryBackend)
	require.True(t, ok)

	instances := bk.DescribeInstances(nil, "")
	require.Len(t, instances, 1)

	return instances[0]
}

func TestHandler_PeerDockerCompute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, peer *Handler, api *fakeDockerAPI, dc *DockerCompute)
		name  string
	}{
		{
			name: "launch-publishes-region-dns",
			check: func(t *testing.T, peer *Handler, api *fakeDockerAPI, _ *DockerCompute) {
				t.Helper()

				inst := runInstance(t, peer)
				assert.Contains(t, inst.PublicDNSName, "."+peerRegion+".compute.amazonaws.com")
				assert.Equal(t, "ctr-1", inst.ProviderID)
				assert.Equal(t, []string{"ctr-1"}, api.started)
			},
		},
		{
			name: "terminate-removes-container",
			check: func(t *testing.T, peer *Handler, api *fakeDockerAPI, _ *DockerCompute) {
				t.Helper()

				inst := runInstance(t, peer)
				_, err := peer.handleTerminateInstances(map[string][]string{testInstanceIDKey: {inst.ID}}, "req")
				require.NoError(t, err)
				assert.Contains(t, api.removed, "ctr-1")
			},
		},
		{
			name: "ssh-ports-shared-across-regions",
			check: func(t *testing.T, peer *Handler, _ *fakeDockerAPI, dc *DockerCompute) {
				t.Helper()

				regional, ok := peer.Backend.(*InMemoryBackend).Compute().(*DockerCompute)
				require.True(t, ok)
				assert.NotSame(t, dc, regional)

				first, err := dc.reserveSSHPort()
				require.NoError(t, err)

				second, err := regional.reserveSSHPort()
				require.NoError(t, err)
				assert.NotEqual(t, first, second)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, api, dc := newDockerRegionHandler(t, DockerComputeConfig{SSHPortMin: 22000, SSHPortMax: 22009})
			peer := h.peers.Get(peerRegion)
			require.NotNil(t, peer)
			tc.check(t, peer, api, dc)
		})
	}
}
