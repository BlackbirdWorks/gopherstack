package ec2

import (
	"errors"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"context"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errPoolOverlap = errors.New("pool overlaps with other one on this address space")

type fakeNetwork struct {
	labels map[string]string
	opts   mobyclient.NetworkCreateOptions
	name   string
	id     string
}

// fakeNetDockerAPI adds Docker network calls to fakeDockerAPI.
type fakeNetDockerAPI struct {
	*fakeDockerAPI
	removeErr      error
	networks       []fakeNetwork
	createdNames   []string
	removedNames   []string
	nmu            sync.Mutex
	overlapOnIPAM  bool
	failAllCreates bool
}

func newFakeNetDocker() *fakeNetDockerAPI {
	return &fakeNetDockerAPI{fakeDockerAPI: &fakeDockerAPI{containerID: "ctr-1", containerIP: "172.17.0.9"}}
}

func (f *fakeNetDockerAPI) NetworkCreate(
	_ context.Context, name string, opts mobyclient.NetworkCreateOptions,
) (mobyclient.NetworkCreateResult, error) {
	f.nmu.Lock()
	defer f.nmu.Unlock()

	if f.failAllCreates || (f.overlapOnIPAM && opts.IPAM != nil) {
		return mobyclient.NetworkCreateResult{}, errPoolOverlap
	}

	f.networks = append(f.networks, fakeNetwork{name: name, id: "net-" + name, labels: opts.Labels, opts: opts})
	f.createdNames = append(f.createdNames, name)

	return mobyclient.NetworkCreateResult{ID: "net-" + name}, nil
}

func (f *fakeNetDockerAPI) NetworkList(
	_ context.Context, _ mobyclient.NetworkListOptions,
) (mobyclient.NetworkListResult, error) {
	f.nmu.Lock()
	defer f.nmu.Unlock()

	var res mobyclient.NetworkListResult

	for _, n := range f.networks {
		var sum mobynetwork.Summary

		sum.Name, sum.ID, sum.Labels = n.name, n.id, n.labels
		res.Items = append(res.Items, sum)
	}

	return res, nil
}

func (f *fakeNetDockerAPI) NetworkRemove(
	_ context.Context, id string, _ mobyclient.NetworkRemoveOptions,
) (mobyclient.NetworkRemoveResult, error) {
	f.nmu.Lock()
	defer f.nmu.Unlock()

	if f.removeErr != nil {
		return mobyclient.NetworkRemoveResult{}, f.removeErr
	}

	for i, n := range f.networks {
		if n.id == id {
			f.removedNames = append(f.removedNames, n.name)
			f.networks = append(f.networks[:i], f.networks[i+1:]...)

			break
		}
	}

	return mobyclient.NetworkRemoveResult{}, nil
}

// ContainerInspect reports the endpoint and port mappings the last created container asked for.
func (f *fakeNetDockerAPI) ContainerInspect(
	_ context.Context, _ string, _ mobyclient.ContainerInspectOptions,
) (mobyclient.ContainerInspectResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	nets := map[string]*mobynetwork.EndpointSettings{"bridge": {IPAddress: netip.MustParseAddr(f.containerIP)}}
	ports := mobynetwork.PortMap{}

	if n := len(f.created); n > 0 {
		last := f.created[n-1]
		nets = endpointsOf(last.NetworkingConfig, f.containerIP)
		ports = mappedPorts(last.HostConfig)
	}

	return mobyclient.ContainerInspectResult{Container: mobycontainer.InspectResponse{
		ID:              f.containerID,
		NetworkSettings: &mobycontainer.NetworkSettings{Networks: nets, Ports: ports},
	}}, nil
}

func endpointsOf(cfg *mobynetwork.NetworkingConfig, dockerIP string) map[string]*mobynetwork.EndpointSettings {
	out := map[string]*mobynetwork.EndpointSettings{}
	if cfg == nil {
		return map[string]*mobynetwork.EndpointSettings{"bridge": {IPAddress: netip.MustParseAddr(dockerIP)}}
	}

	for name, ep := range cfg.EndpointsConfig {
		ip := netip.MustParseAddr(dockerIP)
		if ep != nil && ep.IPAMConfig != nil && ep.IPAMConfig.IPv4Address.IsValid() {
			ip = ep.IPAMConfig.IPv4Address
		}

		out[name] = &mobynetwork.EndpointSettings{IPAddress: ip}
	}

	return out
}

func mappedPorts(hc *mobycontainer.HostConfig) mobynetwork.PortMap {
	out := mobynetwork.PortMap{}
	next := 40000

	if hc == nil {
		return out
	}

	ports := make([]mobynetwork.Port, 0, len(hc.PortBindings))
	for port := range hc.PortBindings {
		ports = append(ports, port)
	}

	slices.SortFunc(ports, func(a, b mobynetwork.Port) int { return strings.Compare(a.String(), b.String()) })

	for _, port := range ports {
		for _, b := range hc.PortBindings[port] {
			next++
			out[port] = append(out[port], mobynetwork.PortBinding{HostIP: b.HostIP, HostPort: strconv.Itoa(next)})
		}
	}

	return out
}

func launchIn(t *testing.T, dc *DockerCompute, vpc, cidr, ip string) LaunchResult {
	t.Helper()

	res, err := dc.Launch(t.Context(), LaunchRequest{
		InstanceID: "i-" + ip, VPCID: vpc, VPCCIDR: cidr, PrivateIP: ip,
	})
	require.NoError(t, err)

	return res
}

func TestDockerComputeVPCNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*fakeNetDockerAPI, *DockerComputeConfig)
		check func(t *testing.T, api *fakeNetDockerAPI, res []LaunchResult)
		name  string
		vpcs  [][3]string
	}{
		{
			name: "network-with-vpc-cidr-and-static-ip",
			vpcs: [][3]string{{"vpc-1", "10.0.0.0/16", "10.0.1.4"}},
			check: func(t *testing.T, api *fakeNetDockerAPI, res []LaunchResult) {
				t.Helper()
				require.Len(t, api.networks, 1)

				n := api.networks[0]
				assert.Equal(t, "gopherstack-vpc-vpc-1", n.name)
				assert.Equal(t, "bridge", n.opts.Driver)
				assert.Equal(t, map[string]string{
					labelManaged: "true", labelVPC: "vpc-1", labelIPAM: ipamVPCCIDR,
				}, n.labels)
				require.NotNil(t, n.opts.IPAM)
				assert.Equal(t, netip.MustParsePrefix("10.0.0.0/16"), n.opts.IPAM.Config[0].Subnet)

				created := api.created[0]
				assert.Equal(t, mobycontainer.NetworkMode("gopherstack-vpc-vpc-1"), created.HostConfig.NetworkMode)
				ep := created.NetworkingConfig.EndpointsConfig["gopherstack-vpc-vpc-1"]
				require.NotNil(t, ep.IPAMConfig)
				assert.Equal(t, netip.MustParseAddr("10.0.1.4"), ep.IPAMConfig.IPv4Address)
				assert.Equal(t, "10.0.1.4", res[0].PrivateIP)
				assert.Empty(t, res[0].NetworkNote)
			},
		},
		{
			name: "same-vpc-shares-network",
			vpcs: [][3]string{{"vpc-1", "10.0.0.0/16", "10.0.1.4"}, {"vpc-1", "10.0.0.0/16", "10.0.2.4"}},
			check: func(t *testing.T, api *fakeNetDockerAPI, res []LaunchResult) {
				t.Helper()
				assert.Equal(t, []string{"gopherstack-vpc-vpc-1"}, api.createdNames)
				assert.Equal(t, "10.0.2.4", res[1].PrivateIP)
			},
		},
		{
			name: "different-vpcs-different-networks",
			vpcs: [][3]string{{"vpc-1", "10.0.0.0/16", "10.0.1.4"}, {"vpc-2", "10.0.0.0/16", "10.0.1.4"}},
			check: func(t *testing.T, api *fakeNetDockerAPI, _ []LaunchResult) {
				t.Helper()
				assert.Equal(t, []string{"gopherstack-vpc-vpc-1", "gopherstack-vpc-vpc-2"}, api.createdNames)
			},
		},
		{
			name:  "overlap-falls-back-without-static-ip",
			vpcs:  [][3]string{{"vpc-1", "172.17.0.0/16", "172.17.1.4"}},
			setup: func(f *fakeNetDockerAPI, _ *DockerComputeConfig) { f.overlapOnIPAM = true },
			check: func(t *testing.T, api *fakeNetDockerAPI, res []LaunchResult) {
				t.Helper()
				require.Len(t, api.networks, 1)
				assert.Nil(t, api.networks[0].opts.IPAM)
				assert.Equal(t, ipamAuto, api.networks[0].labels[labelIPAM])

				ep := api.created[0].NetworkingConfig.EndpointsConfig["gopherstack-vpc-vpc-1"]
				assert.Nil(t, ep.IPAMConfig)
				assert.Equal(t, "172.17.0.9", res[0].PrivateIP)
				assert.Contains(t, res[0].NetworkNote, "172.17.0.0/16")
				assert.Contains(t, res[0].NetworkNote, errPoolOverlap.Error())
			},
		},
		{
			name: "explicit-network-disables-vpc-networks",
			vpcs: [][3]string{{"vpc-1", "10.0.0.0/16", "10.0.1.4"}},
			setup: func(_ *fakeNetDockerAPI, cfg *DockerComputeConfig) {
				cfg.Network = "shared"
			},
			check: func(t *testing.T, api *fakeNetDockerAPI, _ []LaunchResult) {
				t.Helper()
				assert.Empty(t, api.networks)
				assert.Equal(t, mobycontainer.NetworkMode("shared"), api.created[0].HostConfig.NetworkMode)
			},
		},
		{
			name: "existing-labelled-network-is-reattached",
			vpcs: [][3]string{{"vpc-1", "10.0.0.0/16", "10.0.1.4"}},
			setup: func(f *fakeNetDockerAPI, _ *DockerComputeConfig) {
				f.networks = []fakeNetwork{{
					name: "gopherstack-vpc-vpc-1", id: "old",
					labels: vpcNetworkLabels("vpc-1", ipamVPCCIDR),
				}}
			},
			check: func(t *testing.T, api *fakeNetDockerAPI, _ []LaunchResult) {
				t.Helper()
				assert.Empty(t, api.createdNames)
				ep := api.created[0].NetworkingConfig.EndpointsConfig["gopherstack-vpc-vpc-1"]
				require.NotNil(t, ep.IPAMConfig)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newFakeNetDocker()
			cfg := DockerComputeConfig{}

			if tt.setup != nil {
				tt.setup(api, &cfg)
			}

			dc := newDockerComputeWithAPI(api, cfg)

			var res []LaunchResult
			for _, v := range tt.vpcs {
				res = append(res, launchIn(t, dc, v[0], v[1], v[2]))
			}

			tt.check(t, api, res)
		})
	}
}

func TestDockerComputeNetworkCreateFailure(t *testing.T) {
	t.Parallel()

	api := newFakeNetDocker()
	api.failAllCreates = true
	dc := newDockerComputeWithAPI(api, DockerComputeConfig{})

	_, err := dc.Launch(t.Context(), LaunchRequest{InstanceID: "i-1", VPCID: "vpc-1", VPCCIDR: "10.0.0.0/16"})
	require.Error(t, err)
	assert.Empty(t, api.created)
}

func TestDockerComputeRemoveVPCNetwork(t *testing.T) {
	t.Parallel()

	foreign := fakeNetwork{name: "gopherstack-vpc-vpc-1", id: "foreign"}
	other := fakeNetwork{name: "gopherstack-vpc-vpc-2", id: "other", labels: vpcNetworkLabels("vpc-2", ipamAuto)}
	ours := fakeNetwork{name: "gopherstack-vpc-vpc-1", id: "ours", labels: vpcNetworkLabels("vpc-1", ipamVPCCIDR)}

	tests := []struct {
		removeErr   error
		name        string
		wantRemoved []string
		wantLeft    []string
		wantErr     bool
	}{
		{
			name:        "only-labelled-network",
			wantRemoved: []string{"gopherstack-vpc-vpc-1"},
			wantLeft:    []string{"foreign", "other"},
		},
		{
			name: "in-use-network-is-kept", removeErr: errFakeBoom,
			wantErr: true, wantLeft: []string{"foreign", "other", "ours"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newFakeNetDocker()
			api.networks = []fakeNetwork{foreign, other, ours}
			api.removeErr = tt.removeErr
			dc := newDockerComputeWithAPI(api, DockerComputeConfig{})

			err := dc.RemoveVPCNetwork(t.Context(), "vpc-1")
			if tt.wantErr {
				require.ErrorIs(t, err, errNetworkInUse)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRemoved, api.removedNames)

			left := make([]string, 0, len(api.networks))
			for _, n := range api.networks {
				left = append(left, n.id)
			}

			assert.ElementsMatch(t, tt.wantLeft, left)
		})
	}
}

func TestDockerComputePruneIdleNetworks(t *testing.T) {
	t.Parallel()

	api := newFakeNetDocker()
	api.networks = []fakeNetwork{
		{name: "user-net", id: "user"},
		{name: "gopherstack-vpc-a", id: "a", labels: vpcNetworkLabels("a", ipamAuto)},
	}
	dc := newDockerComputeWithAPI(api, DockerComputeConfig{})

	assert.Equal(t, []string{"gopherstack-vpc-a"}, dc.PruneIdleNetworks(t.Context()))
	require.Len(t, api.networks, 1)
	assert.Equal(t, "user", api.networks[0].id)
}
