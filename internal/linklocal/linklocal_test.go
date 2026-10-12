package linklocal_test

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobyimage "github.com/moby/moby/api/types/image"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/linklocal"
)

var errNoSuchImage = errors.New("no such image")

type fakeDocker struct {
	imageErr    error
	endpoints   map[string]mobynetwork.EndpointResource
	labels      map[string]map[string]string
	netCreated  []mobyclient.NetworkCreateOptions
	netsListed  []mobynetwork.Summary
	ctrsListed  []mobycontainer.Summary
	created     []mobyclient.ContainerCreateOptions
	connected   []mobyclient.NetworkConnectOptions
	removedCtrs []string
	mu          sync.Mutex
}

func (f *fakeDocker) ImageInspect(
	context.Context, string, ...mobyclient.ImageInspectOption,
) (mobyclient.ImageInspectResult, error) {
	return mobyclient.ImageInspectResult{InspectResponse: mobyimage.InspectResponse{}}, f.imageErr
}

func (f *fakeDocker) NetworkCreate(
	_ context.Context, _ string, o mobyclient.NetworkCreateOptions,
) (mobyclient.NetworkCreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.netCreated = append(f.netCreated, o)

	return mobyclient.NetworkCreateResult{}, nil
}

func (f *fakeDocker) NetworkList(
	context.Context, mobyclient.NetworkListOptions,
) (mobyclient.NetworkListResult, error) {
	return mobyclient.NetworkListResult{Items: f.netsListed}, nil
}

func (f *fakeDocker) NetworkInspect(
	context.Context, string, mobyclient.NetworkInspectOptions,
) (mobyclient.NetworkInspectResult, error) {
	return mobyclient.NetworkInspectResult{Network: mobynetwork.Inspect{
		Network:    mobynetwork.Network{},
		Containers: f.endpoints,
	}}, nil
}

func (f *fakeDocker) NetworkConnect(
	_ context.Context, _ string, o mobyclient.NetworkConnectOptions,
) (mobyclient.NetworkConnectResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connected = append(f.connected, o)

	return mobyclient.NetworkConnectResult{}, nil
}

func (f *fakeDocker) NetworkRemove(
	context.Context, string, mobyclient.NetworkRemoveOptions,
) (mobyclient.NetworkRemoveResult, error) {
	return mobyclient.NetworkRemoveResult{}, nil
}

func (f *fakeDocker) ContainerCreate(
	_ context.Context, o mobyclient.ContainerCreateOptions,
) (mobyclient.ContainerCreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, o)

	return mobyclient.ContainerCreateResult{ID: "relay1"}, nil
}

func (f *fakeDocker) ContainerStart(
	context.Context, string, mobyclient.ContainerStartOptions,
) (mobyclient.ContainerStartResult, error) {
	return mobyclient.ContainerStartResult{}, nil
}

func (f *fakeDocker) ContainerList(
	context.Context, mobyclient.ContainerListOptions,
) (mobyclient.ContainerListResult, error) {
	return mobyclient.ContainerListResult{Items: f.ctrsListed}, nil
}

func (f *fakeDocker) ContainerInspect(
	_ context.Context, id string, _ mobyclient.ContainerInspectOptions,
) (mobyclient.ContainerInspectResult, error) {
	return mobyclient.ContainerInspectResult{Container: mobycontainer.InspectResponse{
		ID:     id,
		State:  &mobycontainer.State{Running: true},
		Config: &mobycontainer.Config{Labels: f.labels[id]},
	}}, nil
}

func (f *fakeDocker) ContainerRemove(
	_ context.Context, id string, _ mobyclient.ContainerRemoveOptions,
) (mobyclient.ContainerRemoveResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removedCtrs = append(f.removedCtrs, id)

	return mobyclient.ContainerRemoveResult{}, nil
}

func TestManagerEnsure(t *testing.T) {
	t.Parallel()

	spec := linklocal.ECSSpec("", "172.17.0.1", "8123")

	tests := []struct {
		setup       func(f *fakeDocker, hash string)
		name        string
		wantNets    int
		wantCreated int
		wantRemoved int
		wantActive  bool
	}{
		{name: "fresh", wantActive: true, wantNets: 1, wantCreated: 1},
		{
			name: "image_missing",
			setup: func(f *fakeDocker, _ string) {
				f.imageErr = errNoSuchImage
			},
		},
		{
			name: "network_reused",
			setup: func(f *fakeDocker, _ string) {
				var n mobynetwork.Summary

				n.Name, n.Labels = "gopherstack-linklocal-ecs", map[string]string{"gopherstack.linklocal": "ecs"}
				f.netsListed = []mobynetwork.Summary{n}
			},
			wantActive: true, wantCreated: 1,
		},
		{
			name: "stale_relay_replaced",
			setup: func(f *fakeDocker, _ string) {
				f.ctrsListed = []mobycontainer.Summary{{ID: "old", Labels: map[string]string{
					"gopherstack.linklocal.config": "stale",
				}}}
			},
			wantActive: true, wantNets: 1, wantCreated: 1, wantRemoved: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fakeDocker{}
			if tt.setup != nil {
				tt.setup(f, "")
			}

			m := linklocal.New(f, spec)

			assert.Equal(t, tt.wantActive, m.Active(t.Context()))
			assert.Len(t, f.netCreated, tt.wantNets)
			assert.Len(t, f.created, tt.wantCreated)
			assert.Len(t, f.removedCtrs, tt.wantRemoved)
		})
	}
}

func TestManagerRelaySpec(t *testing.T) {
	t.Parallel()

	f := &fakeDocker{}
	m := linklocal.New(f, linklocal.IMDSSpec("busybox:1", "172.17.0.1", "8123", "gopherstack.ec2.instance"))

	require.True(t, m.Active(t.Context()))
	require.Len(t, f.netCreated, 1)
	require.Len(t, f.created, 1)

	cfg := f.netCreated[0]
	assert.Equal(t, netip.MustParsePrefix("169.254.169.0/24"), cfg.IPAM.Config[0].Subnet)

	c := f.created[0]
	assert.Equal(t, "busybox:1", c.Config.Image)
	assert.Contains(t, c.Config.Env, "LL_HOST=172.17.0.1")
	assert.Contains(t, c.Config.Env, "LL_PORT=8123")
	assert.Contains(t, c.Config.Env, "LL_PREFIX=/_gopherstack/ec2/imds/linklocal")
	assert.Equal(t, netip.MustParseAddr("169.254.169.254"),
		c.NetworkingConfig.EndpointsConfig["gopherstack-linklocal-imds"].IPAMConfig.IPv4Address)
}

func TestManagerAttachAndOwner(t *testing.T) {
	t.Parallel()

	f := &fakeDocker{
		endpoints: map[string]mobynetwork.EndpointResource{
			"c-attached": {IPv4Address: netip.MustParsePrefix("169.254.169.3/24")},
			"c-restart":  {IPv4Address: netip.MustParsePrefix("169.254.169.4/24")},
		},
		labels: map[string]map[string]string{"c-restart": {"gopherstack.ec2.instance": "i-restart"}},
	}
	m := linklocal.New(f, linklocal.IMDSSpec("", "h", "1", "gopherstack.ec2.instance"))

	require.NoError(t, m.Attach(t.Context(), "c-attached", "i-attached"))
	require.Len(t, f.connected, 1)
	assert.Equal(t, -1, f.connected[0].EndpointConfig.GwPriority)

	tests := []struct {
		name      string
		ip        string
		wantOwner string
		wantOK    bool
	}{
		{name: "attached", ip: "169.254.169.3", wantOwner: "i-attached", wantOK: true},
		{name: "recovered_from_label", ip: "169.254.169.4", wantOwner: "i-restart", wantOK: true},
		{name: "unknown", ip: "169.254.169.99"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			owner, ok := m.OwnerOf(t.Context(), tt.ip)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantOwner, owner)
		})
	}
}
