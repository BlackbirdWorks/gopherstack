package ocicontainerinstances_test

import (
	"net/http"
	"sync"
	"testing"

	ci "github.com/oracle/oci-go-sdk/v65/containerinstances"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
	svc "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
)

func TestSecurityContextReachesRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(*ci.CreateContainerInstanceDetails)
		name       string
		user       string
		wantAdd    []string
		wantDrop   []string
		wantGroups []string
		readOnly   bool
	}{
		{
			name: "caps and rootfs",
			mutate: func(d *ci.CreateContainerInstanceDetails) {
				d.Containers[0].SecurityContext = ci.CreateLinuxSecurityContextDetails{
					IsRootFileSystemReadonly: new(true), RunAsUser: new(1000), RunAsGroup: new(2000),
					Capabilities: &ci.ContainerCapabilities{
						AddCapabilities:  []ci.ContainerCapabilityTypeEnum{ci.ContainerCapabilityTypeCapNetBindService},
						DropCapabilities: []ci.ContainerCapabilityTypeEnum{ci.ContainerCapabilityTypeAll},
					},
				}
			},
			user: "1000:2000", wantAdd: []string{"NET_BIND_SERVICE"}, wantDrop: []string{"ALL"}, readOnly: true,
		},
		{
			name: "fs group from the instance",
			mutate: func(d *ci.CreateContainerInstanceDetails) {
				d.SecurityContext = ci.CreateLinuxContainerInstanceSecurityContextDetails{FsGroup: new(4242)}
			},
			wantGroups: []string{"4242"},
		},
		{name: "defaults", mutate: func(*ci.CreateContainerInstanceDetails) {}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := newDockerEnv(t, nil)
			resp := d.mustCreate(t, createOpts{mutate: tt.mutate})
			d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

			sp := d.rt.started(t, 1)[0]
			assert.Equal(t, tt.user, sp.User)
			assert.ElementsMatch(t, tt.wantAdd, sp.CapAdd)
			assert.ElementsMatch(t, tt.wantDrop, sp.CapDrop)
			assert.ElementsMatch(t, tt.wantGroups, sp.GroupAdd)
			assert.Equal(t, tt.readOnly, sp.ReadOnlyRootfs)
			assert.False(t, sp.Privileged)
			assert.Contains(t, sp.SecurityOpt, "no-new-privileges")
		})
	}
}

func TestSecurityContextRejected(t *testing.T) {
	t.Parallel()

	caps := func(add, drop []ci.ContainerCapabilityTypeEnum) func(*ci.CreateContainerInstanceDetails) {
		return func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].SecurityContext = ci.CreateLinuxSecurityContextDetails{
				Capabilities: &ci.ContainerCapabilities{AddCapabilities: add, DropCapabilities: drop},
			}
		}
	}

	tests := []struct {
		mutate func(*ci.CreateContainerInstanceDetails)
		name   string
	}{
		{name: "add all", mutate: caps([]ci.ContainerCapabilityTypeEnum{ci.ContainerCapabilityTypeAll}, nil)},
		{name: "add privileged cap", mutate: caps([]ci.ContainerCapabilityTypeEnum{"CAP_SYS_ADMIN"}, nil)},
		{name: "drop unknown", mutate: caps(nil, []ci.ContainerCapabilityTypeEnum{"CAP_SYS_MODULE"})},
		{name: "non root with uid 0", mutate: func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].SecurityContext = ci.CreateLinuxSecurityContextDetails{
				IsNonRootUserCheckEnabled: new(true), RunAsUser: new(0),
			}
		}},
		{name: "negative uid", mutate: func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].SecurityContext = ci.CreateLinuxSecurityContextDetails{RunAsUser: new(-1)}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.create(t, createOpts{mutate: tt.mutate})
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestGracefulShutdownReachesRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		grace *int64
		want  *int
		name  string
	}{
		{name: "unset", grace: nil, want: nil},
		{name: "zero", grace: new(int64(0)), want: new(0)},
		{name: "thirty", grace: new(int64(30)), want: new(30)},
		{name: "capped", grace: new(int64(100000)), want: new(300)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := newDockerEnv(t, nil)
			resp := d.mustCreate(t, createOpts{mutate: func(c *ci.CreateContainerInstanceDetails) {
				c.GracefulShutdownTimeoutInSeconds = tt.grace
			}})
			d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

			assert.Equal(t, tt.want, d.rt.started(t, 1)[0].StopTimeoutSecs)
		})
	}
}

type fakeNet struct {
	failWith  error
	allocated []svc.VnicRequest
	released  []string
	mu        sync.Mutex
}

func (f *fakeNet) network() svc.Network {
	return svc.Network{
		Allocate: func(r svc.VnicRequest) (svc.VnicAllocation, error) {
			f.mu.Lock()
			defer f.mu.Unlock()

			if f.failWith != nil {
				return svc.VnicAllocation{}, f.failWith
			}

			f.allocated = append(f.allocated, r)

			return svc.VnicAllocation{
				VnicID:    "ocid1.vnic.oc1..n" + string(rune('a'+len(f.allocated))),
				PrivateIP: "10.0.0.5",
			}, nil
		},
		Release: func(id string) {
			f.mu.Lock()
			f.released = append(f.released, id)
			f.mu.Unlock()
		},
	}
}

func TestVnicBackedByNetwork(t *testing.T) {
	t.Parallel()

	fn := &fakeNet{}
	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) { b.SetNetwork(fn.network()) }})

	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		d.Vnics[0].NsgIds = []string{"ocid1.nsg.oc1..x"}
		d.Vnics[0].HostnameLabel = new("web")
		d.Vnics[0].IsPublicIpAssigned = new(false)
	}})

	got := e.get(t, resp.Id)
	require.Len(t, got.Vnics, 1)
	assert.Equal(t, "ocid1.vnic.oc1..nb", *got.Vnics[0].VnicId)

	require.Len(t, fn.allocated, 1)
	assert.Equal(t, testSubnet, fn.allocated[0].SubnetID)
	assert.Equal(t, []string{"ocid1.nsg.oc1..x"}, fn.allocated[0].NsgIDs)
	assert.Equal(t, "web", fn.allocated[0].HostnameLabel)
	assert.Equal(t, testCompartment, fn.allocated[0].CompartmentID)

	e.settle()

	_, err := e.client.DeleteContainerInstance(
		t.Context(),
		ci.DeleteContainerInstanceRequest{ContainerInstanceId: resp.Id},
	)
	require.NoError(t, err)

	fn.mu.Lock()
	defer fn.mu.Unlock()

	assert.Equal(t, []string{"ocid1.vnic.oc1..nb"}, fn.released)
}

func TestVnicReleasedWhenCreateFails(t *testing.T) {
	t.Parallel()

	fn := &fakeNet{}
	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) { b.SetNetwork(fn.network()) }})

	_, err := e.create(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) { d.Shape = new("nope") }})
	require.Error(t, err)

	fn.mu.Lock()
	defer fn.mu.Unlock()

	require.Len(t, fn.allocated, 1)
	assert.Len(t, fn.released, 1, "the VNIC of a rejected create is released")
}

func TestVnicAllocationFailureSurfaces(t *testing.T) {
	t.Parallel()

	fn := &fakeNet{
		failWith: &svc.APIError{Status: http.StatusNotFound, Code: "NotAuthorizedOrNotFound", Message: "subnet"},
	}
	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) { b.SetNetwork(fn.network()) }})

	_, err := e.create(t, createOpts{})
	require.Error(t, err)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	var ae *svc.APIError

	require.ErrorAs(t, fn.failWith, &ae)
	assert.NotErrorIs(t, err, fn.failWith)
}

func TestVnicRetryTokenReplayReleasesDuplicate(t *testing.T) {
	t.Parallel()

	fn := &fakeNet{}
	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) { b.SetNetwork(fn.network()) }})

	first := e.mustCreate(t, createOpts{token: "tok"})
	second := e.mustCreate(t, createOpts{token: "tok"})
	assert.Equal(t, *first.Id, *second.Id)

	fn.mu.Lock()
	defer fn.mu.Unlock()

	assert.Len(t, fn.allocated, 2)
	assert.Len(t, fn.released, 1, "the replayed request's VNIC is freed")
}

func TestPlacementValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		ad     string
		fd     string
		wantOK bool
	}{
		{name: "valid", ad: testAD, fd: "FAULT-DOMAIN-2", wantOK: true},
		{name: "no fault domain", ad: testAD, wantOK: true},
		{name: "bad ad", ad: "ad-1"},
		{name: "bad fd", ad: testAD, fd: "FD-9"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.create(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
				d.AvailabilityDomain = new(tt.ad)
				if tt.fd != "" {
					d.FaultDomain = new(tt.fd)
				}
			}})

			if tt.wantOK {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestDefinedTagNamespaceChecked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		known    bool
		wantCode int
	}{
		{name: "known namespace", known: true},
		{name: "unknown namespace", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
				b.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
					return ocitags.Def{}, tt.known && ns == "ops" && key == "owner"
				})
			}})

			_, err := e.create(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
				d.DefinedTags = map[string]map[string]any{"ops": {"owner": "me"}}
			}})

			if tt.wantCode == 0 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
