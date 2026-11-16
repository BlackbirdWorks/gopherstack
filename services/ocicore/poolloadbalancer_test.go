package ocicore_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

const (
	testLB  = "ocid1.loadbalancer.oc1.iad.fake"
	testSet = "web"
)

var errBusy = errors.New("busy")

type fakeLB struct {
	sets   map[string]map[string]bool
	gone   bool
	failed bool
	calls  int
	mu     sync.Mutex
}

func newFakeLB() *fakeLB {
	return &fakeLB{sets: map[string]map[string]bool{testSet: {}, "other": {}}}
}

func (f *fakeLB) HasBackendSet(lbID, set string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.sets[set]

	return lbID == testLB && ok
}

func (f *fakeLB) ApplyBackends(_ string, changes []oci.BackendChange) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++

	if f.gone {
		return fmt.Errorf("%w: gone", oci.ErrTargetGone)
	}

	if f.failed {
		return errBusy
	}

	for _, c := range changes {
		set, ok := f.sets[c.Set]
		if !ok {
			return oci.ErrTargetGone
		}

		for _, a := range c.Add {
			set[a.IP+":"+strconv.Itoa(a.Port)] = true
		}

		for _, r := range c.Remove {
			delete(set, r.IP+":"+strconv.Itoa(r.Port))
		}
	}

	return nil
}

func (f *fakeLB) backends() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.sets[testSet]))
	for k := range f.sets[testSet] {
		out = append(out, k)
	}

	slices.Sort(out)

	return out
}

func (f *fakeLB) set(fn func(f *fakeLB)) {
	f.mu.Lock()
	fn(f)
	f.mu.Unlock()
}

type poolFix struct {
	pool core.InstancePool
	f    netFix
	cfg  core.InstanceConfiguration
}

func (e *env) poolWithLB(t *testing.T, size int, lbs []core.AttachLoadBalancerDetails) poolFix {
	t.Helper()

	f := e.fixture(t)
	c := e.mkConfig(t, nil)

	pool, err := e.mgmt.CreateInstancePool(t.Context(), core.CreateInstancePoolRequest{
		CompartmentId: new(testComp), InstanceConfigurationId: c.Id, Size: new(size),
		LoadBalancers: lbs,
		PlacementConfigurations: []core.CreateInstancePoolPlacementConfigurationDetails{
			{AvailabilityDomain: new(testAD), PrimarySubnetId: f.pub.Id},
		},
	})
	require.NoError(t, err)
	e.settle()

	return poolFix{pool: pool.InstancePool, f: f, cfg: c}
}

func (e *env) memberIPs(t *testing.T, pool *string) []string {
	t.Helper()

	members, err := e.mgmt.ListInstancePoolInstances(t.Context(), core.ListInstancePoolInstancesRequest{
		CompartmentId: new(testComp), InstancePoolId: pool,
	})
	require.NoError(t, err)

	var ips []string

	for _, m := range members.Items {
		atts, aerr := e.compute.ListVnicAttachments(t.Context(), core.ListVnicAttachmentsRequest{
			CompartmentId: new(testComp), InstanceId: m.Id,
		})
		require.NoError(t, aerr)

		for _, a := range atts.Items {
			v, verr := e.client.GetVnic(t.Context(), core.GetVnicRequest{VnicId: a.VnicId})
			require.NoError(t, verr)

			if a.DisplayName != nil && *a.DisplayName == "" || a.DisplayName == nil || *a.DisplayName != "sec" {
				ips = append(ips, *v.PrivateIp)
			}
		}
	}

	slices.Sort(ips)

	return ips
}

func wantBackends(ips []string, port int) []string {
	out := make([]string, len(ips))
	for i, ip := range ips {
		out[i] = ip + ":" + strconv.Itoa(port)
	}

	return out
}

func lbAttach(port int) core.AttachLoadBalancerDetails {
	return core.AttachLoadBalancerDetails{
		LoadBalancerId: new(testLB), BackendSetName: new(testSet), Port: new(port), VnicSelection: new("PrimaryVnic"),
	}
}

func TestPoolLoadBalancerFollowsMembers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fk := newFakeLB()
	e.backend.SetLoadBalancerClient(fk)

	pf := e.poolWithLB(t, 2, nil)
	ctx := t.Context()

	attached, err := e.mgmt.AttachLoadBalancer(ctx, core.AttachLoadBalancerRequest{
		InstancePoolId: pf.pool.Id, AttachLoadBalancerDetails: lbAttach(8080),
	})
	require.NoError(t, err)
	require.Len(t, attached.LoadBalancers, 1)
	att := attached.LoadBalancers[0]
	assert.Equal(t, core.InstancePoolLoadBalancerAttachmentLifecycleStateAttaching, att.LifecycleState)
	assert.Equal(t, testSet, *att.BackendSetName)
	assert.Equal(t, 8080, *att.Port)
	assert.Equal(t, pf.pool.Id, att.InstancePoolId)

	assert.Equal(t, wantBackends(e.memberIPs(t, pf.pool.Id), 8080), fk.backends())

	got, err := e.mgmt.GetInstancePoolLoadBalancerAttachment(ctx, core.GetInstancePoolLoadBalancerAttachmentRequest{
		InstancePoolId: pf.pool.Id, InstancePoolLoadBalancerAttachmentId: att.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, core.InstancePoolLoadBalancerAttachmentLifecycleStateAttached, got.LifecycleState)

	_, err = e.mgmt.UpdateInstancePool(ctx, core.UpdateInstancePoolRequest{
		InstancePoolId: pf.pool.Id, Size: new(3),
	})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, wantBackends(e.memberIPs(t, pf.pool.Id), 8080), fk.backends(), "grown pool")
	assert.Len(t, fk.backends(), 3)

	_, err = e.mgmt.UpdateInstancePool(ctx, core.UpdateInstancePoolRequest{
		InstancePoolId: pf.pool.Id, Size: new(1),
	})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, wantBackends(e.memberIPs(t, pf.pool.Id), 8080), fk.backends(), "shrunk pool")
	assert.Len(t, fk.backends(), 1)

	detached, err := e.mgmt.DetachLoadBalancer(ctx, core.DetachLoadBalancerRequest{
		InstancePoolId: pf.pool.Id,
		LoadBalancerId: new(testLB), BackendSetName: new(testSet),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		core.InstancePoolLoadBalancerAttachmentLifecycleStateDetaching,
		detached.LoadBalancers[0].LifecycleState,
	)
	assert.Empty(t, fk.backends())

	got, err = e.mgmt.GetInstancePoolLoadBalancerAttachment(ctx, core.GetInstancePoolLoadBalancerAttachmentRequest{
		InstancePoolId: pf.pool.Id, InstancePoolLoadBalancerAttachmentId: att.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, core.InstancePoolLoadBalancerAttachmentLifecycleStateDetached, got.LifecycleState)
}

func TestPoolLoadBalancerOnCreate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fk := newFakeLB()
	e.backend.SetLoadBalancerClient(fk)

	pf := e.poolWithLB(t, 2, []core.AttachLoadBalancerDetails{lbAttach(80)})

	got, err := e.mgmt.GetInstancePool(t.Context(), core.GetInstancePoolRequest{InstancePoolId: pf.pool.Id})
	require.NoError(t, err)
	require.Len(t, got.LoadBalancers, 1)
	assert.Equal(t, core.InstancePoolLoadBalancerAttachmentLifecycleStateAttached, got.LoadBalancers[0].LifecycleState)
	assert.Equal(t, wantBackends(e.memberIPs(t, pf.pool.Id), 80), fk.backends())

	_, err = e.mgmt.TerminateInstancePool(t.Context(), core.TerminateInstancePoolRequest{InstancePoolId: pf.pool.Id})
	require.NoError(t, err)
	assert.Empty(t, fk.backends(), "terminating the pool empties the backend set")
}

func TestPoolLoadBalancerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod      func(d *core.AttachLoadBalancerDetails)
		name     string
		wantCode string
		wantHTTP int
	}{
		{
			name: "unknown_set", mod: func(d *core.AttachLoadBalancerDetails) { d.BackendSetName = new("nope") },
			wantHTTP: http.StatusNotFound, wantCode: "NotAuthorizedOrNotFound",
		},
		{
			name:     "unknown_lb",
			mod:      func(d *core.AttachLoadBalancerDetails) { d.LoadBalancerId = new("ocid1.loadbalancer.oc1.iad.x") },
			wantHTTP: http.StatusNotFound,
			wantCode: "NotAuthorizedOrNotFound",
		},
		{
			name: "bad_port", mod: func(d *core.AttachLoadBalancerDetails) { d.Port = new(70000) },
			wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
		},
		{
			name: "bad_vnic", mod: func(d *core.AttachLoadBalancerDetails) { d.VnicSelection = new("nope") },
			wantHTTP: http.StatusBadRequest, wantCode: "InvalidParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.backend.SetLoadBalancerClient(newFakeLB())
			pf := e.poolWithLB(t, 1, nil)

			d := lbAttach(80)
			tt.mod(&d)

			_, err := e.mgmt.AttachLoadBalancer(t.Context(), core.AttachLoadBalancerRequest{
				InstancePoolId: pf.pool.Id, AttachLoadBalancerDetails: d,
			})
			require.Error(t, err)
			assert.Equal(t, tt.wantHTTP, svcErr(t, err).GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())
		})
	}
}

func TestPoolLoadBalancerDuplicateAndDetachMissing(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.backend.SetLoadBalancerClient(newFakeLB())
	pf := e.poolWithLB(t, 1, nil)
	ctx := t.Context()

	req := core.AttachLoadBalancerRequest{InstancePoolId: pf.pool.Id, AttachLoadBalancerDetails: lbAttach(80)}
	_, err := e.mgmt.AttachLoadBalancer(ctx, req)
	require.NoError(t, err)

	_, err = e.mgmt.AttachLoadBalancer(ctx, req)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	_, err = e.mgmt.DetachLoadBalancer(ctx, core.DetachLoadBalancerRequest{
		InstancePoolId: pf.pool.Id,
		LoadBalancerId: new(testLB), BackendSetName: new("other"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestPoolLoadBalancerWithoutClient(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	pf := e.poolWithLB(t, 1, nil)

	_, err := e.mgmt.AttachLoadBalancer(t.Context(), core.AttachLoadBalancerRequest{
		InstancePoolId: pf.pool.Id, AttachLoadBalancerDetails: lbAttach(80),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestPoolLoadBalancerSecondaryVnic(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fk := newFakeLB()
	e.backend.SetLoadBalancerClient(fk)

	f := e.fixture(t)
	l := &core.InstanceConfigurationLaunchInstanceDetails{
		Shape:       new("VM.Standard.E4.Flex"),
		ShapeConfig: &core.InstanceConfigurationLaunchInstanceShapeConfigDetails{Ocpus: new(float32(1))},
		SourceDetails: core.InstanceConfigurationInstanceSourceViaImageDetails{
			ImageId: e.imageID(t, "Oracle Linux", "9", false),
		},
	}

	cfg, err := e.mgmt.CreateInstanceConfiguration(t.Context(), core.CreateInstanceConfigurationRequest{
		CreateInstanceConfiguration: core.CreateInstanceConfigurationDetails{
			CompartmentId: new(testComp),
			InstanceDetails: core.ComputeInstanceDetails{
				LaunchDetails: l,
				SecondaryVnics: []core.InstanceConfigurationAttachVnicDetails{{
					DisplayName: new("sec"),
					CreateVnicDetails: &core.InstanceConfigurationCreateVnicDetails{
						SubnetId: f.private.Id,
					},
				}},
			},
		},
	})
	require.NoError(t, err)

	pool, err := e.mgmt.CreateInstancePool(t.Context(), core.CreateInstancePoolRequest{
		CompartmentId: new(testComp), InstanceConfigurationId: cfg.Id, Size: new(2),
		PlacementConfigurations: []core.CreateInstancePoolPlacementConfigurationDetails{
			{AvailabilityDomain: new(testAD), PrimarySubnetId: f.pub.Id},
		},
	})
	require.NoError(t, err)
	e.settle()

	d := lbAttach(9000)
	d.VnicSelection = new("sec")

	_, err = e.mgmt.AttachLoadBalancer(t.Context(), core.AttachLoadBalancerRequest{
		InstancePoolId: pool.Id, AttachLoadBalancerDetails: d,
	})
	require.NoError(t, err)

	got := fk.backends()
	require.Len(t, got, 2)

	for _, b := range got {
		assert.Contains(t, b, "10.0.2.", "backends are the secondary VNIC addresses")
	}
}

func TestPoolLoadBalancerRetriesAndGone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(f *fakeLB)
		wantState core.InstancePoolLoadBalancerAttachmentLifecycleStateEnum
	}{
		{
			name: "busy_stays_attaching", setup: func(f *fakeLB) { f.failed = true },
			wantState: core.InstancePoolLoadBalancerAttachmentLifecycleStateAttaching,
		},
		{
			name: "gone_stays_attaching", setup: func(f *fakeLB) { f.gone = true },
			wantState: core.InstancePoolLoadBalancerAttachmentLifecycleStateAttaching,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			fk := newFakeLB()
			e.backend.SetLoadBalancerClient(fk)
			pf := e.poolWithLB(t, 1, nil)
			fk.set(tt.setup)

			pool, err := e.mgmt.AttachLoadBalancer(t.Context(), core.AttachLoadBalancerRequest{
				InstancePoolId: pf.pool.Id, AttachLoadBalancerDetails: lbAttach(80),
			})
			require.NoError(t, err)

			require.NoError(t, e.backend.SyncLoadBalancers(t.Context()))

			got, err := e.mgmt.GetInstancePool(t.Context(), core.GetInstancePoolRequest{InstancePoolId: pf.pool.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, got.LoadBalancers[0].LifecycleState)

			fk.set(func(f *fakeLB) { f.failed, f.gone = false, false })
			require.NoError(t, e.backend.SyncLoadBalancers(t.Context()))

			got, err = e.mgmt.GetInstancePool(t.Context(), core.GetInstancePoolRequest{InstancePoolId: pf.pool.Id})
			require.NoError(t, err)
			assert.Equal(
				t,
				core.InstancePoolLoadBalancerAttachmentLifecycleStateAttached,
				got.LoadBalancers[0].LifecycleState,
			)
			assert.Equal(t, wantBackends(e.memberIPs(t, pool.Id), 80), fk.backends())
		})
	}
}

func TestPoolLoadBalancerDetachWhenTargetGone(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fk := newFakeLB()
	e.backend.SetLoadBalancerClient(fk)
	pf := e.poolWithLB(t, 1, []core.AttachLoadBalancerDetails{lbAttach(80)})

	fk.set(func(f *fakeLB) { f.gone = true })

	_, err := e.mgmt.DetachLoadBalancer(t.Context(), core.DetachLoadBalancerRequest{
		InstancePoolId: pf.pool.Id,
		LoadBalancerId: new(testLB), BackendSetName: new(testSet),
	})
	require.NoError(t, err)

	got, err := e.mgmt.GetInstancePool(t.Context(), core.GetInstancePoolRequest{InstancePoolId: pf.pool.Id})
	require.NoError(t, err)
	assert.Equal(t, core.InstancePoolLoadBalancerAttachmentLifecycleStateDetached, got.LoadBalancers[0].LifecycleState)
}

func TestSyncLoadBalancersHonoursContextAndClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(be *oci.InMemoryBackend) context.Context
		name string
	}{
		{name: "cancelled_ctx", stop: func(*oci.InMemoryBackend) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			return ctx
		}},
		{name: "closed_backend", stop: func(be *oci.InMemoryBackend) context.Context {
			be.Close()

			return context.Background()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			fk := newFakeLB()
			e.backend.SetLoadBalancerClient(fk)
			pf := e.poolWithLB(t, 1, nil)

			fk.set(func(f *fakeLB) { f.failed = true })

			_, err := e.mgmt.AttachLoadBalancer(t.Context(), core.AttachLoadBalancerRequest{
				InstancePoolId: pf.pool.Id, AttachLoadBalancerDetails: lbAttach(80),
			})
			require.NoError(t, err)

			fk.set(func(f *fakeLB) { f.failed = false; f.calls = 0 })

			err = e.backend.SyncLoadBalancers(tt.stop(e.backend))
			require.ErrorIs(t, err, context.Canceled)
			assert.Zero(t, fk.calls, "no load balancer call after cancellation")
			assert.Empty(t, fk.backends())
		})
	}
}

func TestPoolLoadBalancerSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fk := newFakeLB()
	e.backend.SetLoadBalancerClient(fk)
	pf := e.poolWithLB(t, 2, []core.AttachLoadBalancerDetails{lbAttach(80)})
	require.NotEmpty(t, fk.backends())

	snap := e.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	t.Cleanup(restored.Close)
	restored.SetLoadBalancerClient(fk)
	require.NoError(t, restored.Restore(t.Context(), snap))

	_, _, aerr := restored.DetachLoadBalancer(*pf.pool.Id, "", oci.DetachLoadBalancerIn{
		LoadBalancerID: testLB, BackendSetName: testSet,
	})
	require.Nil(t, aerr)
	assert.Empty(t, fk.backends(), "restored attachment remembers the backends it registered")

	a, aerr := restored.GetPoolLoadBalancer(*pf.pool.Id, *pf.pool.Id)
	require.NotNil(t, aerr)
	assert.Empty(t, a.ID)
}
