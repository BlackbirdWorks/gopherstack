package gcpcompute_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func (e *env) webMIG(t *testing.T, size int32, policy *computepb.StatefulPolicy) {
	t.Helper()
	e.baseNet(t)

	tmpl := e.template(t, "tpl")
	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(tmpl), TargetSize: new(size),
			StatefulPolicy: policy,
		},
	})
	e.done(t, op, err)
}

func (e *env) migInstances(t *testing.T) []string {
	t.Helper()

	managed := e.managed(t)
	out := make([]string, 0, len(managed))

	for _, mi := range managed {
		out = append(out, mi.GetName())
	}

	return out
}

func (e *env) instRef(name string) string { return "zones/" + zone + "/instances/" + name }

func (e *env) migOp(t *testing.T, op *compute.Operation, err error) error {
	t.Helper()

	return e.finish(t, op, err)
}

func TestMIGPowerOps(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 3, nil)
	e.settle()

	names := e.migInstances(t)
	require.Len(t, names, 3)

	stop := func(ns ...string) error {
		refs := make([]string, len(ns))
		for i, n := range ns {
			refs[i] = e.instRef(n)
		}

		op, err := e.migs(t).StopInstances(t.Context(), &computepb.StopInstancesInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web",
			InstanceGroupManagersStopInstancesRequestResource: &computepb.InstanceGroupManagersStopInstancesRequest{
				Instances: refs,
			},
		})

		return e.migOp(t, op, err)
	}

	require.NoError(t, stop(names[0]))
	m := e.getMig(t)
	assert.Equal(t, int32(2), m.GetTargetSize())
	assert.Equal(t, int32(1), m.GetTargetStoppedSize())
	assert.Equal(t, "TERMINATED", e.getVM(t, names[0]).GetStatus())

	for _, mi := range e.managed(t) {
		if mi.GetName() == names[0] {
			assert.Equal(t, "STOPPED", mi.GetInstanceStatus())
			assert.Equal(t, "NONE", mi.GetCurrentAction())
		}
	}

	assert.True(t, e.getMig(t).GetStatus().GetIsStable())
	assertAPIError(t, stop(names[0]), http.StatusBadRequest, "")
	assertAPIError(t, stop("web-nope"), http.StatusBadRequest, "")

	op, err := e.migs(t).StartInstances(t.Context(), &computepb.StartInstancesInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagersStartInstancesRequestResource: &computepb.InstanceGroupManagersStartInstancesRequest{
			Instances: []string{e.instRef(names[0])},
		},
	})
	require.NoError(t, e.migOp(t, op, err))

	m = e.getMig(t)
	assert.Equal(t, int32(3), m.GetTargetSize())
	assert.Equal(t, int32(0), m.GetTargetStoppedSize())
	assert.Equal(t, "RUNNING", e.getVM(t, names[0]).GetStatus())
}

func TestMIGSuspendResume(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 2, nil)
	e.settle()

	names := e.migInstances(t)
	ref := []string{e.instRef(names[1])}

	op, err := e.migs(t).SuspendInstances(t.Context(), &computepb.SuspendInstancesInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagersSuspendInstancesRequestResource: &computepb.InstanceGroupManagersSuspendInstancesRequest{
			Instances: ref,
		},
	})
	require.NoError(t, e.migOp(t, op, err))

	m := e.getMig(t)
	assert.Equal(t, int32(1), m.GetTargetSize())
	assert.Equal(t, int32(1), m.GetTargetSuspendedSize())
	assert.Equal(t, "SUSPENDED", e.getVM(t, names[1]).GetStatus())

	_, err = e.migs(t).StartInstances(t.Context(), &computepb.StartInstancesInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagersStartInstancesRequestResource: &computepb.InstanceGroupManagersStartInstancesRequest{
			Instances: ref,
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "")

	op, err = e.migs(t).ResumeInstances(t.Context(), &computepb.ResumeInstancesInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagersResumeInstancesRequestResource: &computepb.InstanceGroupManagersResumeInstancesRequest{
			Instances: ref,
		},
	})
	require.NoError(t, e.migOp(t, op, err))
	assert.Equal(t, int32(2), e.getMig(t).GetTargetSize())
	assert.Equal(t, "RUNNING", e.getVM(t, names[1]).GetStatus())
}

func meta(kv map[string]string) *computepb.PreservedState {
	return &computepb.PreservedState{Metadata: kv}
}

func (e *env) listConfigs(t *testing.T) []*computepb.PerInstanceConfig {
	t.Helper()

	it := e.migs(t).ListPerInstanceConfigs(t.Context(), &computepb.ListPerInstanceConfigsInstanceGroupManagersRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	})

	var out []*computepb.PerInstanceConfig

	for {
		c, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, c)
	}
}

func (e *env) upsertConfig(t *testing.T, patch bool, cfg *computepb.PerInstanceConfig) error {
	t.Helper()

	var (
		op  *compute.Operation
		err error
	)

	if patch {
		body := &computepb.InstanceGroupManagersPatchPerInstanceConfigsReq{
			PerInstanceConfigs: []*computepb.PerInstanceConfig{cfg},
		}
		req := &computepb.PatchPerInstanceConfigsInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web",
		}
		req.InstanceGroupManagersPatchPerInstanceConfigsReqResource = body
		op, err = e.migs(t).PatchPerInstanceConfigs(t.Context(), req)
	} else {
		op, err = e.migs(t).UpdatePerInstanceConfigs(t.Context(), updateCfgRequest(cfg))
	}

	return e.migOp(t, op, err)
}

func TestPerInstanceConfigs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 2, nil)
	e.settle()

	names := e.migInstances(t)
	assert.False(t, e.getMig(t).GetStatus().GetStateful().GetHasStatefulConfig())

	require.NoError(t, e.upsertConfig(t, false, &computepb.PerInstanceConfig{
		Name: s(names[0]), PreservedState: meta(map[string]string{"role": "primary", "tier": "a"}),
	}))

	cfgs := e.listConfigs(t)
	require.Len(t, cfgs, 1)
	assert.Equal(t, names[0], cfgs[0].GetName())
	assert.Equal(t, "EFFECTIVE", cfgs[0].GetStatus())
	assert.NotEmpty(t, cfgs[0].GetFingerprint())
	assert.True(t, e.getMig(t).GetStatus().GetStateful().GetHasStatefulConfig())
	assert.True(t, e.getMig(t).GetStatus().GetStateful().GetPerInstanceConfigs().GetAllEffective())

	md := map[string]string{}
	for _, it := range e.getVM(t, names[0]).GetMetadata().GetItems() {
		md[it.GetKey()] = it.GetValue()
	}

	assert.Equal(t, "primary", md["role"])

	require.NoError(t, e.upsertConfig(t, true, &computepb.PerInstanceConfig{
		Name: s(names[0]), PreservedState: meta(map[string]string{"tier": "b", "extra": "x"}),
	}))
	assert.Equal(
		t,
		map[string]string{"role": "primary", "tier": "b", "extra": "x"},
		e.listConfigs(t)[0].GetPreservedState().GetMetadata(),
	)

	require.NoError(t, e.upsertConfig(t, false, &computepb.PerInstanceConfig{
		Name: s(names[0]), PreservedState: meta(map[string]string{"only": "1"}),
	}))
	assert.Equal(t, map[string]string{"only": "1"}, e.listConfigs(t)[0].GetPreservedState().GetMetadata())

	assertAPIError(t, e.upsertConfig(t, false, &computepb.PerInstanceConfig{
		Name: s(names[0]), Fingerprint: s("stale"), PreservedState: meta(nil),
	}), http.StatusPreconditionFailed, "")
	assertAPIError(t, e.upsertConfig(t, false, &computepb.PerInstanceConfig{
		Name: s("web-ghost"), PreservedState: meta(nil),
	}), http.StatusBadRequest, "")
	assertAPIError(t, e.upsertConfig(t, false, &computepb.PerInstanceConfig{
		Name: s(names[1]), PreservedState: &computepb.PreservedState{
			Disks: map[string]*computepb.PreservedStatePreservedDisk{"d": {Source: s("zones/" + zone + "/disks/none")}},
		},
	}), http.StatusNotFound, "")

	del := func(n string) error {
		req := &computepb.DeletePerInstanceConfigsInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web",
		}
		body := &computepb.InstanceGroupManagersDeletePerInstanceConfigsReq{Names: []string{n}}
		req.InstanceGroupManagersDeletePerInstanceConfigsReqResource = body
		op, err := e.migs(t).DeletePerInstanceConfigs(t.Context(), req)

		return e.migOp(t, op, err)
	}

	assertAPIError(t, del("web-ghost"), http.StatusNotFound, "")
	require.NoError(t, del(names[0]))
	assert.Empty(t, e.listConfigs(t))
}

func TestPerInstanceConfigStatusOnClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 1, nil)
	e.settle()

	name := e.migInstances(t)[0]

	op, err := e.migs(t).UpdatePerInstanceConfigs(t.Context(), updateCfgRequest(
		&computepb.PerInstanceConfig{Name: s(name), PreservedState: meta(map[string]string{"a": "b"})},
	))
	require.NoError(t, err)
	require.NotNil(t, op)

	assert.Equal(t, "APPLYING", e.listConfigs(t)[0].GetStatus())
	assert.False(t, e.getMig(t).GetStatus().GetStateful().GetPerInstanceConfigs().GetAllEffective())

	e.clock.Advance(time.Minute)
	assert.Equal(t, "EFFECTIVE", e.listConfigs(t)[0].GetStatus())
}

func TestCreateInstancesWithConfigs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 1, nil)
	e.settle()

	create := func(names ...string) error {
		cfgs := make([]*computepb.PerInstanceConfig, len(names))
		for i, n := range names {
			cfgs[i] = &computepb.PerInstanceConfig{Name: s(n), PreservedState: meta(map[string]string{"owner": n})}
		}

		op, err := e.migs(t).CreateInstances(t.Context(), &computepb.CreateInstancesInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web",
			InstanceGroupManagersCreateInstancesRequestResource: &computepb.InstanceGroupManagersCreateInstancesRequest{
				Instances: cfgs,
			},
		})

		return e.migOp(t, op, err)
	}

	require.NoError(t, create("named-1", "named-2"))
	assert.Equal(t, int32(3), e.getMig(t).GetTargetSize())
	assert.Contains(t, e.migInstances(t), "named-1")
	assert.Len(t, e.listConfigs(t), 2)

	md := map[string]string{}
	for _, it := range e.getVM(t, "named-2").GetMetadata().GetItems() {
		md[it.GetKey()] = it.GetValue()
	}

	assert.Equal(t, "named-2", md["owner"])

	assertAPIError(t, create("named-1"), http.StatusConflict, "")
	assertAPIError(t, create("Bad_Name"), http.StatusBadRequest, "")
	assertAPIError(t, create("fresh", "fresh"), http.StatusConflict, "")
	assert.Equal(t, int32(3), e.getMig(t).GetTargetSize())
	assert.NotContains(t, e.migInstances(t), "fresh")
}

func bootDiskID(t *testing.T, e *env, vm string) uint64 {
	t.Helper()

	v := e.getVM(t, vm)
	d, err := e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{
		Project: project, Zone: zone, Disk: lastSeg(v.GetDisks()[0].GetSource()),
	})
	require.NoError(t, err)

	return d.GetId()
}

func lastSeg(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}

	return s
}

func TestStatefulPolicyKeepsDisks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		autoDelete string
		wantGone   bool
	}{
		{name: "never", autoDelete: "NEVER", wantGone: false},
		{name: "on permanent deletion", autoDelete: "ON_PERMANENT_INSTANCE_DELETION", wantGone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.webMIG(t, 1, &computepb.StatefulPolicy{PreservedState: &computepb.StatefulPolicyPreservedState{
				Disks: map[string]*computepb.StatefulPolicyPreservedStateDiskDevice{
					"persistent-disk-0": {AutoDelete: s(tt.autoDelete)},
				},
			}})
			e.settle()

			assert.True(t, e.getMig(t).GetStatus().GetStateful().GetHasStatefulConfig())

			name := e.migInstances(t)[0]
			before := bootDiskID(t, e, name)

			require.NoError(t, e.recreateMember(t, name))
			assert.Equal(t, before, bootDiskID(t, e, name), "the boot disk survives a recreate")

			require.NoError(t, e.deleteMember(t, name))

			_, err := e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: name})
			if tt.wantGone {
				assertAPIError(t, err, http.StatusNotFound, "")

				return
			}

			require.NoError(t, err, "autoDelete NEVER keeps the disk after the instance is deleted")
		})
	}
}

func TestStatefulPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy *computepb.StatefulPolicy
		name   string
	}{
		{
			name: "bad autodelete",
			policy: &computepb.StatefulPolicy{PreservedState: &computepb.StatefulPolicyPreservedState{
				Disks: map[string]*computepb.StatefulPolicyPreservedStateDiskDevice{"d": {AutoDelete: s("SOMETIMES")}},
			}},
		},
		{name: "bad nic key", policy: &computepb.StatefulPolicy{PreservedState: &computepb.StatefulPolicyPreservedState{
			InternalIPs: map[string]*computepb.StatefulPolicyPreservedStateNetworkIp{"eth0": {AutoDelete: s("NEVER")}},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			tmpl := e.template(t, "tpl")
			_, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
					Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(tmpl), StatefulPolicy: tt.policy,
				},
			})
			assertAPIError(t, err, http.StatusBadRequest, "")
		})
	}
}

func TestStatefulPolicyKeepsInternalIP(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 1, &computepb.StatefulPolicy{PreservedState: &computepb.StatefulPolicyPreservedState{
		InternalIPs: map[string]*computepb.StatefulPolicyPreservedStateNetworkIp{"nic0": {}},
	}})
	e.settle()

	name := e.migInstances(t)[0]
	ip := e.getVM(t, name).GetNetworkInterfaces()[0].GetNetworkIP()

	require.NoError(t, e.recreateMember(t, name))
	assert.Equal(t, ip, e.getVM(t, name).GetNetworkInterfaces()[0].GetNetworkIP())
}

func (e *env) resizeClient(t *testing.T) *compute.InstanceGroupManagerResizeRequestsClient {
	t.Helper()

	return client(t, e, compute.NewInstanceGroupManagerResizeRequestsRESTClient)
}

func (e *env) resizeInsert(t *testing.T, rr *computepb.InstanceGroupManagerResizeRequest) error {
	t.Helper()

	_, err := e.resizeClient(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", InstanceGroupManagerResizeRequestResource: rr,
	})

	return err
}

func (e *env) resizeGet(t *testing.T, name string) *computepb.InstanceGroupManagerResizeRequest {
	t.Helper()

	rr, err := e.resizeClient(t).Get(t.Context(), &computepb.GetInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", ResizeRequest: name,
	})
	require.NoError(t, err)

	return rr
}

func TestResizeRequestLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 1, nil)
	e.settle()

	require.NoError(
		t,
		e.resizeInsert(t, &computepb.InstanceGroupManagerResizeRequest{Name: s("rr1"), ResizeBy: new(int32(2))}),
	)

	rr := e.resizeGet(t, "rr1")
	assert.Equal(t, "ACCEPTED", rr.GetState())
	assert.Equal(t, int32(2), rr.GetResizeBy())
	assert.Contains(t, rr.GetSelfLink(), "/instanceGroupManagers/web/resizeRequests/rr1")
	assert.Contains(t, rr.GetZone(), "/zones/"+zone)
	assert.Equal(t, int32(1), e.getMig(t).GetTargetSize())

	e.settle()
	assert.Equal(t, "SUCCEEDED", e.resizeGet(t, "rr1").GetState())
	assert.Equal(t, int32(3), e.getMig(t).GetTargetSize())
	assert.Len(t, e.migInstances(t), 3)

	it := e.resizeClient(t).List(t.Context(), &computepb.ListInstanceGroupManagerResizeRequestsRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	})
	item, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "rr1", item.GetName())

	op, err := e.resizeClient(t).Delete(t.Context(), &computepb.DeleteInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", ResizeRequest: "rr1",
	})
	e.done(t, op, err)

	_, err = e.resizeClient(t).Get(t.Context(), &computepb.GetInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", ResizeRequest: "rr1",
	})
	assertAPIError(t, err, http.StatusNotFound, "")
	assert.Equal(t, int32(3), e.getMig(t).GetTargetSize(), "deleting a request keeps its instances")
}

func TestResizeRequestCancelAndDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 0, nil)
	require.NoError(
		t,
		e.resizeInsert(t, &computepb.InstanceGroupManagerResizeRequest{Name: s("rr"), ResizeBy: new(int32(1))}),
	)

	_, err := e.resizeClient(t).Delete(t.Context(), &computepb.DeleteInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", ResizeRequest: "rr",
	})
	assertAPIError(t, err, http.StatusBadRequest, "")

	op, err := e.resizeClient(t).Cancel(t.Context(), &computepb.CancelInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", ResizeRequest: "rr",
	})
	e.done(t, op, err)
	assert.Equal(t, "CANCELLED", e.resizeGet(t, "rr").GetState())
	assert.Equal(t, int32(0), e.getMig(t).GetTargetSize(), "a cancelled request never provisions")
	assert.Empty(t, e.migInstances(t))

	_, err = e.resizeClient(t).Cancel(t.Context(), &computepb.CancelInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", ResizeRequest: "rr",
	})
	assertAPIError(t, err, http.StatusBadRequest, "")

	op, err = e.resizeClient(t).Delete(t.Context(), &computepb.DeleteInstanceGroupManagerResizeRequestRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", ResizeRequest: "rr",
	})
	e.done(t, op, err)
}

func TestResizeRequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rr     *computepb.InstanceGroupManagerResizeRequest
		name   string
		status int
	}{
		{
			name:   "neither",
			rr:     &computepb.InstanceGroupManagerResizeRequest{Name: s("rr")},
			status: http.StatusBadRequest,
		},
		{
			name: "both", status: http.StatusBadRequest,
			rr: &computepb.InstanceGroupManagerResizeRequest{
				Name: s("rr"), ResizeBy: new(int32(1)), Instances: []*computepb.PerInstanceConfig{{Name: s("a")}},
			},
		},
		{
			name:   "bad name",
			rr:     &computepb.InstanceGroupManagerResizeRequest{Name: s("A_b"), ResizeBy: new(int32(1))},
			status: http.StatusBadRequest,
		},
		{name: "ok", rr: &computepb.InstanceGroupManagerResizeRequest{Name: s("rr"), ResizeBy: new(int32(1))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.webMIG(t, 0, nil)

			err := e.resizeInsert(t, tt.rr)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)
			assertAPIError(t, e.resizeInsert(t, tt.rr), http.StatusConflict, "")
		})
	}
}

func TestResizeRequestNamedInstancesAndRunDuration(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.webMIG(t, 0, nil)

	require.NoError(t, e.resizeInsert(t, &computepb.InstanceGroupManagerResizeRequest{
		Name:                 s("rr"),
		Instances:            []*computepb.PerInstanceConfig{{Name: s("flex-1")}, {Name: s("flex-2")}},
		RequestedRunDuration: &computepb.Duration{Seconds: new(int64(600))},
	}))
	e.clock.Advance(time.Second)

	assert.Equal(t, "SUCCEEDED", e.resizeGet(t, "rr").GetState())
	assert.ElementsMatch(t, []string{"flex-1", "flex-2"}, e.migInstances(t))
	assert.Equal(t, int32(2), e.getMig(t).GetTargetSize())

	e.clock.Advance(11 * time.Minute)
	assert.Equal(t, int32(0), e.getMig(t).GetTargetSize(), "the run duration ended")
	assert.Empty(t, e.vmNames(t))
}

func TestRegionalResizeRequest(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	tmpl := e.template(t, "tpl")
	op, err := e.regionMigs(t).Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerRequest{
		Project: project, Region: region, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("rweb"), BaseInstanceName: s("rweb"), InstanceTemplate: s(tmpl),
		},
	})
	e.done(t, op, err)

	c := client(t, e, compute.NewRegionInstanceGroupManagerResizeRequestsRESTClient)
	op, err = c.Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerResizeRequestRequest{
		Project: project, Region: region, InstanceGroupManager: "rweb",
		InstanceGroupManagerResizeRequestResource: &computepb.InstanceGroupManagerResizeRequest{
			Name: s("rr"), ResizeBy: new(int32(2)),
		},
	})
	e.done(t, op, err)

	rr, err := c.Get(t.Context(), &computepb.GetRegionInstanceGroupManagerResizeRequestRequest{
		Project: project, Region: region, InstanceGroupManager: "rweb", ResizeRequest: "rr",
	})
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", rr.GetState())
	assert.Contains(t, rr.GetRegion(), "/regions/"+region)

	m, err := e.regionMigs(t).Get(t.Context(), &computepb.GetRegionInstanceGroupManagerRequest{
		Project: project, Region: region, InstanceGroupManager: "rweb",
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), m.GetTargetSize())
}

func (e *env) webMIGOnSub(t *testing.T) {
	t.Helper()

	tmpl := e.template(t, "tpl")
	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(tmpl),
		},
	})
	e.done(t, op, err)
}

func updateCfgRequest(cfg *computepb.PerInstanceConfig) *computepb.UpdatePerInstanceConfigsInstanceGroupManagerRequest {
	req := &computepb.UpdatePerInstanceConfigsInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	}
	body := &computepb.InstanceGroupManagersUpdatePerInstanceConfigsReq{
		PerInstanceConfigs: []*computepb.PerInstanceConfig{cfg},
	}
	req.InstanceGroupManagersUpdatePerInstanceConfigsReqResource = body

	return req
}

func (e *env) recreateMember(t *testing.T, name string) error {
	t.Helper()

	req := &computepb.RecreateInstancesInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	}
	req.InstanceGroupManagersRecreateInstancesRequestResource = &computepb.InstanceGroupManagersRecreateInstancesRequest{
		Instances: []string{e.instRef(name)},
	}
	op, err := e.migs(t).RecreateInstances(t.Context(), req)

	return e.migOp(t, op, err)
}

func (e *env) deleteMember(t *testing.T, name string) error {
	t.Helper()

	req := &computepb.DeleteInstancesInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	}
	req.InstanceGroupManagersDeleteInstancesRequestResource = &computepb.InstanceGroupManagersDeleteInstancesRequest{
		Instances: []string{e.instRef(name)},
	}
	op, err := e.migs(t).DeleteInstances(t.Context(), req)

	return e.migOp(t, op, err)
}
