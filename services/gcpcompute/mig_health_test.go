package gcpcompute_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
)

const probeEvery = 5

type fakeProber struct {
	result atomic.Value
	mu     sync.Mutex
	probes []gcpcompute.HealthProbe
}

func (p *fakeProber) set(v string) { p.result.Store(v) }

func (p *fakeProber) Probe(_ context.Context, hp gcpcompute.HealthProbe) string {
	p.mu.Lock()
	p.probes = append(p.probes, hp)
	p.mu.Unlock()

	v, _ := p.result.Load().(string)

	return v
}

func (p *fakeProber) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.probes)
}

func (e *env) endpointTemplate(t *testing.T, name, endpoint string) string {
	t.Helper()

	spec := tmplSpec(name)
	if endpoint != "" {
		spec.Properties.Metadata = &computepb.Metadata{
			Items: []*computepb.Items{{Key: s(gcpcompute.HealthEndpointKey), Value: s(endpoint)}},
		}
	}

	op, err := e.templates(t).Insert(t.Context(), &computepb.InsertInstanceTemplateRequest{
		Project: project, InstanceTemplateResource: spec,
	})
	e.done(t, op, err)

	return globalTmplPath + name
}

func (e *env) httpCheck(t *testing.T, name string) string {
	t.Helper()

	op, err := e.healthChecks(t).Insert(t.Context(), &computepb.InsertHealthCheckRequest{
		Project: project, HealthCheckResource: &computepb.HealthCheck{
			Name: s(name), Type: s("HTTP"), CheckIntervalSec: proto.Int32(probeEvery), TimeoutSec: proto.Int32(probeEvery),
			HealthyThreshold: proto.Int32(2), UnhealthyThreshold: proto.Int32(2),
			HttpHealthCheck: &computepb.HTTPHealthCheck{Port: proto.Int32(80), RequestPath: s("/healthz")},
		},
	})
	e.done(t, op, err)

	return "projects/" + project + "/global/healthChecks/" + name
}

func (e *env) healingMig(
	t *testing.T, tmpl, hc string, delay int32, mut func(*computepb.InstanceGroupManager),
) {
	t.Helper()

	m := &computepb.InstanceGroupManager{
		Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(tmpl), TargetSize: proto.Int32(1),
		AutoHealingPolicies: []*computepb.InstanceGroupManagerAutoHealingPolicy{
			{HealthCheck: s(hc), InitialDelaySec: proto.Int32(delay)},
		},
	}
	if mut != nil {
		mut(m)
	}

	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: m,
	})
	e.done(t, op, err)
}

func (e *env) ticks(t *testing.T, n int) *computepb.ManagedInstance {
	t.Helper()

	var mi *computepb.ManagedInstance

	for range n {
		e.clock.Advance(probeEvery * time.Second)

		mis := e.managed(t)
		require.Len(t, mis, 1)

		mi = mis[0]
	}

	return mi
}

func TestAutohealingProbesRealEndpoint(t *testing.T) {
	t.Parallel()

	var healthy atomic.Bool

	healthy.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" || !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	e := newEnv(t)
	e.baseNet(t)
	tmpl := e.endpointTemplate(t, "tpl", srv.Listener.Addr().String())
	e.healingMig(t, tmpl, e.httpCheck(t, "hc"), 0, nil)

	first := e.managed(t)[0]
	assert.Equal(t, "VERIFYING", first.GetCurrentAction(), "no healthy probe yet")

	mi := e.ticks(t, 2)
	assert.Equal(t, "NONE", mi.GetCurrentAction())
	require.Len(t, mi.GetInstanceHealth(), 1)
	assert.Equal(t, "HEALTHY", mi.GetInstanceHealth()[0].GetDetailedHealthState())
	assert.True(t, e.getMig(t).GetStatus().GetIsStable())

	healthy.Store(false)

	mi = e.ticks(t, 1)
	assert.Equal(t, first.GetId(), mi.GetId(), "one failure is below the unhealthy threshold")

	mi = e.ticks(t, 1)
	assert.NotEqual(t, first.GetId(), mi.GetId(), "instance recreated after the unhealthy threshold")
	assert.Equal(t, first.GetName(), mi.GetName(), "autohealing recreates under the same name")
	assert.Equal(t, "RECREATING", mi.GetCurrentAction())

	healthy.Store(true)
	e.settle()

	mi = e.ticks(t, 2)
	assert.Equal(t, "NONE", mi.GetCurrentAction())
	assert.Equal(t, "HEALTHY", mi.GetInstanceHealth()[0].GetDetailedHealthState())
}

func TestAutohealingWithFakeProber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		result     string
		delay      int32
		lifecycle  string
		wantHealth string
		wantAction string
		recreated  bool
		probe      bool
	}{
		{name: "state only is healthy", wantHealth: "HEALTHY", wantAction: "NONE"},
		{name: "unhealthy repaired", probe: true, result: "UNHEALTHY", wantAction: "RECREATING", recreated: true},
		{name: "timeout repaired", probe: true, result: "TIMEOUT", wantAction: "RECREATING", recreated: true},
		{
			name: "initial delay ignores failures", probe: true, result: "UNHEALTHY", delay: 3600,
			wantHealth: "UNKNOWN", wantAction: "VERIFYING",
		},
		{
			name: "do nothing on failure", probe: true, result: "UNHEALTHY", lifecycle: "DO_NOTHING",
			wantHealth: "UNHEALTHY", wantAction: "VERIFYING",
		},
		{name: "healthy probe", probe: true, result: "HEALTHY", wantHealth: "HEALTHY", wantAction: "NONE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			fp := &fakeProber{}
			fp.set(tt.result)
			e.backend.SetHealthProber(fp)

			endpoint := ""
			if tt.probe {
				endpoint = "127.0.0.1:9"
			}

			tmpl := e.endpointTemplate(t, "tpl", endpoint)
			e.healingMig(t, tmpl, e.httpCheck(t, "hc"), tt.delay, func(m *computepb.InstanceGroupManager) {
				if tt.lifecycle != "" {
					m.InstanceLifecyclePolicy = &computepb.InstanceGroupManagerInstanceLifecyclePolicy{
						OnFailedHealthCheck: s(tt.lifecycle),
					}
				}
			})

			id := e.managed(t)[0].GetId()
			mi := e.ticks(t, 3)

			assert.Equal(t, tt.wantAction, mi.GetCurrentAction())
			assert.Equal(t, tt.recreated, mi.GetId() != id)

			if tt.wantHealth != "" {
				require.Len(t, mi.GetInstanceHealth(), 1)
				assert.Equal(t, tt.wantHealth, mi.GetInstanceHealth()[0].GetDetailedHealthState())
			}

			if tt.probe {
				assert.Positive(t, fp.count())
				assert.Equal(t, "127.0.0.1:9", fp.probes[0].Address)
				assert.Equal(t, "/healthz", fp.probes[0].Path)
			} else {
				assert.Zero(t, fp.count(), "state-only instances are never probed")
			}
		})
	}
}

func (e *env) liveTemplates(t *testing.T) (old, fresh int, total int, names map[string]bool) {
	t.Helper()

	names = map[string]bool{}

	for _, mi := range e.managed(t) {
		if mi.GetCurrentAction() == "DELETING" {
			continue
		}

		total++
		names[mi.GetName()] = true

		if trimToPath(mi.GetVersion().GetInstanceTemplate()) == globalTmplPath+"v1" {
			old++
		} else {
			fresh++
		}
	}

	return old, fresh, total, names
}

func TestRollingUpdatePacing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		surge      int32
		unavail    int32
		maxTotal   int
		keepsNames bool
	}{
		{name: "substitute surge one", method: "SUBSTITUTE", surge: 1, unavail: 0, maxTotal: 5},
		{name: "substitute surge two", method: "SUBSTITUTE", surge: 2, unavail: 1, maxTotal: 6},
		{name: "recreate one at a time", method: "RECREATE", surge: 0, unavail: 1, maxTotal: 4, keepsNames: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.template(t, "v1")
			e.template(t, "v2")

			op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
					Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(globalTmplPath + "v1"),
					TargetSize: proto.Int32(4),
					UpdatePolicy: &computepb.InstanceGroupManagerUpdatePolicy{
						Type: s("PROACTIVE"), ReplacementMethod: s(tt.method),
						MaxSurge: &computepb.FixedOrPercent{Fixed: proto.Int32(tt.surge)},
						MaxUnavailable: &computepb.FixedOrPercent{Fixed: proto.Int32(tt.unavail)},
					},
				},
			})
			e.done(t, op, err)

			_, _, _, before := e.liveTemplates(t)

			op, err = e.migs(t).SetInstanceTemplate(t.Context(), &computepb.SetInstanceTemplateInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManager: "web",
				InstanceGroupManagersSetInstanceTemplateRequestResource: setTmplReq(globalTmplPath + "v2"),
			})
			require.NoError(t, err)
			require.NotNil(t, op)

			old, fresh, total, _ := e.liveTemplates(t)
			assert.Positive(t, old, "the roll is paced, not instantaneous")
			assert.LessOrEqual(t, total, tt.maxTotal)

			for i := 0; i < 20 && old > 0; i++ {
				e.clock.Advance(time.Minute)

				old, fresh, total, _ = e.liveTemplates(t)
				assert.LessOrEqual(t, total, tt.maxTotal)
			}

			e.clock.Advance(time.Minute)

			old, fresh, total, after := e.liveTemplates(t)
			assert.Zero(t, old)
			assert.Equal(t, 4, fresh)
			assert.Equal(t, 4, total)

			if tt.keepsNames {
				assert.Equal(t, before, after)
			} else {
				for n := range after {
					assert.False(t, before[n], "SUBSTITUTE uses new names")
				}
			}
		})
	}
}

func TestRollingUpdateKeepsAvailability(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.template(t, "v1")
	e.template(t, "v2")

	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(globalTmplPath + "v1"),
			TargetSize: proto.Int32(3),
			UpdatePolicy: &computepb.InstanceGroupManagerUpdatePolicy{
				Type:           s("PROACTIVE"),
				MaxSurge:       &computepb.FixedOrPercent{Fixed: proto.Int32(1)},
				MaxUnavailable: &computepb.FixedOrPercent{Fixed: proto.Int32(0)},
			},
		},
	})
	e.done(t, op, err)

	_, err = e.migs(t).SetInstanceTemplate(t.Context(), &computepb.SetInstanceTemplateInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagersSetInstanceTemplateRequestResource: setTmplReq(globalTmplPath + "v2"),
	})
	require.NoError(t, err)

	for range 30 {
		running := 0

		for _, mi := range e.managed(t) {
			if mi.GetInstanceStatus() == "RUNNING" && mi.GetCurrentAction() != "DELETING" {
				running++
			}
		}

		assert.GreaterOrEqual(t, running, 3, "maxUnavailable 0 never drops below the target size")

		old, _, _, _ := e.liveTemplates(t)
		if old == 0 {
			return
		}

		e.clock.Advance(time.Minute)
	}

	t.Fatal("rolling update did not finish")
}

func TestApplyUpdatesToInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		minimal     string
		mostAllowed string
		wantFresh   int
		wantErr     bool
	}{
		{name: "replaces stale instance", wantFresh: 1},
		{name: "blocked by most disruptive", mostAllowed: "RESTART", wantFresh: 0, wantErr: true},
		{name: "refresh minimal still replaces stale", minimal: "REFRESH", wantFresh: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.template(t, "v1")
			e.template(t, "v2")
			e.mig(t, "web", globalTmplPath+"v1", 2)

			op, err := e.migs(t).SetInstanceTemplate(t.Context(), &computepb.SetInstanceTemplateInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManager: "web",
				InstanceGroupManagersSetInstanceTemplateRequestResource: setTmplReq(globalTmplPath + "v2"),
			})
			e.done(t, op, err)

			victim := e.managed(t)[0]

			op, err = e.migs(t).ApplyUpdatesToInstances(t.Context(), &computepb.ApplyUpdatesToInstancesInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManager: "web",
				InstanceGroupManagersApplyUpdatesRequestResource: &computepb.InstanceGroupManagersApplyUpdatesRequest{
					Instances: []string{victim.GetInstance()}, MinimalAction: s(tt.minimal),
					MostDisruptiveAllowedAction: s(tt.mostAllowed),
				},
			})
			e.done(t, op, err)

			_, fresh, _, _ := e.liveTemplates(t)
			assert.Equal(t, tt.wantFresh, fresh, "fresh count after apply")

			got := e.managed(t)
			var victimNow *computepb.ManagedInstance

			for _, mi := range got {
				if mi.GetName() == victim.GetName() {
					victimNow = mi
				}
			}

			require.NotNil(t, victimNow)

			if tt.wantErr {
				assert.NotEmpty(t, victimNow.GetLastAttempt().GetErrors().GetErrors())
				assert.Equal(t, victim.GetId(), victimNow.GetId())
			} else {
				assert.NotEqual(t, victim.GetId(), victimNow.GetId())
				assert.Empty(t, victimNow.GetLastAttempt().GetErrors().GetErrors())
			}
		})
	}
}
