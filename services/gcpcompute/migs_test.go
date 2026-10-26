package gcpcompute_test

import (
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func TestMIGLifecycleOnClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	tmpl := e.template(t, "tpl")

	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(tmpl), TargetSize: proto.Int32(3),
			NamedPorts: []*computepb.NamedPort{{Name: s("http"), Port: proto.Int32(80)}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, op)

	m := e.getMig(t, "web")
	assert.Equal(t, int32(3), m.GetTargetSize())
	assert.Equal(t, "web", m.GetBaseInstanceName())
	assert.Contains(t, m.GetInstanceGroup(), "/zones/"+zone+"/instanceGroups/web")
	assert.Equal(t, tmpl, trimToPath(m.GetInstanceTemplate()))
	assert.False(t, m.GetStatus().GetIsStable(), "instances still provisioning")
	assert.Equal(t, int32(3), m.GetCurrentActions().GetCreating())
	require.Len(t, m.GetVersions(), 1)
	assert.Equal(t, int32(3), m.GetVersions()[0].GetTargetSize().GetCalculated())

	names := e.vmNames(t)
	require.Len(t, names, 3)
	assert.True(t, hasPrefixAll(names, "web-"))

	for _, n := range names {
		assert.Len(t, n, len("web-")+4)
	}

	e.settle()

	m = e.getMig(t, "web")
	assert.True(t, m.GetStatus().GetIsStable())
	assert.True(t, m.GetStatus().GetVersionTarget().GetIsReached())
	assert.Equal(t, int32(3), m.GetCurrentActions().GetNone())

	mis := e.managed(t, "web")
	require.Len(t, mis, 3)

	for _, mi := range mis {
		assert.Equal(t, "NONE", mi.GetCurrentAction())
		assert.Equal(t, "RUNNING", mi.GetInstanceStatus())
		assert.Contains(t, mi.GetInstance(), "/zones/"+zone+"/instances/web-")
		assert.Equal(t, tmpl, trimToPath(mi.GetVersion().GetInstanceTemplate()))
	}

	vm := e.getVM(t, names[0])
	assert.Equal(t, "test", vm.GetLabels()["env"])
	assert.Contains(t, metadataValue(vm, "created-by"), "/instanceGroupManagers/web")

	op, err = e.migs(t).Resize(t.Context(), &computepb.ResizeInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", Size: 1,
	})
	e.done(t, op, err)

	e.settle()
	assert.Len(t, e.vmNames(t), 1)
	assert.Len(t, e.managed(t, "web"), 1)
}

func TestMIGResizeTransitions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.mig(t, "web", e.template(t, "tpl"), 2)
	e.settle()

	_, err := e.migs(t).Resize(t.Context(), &computepb.ResizeInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", Size: 1,
	})
	require.NoError(t, err)

	mis := e.managed(t, "web")
	require.Len(t, mis, 2)

	actions := []string{mis[0].GetCurrentAction(), mis[1].GetCurrentAction()}
	sort.Strings(actions)
	assert.Equal(t, []string{"DELETING", "NONE"}, actions)
	assert.False(t, e.getMig(t, "web").GetStatus().GetIsStable())
	assert.Len(t, e.vmNames(t), 1)

	e.clock.Advance(gcpcomputeTiming().Stop)

	assert.Len(t, e.managed(t, "web"), 1)
	assert.True(t, e.getMig(t, "web").GetStatus().GetIsStable())

	_, err = e.migs(t).Resize(t.Context(), &computepb.ResizeInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web", Size: 4,
	})
	require.NoError(t, err)
	assert.Len(t, e.vmNames(t), 4)

	e.clock.Advance(time.Second)
	assert.True(t, e.getMig(t, "web").GetStatus().GetIsStable())
}

func TestMIGValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(m *computepb.InstanceGroupManager)
		name   string
		reason string
		status int
	}{
		{name: "no base name", mod: func(m *computepb.InstanceGroupManager) { m.BaseInstanceName = nil }, status: 400, reason: "required"},
		{name: "bad base name", mod: func(m *computepb.InstanceGroupManager) { m.BaseInstanceName = s("Web_X") }, status: 400, reason: "invalid"},
		{
			name: "long base name", status: 400, reason: "invalid",
			mod: func(m *computepb.InstanceGroupManager) { m.BaseInstanceName = s(strings.Repeat("a", 59)) },
		},
		{name: "no template", mod: func(m *computepb.InstanceGroupManager) { m.InstanceTemplate = nil }, status: 400, reason: "required"},
		{
			name: "missing template", status: 404, reason: "notFound",
			mod: func(m *computepb.InstanceGroupManager) { m.InstanceTemplate = s(globalTmplPath + "none") },
		},
		{name: "negative size", mod: func(m *computepb.InstanceGroupManager) { m.TargetSize = proto.Int32(-1) }, status: 400, reason: "invalid"},
		{
			name: "template and versions", status: 400, reason: "invalid",
			mod: func(m *computepb.InstanceGroupManager) {
				m.Versions = []*computepb.InstanceGroupManagerVersion{{InstanceTemplate: s(globalTmplPath + "other")}}
			},
		},
		{
			name: "duplicate named port", status: 400, reason: "invalid",
			mod: func(m *computepb.InstanceGroupManager) {
				m.NamedPorts = []*computepb.NamedPort{{Name: s("a"), Port: proto.Int32(1)}, {Name: s("a"), Port: proto.Int32(2)}}
			},
		},
		{
			name: "unknown health check", status: 404, reason: "notFound",
			mod: func(m *computepb.InstanceGroupManager) {
				m.AutoHealingPolicies = []*computepb.InstanceGroupManagerAutoHealingPolicy{{HealthCheck: s("global/healthChecks/none")}}
			},
		},
		{
			name: "distribution on zonal", status: 400, reason: "invalid",
			mod: func(m *computepb.InstanceGroupManager) {
				m.DistributionPolicy = &computepb.DistributionPolicy{TargetShape: s("EVEN")}
			},
		},
		{
			name: "bad update policy", status: 400, reason: "invalid",
			mod: func(m *computepb.InstanceGroupManager) {
				m.UpdatePolicy = &computepb.InstanceGroupManagerUpdatePolicy{Type: s("NOPE")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			e.template(t, "other")

			spec := &computepb.InstanceGroupManager{
				Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(e.template(t, "tpl")),
			}
			tt.mod(spec)

			_, err := e.migs(t).Insert(t.Context(),
				&computepb.InsertInstanceGroupManagerRequest{Project: project, Zone: zone, InstanceGroupManagerResource: spec})
			assertAPIError(t, err, tt.status, tt.reason)
			assert.Empty(t, e.vmNames(t))
		})
	}

	t.Run("duplicate and unknown zone", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.baseNet(t)
		e.mig(t, "web", e.template(t, "tpl"), 0)

		_, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
				Name: s("web"), BaseInstanceName: s("w"), InstanceTemplate: s(globalTmplPath + "tpl"),
			},
		})
		assertAPIError(t, err, http.StatusConflict, "alreadyExists")

		_, err = e.migs(t).Get(t.Context(), &computepb.GetInstanceGroupManagerRequest{
			Project: project, Zone: "nowhere-a", InstanceGroupManager: "web",
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})

	t.Run("failed creation rolls back", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)

		_, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
				Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(e.template(t, "tpl")), TargetSize: proto.Int32(2),
			},
		})
		require.Error(t, err, "no subnetwork exists in this project")
		assert.Empty(t, e.vmNames(t))

		_, err = e.migs(t).Get(t.Context(), &computepb.GetInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web",
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})
}

func TestRegionalMIGSpreadsAcrossZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		zones     []string
		size      int32
		wantZones int
	}{
		{name: "all zones of the region", size: 6, wantZones: 4},
		{name: "two zones", zones: []string{"us-central1-a", "us-central1-b"}, size: 5, wantZones: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			tmpl := e.template(t, "tpl")

			dp := &computepb.DistributionPolicy{}
			for _, z := range tt.zones {
				dp.Zones = append(dp.Zones, &computepb.DistributionPolicyZoneConfiguration{Zone: s("zones/" + z)})
			}

			op, err := e.regionMigs(t).Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerRequest{
				Project: project, Region: region, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
					Name: s("rweb"), BaseInstanceName: s("rweb"), InstanceTemplate: s(tmpl),
					TargetSize: proto.Int32(tt.size), DistributionPolicy: dp,
				},
			})
			e.done(t, op, err)

			m, err := e.regionMigs(t).Get(t.Context(), &computepb.GetRegionInstanceGroupManagerRequest{
				Project: project, Region: region, InstanceGroupManager: "rweb",
			})
			require.NoError(t, err)
			assert.Contains(t, m.GetRegion(), "/regions/"+region)
			assert.True(t, m.GetStatus().GetIsStable())
			assert.Equal(t, "EVEN", m.GetDistributionPolicy().GetTargetShape())

			it := e.regionMigs(t).ListManagedInstances(t.Context(), &computepb.ListManagedInstancesRegionInstanceGroupManagersRequest{
				Project: project, Region: region, InstanceGroupManager: "rweb",
			})

			perZone := map[string]int{}

			for {
				mi, nerr := it.Next()
				if nerr == iterator.Done {
					break
				}

				require.NoError(t, nerr)

				perZone[zoneOf(mi.GetInstance())]++
			}

			assert.Len(t, perZone, tt.wantZones)

			lo, hi := int(tt.size), 0
			for _, n := range perZone {
				lo, hi = min(lo, n), max(hi, n)
			}

			assert.LessOrEqual(t, hi-lo, 1, "even distribution: %v", perZone)

			op, err = e.regionMigs(t).Resize(t.Context(), &computepb.ResizeRegionInstanceGroupManagerRequest{
				Project: project, Region: region, InstanceGroupManager: "rweb", Size: 0,
			})
			e.done(t, op, err)
			e.settle()

			assert.Empty(t, e.vmNames(t))
		})
	}
}

func TestMIGSetInstanceTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		updateType   string
		wantOldCount int
	}{
		{name: "opportunistic keeps instances", updateType: "OPPORTUNISTIC", wantOldCount: 2},
		{name: "proactive replaces instances", updateType: "PROACTIVE", wantOldCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			oldT, newT := e.template(t, "v1"), e.template(t, "v2")

			op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
					Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(oldT), TargetSize: proto.Int32(2),
					UpdatePolicy: &computepb.InstanceGroupManagerUpdatePolicy{Type: s(tt.updateType)},
				},
			})
			e.done(t, op, err)
			e.settle()

			op, err = e.migs(t).SetInstanceTemplate(t.Context(), &computepb.SetInstanceTemplateInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManager: "web",
				InstanceGroupManagersSetInstanceTemplateRequestResource: &computepb.InstanceGroupManagersSetInstanceTemplateRequest{
					InstanceTemplate: s(newT),
				},
			})
			e.done(t, op, err)

			m := e.getMig(t, "web")
			assert.Equal(t, newT, trimToPath(m.GetInstanceTemplate()))

			count := func(tmpl string) int {
				n := 0

				for _, mi := range e.managed(t, "web") {
					if trimToPath(mi.GetVersion().GetInstanceTemplate()) == tmpl {
						n++
					}
				}

				return n
			}

			assert.Equal(t, tt.wantOldCount, count(oldT))
			assert.Equal(t, 2-tt.wantOldCount, count(newT))
			assert.Equal(t, tt.wantOldCount == 0, m.GetStatus().GetVersionTarget().GetIsReached())

			_, err = e.templates(t).Delete(t.Context(), &computepb.DeleteInstanceTemplateRequest{Project: project, InstanceTemplate: "v2"})
			assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

			e.settle()

			_, err = e.migs(t).Resize(t.Context(), &computepb.ResizeInstanceGroupManagerRequest{
				Project: project, Zone: zone, InstanceGroupManager: "web", Size: 3,
			})
			require.NoError(t, err)
			assert.Equal(t, 3-tt.wantOldCount, count(newT), "the new instance uses the new template")
		})
	}

	t.Run("unknown template", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.baseNet(t)
		e.mig(t, "web", e.template(t, "tpl"), 0)

		_, err := e.migs(t).SetInstanceTemplate(t.Context(), &computepb.SetInstanceTemplateInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web",
			InstanceGroupManagersSetInstanceTemplateRequestResource: &computepb.InstanceGroupManagersSetInstanceTemplateRequest{
				InstanceTemplate: s(globalTmplPath + "none"),
			},
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})
}

func TestMIGVersionsCanary(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	stable, canary := e.template(t, "stable"), e.template(t, "canary")

	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("web"), BaseInstanceName: s("web"), TargetSize: proto.Int32(5),
			Versions: []*computepb.InstanceGroupManagerVersion{
				{InstanceTemplate: s(stable), Name: s("stable")},
				{InstanceTemplate: s(canary), Name: s("canary"), TargetSize: &computepb.FixedOrPercent{Percent: proto.Int32(40)}},
			},
		},
	})
	e.done(t, op, err)
	e.settle()

	m := e.getMig(t, "web")
	require.Len(t, m.GetVersions(), 2)
	assert.Empty(t, m.GetInstanceTemplate())
	assert.True(t, m.GetStatus().GetVersionTarget().GetIsReached())

	byTmpl := map[string]int{}
	for _, mi := range e.managed(t, "web") {
		byTmpl[trimToPath(mi.GetVersion().GetInstanceTemplate())]++
	}

	assert.Equal(t, map[string]int{stable: 3, canary: 2}, byTmpl)

	_, err = e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("bad"), BaseInstanceName: s("bad"),
			Versions: []*computepb.InstanceGroupManagerVersion{{InstanceTemplate: s(stable)}, {InstanceTemplate: s(canary)}},
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
}

func TestMIGInstanceOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		verb       string
		wantTarget int32
		wantVMs    int
		wantMIs    int
	}{
		{name: "recreate", verb: "recreate", wantTarget: 3, wantVMs: 3, wantMIs: 3},
		{name: "delete", verb: "delete", wantTarget: 2, wantVMs: 2, wantMIs: 3},
		{name: "abandon", verb: "abandon", wantTarget: 2, wantVMs: 3, wantMIs: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.mig(t, "web", e.template(t, "tpl"), 3)
			e.settle()

			mis := e.managed(t, "web")
			victim := mis[0]
			oldID := victim.GetId()
			urls := []string{victim.GetInstance()}

			var err error

			switch tt.verb {
			case "recreate":
				_, err = e.migs(t).RecreateInstances(t.Context(), &computepb.RecreateInstancesInstanceGroupManagerRequest{
					Project: project, Zone: zone, InstanceGroupManager: "web",
					InstanceGroupManagersRecreateInstancesRequestResource: &computepb.InstanceGroupManagersRecreateInstancesRequest{Instances: urls},
				})
			case "delete":
				_, err = e.migs(t).DeleteInstances(t.Context(), &computepb.DeleteInstancesInstanceGroupManagerRequest{
					Project: project, Zone: zone, InstanceGroupManager: "web",
					InstanceGroupManagersDeleteInstancesRequestResource: &computepb.InstanceGroupManagersDeleteInstancesRequest{Instances: urls},
				})
			case "abandon":
				_, err = e.migs(t).AbandonInstances(t.Context(), &computepb.AbandonInstancesInstanceGroupManagerRequest{
					Project: project, Zone: zone, InstanceGroupManager: "web",
					InstanceGroupManagersAbandonInstancesRequestResource: &computepb.InstanceGroupManagersAbandonInstancesRequest{Instances: urls},
				})
			}

			require.NoError(t, err)

			m := e.getMig(t, "web")
			assert.Equal(t, tt.wantTarget, m.GetTargetSize())
			assert.Len(t, e.vmNames(t), tt.wantVMs)
			assert.Len(t, e.managed(t, "web"), tt.wantMIs)

			switch tt.verb {
			case "recreate":
				assert.Equal(t, int32(1), m.GetCurrentActions().GetRecreating())

				for _, mi := range e.managed(t, "web") {
					if mi.GetName() == victim.GetName() {
						assert.Equal(t, "RECREATING", mi.GetCurrentAction())
						assert.NotEqual(t, oldID, mi.GetId(), "a new instance with the same name")
					}
				}

				e.settle()
				assert.True(t, e.getMig(t, "web").GetStatus().GetIsStable())
			case "delete":
				assert.Equal(t, int32(1), m.GetCurrentActions().GetDeleting())
				e.settle()
				assert.Len(t, e.managed(t, "web"), 2)
			case "abandon":
				_ = e.getVM(t, victim.GetName())
			}
		})
	}

	t.Run("not a member", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.baseNet(t)
		e.mig(t, "web", e.template(t, "tpl"), 1)

		req := &computepb.DeleteInstancesInstanceGroupManagerRequest{
			Project: project, Zone: zone, InstanceGroupManager: "web",
			InstanceGroupManagersDeleteInstancesRequestResource: &computepb.InstanceGroupManagersDeleteInstancesRequest{
				Instances: []string{"zones/" + zone + "/instances/stranger"},
			},
		}

		_, err := e.migs(t).DeleteInstances(t.Context(), req)
		assertAPIError(t, err, http.StatusBadRequest, "invalid")

		req.InstanceGroupManagersDeleteInstancesRequestResource.SkipInstancesOnValidationError = new(true)
		_, err = e.migs(t).DeleteInstances(t.Context(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(1), e.getMig(t, "web").GetTargetSize())
	})
}

func TestMIGHealsDeletedInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.mig(t, "web", e.template(t, "tpl"), 2)
	e.settle()

	victim := e.managed(t, "web")[0]

	op, err := e.instances(t).Delete(t.Context(), &computepb.DeleteInstanceRequest{
		Project: project, Zone: zone, Instance: victim.GetName(),
	})
	e.done(t, op, err)

	names := e.vmNames(t)
	assert.Len(t, names, 2)
	assert.Contains(t, names, victim.GetName())
	assert.NotEqual(t, victim.GetId(), e.getVM(t, victim.GetName()).GetId())
}

func TestMIGDeleteRemovesInstances(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	hc := e.healthCheck(t, "hc")
	tmpl := e.template(t, "tpl")

	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("web"), BaseInstanceName: s("web"), InstanceTemplate: s(tmpl), TargetSize: proto.Int32(2),
			AutoHealingPolicies: []*computepb.InstanceGroupManagerAutoHealingPolicy{{HealthCheck: s(hc), InitialDelaySec: proto.Int32(60)}},
		},
	})
	e.done(t, op, err)

	m := e.getMig(t, "web")
	require.Len(t, m.GetAutoHealingPolicies(), 1)
	assert.Equal(t, int32(60), m.GetAutoHealingPolicies()[0].GetInitialDelaySec())

	_, err = e.healthChecks(t).Delete(t.Context(), &computepb.DeleteHealthCheckRequest{Project: project, HealthCheck: "hc"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = e.templates(t).Delete(t.Context(), &computepb.DeleteInstanceTemplateRequest{Project: project, InstanceTemplate: "tpl"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err = e.migs(t).Delete(t.Context(), &computepb.DeleteInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	})
	e.done(t, op, err)

	assert.Empty(t, e.vmNames(t))

	_, err = e.migs(t).Get(t.Context(), &computepb.GetInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err = e.healthChecks(t).Delete(t.Context(), &computepb.DeleteHealthCheckRequest{Project: project, HealthCheck: "hc"})
	e.done(t, op, err)

	op, err = e.templates(t).Delete(t.Context(), &computepb.DeleteInstanceTemplateRequest{Project: project, InstanceTemplate: "tpl"})
	e.done(t, op, err)
}

func TestMIGPatchAndTargetPools(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.mig(t, "web", e.template(t, "tpl"), 1)

	op, err := e.migs(t).Patch(t.Context(), &computepb.PatchInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			TargetSize: proto.Int32(3), NamedPorts: []*computepb.NamedPort{{Name: s("http"), Port: proto.Int32(8080)}},
		},
	})
	e.done(t, op, err)

	m := e.getMig(t, "web")
	assert.Equal(t, int32(3), m.GetTargetSize())
	require.Len(t, m.GetNamedPorts(), 1)
	assert.Equal(t, int32(8080), m.GetNamedPorts()[0].GetPort())
	assert.Len(t, e.vmNames(t), 3)

	_, err = e.migs(t).Patch(t.Context(), &computepb.PatchInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagerResource: &computepb.InstanceGroupManager{BaseInstanceName: s("other")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	pools := []string{"projects/" + project + "/regions/" + region + "/targetPools/tp"}
	_, err = e.migs(t).SetTargetPools(t.Context(), &computepb.SetTargetPoolsInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagersSetTargetPoolsRequestResource: &computepb.InstanceGroupManagersSetTargetPoolsRequest{
			TargetPools: pools, Fingerprint: s("stale"),
		},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	_, err = e.migs(t).SetTargetPools(t.Context(), &computepb.SetTargetPoolsInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
		InstanceGroupManagersSetTargetPoolsRequestResource: &computepb.InstanceGroupManagersSetTargetPoolsRequest{
			TargetPools: pools, Fingerprint: m.Fingerprint,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, pools, e.getMig(t, "web").GetTargetPools())
}

func TestMIGListAndAggregated(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	tmpl := e.template(t, "tpl")
	e.mig(t, "a", tmpl, 0)
	e.mig(t, "b", tmpl, 0)

	op, err := e.regionMigs(t).Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerRequest{
		Project: project, Region: region, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("r"), BaseInstanceName: s("r"), InstanceTemplate: s(tmpl),
		},
	})
	e.done(t, op, err)

	collect := func(filter string) []string {
		it := e.migs(t).List(t.Context(), &computepb.ListInstanceGroupManagersRequest{Project: project, Zone: zone, Filter: &filter})

		var out []string

		for {
			m, nerr := it.Next()
			if nerr == iterator.Done {
				return out
			}

			require.NoError(t, nerr)

			out = append(out, m.GetName())
		}
	}

	assert.Equal(t, []string{"a", "b"}, collect(""))
	assert.Equal(t, []string{"b"}, collect("name = b"))

	agg := e.migs(t).AggregatedList(t.Context(), &computepb.AggregatedListInstanceGroupManagersRequest{Project: project})
	total := 0

	for {
		pair, nerr := agg.Next()
		if nerr == iterator.Done {
			break
		}

		require.NoError(t, nerr)

		total += len(pair.Value.GetInstanceGroupManagers())
	}

	assert.Equal(t, 3, total)
}

func zoneOf(selfLink string) string {
	_, after, _ := strings.Cut(selfLink, "/zones/")
	z, _, _ := strings.Cut(after, "/")

	return z
}

func trimToPath(link string) string {
	_, after, ok := strings.Cut(link, "/projects/")
	if !ok {
		return link
	}

	return "projects/" + after
}

func metadataValue(vm *computepb.Instance, key string) string {
	for _, it := range vm.GetMetadata().GetItems() {
		if it.GetKey() == key {
			return it.GetValue()
		}
	}

	return ""
}
