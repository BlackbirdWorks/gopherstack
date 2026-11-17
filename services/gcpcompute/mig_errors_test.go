package gcpcompute_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func (e *env) migErrors(t *testing.T) []*computepb.InstanceManagedByIgmError {
	t.Helper()

	it := e.migs(t).ListErrors(t.Context(), &computepb.ListErrorsInstanceGroupManagersRequest{
		Project: project, Zone: zone, InstanceGroupManager: "web",
	})

	var out []*computepb.InstanceManagedByIgmError

	for {
		item, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, item)
	}
}

func TestMIGCreationFailuresSurfaceAsGroupErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tmpl := e.template(t, "tpl")

	e.mig(t, "web", tmpl, 2)
	assert.Empty(t, e.vmNames(t), "no instance exists yet")

	errs := e.migErrors(t)
	require.NotEmpty(t, errs)

	instances := map[string]bool{}
	for _, it := range errs {
		instances[it.GetInstanceActionDetails().GetInstance()] = true
	}

	assert.Len(t, instances, 2, "each instance that could not be created is named")

	for _, it := range errs {
		assert.Equal(t, "RESOURCE_NOT_FOUND", it.GetError().GetCode())
		assert.NotEmpty(t, it.GetError().GetMessage())
		assert.Equal(t, "CREATING", it.GetInstanceActionDetails().GetAction())
		assert.Contains(t, it.GetInstanceActionDetails().GetInstance(), "/zones/"+zone+"/instances/web-")
		assert.Equal(t, tmpl, trimToPath(it.GetInstanceActionDetails().GetVersion().GetInstanceTemplate()))
		assert.NotEmpty(t, it.GetTimestamp())
	}

	managed := e.managed(t)
	require.Len(t, managed, 2)

	for _, mi := range managed {
		assert.Equal(t, "CREATING", mi.GetCurrentAction())
		require.NotEmpty(t, mi.GetLastAttempt().GetErrors().GetErrors())
		assert.Equal(t, "RESOURCE_NOT_FOUND", mi.GetLastAttempt().GetErrors().GetErrors()[0].GetCode())
	}

	assert.False(t, e.getMig(t).GetStatus().GetIsStable(), "a group that cannot create its instances is not stable")

	before := len(e.migErrors(t))

	e.backend.TickGroups(t.Context())
	e.backend.TickGroups(t.Context())
	assert.Empty(t, e.vmNames(t))
	assert.Len(t, e.migErrors(t), before, "a failed creation is not retried before its delay passes")

	e.baseNet(t)
	e.clock.Advance(10 * time.Minute)
	e.backend.TickGroups(t.Context())
	assert.Len(t, e.vmNames(t), 2, "the group retries and creates the instances once the cause is gone")

	e.settle()
	assert.True(t, e.getMig(t).GetStatus().GetIsStable())

	for _, mi := range e.managed(t) {
		assert.Nil(t, mi.GetLastAttempt().GetErrors(), "a successful retry clears the member's last attempt")
	}

	assert.GreaterOrEqual(t, len(e.migErrors(t)), before, "past errors stay in the log")
}

func TestRegionalMIGListErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tmpl := e.template(t, "tpl")

	op, err := e.regionMigs(t).Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerRequest{
		Project: project, Region: region, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s("rweb"), BaseInstanceName: s("rweb"), InstanceTemplate: s(tmpl), TargetSize: proto.Int32(3),
		},
	})
	e.done(t, op, err)

	it := e.regionMigs(t).ListErrors(t.Context(), &computepb.ListErrorsRegionInstanceGroupManagersRequest{
		Project: project, Region: region, InstanceGroupManager: "rweb", MaxResults: proto.Uint32(2),
	})

	n := 0

	for {
		_, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		n++
	}

	all := e.regionMigs(t).ListErrors(t.Context(), &computepb.ListErrorsRegionInstanceGroupManagersRequest{
		Project: project, Region: region, InstanceGroupManager: "rweb",
	})

	total := 0

	for {
		_, nerr := all.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		total++
	}

	assert.GreaterOrEqual(t, total, 3)
	assert.Equal(t, total, n, "paging walks every error")

	_, err = e.migs(t).ListErrors(t.Context(), &computepb.ListErrorsInstanceGroupManagersRequest{
		Project: project, Zone: zone, InstanceGroupManager: "ghost",
	}).Next()
	assertAPIError(t, err, 404, "notFound")
}

func TestMIGErrorLogIsBounded(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mig(t, "web", e.template(t, "tpl"), 150)

	assert.Len(t, e.migErrors(t), 100, "the log keeps the newest 100 errors")
}

func TestMIGErrorsSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mig(t, "web", e.template(t, "tpl"), 1)

	want := e.migErrors(t)
	require.NotEmpty(t, want)

	fresh := newEnv(t)
	require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	got := fresh.migErrors(t)
	require.Len(t, got, len(want))
	assert.Equal(t, want[0].GetTimestamp(), got[0].GetTimestamp())
	assert.Equal(t, want[0].GetError().GetMessage(), got[0].GetError().GetMessage())
}

func TestInstanceFlexibilityPolicy(t *testing.T) {
	t.Parallel()

	sel := func(rank int32, mts ...string) *computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection {
		return &computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
			MachineTypes: mts, Rank: new(rank),
		}
	}

	tests := []struct {
		policy   map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection
		name     string
		wantType string
		zonal    bool
		invalid  bool
	}{
		{
			name: "lowest rank wins",
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
				"a": sel(1, "e2-medium"),
				"b": sel(0, "n2-standard-2"),
			},
			wantType: "n2-standard-2",
		},
		{
			name: "first machine type of the selection",
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
				"a": sel(0, "e2-standard-2", "n2-standard-2"),
			},
			wantType: "e2-standard-2",
		},
		{
			name:    "zonal group",
			zonal:   true,
			invalid: true,
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
				"a": sel(0, "e2-medium"),
			},
		},
		{
			name: "no machine type", invalid: true,
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{"a": sel(0)},
		},
		{
			name:    "unknown machine type",
			invalid: true,
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
				"a": sel(0, "z9-huge"),
			},
		},
		{
			name:    "negative rank",
			invalid: true,
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
				"a": sel(-1, "e2-medium"),
			},
		},
		{
			name:    "duplicate machine type",
			invalid: true,
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
				"a": sel(0, "e2-medium", "e2-medium"),
			},
		},
		{
			name:    "bad selection name",
			invalid: true,
			policy: map[string]*computepb.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection{
				"Bad_Name": sel(0, "e2-medium"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			tmpl := e.template(t, "tpl")

			res := &computepb.InstanceGroupManager{
				Name: s("fx"), BaseInstanceName: s("fx"), InstanceTemplate: s(tmpl), TargetSize: proto.Int32(1),
				InstanceFlexibilityPolicy: &computepb.InstanceGroupManagerInstanceFlexibilityPolicy{
					InstanceSelections: tt.policy,
				},
			}

			if tt.zonal {
				_, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
					Project: project, Zone: zone, InstanceGroupManagerResource: res,
				})
				assertAPIError(t, err, 400, "invalid")

				return
			}

			op, err := e.regionMigs(t).Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerRequest{
				Project: project, Region: region, InstanceGroupManagerResource: res,
			})
			if tt.invalid {
				assertAPIError(t, err, 400, "invalid")
				assert.Empty(t, e.allVMs(t))

				return
			}

			e.done(t, op, err)

			vms := e.allVMs(t)
			require.Len(t, vms, 1)
			assert.True(
				t,
				strings.HasSuffix(vms[0].GetMachineType(), "/machineTypes/"+tt.wantType),
				vms[0].GetMachineType(),
			)
		})
	}
}

func (e *env) allVMs(t *testing.T) []*computepb.Instance {
	t.Helper()

	var out []*computepb.Instance

	for _, z := range []string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"} {
		it := e.instances(t).List(t.Context(), &computepb.ListInstancesRequest{Project: project, Zone: z})

		for {
			vm, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}

			require.NoError(t, err)

			out = append(out, vm)
		}
	}

	return out
}

func TestConcurrentTicksKeepErrorLogBounded(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.mig(t, "web", e.template(t, "tpl"), 30)

	var wg sync.WaitGroup

	for i := range 8 {
		wg.Go(func() {
			for range 5 {
				e.clock.Advance(time.Duration(i+1) * time.Minute)
				e.backend.TickGroups(t.Context())
			}
		})
	}

	wg.Wait()
	assert.LessOrEqual(t, len(e.migErrors(t)), 100)
}

func TestMIGFailingMembersAreBounded(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tmpl := e.template(t, "tpl")

	e.mig(t, "web", tmpl, 100000)
	e.backend.TickGroups(t.Context())
	e.clock.Advance(time.Hour)
	e.backend.TickGroups(t.Context())

	assert.LessOrEqual(t, len(e.managed(t)), 100, "a creation that can never succeed does not grow the group")
	assert.LessOrEqual(t, len(e.migErrors(t)), 100, "the error log is capped")
}
