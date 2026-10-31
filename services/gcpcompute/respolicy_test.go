package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func rpRef(name string) string {
	return "projects/" + project + "/regions/" + region + "/resourcePolicies/" + name
}

func (e *env) resPolicies(t *testing.T) *compute.ResourcePoliciesClient {
	t.Helper()

	return client(t, e, compute.NewResourcePoliciesRESTClient)
}

func (e *env) resPolicy(t *testing.T, p *computepb.ResourcePolicy) error {
	t.Helper()

	op, err := e.resPolicies(t).Insert(t.Context(), &computepb.InsertResourcePolicyRequest{
		Project: project, Region: region, ResourcePolicyResource: p,
	})

	return e.finish(t, op, err)
}

func daily(start string) *computepb.ResourcePolicy {
	return &computepb.ResourcePolicy{SnapshotSchedulePolicy: &computepb.ResourcePolicySnapshotSchedulePolicy{
		Schedule: &computepb.ResourcePolicySnapshotSchedulePolicySchedule{
			DailySchedule: &computepb.ResourcePolicyDailyCycle{DaysInCycle: proto.Int32(1), StartTime: s(start)},
		},
		RetentionPolicy: &computepb.ResourcePolicySnapshotSchedulePolicyRetentionPolicy{
			MaxRetentionDays: proto.Int32(7),
		},
	}}
}

func instanceSchedule(cron string) *computepb.ResourcePolicy {
	return &computepb.ResourcePolicy{InstanceSchedulePolicy: &computepb.ResourcePolicyInstanceSchedulePolicy{
		VmStartSchedule: &computepb.ResourcePolicyInstanceSchedulePolicySchedule{Schedule: s(cron)}, TimeZone: s("UTC"),
	}}
}

func named(p *computepb.ResourcePolicy, name string) *computepb.ResourcePolicy {
	p.Name = s(name)

	return p
}

func TestResourcePolicyCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.resPolicy(t, named(daily("13:00-5"), "snap")))
	require.NoError(t, e.resPolicy(t, named(instanceSchedule("0 8 * * 1-5"), "sched")))

	p, err := e.resPolicies(t).
		Get(t.Context(), &computepb.GetResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "snap"})
	require.NoError(t, err)
	assert.Equal(t, "READY", p.GetStatus())
	assert.Equal(t, "compute#resourcePolicy", p.GetKind())
	assert.Contains(t, p.GetRegion(), "/regions/"+region)
	assert.Contains(t, p.GetSelfLink(), "/regions/"+region+"/resourcePolicies/snap")
	assert.Equal(t, "08:00", p.GetSnapshotSchedulePolicy().GetSchedule().GetDailySchedule().GetStartTime())
	assert.Equal(t, "KEEP_AUTO_SNAPSHOTS", p.GetSnapshotSchedulePolicy().GetRetentionPolicy().GetOnSourceDiskDelete())

	assertAPIError(t, e.resPolicy(t, named(daily("00:00"), "snap")), http.StatusConflict, "alreadyExists")

	count := 0
	it := e.resPolicies(t).List(t.Context(), &computepb.ListResourcePoliciesRequest{Project: project, Region: region})

	for _, err = it.Next(); err == nil; _, err = it.Next() {
		count++
	}

	assert.Equal(t, 2, count)

	op, err := e.resPolicies(t).Patch(t.Context(), &computepb.PatchResourcePolicyRequest{
		Project: project, Region: region, ResourcePolicy: "sched",
		ResourcePolicyResource: &computepb.ResourcePolicy{
			InstanceSchedulePolicy: &computepb.ResourcePolicyInstanceSchedulePolicy{
				VmStopSchedule: &computepb.ResourcePolicyInstanceSchedulePolicySchedule{Schedule: s("0 20 * * *")},
			},
		},
	})
	require.NoError(t, e.finish(t, op, err))

	p, err = e.resPolicies(t).
		Get(t.Context(), &computepb.GetResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "sched"})
	require.NoError(t, err)
	assert.Equal(t, "0 20 * * *", p.GetInstanceSchedulePolicy().GetVmStopSchedule().GetSchedule())
	assert.Equal(t, "0 8 * * 1-5", p.GetInstanceSchedulePolicy().GetVmStartSchedule().GetSchedule())

	op, err = e.resPolicies(t).
		Delete(t.Context(), &computepb.DeleteResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "snap"})
	require.NoError(t, e.finish(t, op, err))

	_, err = e.resPolicies(t).
		Get(t.Context(), &computepb.GetResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "snap"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestResourcePolicyValidation(t *testing.T) {
	t.Parallel()

	weekly := func(day, start string) *computepb.ResourcePolicy {
		return &computepb.ResourcePolicy{SnapshotSchedulePolicy: &computepb.ResourcePolicySnapshotSchedulePolicy{
			Schedule: &computepb.ResourcePolicySnapshotSchedulePolicySchedule{
				WeeklySchedule: &computepb.ResourcePolicyWeeklyCycle{
					DayOfWeeks: []*computepb.ResourcePolicyWeeklyCycleDayOfWeek{
						{Day: s(day), StartTime: s(start)},
					},
				},
			},
		}}
	}
	hourly := func(n int32) *computepb.ResourcePolicy {
		return &computepb.ResourcePolicy{SnapshotSchedulePolicy: &computepb.ResourcePolicySnapshotSchedulePolicy{
			Schedule: &computepb.ResourcePolicySnapshotSchedulePolicySchedule{
				HourlySchedule: &computepb.ResourcePolicyHourlyCycle{
					HoursInCycle: new(n),
					StartTime:    s("04:00"),
				},
			},
		}}
	}

	tests := []struct {
		pol   *computepb.ResourcePolicy
		name  string
		valid bool
	}{
		{name: "weekly", valid: true, pol: weekly("MONDAY", "08:00")},
		{name: "hourly divisor", valid: true, pol: hourly(6)},
		{name: "hourly not divisor", pol: hourly(5)},
		{name: "weekly bad day", pol: weekly("FUNDAY", "08:00")},
		{name: "weekly bad start", pol: weekly("MONDAY", "09:00")},
		{name: "daily bad start", pol: daily("10:00")},
		{name: "no type", pol: &computepb.ResourcePolicy{}},
		{name: "two types", pol: &computepb.ResourcePolicy{
			SnapshotSchedulePolicy: daily("00:00").GetSnapshotSchedulePolicy(),
			InstanceSchedulePolicy: instanceSchedule("0 8 * * *").GetInstanceSchedulePolicy(),
		}},
		{
			name: "snapshot without schedule",
			pol:  &computepb.ResourcePolicy{SnapshotSchedulePolicy: &computepb.ResourcePolicySnapshotSchedulePolicy{}},
		},
		{name: "bad cron", pol: instanceSchedule("0 8 * *")},
		{name: "bad cron chars", pol: instanceSchedule("0 8 * * $")},
		{
			name: "no schedule",
			pol:  &computepb.ResourcePolicy{InstanceSchedulePolicy: &computepb.ResourcePolicyInstanceSchedulePolicy{}},
		},
		{
			name: "bad time zone",
			pol: &computepb.ResourcePolicy{InstanceSchedulePolicy: &computepb.ResourcePolicyInstanceSchedulePolicy{
				VmStopSchedule: &computepb.ResourcePolicyInstanceSchedulePolicySchedule{
					Schedule: s("0 8 * * *"),
				}, TimeZone: s("Mars/Base"),
			}},
		},
		{
			name:  "placement",
			valid: true,
			pol: &computepb.ResourcePolicy{GroupPlacementPolicy: &computepb.ResourcePolicyGroupPlacementPolicy{
				Collocation: s("COLLOCATED"), VmCount: proto.Int32(2),
			}},
		},
		{
			name: "placement bad collocation",
			pol: &computepb.ResourcePolicy{GroupPlacementPolicy: &computepb.ResourcePolicyGroupPlacementPolicy{
				Collocation: s("SPREAD"),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			err := e.resPolicy(t, named(tt.pol, "pol"))

			if tt.valid {
				require.NoError(t, err)

				return
			}

			assertAPIError(t, err, http.StatusBadRequest, "")
		})
	}
}

func (e *env) disk(t *testing.T, name string) {
	t.Helper()

	op, err := e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{
		Project: project, Zone: zone, DiskResource: &computepb.Disk{Name: s(name), SizeGb: proto.Int64(10)},
	})
	e.done(t, op, err)
}

func (e *env) changeDisk(t *testing.T, disk string, add bool, refs ...string) error {
	t.Helper()

	var (
		op  *compute.Operation
		err error
	)

	if add {
		op, err = e.disks(t).AddResourcePolicies(t.Context(), &computepb.AddResourcePoliciesDiskRequest{
			Project: project, Zone: zone, Disk: disk,
			DisksAddResourcePoliciesRequestResource: &computepb.DisksAddResourcePoliciesRequest{ResourcePolicies: refs},
		})
	} else {
		op, err = e.disks(t).RemoveResourcePolicies(t.Context(), &computepb.RemoveResourcePoliciesDiskRequest{
			Project: project, Zone: zone, Disk: disk,
			DisksRemoveResourcePoliciesRequestResource: &computepb.DisksRemoveResourcePoliciesRequest{ResourcePolicies: refs},
		})
	}

	return e.finish(t, op, err)
}

func (e *env) changeVM(t *testing.T, add bool, refs ...string) error {
	t.Helper()

	var (
		op  *compute.Operation
		err error
	)

	if add {
		op, err = e.instances(t).AddResourcePolicies(t.Context(), &computepb.AddResourcePoliciesInstanceRequest{
			Project: project, Zone: zone, Instance: "vm1",
			InstancesAddResourcePoliciesRequestResource: &computepb.InstancesAddResourcePoliciesRequest{
				ResourcePolicies: refs,
			},
		})
	} else {
		op, err = e.instances(t).RemoveResourcePolicies(t.Context(), &computepb.RemoveResourcePoliciesInstanceRequest{
			Project: project, Zone: zone, Instance: "vm1",
			InstancesRemoveResourcePoliciesRequestResource: &computepb.InstancesRemoveResourcePoliciesRequest{
				ResourcePolicies: refs,
			},
		})
	}

	return e.finish(t, op, err)
}

func TestResourcePolicyAttachToDisk(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.disk(t, "d1")
	require.NoError(t, e.resPolicy(t, named(daily("00:00"), "snap")))
	require.NoError(t, e.resPolicy(t, named(daily("04:00"), "snap2")))
	require.NoError(t, e.resPolicy(t, named(instanceSchedule("0 8 * * *"), "sched")))

	getDisk := func() *computepb.Disk {
		d, err := e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "d1"})
		require.NoError(t, err)

		return d
	}

	assertAPIError(t, e.changeDisk(t, "d1", true, rpRef("sched")), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.changeDisk(t, "d1", true, rpRef("ghost")), http.StatusNotFound, "notFound")
	assertAPIError(t, e.changeDisk(t, "ghost", true, rpRef("snap")), http.StatusNotFound, "notFound")
	assertAPIError(t, e.changeDisk(t, "d1", false, rpRef("snap")), http.StatusBadRequest, "invalid")

	require.NoError(t, e.changeDisk(t, "d1", true, rpRef("snap")))
	assert.Equal(t, []string{"https://www.googleapis.com/compute/v1/" + rpRef("snap")}, getDisk().GetResourcePolicies())

	assertAPIError(t, e.changeDisk(t, "d1", true, rpRef("snap")), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.changeDisk(t, "d1", true, rpRef("snap2")), http.StatusBadRequest, "invalid")

	_, err := e.resPolicies(t).
		Delete(t.Context(), &computepb.DeleteResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "snap"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	require.NoError(t, e.changeDisk(t, "d1", false, rpRef("snap")))
	assert.Empty(t, getDisk().GetResourcePolicies())

	op, err := e.resPolicies(t).
		Delete(t.Context(), &computepb.DeleteResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "snap"})
	require.NoError(t, e.finish(t, op, err))

	t.Run("create time", func(t *testing.T) {
		t.Parallel()

		cop, cerr := e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{
			Project: project, Zone: zone, DiskResource: &computepb.Disk{
				Name: s("d2"), SizeGb: proto.Int64(10), ResourcePolicies: []string{rpRef("snap2")},
			},
		})
		require.NoError(t, e.finish(t, cop, cerr))

		d, gerr := e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "d2"})
		require.NoError(t, gerr)
		assert.Len(t, d.GetResourcePolicies(), 1)

		_, ierr := e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{
			Project: project, Zone: zone, DiskResource: &computepb.Disk{
				Name: s("d3"), SizeGb: proto.Int64(10), ResourcePolicies: []string{rpRef("ghost")},
			},
		})
		assertAPIError(t, ierr, http.StatusNotFound, "notFound")
	})
}

func TestResourcePolicyAttachToInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))
	require.NoError(t, e.resPolicy(t, named(daily("00:00"), "snap")))
	require.NoError(t, e.resPolicy(t, named(instanceSchedule("0 8 * * *"), "sched")))
	require.NoError(t, e.resPolicy(t, named(&computepb.ResourcePolicy{
		GroupPlacementPolicy: &computepb.ResourcePolicyGroupPlacementPolicy{Collocation: s("COLLOCATED")},
	}, "place")))

	assertAPIError(t, e.changeVM(t, true, rpRef("snap")), http.StatusBadRequest, "invalid")
	require.NoError(t, e.changeVM(t, true, rpRef("sched"), rpRef("place")))
	assert.Len(t, e.getVM(t, "vm1").GetResourcePolicies(), 2)

	_, err := e.resPolicies(t).
		Delete(t.Context(), &computepb.DeleteResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "sched"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
	assertAPIError(t, e.changeVM(t, true, rpRef("sched")), http.StatusBadRequest, "invalid")

	require.NoError(t, e.changeVM(t, false, rpRef("sched")))
	assert.Len(t, e.getVM(t, "vm1").GetResourcePolicies(), 1)
	assertAPIError(t, e.changeVM(t, false, rpRef("sched")), http.StatusBadRequest, "invalid")

	spec := vmSpec("vm2")
	spec.ResourcePolicies = []string{rpRef("sched")}
	assert.Len(t, e.vm(t, spec).GetResourcePolicies(), 1)
}
