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

func instSched(tz, start, stop string) *computepb.ResourcePolicy {
	is := &computepb.ResourcePolicyInstanceSchedulePolicy{TimeZone: s(tz)}
	if start != "" {
		is.VmStartSchedule = &computepb.ResourcePolicyInstanceSchedulePolicySchedule{Schedule: s(start)}
	}

	if stop != "" {
		is.VmStopSchedule = &computepb.ResourcePolicyInstanceSchedulePolicySchedule{Schedule: s(stop)}
	}

	return &computepb.ResourcePolicy{InstanceSchedulePolicy: is}
}

func (e *env) tick(t *testing.T) { t.Helper(); e.backend.TickSchedules(t.Context()) }

func (e *env) vmStatus(t *testing.T) string {
	t.Helper()

	return e.getVM(t, "vm1").GetStatus()
}

func (e *env) snapshotNames(t *testing.T) []string {
	t.Helper()

	var out []string

	it := e.snapshots(t).List(t.Context(), &computepb.ListSnapshotsRequest{Project: project})

	for {
		sn, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)
		out = append(out, sn.GetName())
	}
}

func TestInstanceSchedulesStartAndStop(t *testing.T) {
	t.Parallel()

	const (
		stop20 = "0 20 * * *"
		up08   = "0 8 * * *"
	)

	tests := []struct {
		name    string
		tz      string
		start   string
		stop    string
		expires string
		starts  string
		want    string
		steps   []time.Duration
	}{
		{
			name:  "stop fires",
			tz:    "UTC",
			stop:  stop20,
			steps: []time.Duration{8*time.Hour + 30*time.Second},
			want:  stTerm,
		},
		{name: "before the stop", tz: "UTC", stop: stop20, steps: []time.Duration{7 * time.Hour}, want: stRunning},
		{
			name: "stop then start next day", tz: "UTC", start: up08, stop: stop20,
			steps: []time.Duration{8*time.Hour + 30*time.Second, 12 * time.Hour}, want: stRunning,
		},
		{
			name: "time zone applies", tz: "America/New_York", stop: stop20,
			steps: []time.Duration{8*time.Hour + 30*time.Second}, want: stRunning,
		},
		{
			name: "time zone fires at local time", tz: "America/New_York", stop: stop20,
			steps: []time.Duration{12*time.Hour + 30*time.Second}, want: stTerm,
		},
		{
			name: "expired schedule does not fire", tz: "UTC", stop: stop20, expires: "2026-10-26T18:00:00Z",
			steps: []time.Duration{9 * time.Hour}, want: stRunning,
		},
		{
			name: "not started schedule does not fire", tz: "UTC", stop: stop20, starts: "2026-10-27T00:00:00Z",
			steps: []time.Duration{9 * time.Hour}, want: stRunning,
		},
		{
			name: "latest start wins after downtime", tz: "UTC", start: up08, stop: stop20,
			steps: []time.Duration{22 * time.Hour}, want: stRunning,
		},
		{
			name: "latest stop wins after downtime", tz: "UTC", start: up08, stop: stop20,
			steps: []time.Duration{33 * time.Hour}, want: stTerm,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.vm(t, vmSpec("vm1"))

			pol := instSched(tt.tz, tt.start, tt.stop)
			pol.InstanceSchedulePolicy.ExpirationTime = optStr(tt.expires)
			pol.InstanceSchedulePolicy.StartTime = optStr(tt.starts)
			require.NoError(t, e.resPolicy(t, named(pol, "sched")))
			require.NoError(t, e.changeVM(t, true, rpRef("sched")))

			e.settle()
			e.tick(t)

			for _, d := range tt.steps {
				e.clock.Advance(d)
				e.tick(t)
				e.settle()
			}

			assert.Equal(t, tt.want, e.vmStatus(t))
		})
	}
}

func optStr(v string) *string {
	if v == "" {
		return nil
	}

	return &v
}

func TestScheduledStopRunsOnce(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))
	require.NoError(t, e.resPolicy(t, named(instSched("UTC", "", "0 20 * * *"), "sched")))
	require.NoError(t, e.changeVM(t, true, rpRef("sched")))

	e.tick(t)
	e.clock.Advance(8*time.Hour + 30*time.Second)
	e.tick(t)
	e.settle()
	require.Equal(t, stTerm, e.vmStatus(t))

	op, err := e.instances(t).Start(t.Context(), &computepb.StartInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1",
	})
	e.done(t, op, err)
	require.Equal(t, stRunning, e.vmStatus(t))

	e.clock.Advance(5 * time.Minute)
	e.tick(t)
	e.settle()
	assert.Equal(t, stRunning, e.vmStatus(t), "an already applied schedule fire does not stop the instance again")
}

func TestScheduleStatusIsReported(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))
	require.NoError(t, e.resPolicy(t, named(instSched("UTC", "0 8 * * *", "0 20 * * *"), "sched")))
	require.NoError(t, e.changeVM(t, true, rpRef("sched")))

	get := func() *computepb.ResourcePolicyResourceStatusInstanceSchedulePolicyStatus {
		p, err := e.resPolicies(t).Get(t.Context(),
			&computepb.GetResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "sched"})
		require.NoError(t, err)

		return p.GetResourceStatus().GetInstanceSchedulePolicy()
	}

	assert.Equal(t, "2026-10-26T20:00:00Z", get().GetNextRunStartTime())
	assert.Empty(t, get().GetLastRunStartTime())

	e.tick(t)
	e.clock.Advance(8*time.Hour + 30*time.Second)
	e.tick(t)

	assert.Equal(t, "2026-10-26T20:00:00Z", get().GetLastRunStartTime())
	assert.Equal(t, "2026-10-27T08:00:00Z", get().GetNextRunStartTime())
}

func TestSnapshotScheduleCreatesAndExpiresSnapshots(t *testing.T) {
	t.Parallel()

	hourly6 := &computepb.ResourcePolicy{SnapshotSchedulePolicy: &computepb.ResourcePolicySnapshotSchedulePolicy{
		Schedule: &computepb.ResourcePolicySnapshotSchedulePolicySchedule{
			HourlySchedule: &computepb.ResourcePolicyHourlyCycle{HoursInCycle: proto.Int32(6), StartTime: s("04:00")},
		},
		SnapshotProperties: &computepb.ResourcePolicySnapshotSchedulePolicySnapshotProperties{
			Labels: map[string]string{"auto": "yes"},
		},
	}}

	tests := []struct {
		pol    *computepb.ResourcePolicy
		name   string
		step   time.Duration
		ticks  int
		want   int
		labels bool
	}{
		{name: "hourly cycle", pol: hourly6, step: time.Hour, ticks: 24, want: 4, labels: true},
		{name: "daily with retention", pol: daily("04:00"), step: 24 * time.Hour, ticks: 10, want: 7},
		{name: "downtime takes one snapshot", pol: hourly6, step: 48 * time.Hour, ticks: 1, want: 1},
		{name: "no snapshot before the first slot", pol: daily("04:00"), step: time.Hour, ticks: 5, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.disk(t, "d1")
			require.NoError(t, e.resPolicy(t, named(tt.pol, "snap")))
			require.NoError(t, e.changeDisk(t, "d1", true, rpRef("snap")))

			e.tick(t)

			for range tt.ticks {
				e.clock.Advance(tt.step)
				e.tick(t)
			}

			names := e.snapshotNames(t)
			assert.Len(t, names, tt.want)

			for _, n := range names {
				assert.True(t, strings.HasPrefix(n, "d1-"), n)

				sn, err := e.snapshots(t).Get(t.Context(), &computepb.GetSnapshotRequest{Project: project, Snapshot: n})
				require.NoError(t, err)
				assert.True(t, sn.GetAutoCreated())
				assert.Contains(t, sn.GetSourceDisk(), "/disks/d1")

				if tt.labels {
					assert.Equal(t, "yes", sn.GetLabels()["auto"])
				}
			}
		})
	}
}

func TestSnapshotRetentionAfterDiskDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		onDelete string
		want     int
	}{
		{name: "keep auto snapshots", onDelete: "KEEP_AUTO_SNAPSHOTS", want: 3},
		{name: "apply retention policy", onDelete: "APPLY_RETENTION_POLICY", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.disk(t, "d1")

			pol := daily("04:00")
			pol.SnapshotSchedulePolicy.RetentionPolicy.OnSourceDiskDelete = s(tt.onDelete)
			require.NoError(t, e.resPolicy(t, named(pol, "snap")))
			require.NoError(t, e.changeDisk(t, "d1", true, rpRef("snap")))

			e.tick(t)

			for range 3 {
				e.clock.Advance(24 * time.Hour)
				e.tick(t)
			}

			require.Len(t, e.snapshotNames(t), 3)
			require.NoError(t, e.changeDisk(t, "d1", false, rpRef("snap")))

			op, err := e.disks(t).
				Delete(t.Context(), &computepb.DeleteDiskRequest{Project: project, Zone: zone, Disk: "d1"})
			e.done(t, op, err)

			e.tick(t)
			e.clock.Advance(8 * 24 * time.Hour)
			e.tick(t)

			if tt.onDelete == "KEEP_AUTO_SNAPSHOTS" {
				assert.Len(t, e.snapshotNames(t), tt.want)

				return
			}

			assert.Len(t, e.snapshotNames(t), tt.want, "retention keeps applying after the source disk is gone")
		})
	}
}

func TestScheduleProgressSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.disk(t, "d1")
	require.NoError(t, e.resPolicy(t, named(daily("04:00"), "snap")))
	require.NoError(t, e.changeDisk(t, "d1", true, rpRef("snap")))

	e.tick(t)
	e.clock.Advance(17 * time.Hour)
	e.tick(t)
	require.Len(t, e.snapshotNames(t), 1)

	fresh := newEnv(t)
	fresh.clock.Advance(17 * time.Hour)
	require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	fresh.clock.Advance(24 * time.Hour)
	fresh.tick(t)
	assert.Len(t, fresh.snapshotNames(t), 2, "the restored schedule runs the slot it missed while down")

	fresh.tick(t)
	assert.Len(t, fresh.snapshotNames(t), 2, "and does not repeat it")
}

func TestConcurrentTicksRunAScheduleOnce(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.disk(t, "d2")
	require.NoError(t, e.resPolicy(t, named(daily("04:00"), "snap")))
	require.NoError(t, e.changeDisk(t, "d2", true, rpRef("snap")))

	e.tick(t)
	e.clock.Advance(17 * time.Hour)

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() { e.backend.TickSchedules(t.Context()) })
	}

	wg.Wait()
	assert.Len(t, e.snapshotNames(t), 1)
}

func TestInstanceScheduleCronValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cron  string
		valid bool
	}{
		{cron: "0 8 * * MON-FRI", valid: true},
		{cron: "*/15 * * * *", valid: true},
		{cron: "0 0 1,15 JAN,JUL *", valid: true},
		{cron: "0 0 * * 7", valid: true},
		{cron: "61 8 * * *"},
		{cron: "0 25 * * *"},
		{cron: "0 8 32 * *"},
		{cron: "0 8 * 13 *"},
		{cron: "0 8 * * 8"},
		{cron: "5-1 * * * *"},
		{cron: "*/0 * * * *"},
		{cron: "0 8 * * FOO"},
	}

	for _, tt := range tests {
		t.Run(tt.cron, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			err := e.resPolicy(t, named(instanceSchedule(tt.cron), "p"))

			if tt.valid {
				require.NoError(t, err)

				return
			}

			assertAPIError(t, err, 400, "")
		})
	}
}
