package ocifilestorage_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) createPolicy(
	t *testing.T,
	mutate func(*filestorage.CreateFilesystemSnapshotPolicyDetails),
) filestorage.FilesystemSnapshotPolicy {
	t.Helper()

	d := filestorage.CreateFilesystemSnapshotPolicyDetails{
		AvailabilityDomain: new(
			testAD,
		),
		CompartmentId: new(testCompartment),
		DisplayName:   new("pol"),
		PolicyPrefix:  new("acme"),
		Schedules: []filestorage.SnapshotSchedule{{
			Period: filestorage.SnapshotSchedulePeriodHourly, TimeZone: filestorage.SnapshotScheduleTimeZoneUtc,
			SchedulePrefix: new("h"), RetentionDurationInSeconds: new(int64(90000)),
		}},
	}
	if mutate != nil {
		mutate(&d)
	}

	resp, err := e.client.CreateFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.CreateFilesystemSnapshotPolicyRequest{CreateFilesystemSnapshotPolicyDetails: d},
	)
	require.NoError(t, err)
	e.settle()

	got, err := e.client.GetFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.GetFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: resp.Id},
	)
	require.NoError(t, err)

	return got.FilesystemSnapshotPolicy
}

func TestPolicyLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	p := e.createPolicy(t, func(d *filestorage.CreateFilesystemSnapshotPolicyDetails) {
		d.Schedules = append(
			d.Schedules,
			filestorage.SnapshotSchedule{Period: filestorage.SnapshotSchedulePeriodWeekly},
		)
	})
	assert.Equal(t, filestorage.FilesystemSnapshotPolicyLifecycleStateActive, p.LifecycleState)
	require.Len(t, p.Schedules, 2)

	weekly := p.Schedules[1]
	assert.NotNil(t, weekly.HourOfDay, "the service picks unset fields")
	assert.NotEmpty(t, weekly.DayOfWeek)
	assert.Equal(t, filestorage.SnapshotScheduleTimeZoneUtc, weekly.TimeZone)
	assert.NotNil(t, weekly.TimeScheduleStart)

	paused, err := e.client.PauseFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.PauseFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, filestorage.FilesystemSnapshotPolicyLifecycleStateInactive, paused.LifecycleState)

	_, err = e.client.PauseFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.PauseFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	assertStatus(t, err, http.StatusConflict, "IncorrectState")

	un, err := e.client.UnpauseFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.UnpauseFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, filestorage.FilesystemSnapshotPolicyLifecycleStateActive, un.LifecycleState)

	_, err = e.client.UnpauseFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.UnpauseFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	assertStatus(t, err, http.StatusConflict, "IncorrectState")

	upd, err := e.client.UpdateFilesystemSnapshotPolicy(t.Context(), filestorage.UpdateFilesystemSnapshotPolicyRequest{
		FilesystemSnapshotPolicyId: p.Id,
		DisplayName:                new("renamed"), Schedules: []filestorage.SnapshotSchedule{},
	})
	require.NoError(t, err)
	assert.Empty(t, upd.Schedules)

	list, err := e.client.ListFilesystemSnapshotPolicies(t.Context(), filestorage.ListFilesystemSnapshotPoliciesRequest{
		CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "acme", *list.Items[0].PolicyPrefix)

	_, err = e.client.ChangeFilesystemSnapshotPolicyCompartment(
		t.Context(),
		filestorage.ChangeFilesystemSnapshotPolicyCompartmentRequest{
			FilesystemSnapshotPolicyId: p.Id,
			CompartmentId:              new(otherCompart),
		},
	)
	require.NoError(t, err)

	_, err = e.client.DeleteFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.DeleteFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	require.NoError(t, err)
}

func TestPolicyScheduleRejects(t *testing.T) {
	t.Parallel()

	sched := func(mut func(*filestorage.SnapshotSchedule)) []filestorage.SnapshotSchedule {
		s := filestorage.SnapshotSchedule{Period: filestorage.SnapshotSchedulePeriodDaily}
		mut(&s)

		return []filestorage.SnapshotSchedule{s}
	}

	tests := []struct {
		name  string
		sched []filestorage.SnapshotSchedule
	}{
		{name: "bad-period", sched: sched(func(s *filestorage.SnapshotSchedule) { s.Period = "SOMETIMES" })},
		{name: "bad-hour", sched: sched(func(s *filestorage.SnapshotSchedule) { s.HourOfDay = new(24) })},
		{name: "hourly-with-hour", sched: sched(func(s *filestorage.SnapshotSchedule) {
			s.Period = filestorage.SnapshotSchedulePeriodHourly
			s.HourOfDay = new(3)
		})},
		{name: "bad-day-of-month", sched: sched(func(s *filestorage.SnapshotSchedule) {
			s.Period = filestorage.SnapshotSchedulePeriodMonthly
			s.DayOfMonth = new(32)
		})},
		{name: "bad-weekday", sched: sched(func(s *filestorage.SnapshotSchedule) {
			s.Period = filestorage.SnapshotSchedulePeriodWeekly
			s.DayOfWeek = "FUNDAY"
		})},
		{name: "bad-month", sched: sched(func(s *filestorage.SnapshotSchedule) {
			s.Period = filestorage.SnapshotSchedulePeriodYearly
			s.Month = "SMARCH"
		})},
		{
			name:  "bad-retention",
			sched: sched(func(s *filestorage.SnapshotSchedule) { s.RetentionDurationInSeconds = new(int64(0)) }),
		},
		{name: "bad-zone", sched: sched(func(s *filestorage.SnapshotSchedule) { s.TimeZone = "MARS" })},
		{name: "too-many", sched: func() []filestorage.SnapshotSchedule {
			out := make([]filestorage.SnapshotSchedule, 11)
			for i := range out {
				out[i] = filestorage.SnapshotSchedule{Period: filestorage.SnapshotSchedulePeriodDaily}
			}

			return out
		}()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.client.CreateFilesystemSnapshotPolicy(
				t.Context(),
				filestorage.CreateFilesystemSnapshotPolicyRequest{
					AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), Schedules: tc.sched,
				},
			)
			assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")
		})
	}
}

func TestPolicyTakesSnapshots(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	start := common.SDKTime{Time: e.clock.Now()}
	p := e.createPolicy(t, func(d *filestorage.CreateFilesystemSnapshotPolicyDetails) {
		d.Schedules[0].TimeScheduleStart = &start
	})
	fs := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.FilesystemSnapshotPolicyId = p.Id })
	assert.Equal(t, *p.Id, *fs.FilesystemSnapshotPolicyId)

	names := func() []string {
		list, err := e.client.ListSnapshots(
			t.Context(),
			filestorage.ListSnapshotsRequest{FilesystemSnapshotPolicyId: p.Id},
		)
		require.NoError(t, err)

		out := make([]string, 0, len(list.Items))
		for _, s := range list.Items {
			assert.Equal(t, filestorage.SnapshotSummarySnapshotTypePolicyBased, s.SnapshotType)
			assert.NotNil(t, s.ExpirationTime)
			out = append(out, *s.Name)
		}

		return out
	}

	assert.Empty(t, names())

	e.clock.Advance(time.Hour + time.Minute)
	e.backend.Sweep()

	got := names()
	require.Len(t, got, 1)
	assert.True(t, strings.HasPrefix(got[0], "acme_h_"), got[0])
	assert.True(t, strings.HasSuffix(got[0], "_Hourly_P1DT1H"), got[0])

	e.clock.Advance(time.Hour)
	e.backend.Sweep()
	assert.Len(t, names(), 2)

	_, err := e.client.PauseFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.PauseFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	require.NoError(t, err)
	e.clock.Advance(3 * time.Hour)
	e.backend.Sweep()
	assert.Len(t, names(), 2, "a paused policy takes no snapshots")

	_, err = e.client.UnpauseFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.UnpauseFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	require.NoError(t, err)
	e.clock.Advance(26 * time.Hour)
	e.backend.Sweep()
	e.settle()

	active, err := e.client.ListSnapshots(t.Context(), filestorage.ListSnapshotsRequest{
		FilesystemSnapshotPolicyId: p.Id, LifecycleState: filestorage.ListSnapshotsLifecycleStateActive,
	})
	require.NoError(t, err)
	assert.Len(t, active.Items, 1, "retention (25h) expired the older snapshots")

	_, err = e.client.DeleteFilesystemSnapshotPolicy(
		t.Context(),
		filestorage.DeleteFilesystemSnapshotPolicyRequest{FilesystemSnapshotPolicyId: p.Id},
	)
	require.NoError(t, err)
	assert.Nil(t, e.getFS(t, fs.Id).FilesystemSnapshotPolicyId, "deleting a policy detaches it")
}

func TestPolicyDailyAndWeekly(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	now := e.clock.Now()
	start := common.SDKTime{Time: now}
	hour := (now.Hour() + 2) % 24

	p := e.createPolicy(t, func(d *filestorage.CreateFilesystemSnapshotPolicyDetails) {
		d.PolicyPrefix = new("")
		d.Schedules = []filestorage.SnapshotSchedule{{
			Period: filestorage.SnapshotSchedulePeriodDaily, TimeScheduleStart: &start, HourOfDay: &hour,
			SchedulePrefix: new("d"),
		}}
	})
	e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.FilesystemSnapshotPolicyId = p.Id })

	e.clock.Advance(90 * time.Minute)
	e.backend.Sweep()

	list, err := e.client.ListSnapshots(t.Context(), filestorage.ListSnapshotsRequest{FilesystemSnapshotPolicyId: p.Id})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	e.clock.Advance(3 * time.Hour)
	e.backend.Sweep()

	list, err = e.client.ListSnapshots(t.Context(), filestorage.ListSnapshotsRequest{FilesystemSnapshotPolicyId: p.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.True(t, strings.HasPrefix(*list.Items[0].Name, "d_"))
	assert.True(t, strings.HasSuffix(*list.Items[0].Name, "_Daily"))
}

func TestFileSystemPolicyAttach(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	p := e.createPolicy(t, nil)
	fs := e.createFS(t, nil)

	_, err := e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: fs.Id, FilesystemSnapshotPolicyId: p.Id,
	})
	require.NoError(t, err)

	byPolicy, err := e.client.ListFileSystems(t.Context(), filestorage.ListFileSystemsRequest{
		CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD), FilesystemSnapshotPolicyId: p.Id,
	})
	require.NoError(t, err)
	assert.Len(t, byPolicy.Items, 1)

	_, err = e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: fs.Id, FilesystemSnapshotPolicyId: new(""),
	})
	require.NoError(t, err)
	assert.Nil(t, e.getFS(t, fs.Id).FilesystemSnapshotPolicyId)

	otherAdPolicy := e.createPolicy(
		t,
		func(d *filestorage.CreateFilesystemSnapshotPolicyDetails) { d.AvailabilityDomain = new(otherAD) },
	)
	_, err = e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: fs.Id, FilesystemSnapshotPolicyId: otherAdPolicy.Id,
	})
	assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")
}
