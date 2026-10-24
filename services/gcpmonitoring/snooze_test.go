package gcpmonitoring_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func (e *env) snoozes(t *testing.T) *monitoring.SnoozeClient {
	t.Helper()

	c, err := monitoring.NewSnoozeClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) createSnooze(t *testing.T, s *monitoringpb.Snooze) *monitoringpb.Snooze {
	t.Helper()

	got, err := e.snoozes(t).CreateSnooze(t.Context(), &monitoringpb.CreateSnoozeRequest{Parent: proj, Snooze: s})
	require.NoError(t, err)

	return got
}

func snoozeOf(display string, start, end time.Time, filter string, policies ...string) *monitoringpb.Snooze {
	return &monitoringpb.Snooze{
		DisplayName: display,
		Criteria:    &monitoringpb.Snooze_Criteria{Policies: policies, Filter: filter},
		Interval:    interval(start, end),
	}
}

func TestSnoozeCreateGet(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	pol := e.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3))
	in := snoozeOf("maintenance", epoch(), epoch().Add(time.Hour), "", pol.GetName())

	created := e.createSnooze(t, in)
	assert.Regexp(t, `^projects/test-project/snoozes/\d+$`, created.GetName())
	assert.Equal(t, "maintenance", created.GetDisplayName())
	assert.Equal(t, []string{pol.GetName()}, created.GetCriteria().GetPolicies())
	assert.True(t, created.GetInterval().GetEndTime().AsTime().Equal(epoch().Add(time.Hour)))

	got, err := e.snoozes(t).GetSnooze(t.Context(), &monitoringpb.GetSnoozeRequest{Name: created.GetName()})
	require.NoError(t, err)
	assert.Equal(t, created.GetName(), got.GetName())
	assert.Equal(t, "maintenance", got.GetDisplayName())
}

func TestSnoozeCreateErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	pol := e.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3)).GetName()
	pol2 := e.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_LT, 1)).GetName()
	start, end := epoch(), epoch().Add(time.Hour)
	many := make([]string, 17)

	for i := range many {
		many[i] = pol
	}

	tests := []struct {
		snooze *monitoringpb.Snooze
		name   string
		parent string
		code   codes.Code
	}{
		{snoozeOf("", start, end, "", pol), "no display name", proj, codes.InvalidArgument},
		{snoozeOf("s", start, end, ""), "no policies", proj, codes.InvalidArgument},
		{snoozeOf("s", start, end, "", many...), "too many policies", proj, codes.InvalidArgument},
		{snoozeOf("s", start, end, "", proj+"/alertPolicies/404"), "unknown policy", proj, codes.NotFound},
		{snoozeOf("s", start, end, "", "garbage"), "malformed policy", proj, codes.InvalidArgument},
		{snoozeOf("s", end, start, "", pol), "end before start", proj, codes.InvalidArgument},
		{snoozeOf("s", start.Add(-time.Hour), end, "", pol), "start in the past", proj, codes.InvalidArgument},
		{snoozeOf("s", start, end, `resource.labels.zone = "z"`, pol, pol2), "filter with two policies", proj, codes.InvalidArgument},
		{snoozeOf("s", start, end, `resource.type = "gce_instance"`, pol), "filter on other field", proj, codes.InvalidArgument},
		{snoozeOf("s", start, end, "", pol), "bad parent", "folders/1", codes.InvalidArgument},
		{&monitoringpb.Snooze{DisplayName: "s", Criteria: &monitoringpb.Snooze_Criteria{Policies: []string{pol}}}, "no interval", proj, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.snoozes(t).CreateSnooze(t.Context(),
				&monitoringpb.CreateSnoozeRequest{Parent: tt.parent, Snooze: tt.snooze})
			assert.Equal(t, tt.code, codeOf(err))
		})
	}
}

func TestSnoozeGetErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		name string
		id   string
		code codes.Code
	}{
		{"missing", proj + "/snoozes/404", codes.NotFound},
		{"malformed", "snoozes/1", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.snoozes(t).GetSnooze(t.Context(), &monitoringpb.GetSnoozeRequest{Name: tt.id})
			assert.Equal(t, tt.code, codeOf(err))
		})
	}
}

func TestSnoozeList(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	pol := e.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3)).GetName()
	day := 24 * time.Hour
	early := e.createSnooze(t, snoozeOf("early", epoch(), epoch().Add(time.Hour), "", pol))
	late := e.createSnooze(t, snoozeOf("late", epoch().Add(2*day), epoch().Add(3*day), "", pol))

	tests := []struct {
		name   string
		filter string
		want   []string
	}{
		{"all", "", []string{"early", "late"}},
		{"start after", fmt.Sprintf(`interval.start_time > %q`, epoch().Add(day).Format(time.RFC3339)), []string{"late"}},
		{
			"end before with offset",
			fmt.Sprintf(`interval.end_time < %q`, epoch().Add(day).In(time.FixedZone("x", -8*3600)).Format(time.RFC3339)),
			[]string{"early"},
		},
		{
			"window",
			fmt.Sprintf(`interval.start_time > %q AND interval.end_time < %q`,
				epoch().Add(-day).Format(time.RFC3339), epoch().Add(4*day).Format(time.RFC3339)),
			[]string{"early", "late"},
		},
		{"none", fmt.Sprintf(`interval.start_time > %q`, epoch().Add(10*day).Format(time.RFC3339)), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.snoozes(t).ListSnoozes(t.Context(), &monitoringpb.ListSnoozesRequest{Parent: proj, Filter: tt.filter})

			var got []string

			for {
				s, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				require.NoError(t, err)

				got = append(got, s.GetDisplayName())
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		it := e.snoozes(t).ListSnoozes(t.Context(), &monitoringpb.ListSnoozesRequest{Parent: proj, PageSize: 1})
		first, err := it.Next()
		require.NoError(t, err)
		assert.Contains(t, []string{early.GetName(), late.GetName()}, first.GetName())
		assert.NotEmpty(t, it.PageInfo().Token, "a next page token follows a full page")

		second, err := it.Next()
		require.NoError(t, err)
		assert.NotEqual(t, first.GetName(), second.GetName())

		_, err = it.Next()
		assert.Equal(t, iterator.Done, err)
	})

	t.Run("bad filter", func(t *testing.T) {
		t.Parallel()

		it := e.snoozes(t).ListSnoozes(t.Context(), &monitoringpb.ListSnoozesRequest{Parent: proj, Filter: `display_name = "x"`})
		_, err := it.Next()
		assert.Equal(t, codes.InvalidArgument, codeOf(err))
	})

	t.Run("other project empty", func(t *testing.T) {
		t.Parallel()

		it := e.snoozes(t).ListSnoozes(t.Context(), &monitoringpb.ListSnoozesRequest{Parent: "projects/other"})
		_, err := it.Next()
		assert.Equal(t, iterator.Done, err)
	})
}

func TestSnoozeUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		edit  func(e *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest
		check func(t *testing.T, got *monitoringpb.Snooze)
		name  string
		code  codes.Code
	}{
		{
			name: "display name",
			edit: func(_ *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				s.DisplayName = "renamed"

				return &monitoringpb.UpdateSnoozeRequest{
					Snooze: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
				}
			},
			check: func(t *testing.T, got *monitoringpb.Snooze) {
				t.Helper()
				assert.Equal(t, "renamed", got.GetDisplayName())
				assert.True(t, got.GetInterval().GetEndTime().AsTime().Equal(epoch().Add(time.Hour)), "end untouched")
			},
		},
		{
			name: "extend end of a started snooze",
			edit: func(e *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				e.clk.set(epoch().Add(time.Minute))
				s.Interval = interval(epoch(), epoch().Add(2*time.Hour))

				return &monitoringpb.UpdateSnoozeRequest{
					Snooze: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"interval.end_time"}},
				}
			},
			check: func(t *testing.T, got *monitoringpb.Snooze) {
				t.Helper()
				assert.True(t, got.GetInterval().GetEndTime().AsTime().Equal(epoch().Add(2*time.Hour)))
				assert.True(t, got.GetInterval().GetStartTime().AsTime().Equal(epoch()))
			},
		},
		{
			name: "move start of a started snooze",
			edit: func(e *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				e.clk.set(epoch().Add(time.Minute))
				s.Interval = interval(epoch().Add(time.Second), epoch().Add(time.Hour))

				return &monitoringpb.UpdateSnoozeRequest{
					Snooze: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"interval.start_time"}},
				}
			},
			code: codes.InvalidArgument,
		},
		{
			name: "end in the past",
			edit: func(e *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				e.clk.set(epoch().Add(time.Minute))
				s.Interval = interval(epoch(), epoch().Add(30*time.Second))

				return &monitoringpb.UpdateSnoozeRequest{
					Snooze: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"interval.end_time"}},
				}
			},
			code: codes.InvalidArgument,
		},
		{
			name: "ended snooze",
			edit: func(e *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				e.clk.set(epoch().Add(2 * time.Hour))
				s.DisplayName = "late"

				return &monitoringpb.UpdateSnoozeRequest{
					Snooze: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
				}
			},
			code: codes.FailedPrecondition,
		},
		{
			name: "empty mask",
			edit: func(_ *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				return &monitoringpb.UpdateSnoozeRequest{Snooze: s}
			},
			code: codes.InvalidArgument,
		},
		{
			name: "unsupported path",
			edit: func(_ *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				return &monitoringpb.UpdateSnoozeRequest{
					Snooze: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"criteria"}},
				}
			},
			code: codes.InvalidArgument,
		},
		{
			name: "unknown snooze",
			edit: func(_ *env, s *monitoringpb.Snooze) *monitoringpb.UpdateSnoozeRequest {
				s.Name = proj + "/snoozes/404"

				return &monitoringpb.UpdateSnoozeRequest{
					Snooze: s, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
				}
			},
			code: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			pol := e.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3)).GetName()
			s := e.createSnooze(t, snoozeOf("orig", epoch(), epoch().Add(time.Hour), "", pol))

			got, err := e.snoozes(t).UpdateSnooze(t.Context(), tt.edit(e, s))
			require.Equal(t, tt.code, codeOf(err))

			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestSnoozeSuppressesIncidents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     string
		wantOpened []string
	}{
		{"whole policy", "", nil},
		{"matching resource only", `resource.labels.instance_id = "inst-1"`, []string{"inst-2"}},
		{"non matching filter", `resource.labels.instance_id = "inst-9"`, []string{"inst-1", "inst-2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ch := e.pubsubChannel(t)
			pol := e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))
			e.createSnooze(t, snoozeOf("s", epoch(), epoch().Add(10*time.Minute), tt.filter, pol.GetName()))
			b := e.handler.Backend

			for m := 1; m <= 2; m++ {
				e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9, "inst-2": 9})
			}

			b.Tick(t.Context())

			var opened []string

			for _, inc := range b.Incidents(testProject) {
				opened = append(opened, inc.ResourceLabels["instance_id"])
			}

			assert.ElementsMatch(t, tt.wantOpened, opened)
			assert.Len(t, e.pub.all(), len(tt.wantOpened))
		})
	}
}

func TestSnoozeOnlyAppliesWithinItsIntervalAndPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.pubsubChannel(t)
	pol := e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))
	other := e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 4))
	b := e.handler.Backend

	e.createSnooze(t, snoozeOf("future", epoch().Add(5*time.Minute), epoch().Add(time.Hour), "", pol.GetName()))
	e.createSnooze(t, snoozeOf("other policy", epoch(), epoch().Add(time.Hour), "", other.GetName()))

	for m := 1; m <= 2; m++ {
		e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	b.Tick(t.Context())
	assert.Len(t, b.Incidents(testProject), 1, "only the other policy is snoozed; the future snooze is not active yet")
	assert.Len(t, e.pub.all(), 1)
}

func TestSnoozeEndsAndIncidentsOpen(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.pubsubChannel(t)
	pol := e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))
	e.createSnooze(t, snoozeOf("s", epoch(), epoch().Add(3*time.Minute), "", pol.GetName()))
	b := e.handler.Backend

	for m := 1; m <= 2; m++ {
		e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	b.Tick(t.Context())
	require.Empty(t, b.Incidents(testProject))

	for m := 3; m <= 4; m++ {
		e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	b.Tick(t.Context())
	require.Len(t, b.Incidents(testProject), 1)
	assert.Len(t, e.pub.all(), 1)
}

func TestSnoozeSilencesCloseNotification(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.pubsubChannel(t)
	pol := e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))
	b := e.handler.Backend

	for m := 1; m <= 2; m++ {
		e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	b.Tick(t.Context())
	require.Len(t, e.pub.all(), 1)

	e.createSnooze(t, snoozeOf("s", epoch().Add(2*time.Minute), epoch().Add(time.Hour), "", pol.GetName()))
	e.writeAt(t, epoch().Add(3*time.Minute), "gold", map[string]float64{"inst-1": 1})
	b.Tick(t.Context())

	incs := b.Incidents(testProject)
	require.Len(t, incs, 1)
	assert.Equal(t, "closed", incs[0].State)
	assert.Len(t, e.pub.all(), 1, "no closed notification while snoozed")
}

func TestSnoozeSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	pol := src.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3)).GetName()
	s := src.createSnooze(t, snoozeOf("keep", epoch(), epoch().Add(time.Hour), `resource.labels.zone = "z"`, pol))

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), src.handler.Backend.Snapshot(t.Context())))

	got, err := dst.snoozes(t).GetSnooze(t.Context(), &monitoringpb.GetSnoozeRequest{Name: s.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "keep", got.GetDisplayName())
	assert.Equal(t, `resource.labels.zone = "z"`, got.GetCriteria().GetFilter())

	dst.handler.Reset()

	_, err = dst.snoozes(t).GetSnooze(t.Context(), &monitoringpb.GetSnoozeRequest{Name: s.GetName()})
	assert.Equal(t, codes.NotFound, codeOf(err))
}
