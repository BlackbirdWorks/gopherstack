package autoscaling

import (
	"context"
	"testing"
	"time"
)

// TestScheduledActionDue locks scheduledActionDue's decision table: one-time
// actions fire exactly once at/after StartTime, recurring actions respect
// their StartTime/EndTime window and never refire the same occurrence, and an
// unparseable Recurrence is treated as never-due rather than panicking.
func TestScheduledActionDue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	tick := time.Minute

	tests := []struct {
		action *ScheduledAction
		name   string
		want   bool
	}{
		{
			name:   "one_time_not_yet_started",
			action: &ScheduledAction{StartTime: now.Add(time.Hour)},
			want:   false,
		},
		{
			name:   "one_time_due",
			action: &ScheduledAction{StartTime: now.Add(-time.Minute)},
			want:   true,
		},
		{
			name: "one_time_already_fired",
			action: &ScheduledAction{
				StartTime:        now.Add(-time.Hour),
				LastExecutedTime: now.Add(-time.Minute),
			},
			want: false,
		},
		{
			name:   "recurring_due_within_tick_window",
			action: &ScheduledAction{Recurrence: "* * * * *"},
			want:   true,
		},
		{
			name: "recurring_not_yet_due",
			// Fires at :30 past the hour; "now" is exactly on the hour, so the
			// next occurrence (10:30) is still in the future.
			action: &ScheduledAction{Recurrence: "30 * * * *"},
			want:   false,
		},
		{
			name: "recurring_already_fired_this_occurrence",
			action: &ScheduledAction{
				Recurrence:       "* * * * *",
				LastExecutedTime: now,
			},
			want: false,
		},
		{
			name: "recurring_before_start_time",
			action: &ScheduledAction{
				Recurrence: "* * * * *",
				StartTime:  now.Add(time.Hour),
			},
			want: false,
		},
		{
			name: "recurring_after_end_time",
			action: &ScheduledAction{
				Recurrence: "* * * * *",
				EndTime:    now.Add(-time.Hour),
			},
			want: false,
		},
		{
			name:   "recurring_invalid_cron_never_due",
			action: &ScheduledAction{Recurrence: "not a cron expression"},
			want:   false,
		},
		{
			name:   "no_recurrence_no_start_time_never_due",
			action: &ScheduledAction{},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := scheduledActionDue(tt.action, now, tick); got != tt.want {
				t.Errorf("scheduledActionDue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestApplyDueScheduledActions_OneTimeFiresOnceOnly locks the end-to-end
// path: a due one-time scheduled action mutates its group's capacity exactly
// once, records LastExecutedTime, and does not refire on a later tick.
func TestApplyDueScheduledActions_OneTimeFiresOnceOnly(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	t.Cleanup(b.Close)

	_, err := b.CreateAutoScalingGroup(CreateAutoScalingGroupInput{
		AutoScalingGroupName: "sched-once-asg",
		MinSize:              0,
		MaxSize:              10,
		DesiredCapacity:      1,
	})
	if err != nil {
		t.Fatalf("CreateAutoScalingGroup: %v", err)
	}

	desired := int32(5)
	now := time.Now().UTC()

	err = b.PutScheduledUpdateGroupAction("sched-once-asg", ScheduledUpdateGroupAction{
		ScheduledActionName: "scale-once",
		StartTime:           now.Add(-time.Minute),
		DesiredCapacity:     &desired,
	})
	if err != nil {
		t.Fatalf("PutScheduledUpdateGroupAction: %v", err)
	}

	ctx := context.Background()

	b.applyDueScheduledActions(ctx, now, time.Minute)

	groups, err := b.DescribeAutoScalingGroups([]string{"sched-once-asg"}, nil)
	if err != nil {
		t.Fatalf("DescribeAutoScalingGroups: %v", err)
	}

	if got := groups[0].DesiredCapacity; got != desired {
		t.Fatalf("DesiredCapacity after first tick = %d, want %d", got, desired)
	}

	// A second, later tick must not refire the one-time action even though a
	// naive "StartTime has passed" check alone would still be true.
	err = b.SetDesiredCapacity("sched-once-asg", 1)
	if err != nil {
		t.Fatalf("SetDesiredCapacity: %v", err)
	}

	b.applyDueScheduledActions(ctx, now.Add(time.Hour), time.Minute)

	groups, err = b.DescribeAutoScalingGroups([]string{"sched-once-asg"}, nil)
	if err != nil {
		t.Fatalf("DescribeAutoScalingGroups: %v", err)
	}

	if got := groups[0].DesiredCapacity; got != 1 {
		t.Fatalf("DesiredCapacity after second tick = %d, want 1 (one-time action must not refire)", got)
	}
}

// TestApplyDueScheduledActions_RecurringFiresEveryOccurrence locks that a
// recurring action fires again on each subsequent due occurrence (not just
// once), unlike a one-time action.
func TestApplyDueScheduledActions_RecurringFiresEveryOccurrence(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	t.Cleanup(b.Close)

	_, err := b.CreateAutoScalingGroup(CreateAutoScalingGroupInput{
		AutoScalingGroupName: "sched-recurring-asg",
		MinSize:              0,
		MaxSize:              10,
		DesiredCapacity:      1,
	})
	if err != nil {
		t.Fatalf("CreateAutoScalingGroup: %v", err)
	}

	desired := int32(3)
	now := time.Now().UTC().Truncate(time.Minute)

	err = b.PutScheduledUpdateGroupAction("sched-recurring-asg", ScheduledUpdateGroupAction{
		ScheduledActionName: "scale-every-minute",
		Recurrence:          "* * * * *",
		DesiredCapacity:     &desired,
	})
	if err != nil {
		t.Fatalf("PutScheduledUpdateGroupAction: %v", err)
	}

	ctx := context.Background()

	b.applyDueScheduledActions(ctx, now, time.Minute)

	actions, err := b.DescribeScheduledActions("sched-recurring-asg", nil, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("DescribeScheduledActions: %v", err)
	}

	if actions[0].LastExecutedTime.IsZero() {
		t.Fatalf("LastExecutedTime not stamped after first fire")
	}

	// Reset capacity, then simulate the next minute's tick: a recurring "every
	// minute" action must be due again.
	if setErr := b.SetDesiredCapacity("sched-recurring-asg", 1); setErr != nil {
		t.Fatalf("SetDesiredCapacity: %v", setErr)
	}

	b.applyDueScheduledActions(ctx, now.Add(time.Minute), time.Minute)

	groups, err := b.DescribeAutoScalingGroups([]string{"sched-recurring-asg"}, nil)
	if err != nil {
		t.Fatalf("DescribeAutoScalingGroups: %v", err)
	}

	if got := groups[0].DesiredCapacity; got != desired {
		t.Fatalf("DesiredCapacity after second occurrence = %d, want %d (recurring action must refire)", got, desired)
	}
}

// TestApplyDueScheduledActions_InvalidCapacityDoesNotPanic locks that a
// scheduled action whose Min/Max/Desired combination is no longer valid at
// fire time (e.g. conflicts with a concurrent manual update) is skipped
// safely -- logged, not applied, LastExecutedTime still stamped -- rather
// than panicking or wedging the scheduler.
func TestApplyDueScheduledActions_InvalidCapacityDoesNotPanic(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	t.Cleanup(b.Close)

	_, err := b.CreateAutoScalingGroup(CreateAutoScalingGroupInput{
		AutoScalingGroupName: "sched-invalid-asg",
		MinSize:              0,
		MaxSize:              10,
		DesiredCapacity:      1,
	})
	if err != nil {
		t.Fatalf("CreateAutoScalingGroup: %v", err)
	}

	badMin := int32(20)
	badMax := int32(5)
	now := time.Now().UTC()

	err = b.PutScheduledUpdateGroupAction("sched-invalid-asg", ScheduledUpdateGroupAction{
		ScheduledActionName: "scale-invalid",
		StartTime:           now.Add(-time.Minute),
		MinSize:             &badMin,
		MaxSize:             &badMax,
	})
	if err != nil {
		t.Fatalf("PutScheduledUpdateGroupAction: %v", err)
	}

	ctx := context.Background()

	// Must not panic.
	b.applyDueScheduledActions(ctx, now, time.Minute)

	groups, err := b.DescribeAutoScalingGroups([]string{"sched-invalid-asg"}, nil)
	if err != nil {
		t.Fatalf("DescribeAutoScalingGroups: %v", err)
	}

	if got := groups[0].MinSize; got != 0 {
		t.Fatalf("MinSize = %d, want unchanged 0 (invalid scheduled change must not apply)", got)
	}

	actions, err := b.DescribeScheduledActions("sched-invalid-asg", nil, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("DescribeScheduledActions: %v", err)
	}

	if actions[0].LastExecutedTime.IsZero() {
		t.Fatalf("LastExecutedTime not stamped after a failed apply (would busy-loop retry every tick)")
	}
}

// TestScheduledActionScheduler_RunFiresAndStopsCleanly is an end-to-end check
// that Run, driven by a real ticker, actually applies a due action, and that
// cancelling its context stops the goroutine promptly (no leaked ticker).
func TestScheduledActionScheduler_RunFiresAndStopsCleanly(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	t.Cleanup(b.Close)

	_, err := b.CreateAutoScalingGroup(CreateAutoScalingGroupInput{
		AutoScalingGroupName: "sched-run-asg",
		MinSize:              0,
		MaxSize:              10,
		DesiredCapacity:      1,
	})
	if err != nil {
		t.Fatalf("CreateAutoScalingGroup: %v", err)
	}

	desired := int32(4)

	err = b.PutScheduledUpdateGroupAction("sched-run-asg", ScheduledUpdateGroupAction{
		ScheduledActionName: "scale-run",
		StartTime:           time.Now().UTC().Add(-time.Minute),
		DesiredCapacity:     &desired,
	})
	if err != nil {
		t.Fatalf("PutScheduledUpdateGroupAction: %v", err)
	}

	const tickInterval = 10 * time.Millisecond

	sched := NewScheduledActionScheduler(b, tickInterval)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		sched.Run(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		groups, describeErr := b.DescribeAutoScalingGroups([]string{"sched-run-asg"}, nil)
		if describeErr != nil {
			t.Fatalf("DescribeAutoScalingGroups: %v", describeErr)
		}

		if groups[0].DesiredCapacity == desired {
			break
		}

		time.Sleep(tickInterval)
	}

	groups, err := b.DescribeAutoScalingGroups([]string{"sched-run-asg"}, nil)
	if err != nil {
		t.Fatalf("DescribeAutoScalingGroups: %v", err)
	}

	if got := groups[0].DesiredCapacity; got != desired {
		t.Fatalf("DesiredCapacity = %d, want %d (Run() never applied the due action)", got, desired)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return within 2s of context cancellation")
	}
}
