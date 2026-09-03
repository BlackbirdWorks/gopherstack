package autoscaling_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_BatchScheduledActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		run     func(t *testing.T, b *autoscaling.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "put_then_delete",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sa-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				desired := int32(3)
				failed, err := b.BatchPutScheduledUpdateGroupAction("sa-g", []autoscaling.ScheduledUpdateGroupAction{
					{ScheduledActionName: "scale-up", DesiredCapacity: &desired},
				})
				require.NoError(t, err)
				assert.Empty(t, failed)

				failedDel, err := b.BatchDeleteScheduledAction("sa-g", []string{"scale-up"})
				require.NoError(t, err)
				assert.Empty(t, failedDel)
			},
		},
		{
			name: "delete_nonexistent_action_reports_failure",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sa-empty",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				failed, err := b.BatchDeleteScheduledAction("sa-empty", []string{"ghost"})
				require.NoError(t, err)
				require.Len(t, failed, 1)
				assert.Equal(t, "ghost", failed[0].ScheduledActionName)
			},
		},
		{
			name: "put_missing_name_reports_failure",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sa-badname",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				failed, err := b.BatchPutScheduledUpdateGroupAction("sa-badname",
					[]autoscaling.ScheduledUpdateGroupAction{{ScheduledActionName: ""}})
				require.NoError(t, err)
				require.Len(t, failed, 1)
			},
		},
		{
			name: "put_group_not_found",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.BatchPutScheduledUpdateGroupAction("no-such", nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete_group_not_found",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.BatchDeleteScheduledAction("no-such", []string{"a"})
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			if tt.run != nil {
				tt.run(t, b)
			}
		})
	}
}

func TestInMemoryBackend_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "sa-asg",
		MinSize:              0,
		MaxSize:              10,
	})
	require.NoError(t, err)

	_, err = b.BatchPutScheduledUpdateGroupAction("sa-asg", []autoscaling.ScheduledUpdateGroupAction{
		{ScheduledActionName: "action-a"},
		{ScheduledActionName: "action-b"},
	})
	require.NoError(t, err)

	// All actions for the group
	actions, err := b.DescribeScheduledActions("sa-asg", nil, time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Len(t, actions, 2)

	// Filter by name
	filtered, err := b.DescribeScheduledActions("sa-asg", []string{"action-a"}, time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Len(t, filtered, 1)
	assert.Equal(t, "action-a", filtered[0].ScheduledActionName)

	// Group not found
	_, err = b.DescribeScheduledActions("no-such", nil, time.Time{}, time.Time{})
	require.Error(t, err)
}

func TestInMemoryBackend_PutScheduledUpdateGroupAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action  autoscaling.ScheduledUpdateGroupAction
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "put_scheduled_action_success",
			group: "suga-asg",
			action: autoscaling.ScheduledUpdateGroupAction{
				ScheduledActionName: "scale-out",
				Recurrence:          "0 9 * * MON-FRI",
			},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "suga-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
		},
		{
			name:    "put_scheduled_action_group_not_found",
			group:   "no-such",
			wantErr: true,
			action:  autoscaling.ScheduledUpdateGroupAction{ScheduledActionName: "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.PutScheduledUpdateGroupAction(tt.group, tt.action)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DeleteScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		action  string
		wantErr bool
	}{
		{
			name:   "delete_existing_scheduled_action",
			group:  "dsa-asg",
			action: "scale-out",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dsa-asg",
					MinSize:              0,
					MaxSize:              10,
				})
				_ = b.PutScheduledUpdateGroupAction("dsa-asg", autoscaling.ScheduledUpdateGroupAction{
					ScheduledActionName: "scale-out",
				})
			},
		},
		{
			name:    "delete_nonexistent_action",
			group:   "dsa-asg2",
			action:  "ghost-action",
			wantErr: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dsa-asg2",
					MinSize:              0,
					MaxSize:              10,
				})
			},
		},
		{
			name:    "delete_group_not_found",
			group:   "no-such",
			action:  "a",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeleteScheduledAction(tt.group, tt.action)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
