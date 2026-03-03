package scheduler_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *scheduler.InMemoryBackend) string
		verify func(t *testing.T, b *scheduler.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *scheduler.InMemoryBackend) string {
				sched, err := b.CreateSchedule(
					"test-schedule",
					"rate(1 minute)",
					scheduler.Target{
						ARN:     "arn:aws:lambda:us-east-1:000000000000:function:test",
						RoleARN: "arn:aws:iam::000000000000:role/test",
					},
					"ENABLED",
					scheduler.FlexibleTimeWindow{Mode: "OFF"},
				)
				if err != nil {
					return ""
				}

				return sched.Name
			},
			verify: func(t *testing.T, b *scheduler.InMemoryBackend, id string) {
				t.Helper()

				sched, err := b.GetSchedule(id)
				require.NoError(t, err)
				assert.Equal(t, id, sched.Name)
				assert.Equal(t, "rate(1 minute)", sched.ScheduleExpression)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *scheduler.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *scheduler.InMemoryBackend, _ string) {
				t.Helper()

				schedules := b.ListSchedules()
				assert.Empty(t, schedules)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
