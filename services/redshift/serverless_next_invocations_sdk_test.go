package redshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftserverlesssdk "github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestSDKRoundTrip_ServerlessScheduledActionNextInvocations proves NextInvocations is computed from the Schedule union.
func TestSDKRoundTrip_ServerlessScheduledActionNextInvocations(t *testing.T) {
	t.Parallel()

	future := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second)

	tests := []struct {
		schedule types.Schedule
		check    func(t *testing.T, got []time.Time)
		enabled  *bool
		end      *time.Time
		name     string
	}{
		{
			name:     "at_future",
			schedule: &types.ScheduleMemberAt{Value: future},
			check: func(t *testing.T, got []time.Time) {
				t.Helper()
				require.Len(t, got, 1)
				assert.True(t, future.Equal(got[0]))
			},
		},
		{
			name:     "at_past",
			schedule: &types.ScheduleMemberAt{Value: time.Now().UTC().Add(-time.Hour)},
			check: func(t *testing.T, got []time.Time) {
				t.Helper()
				assert.Empty(t, got)
			},
		},
		{
			name:     "cron_mondays",
			schedule: &types.ScheduleMemberCron{Value: "0 10 ? * MON *"},
			check: func(t *testing.T, got []time.Time) {
				t.Helper()
				require.Len(t, got, 5)

				for i, g := range got {
					assert.Equal(t, time.Monday, g.UTC().Weekday())
					assert.Equal(t, 10, g.UTC().Hour())
					assert.True(t, g.After(time.Now()))

					if i > 0 {
						assert.Equal(t, 7*24*time.Hour, g.Sub(got[i-1]))
					}
				}
			},
		},
		{
			name:     "cron_bounded_by_end_time",
			schedule: &types.ScheduleMemberCron{Value: "0 10 ? * MON *"},
			end:      aws.Time(time.Now().UTC().Add(10 * 24 * time.Hour)),
			check: func(t *testing.T, got []time.Time) {
				t.Helper()
				assert.LessOrEqual(t, len(got), 2)
				assert.NotEmpty(t, got)
			},
		},
		{
			name:     "disabled_has_none",
			schedule: &types.ScheduleMemberCron{Value: "0 10 ? * MON *"},
			enabled:  aws.Bool(false),
			check: func(t *testing.T, got []time.Time) {
				t.Helper()
				assert.Empty(t, got)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
			client := newTestServerlessClient(t, h)

			target := &types.TargetActionMemberCreateSnapshot{
				Value: types.CreateSnapshotScheduleActionParameters{
					NamespaceName:      aws.String("ns"),
					SnapshotNamePrefix: aws.String("p"),
				},
			}

			_, err := client.CreateScheduledAction(t.Context(), &redshiftserverlesssdk.CreateScheduledActionInput{
				ScheduledActionName: aws.String("sa"),
				Schedule:            tt.schedule,
				Enabled:             tt.enabled,
				EndTime:             tt.end,
				RoleArn:             aws.String("arn:aws:iam::000000000000:role/scheduler"),
				NamespaceName:       aws.String("ns"),
				TargetAction:        target,
			})
			require.NoError(t, err)

			out, err := client.GetScheduledAction(t.Context(), &redshiftserverlesssdk.GetScheduledActionInput{
				ScheduledActionName: aws.String("sa"),
			})
			require.NoError(t, err)
			tt.check(t, out.ScheduledAction.NextInvocations)
		})
	}
}
