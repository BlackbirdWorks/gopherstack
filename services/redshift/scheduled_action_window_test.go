package redshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduledAction_WindowAndRangeFilter(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		start    *time.Time
		end      *time.Time
		qStart   *time.Time
		qEnd     *time.Time
		name     string
		wantName string
		want     int
	}{
		{name: "no window no range", want: 1},
		{name: "range inside open window", qStart: aws.Time(now), qEnd: aws.Time(now.Add(72 * time.Hour)), want: 1},
		{name: "range ends before first run", qEnd: aws.Time(now.Add(-time.Hour)), want: 0},
		{
			name: "action ended before range", end: aws.Time(now.Add(24 * time.Hour)),
			qStart: aws.Time(now.Add(72 * time.Hour)), want: 0,
		},
		{
			name: "action starts after range", start: aws.Time(now.Add(240 * time.Hour)),
			qEnd: aws.Time(now.Add(48 * time.Hour)), want: 0,
		},
		{
			name:   "window overlaps range",
			start:  aws.Time(now.Add(-time.Hour)),
			end:    aws.Time(now.Add(240 * time.Hour)),
			qStart: aws.Time(now.Add(48 * time.Hour)),
			qEnd:   aws.Time(now.Add(120 * time.Hour)),
			want:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newPaginationBackendAndClient(t)
			ctx := t.Context()

			created, err := client.CreateScheduledAction(ctx, &redshiftsdk.CreateScheduledActionInput{
				ScheduledActionName: aws.String("win"),
				Schedule:            aws.String("cron(0 12 * * ? *)"),
				IamRole:             aws.String("arn:aws:iam::000000000000:role/R"),
				TargetAction: &types.ScheduledActionType{
					PauseCluster: &types.PauseClusterMessage{ClusterIdentifier: aws.String("c1")},
				},
				StartTime: tt.start,
				EndTime:   tt.end,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.start != nil, created.StartTime != nil)
			assert.Equal(t, tt.end != nil, created.EndTime != nil)

			out, err := client.DescribeScheduledActions(ctx, &redshiftsdk.DescribeScheduledActionsInput{
				StartTime: tt.qStart,
				EndTime:   tt.qEnd,
			})
			require.NoError(t, err)
			assert.Len(t, out.ScheduledActions, tt.want)

			if tt.start != nil && tt.want == 1 {
				assert.WithinDuration(t, *tt.start, aws.ToTime(out.ScheduledActions[0].StartTime), time.Second)
			}
		})
	}
}

func TestModifyScheduledAction_Window(t *testing.T) {
	t.Parallel()

	_, client := newPaginationBackendAndClient(t)
	ctx := t.Context()

	_, err := client.CreateScheduledAction(ctx, &redshiftsdk.CreateScheduledActionInput{
		ScheduledActionName: aws.String("mod"),
		Schedule:            aws.String("cron(0 12 * * ? *)"),
		IamRole:             aws.String("arn:aws:iam::000000000000:role/R"),
		TargetAction: &types.ScheduledActionType{
			PauseCluster: &types.PauseClusterMessage{ClusterIdentifier: aws.String("c1")},
		},
	})
	require.NoError(t, err)

	end := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second)

	mod, err := client.ModifyScheduledAction(ctx, &redshiftsdk.ModifyScheduledActionInput{
		ScheduledActionName: aws.String("mod"),
		EndTime:             aws.Time(end),
	})
	require.NoError(t, err)
	assert.WithinDuration(t, end, aws.ToTime(mod.EndTime), time.Second)
	assert.Nil(t, mod.StartTime)
}
