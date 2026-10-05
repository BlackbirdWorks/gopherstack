package scheduler_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

func TestSchedule_OmittedFieldsTakeSystemDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		update     *schedulersdk.UpdateScheduleInput
		wantState  types.ScheduleState
		wantAction types.ActionAfterCompletion
		wantTZ     string
	}{
		{
			name: "update omitting optionals resets to defaults",
			update: &schedulersdk.UpdateScheduleInput{
				Name:               aws.String("s"),
				ScheduleExpression: aws.String("rate(5 minutes)"),
				FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
				Target: &types.Target{
					Arn:     aws.String("arn:aws:sqs:us-east-1:0:q"),
					RoleArn: aws.String("arn:aws:iam::0:role/r"),
				},
			},
			wantState: types.ScheduleStateEnabled, wantAction: types.ActionAfterCompletionNone, wantTZ: "UTC",
		},
		{
			name: "update with explicit values keeps them",
			update: &schedulersdk.UpdateScheduleInput{
				Name:                       aws.String("s"),
				ScheduleExpression:         aws.String("rate(5 minutes)"),
				State:                      types.ScheduleStateDisabled,
				ActionAfterCompletion:      types.ActionAfterCompletionDelete,
				ScheduleExpressionTimezone: aws.String("Europe/Paris"),
				FlexibleTimeWindow:         &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
				Target: &types.Target{
					Arn:     aws.String("arn:aws:sqs:us-east-1:0:q"),
					RoleArn: aws.String("arn:aws:iam::0:role/r"),
				},
			},
			wantState:  types.ScheduleStateDisabled,
			wantAction: types.ActionAfterCompletionDelete,
			wantTZ:     "Europe/Paris",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSchedulerClient(
				t,
				scheduler.NewHandler(scheduler.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			ctx := t.Context()

			_, err := client.CreateSchedule(ctx, &schedulersdk.CreateScheduleInput{
				Name:                  aws.String("s"),
				ScheduleExpression:    aws.String("rate(1 minute)"),
				State:                 types.ScheduleStateDisabled,
				ActionAfterCompletion: types.ActionAfterCompletionDelete,
				FlexibleTimeWindow:    &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
				Target: &types.Target{
					Arn:     aws.String("arn:aws:sqs:us-east-1:0:q"),
					RoleArn: aws.String("arn:aws:iam::0:role/r"),
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateSchedule(ctx, tc.update)
			require.NoError(t, err)

			got, err := client.GetSchedule(ctx, &schedulersdk.GetScheduleInput{Name: aws.String("s")})
			require.NoError(t, err)
			assert.Equal(t, tc.wantState, got.State)
			assert.Equal(t, tc.wantAction, got.ActionAfterCompletion)
			assert.Equal(t, tc.wantTZ, aws.ToString(got.ScheduleExpressionTimezone))
		})
	}
}
