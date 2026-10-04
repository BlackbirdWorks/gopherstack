package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PutMetricAlarmInput.EvaluationWindow (api_op_PutMetricAlarm.go): sliding or wall clock, round-tripped and validated.
func TestPutMetricAlarm_EvaluationWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		window   cwtypes.EvaluationWindow
		name     string
		wantTZ   string
		period   int32
		wantWall bool
		wantErr  bool
		wantNil  bool
	}{
		{name: "omitted", period: 60, wantNil: true},
		{name: "sliding", period: 60, window: &cwtypes.EvaluationWindowMemberSlidingWindow{}},
		{
			name: "wall-utc-default", period: 3600, wantWall: true,
			window: &cwtypes.EvaluationWindowMemberWallClockWindow{},
		},
		{
			name: "wall-iana", period: 86400, wantWall: true, wantTZ: "America/New_York",
			window: &cwtypes.EvaluationWindowMemberWallClockWindow{
				Value: cwtypes.WallClockWindow{Timezone: aws.String("America/New_York")},
			},
		},
		{
			name: "wall-offset", period: 300, wantWall: true, wantTZ: "+05:30",
			window: &cwtypes.EvaluationWindowMemberWallClockWindow{
				Value: cwtypes.WallClockWindow{Timezone: aws.String("+05:30")},
			},
		},
		{
			name: "wall-bad-period", period: 120, wantErr: true,
			window: &cwtypes.EvaluationWindowMemberWallClockWindow{},
		},
		{
			name: "wall-bad-zone", period: 60, wantErr: true,
			window: &cwtypes.EvaluationWindowMemberWallClockWindow{
				Value: cwtypes.WallClockWindow{Timezone: aws.String("Mars/Olympus")},
			},
		},
		{
			name: "wall-offset-not-multiple-of-5", period: 60, wantErr: true,
			window: &cwtypes.EvaluationWindowMemberWallClockWindow{
				Value: cwtypes.WallClockWindow{Timezone: aws.String("+05:32")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.PutMetricAlarm(ctx, &cwsdk.PutMetricAlarmInput{
				AlarmName: aws.String("ew"), Namespace: aws.String("NS"), MetricName: aws.String("M"),
				ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold, Threshold: aws.Float64(1),
				EvaluationPeriods: aws.Int32(1), Period: aws.Int32(tt.period), Statistic: cwtypes.StatisticAverage,
				EvaluationWindow: tt.window,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			out, err := client.DescribeAlarms(ctx, &cwsdk.DescribeAlarmsInput{AlarmNames: []string{"ew"}})
			require.NoError(t, err)
			require.Len(t, out.MetricAlarms, 1)

			got := out.MetricAlarms[0].EvaluationWindow
			if tt.wantNil {
				assert.Nil(t, got)

				return
			}

			if !tt.wantWall {
				_, isSliding := got.(*cwtypes.EvaluationWindowMemberSlidingWindow)
				assert.True(t, isSliding, "got %T", got)

				return
			}

			wall, isWall := got.(*cwtypes.EvaluationWindowMemberWallClockWindow)
			require.True(t, isWall, "got %T", got)
			assert.Equal(t, tt.wantTZ, aws.ToString(wall.Value.Timezone))
		})
	}
}
