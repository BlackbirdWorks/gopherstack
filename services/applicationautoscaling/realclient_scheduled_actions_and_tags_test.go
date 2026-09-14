package applicationautoscaling_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

// TestRealClient_ScheduledActionsAndTags drives applicationautoscaling's
// typed-coverage-blind ops (gopherstack-n3zi) through the real
// aws-sdk-go-v2 client.
func TestRealClient_ScheduledActionsAndTags(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			client := newTestAASSDKClient(
				t,
				applicationautoscaling.NewHandler(
					applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1"),
				),
			)
			ctx := t.Context()

			regOut, err := client.RegisterScalableTarget(ctx, &aassdk.RegisterScalableTargetInput{
				ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
				ResourceId:        aws.String("table/s15-tags-table"),
				ScalableDimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
				MinCapacity:       aws.Int32(1),
				MaxCapacity:       aws.Int32(10),
			})
			require.NoError(t, err)
			arn := aws.ToString(regOut.ScalableTargetARN)
			require.NotEmpty(t, arn)

			_, err = client.TagResource(ctx, &aassdk.TagResourceInput{
				ResourceARN: aws.String(arn),
				Tags:        map[string]string{"env": "test", "team": "platform"},
			})
			require.NoError(t, err)

			listOut, err := client.ListTagsForResource(ctx, &aassdk.ListTagsForResourceInput{
				ResourceARN: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test", "team": "platform"}, listOut.Tags)

			_, err = client.UntagResource(ctx, &aassdk.UntagResourceInput{
				ResourceARN: aws.String(arn),
				TagKeys:     []string{"team"},
			})
			require.NoError(t, err)

			afterOut, err := client.ListTagsForResource(ctx, &aassdk.ListTagsForResourceInput{
				ResourceARN: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test"}, afterOut.Tags)
		}},
		{name: "scheduled action lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newTestAASSDKClient(
				t,
				applicationautoscaling.NewHandler(
					applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1"),
				),
			)
			ctx := t.Context()

			_, err := client.RegisterScalableTarget(ctx, &aassdk.RegisterScalableTargetInput{
				ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
				ResourceId:        aws.String("table/s15-sched-table"),
				ScalableDimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
				MinCapacity:       aws.Int32(1),
				MaxCapacity:       aws.Int32(10),
			})
			require.NoError(t, err)

			startTime := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
			_, err = client.PutScheduledAction(ctx, &aassdk.PutScheduledActionInput{
				ServiceNamespace:    aastypes.ServiceNamespaceDynamodb,
				ResourceId:          aws.String("table/s15-sched-table"),
				ScalableDimension:   aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
				ScheduledActionName: aws.String("s15-scale-up"),
				Schedule:            aws.String("rate(1 hour)"),
				StartTime:           aws.Time(startTime),
				ScalableTargetAction: &aastypes.ScalableTargetAction{
					MinCapacity: aws.Int32(2),
					MaxCapacity: aws.Int32(20),
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeScheduledActions(ctx, &aassdk.DescribeScheduledActionsInput{
				ServiceNamespace: aastypes.ServiceNamespaceDynamodb,
				ResourceId:       aws.String("table/s15-sched-table"),
			})
			require.NoError(t, err)
			require.Len(t, descOut.ScheduledActions, 1)
			action := descOut.ScheduledActions[0]
			assert.Equal(t, "s15-scale-up", aws.ToString(action.ScheduledActionName))
			require.NotNil(t, action.ScalableTargetAction)
			assert.EqualValues(t, 2, aws.ToInt32(action.ScalableTargetAction.MinCapacity))
			assert.EqualValues(t, 20, aws.ToInt32(action.ScalableTargetAction.MaxCapacity))
			assert.WithinDuration(t, startTime, aws.ToTime(action.StartTime), time.Second)

			_, err = client.DeleteScheduledAction(ctx, &aassdk.DeleteScheduledActionInput{
				ServiceNamespace:    aastypes.ServiceNamespaceDynamodb,
				ResourceId:          aws.String("table/s15-sched-table"),
				ScalableDimension:   aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
				ScheduledActionName: aws.String("s15-scale-up"),
			})
			require.NoError(t, err)

			afterOut, err := client.DescribeScheduledActions(ctx, &aassdk.DescribeScheduledActionsInput{
				ServiceNamespace: aastypes.ServiceNamespaceDynamodb,
				ResourceId:       aws.String("table/s15-sched-table"),
			})
			require.NoError(t, err)
			assert.Empty(t, afterOut.ScheduledActions)
		}},
		{name: "scaling activities", run: func(t *testing.T) {
			t.Helper()

			client := newTestAASSDKClient(
				t,
				applicationautoscaling.NewHandler(
					applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1"),
				),
			)
			ctx := t.Context()

			_, err := client.RegisterScalableTarget(ctx, &aassdk.RegisterScalableTargetInput{
				ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
				ResourceId:        aws.String("table/s15-activity-table"),
				ScalableDimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
				MinCapacity:       aws.Int32(1),
				MaxCapacity:       aws.Int32(10),
			})
			require.NoError(t, err)

			actOut, err := client.DescribeScalingActivities(ctx, &aassdk.DescribeScalingActivitiesInput{
				ServiceNamespace:  aastypes.ServiceNamespaceDynamodb,
				ResourceId:        aws.String("table/s15-activity-table"),
				ScalableDimension: aastypes.ScalableDimensionDynamoDBTableReadCapacityUnits,
			})
			require.NoError(t, err)
			require.NotEmpty(t, actOut.ScalingActivities)
			for _, a := range actOut.ScalingActivities {
				assert.NotEmpty(t, aws.ToString(a.ActivityId))
				assert.Equal(t, aastypes.ServiceNamespaceDynamodb, a.ServiceNamespace)
			}
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
