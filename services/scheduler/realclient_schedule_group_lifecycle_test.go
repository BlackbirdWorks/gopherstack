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

// TestRealClient_ScheduleGroupLifecycle drives scheduler's typed-coverage-
// blind ops (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_ScheduleGroupLifecycle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "schedule group lifecycle and tags",
			run: func(t *testing.T) {
				t.Helper()

				backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
				client := newTestSchedulerClient(t, scheduler.NewHandler(backend))
				ctx := t.Context()

				createOut, err := client.CreateScheduleGroup(ctx, &schedulersdk.CreateScheduleGroupInput{
					Name: aws.String("s15-group"),
					Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)
				groupARN := aws.ToString(createOut.ScheduleGroupArn)
				require.NotEmpty(t, groupARN)

				getOut, err := client.GetScheduleGroup(ctx, &schedulersdk.GetScheduleGroupInput{
					Name: aws.String("s15-group"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s15-group", aws.ToString(getOut.Name))
				assert.Equal(t, groupARN, aws.ToString(getOut.Arn))
				assert.Equal(t, types.ScheduleGroupStateActive, getOut.State)

				_, err = client.CreateScheduleGroup(ctx, &schedulersdk.CreateScheduleGroupInput{
					Name: aws.String("s15-group-2"),
				})
				require.NoError(t, err)

				listOut, err := client.ListScheduleGroups(ctx, &schedulersdk.ListScheduleGroupsInput{})
				require.NoError(t, err)
				names := map[string]bool{}
				for _, g := range listOut.ScheduleGroups {
					names[aws.ToString(g.Name)] = true
				}
				assert.True(t, names["s15-group"])
				assert.True(t, names["s15-group-2"])

				_, err = client.TagResource(ctx, &schedulersdk.TagResourceInput{
					ResourceArn: aws.String(groupARN),
					Tags:        []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
				})
				require.NoError(t, err)

				tagsOut, err := client.ListTagsForResource(ctx, &schedulersdk.ListTagsForResourceInput{
					ResourceArn: aws.String(groupARN),
				})
				require.NoError(t, err)
				tags := map[string]string{}
				for _, tg := range tagsOut.Tags {
					tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
				}
				assert.Equal(t, "test", tags["env"])
				assert.Equal(t, "platform", tags["team"])

				_, err = client.UntagResource(ctx, &schedulersdk.UntagResourceInput{
					ResourceArn: aws.String(groupARN),
					TagKeys:     []string{"team"},
				})
				require.NoError(t, err)

				afterTagsOut, err := client.ListTagsForResource(ctx, &schedulersdk.ListTagsForResourceInput{
					ResourceArn: aws.String(groupARN),
				})
				require.NoError(t, err)
				afterTags := map[string]string{}
				for _, tg := range afterTagsOut.Tags {
					afterTags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
				}
				assert.Equal(t, "test", afterTags["env"])
				assert.NotContains(t, afterTags, "team")

				_, err = client.DeleteScheduleGroup(
					ctx,
					&schedulersdk.DeleteScheduleGroupInput{Name: aws.String("s15-group-2")},
				)
				require.NoError(t, err)

				_, err = client.GetScheduleGroup(
					ctx,
					&schedulersdk.GetScheduleGroupInput{Name: aws.String("s15-group-2")},
				)
				require.Error(t, err)
			},
		},
		{
			name: "schedule update and delete",
			run: func(t *testing.T) {
				t.Helper()

				backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
				client := newTestSchedulerClient(t, scheduler.NewHandler(backend))
				ctx := t.Context()

				_, err := client.CreateSchedule(ctx, &schedulersdk.CreateScheduleInput{
					Name:               aws.String("s15-schedule"),
					ScheduleExpression: aws.String("rate(5 minutes)"),
					Target: &types.Target{
						Arn:     aws.String("arn:aws:sqs:us-east-1:000000000000:s15-queue"),
						RoleArn: aws.String("arn:aws:iam::000000000000:role/s15-role"),
					},
					FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
				})
				require.NoError(t, err)

				updOut, err := client.UpdateSchedule(ctx, &schedulersdk.UpdateScheduleInput{
					Name:               aws.String("s15-schedule"),
					ScheduleExpression: aws.String("rate(10 minutes)"),
					Target: &types.Target{
						Arn:     aws.String("arn:aws:sqs:us-east-1:000000000000:s15-queue"),
						RoleArn: aws.String("arn:aws:iam::000000000000:role/s15-role"),
					},
					FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(updOut.ScheduleArn))

				getOut, err := client.GetSchedule(ctx, &schedulersdk.GetScheduleInput{Name: aws.String("s15-schedule")})
				require.NoError(t, err)
				assert.Equal(t, "rate(10 minutes)", aws.ToString(getOut.ScheduleExpression))

				_, err = client.DeleteSchedule(ctx, &schedulersdk.DeleteScheduleInput{Name: aws.String("s15-schedule")})
				require.NoError(t, err)

				_, err = client.GetSchedule(ctx, &schedulersdk.GetScheduleInput{Name: aws.String("s15-schedule")})
				require.Error(t, err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
