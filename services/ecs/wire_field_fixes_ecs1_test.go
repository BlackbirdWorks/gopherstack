package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/require"
)

// TestDescribeTasks_TagResource_LiveSync_RealClient proves that tags applied
// to a running task via TagResource after RunTask are visible through
// DescribeTasks. Task.Tags is a real member of ecs@v1.90.0's types.Task
// (types/types.go), and the backend already tracks tag mutations correctly
// -- ListTagsForResource for the same task ARN sees them, proven by a second
// op. Before the fix, DescribeTasks read the task's creation-time Tags
// snapshot directly instead of the resourceTags side map that
// TagResource/UntagResource write into, so any tag applied after RunTask was
// permanently invisible to DescribeTasks even though it was tracked and
// correctly returned by ListTagsForResource.
func TestDescribeTasks_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "livesync-tags")

	runOut, err := client.RunTask(ctx, &ecssdk.RunTaskInput{
		TaskDefinition: aws.String(tdArn),
		Tags: []ecstypes.Tag{
			{Key: aws.String("owner"), Value: aws.String("sre")},
		},
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)
	taskArn := runOut.Tasks[0].TaskArn

	_, err = client.TagResource(ctx, &ecssdk.TagResourceInput{
		ResourceArn: taskArn,
		Tags: []ecstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	describeOut, err := client.DescribeTasks(ctx, &ecssdk.DescribeTasksInput{
		Tasks: []string{*taskArn},
	})
	require.NoError(t, err)
	require.Len(t, describeOut.Tasks, 1)

	got := make(map[string]string, len(describeOut.Tasks[0].Tags))
	for _, tag := range describeOut.Tasks[0].Tags {
		got[*tag.Key] = *tag.Value
	}

	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, got)

	_, err = client.UntagResource(ctx, &ecssdk.UntagResourceInput{
		ResourceArn: taskArn,
		TagKeys:     []string{"owner"},
	})
	require.NoError(t, err)

	describeOut, err = client.DescribeTasks(ctx, &ecssdk.DescribeTasksInput{
		Tasks: []string{*taskArn},
	})
	require.NoError(t, err)
	require.Len(t, describeOut.Tasks, 1)

	got = make(map[string]string, len(describeOut.Tasks[0].Tags))
	for _, tag := range describeOut.Tasks[0].Tags {
		got[*tag.Key] = *tag.Value
	}

	require.Equal(t, map[string]string{"env": "prod"}, got)
}

// TestStopTask_TagResource_LiveSync_RealClient proves StopTask's response
// also reflects tags applied after RunTask, not just DescribeTasks. StopTask
// takes the fast (no docker runner configured in this test) path through
// taskWithLiveTagsLocked in tasks.go.
func TestStopTask_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "livesync-stop-tags")

	runOut, err := client.RunTask(ctx, &ecssdk.RunTaskInput{
		TaskDefinition: aws.String(tdArn),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)
	taskArn := runOut.Tasks[0].TaskArn

	_, err = client.TagResource(ctx, &ecssdk.TagResourceInput{
		ResourceArn: taskArn,
		Tags: []ecstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	stopOut, err := client.StopTask(ctx, &ecssdk.StopTaskInput{
		Task: taskArn,
	})
	require.NoError(t, err)

	got := make(map[string]string, len(stopOut.Task.Tags))
	for _, tag := range stopOut.Task.Tags {
		got[*tag.Key] = *tag.Value
	}

	require.Equal(t, map[string]string{"env": "prod"}, got)
}

// TestUpdateCapacityProvider_TagResource_LiveSync_RealClient is the same bug
// on CapacityProvider.Tags, a real member of ecs@v1.90.0's
// types.CapacityProvider that UpdateCapacityProviderOutput echoes back
// unconditionally (unlike DescribeCapacityProviders, which is correctly
// gated behind Include=["TAGS"] and already sources tags live via
// ListTagsForResource -- this bug does not reach that path). Before the fix,
// UpdateCapacityProvider read the capacity provider's creation-time Tags
// snapshot directly instead of the resourceTags side map that
// TagResource/UntagResource write into, so a tag applied after
// CreateCapacityProvider was invisible in every subsequent
// UpdateCapacityProvider response even though ListTagsForResource for the
// same ARN returned it correctly.
func TestUpdateCapacityProvider_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateCapacityProvider(ctx, &ecssdk.CreateCapacityProviderInput{
		Name: aws.String("livesync-cp"),
		AutoScalingGroupProvider: &ecstypes.AutoScalingGroupProvider{
			AutoScalingGroupArn: aws.String(
				"arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:asg-livesync",
			),
		},
		Tags: []ecstypes.Tag{
			{Key: aws.String("owner"), Value: aws.String("sre")},
		},
	})
	require.NoError(t, err)
	cpArn := createOut.CapacityProvider.CapacityProviderArn
	cpName := createOut.CapacityProvider.Name

	_, err = client.TagResource(ctx, &ecssdk.TagResourceInput{
		ResourceArn: cpArn,
		Tags: []ecstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	updateOut, err := client.UpdateCapacityProvider(ctx, &ecssdk.UpdateCapacityProviderInput{
		Name: cpName,
	})
	require.NoError(t, err)

	got := make(map[string]string, len(updateOut.CapacityProvider.Tags))
	for _, tag := range updateOut.CapacityProvider.Tags {
		got[*tag.Key] = *tag.Value
	}

	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, got)
}
