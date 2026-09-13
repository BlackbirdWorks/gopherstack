package autoscaling_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_LifecycleScalingAndNotifications drives autoscaling's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_LifecycleScalingAndNotifications(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "instance lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.CreateLaunchConfiguration(ctx, &assdk.CreateLaunchConfigurationInput{
				LaunchConfigurationName: aws.String("s35-lc"),
				ImageId:                 aws.String("ami-s35"),
				InstanceType:            aws.String("t3.micro"),
			})
			require.NoError(t, err)

			groupName := "s35-instances-group"
			_, err = client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName:    aws.String(groupName),
				LaunchConfigurationName: aws.String("s35-lc"),
				MinSize:                 aws.Int32(0),
				MaxSize:                 aws.Int32(10),
				DesiredCapacity:         aws.Int32(0),
				AvailabilityZones:       []string{"us-east-1a"},
			})
			require.NoError(t, err)

			attachOut, err := client.AttachInstances(ctx, &assdk.AttachInstancesInput{
				AutoScalingGroupName: aws.String(groupName),
				InstanceIds:          []string{"i-s35-attach1", "i-s35-attach2"},
			})
			require.NoError(t, err)
			require.NotNil(t, attachOut)

			launchOut, err := client.LaunchInstances(ctx, &assdk.LaunchInstancesInput{
				AutoScalingGroupName: aws.String(groupName),
				ClientToken:          aws.String("s35-token"),
				RequestedCapacity:    aws.Int32(2),
			})
			require.NoError(t, err)
			assert.Equal(t, groupName, aws.ToString(launchOut.AutoScalingGroupName))
			assert.Equal(t, "s35-token", aws.ToString(launchOut.ClientToken))
			require.NotEmpty(t, launchOut.Instances)

			launchedIDs := make([]string, 0)
			for _, coll := range launchOut.Instances {
				launchedIDs = append(launchedIDs, coll.InstanceIds...)
			}
			require.NotEmpty(t, launchedIDs)

			standbyOut, err := client.EnterStandby(ctx, &assdk.EnterStandbyInput{
				AutoScalingGroupName:           aws.String(groupName),
				InstanceIds:                    launchedIDs,
				ShouldDecrementDesiredCapacity: aws.Bool(false),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, standbyOut.Activities)

			exitOut, err := client.ExitStandby(ctx, &assdk.ExitStandbyInput{
				AutoScalingGroupName: aws.String(groupName),
				InstanceIds:          launchedIDs,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, exitOut.Activities)

			_, err = client.SetInstanceHealth(ctx, &assdk.SetInstanceHealthInput{
				InstanceId:               aws.String("i-s35-attach1"),
				HealthStatus:             aws.String("Unhealthy"),
				ShouldRespectGracePeriod: aws.Bool(false),
			})
			require.NoError(t, err)

			descInst, err := client.DescribeAutoScalingInstances(ctx, &assdk.DescribeAutoScalingInstancesInput{
				InstanceIds: []string{"i-s35-attach1"},
			})
			require.NoError(t, err)
			require.Len(t, descInst.AutoScalingInstances, 1)
			assert.Equal(t, "Unhealthy", aws.ToString(descInst.AutoScalingInstances[0].HealthStatus))

			_, err = client.SetDesiredCapacity(ctx, &assdk.SetDesiredCapacityInput{
				AutoScalingGroupName: aws.String(groupName),
				DesiredCapacity:      aws.Int32(3),
			})
			require.NoError(t, err)

			descGroup, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
			})
			require.NoError(t, err)
			require.Len(t, descGroup.AutoScalingGroups, 1)
			assert.Equal(t, int32(3), aws.ToInt32(descGroup.AutoScalingGroups[0].DesiredCapacity))

			detachOut, err := client.DetachInstances(ctx, &assdk.DetachInstancesInput{
				AutoScalingGroupName:           aws.String(groupName),
				InstanceIds:                    []string{"i-s35-attach2"},
				ShouldDecrementDesiredCapacity: aws.Bool(true),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, detachOut.Activities)

			termOut, err := client.TerminateInstanceInAutoScalingGroup(
				ctx,
				&assdk.TerminateInstanceInAutoScalingGroupInput{
					InstanceId:                     aws.String("i-s35-attach1"),
					ShouldDecrementDesiredCapacity: aws.Bool(true),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, termOut.Activity)
			assert.NotEmpty(t, aws.ToString(termOut.Activity.ActivityId))
			assert.Equal(t, groupName, aws.ToString(termOut.Activity.AutoScalingGroupName))
		}},
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "s35-tags-group"
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(1),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = client.CreateOrUpdateTags(ctx, &assdk.CreateOrUpdateTagsInput{
				Tags: []types.Tag{
					{
						ResourceId:        aws.String(groupName),
						ResourceType:      aws.String("auto-scaling-group"),
						Key:               aws.String("s35-key"),
						Value:             aws.String("s35-value"),
						PropagateAtLaunch: aws.Bool(true),
					},
				},
			})
			require.NoError(t, err)

			tagsOut, err := client.DescribeTags(ctx, &assdk.DescribeTagsInput{})
			require.NoError(t, err)
			require.Len(t, tagsOut.Tags, 1)
			assert.Equal(t, "s35-value", aws.ToString(tagsOut.Tags[0].Value))
			assert.True(t, aws.ToBool(tagsOut.Tags[0].PropagateAtLaunch))

			_, err = client.DeleteTags(ctx, &assdk.DeleteTagsInput{
				Tags: []types.Tag{
					{
						ResourceId:   aws.String(groupName),
						ResourceType: aws.String("auto-scaling-group"),
						Key:          aws.String("s35-key"),
						Value:        aws.String("s35-value"),
					},
				},
			})
			require.NoError(t, err)

			tagsOut2, err := client.DescribeTags(ctx, &assdk.DescribeTagsInput{})
			require.NoError(t, err)
			assert.Empty(t, tagsOut2.Tags)
		}},
		{name: "notifications", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "s35-notif-group"
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(1),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			typesOut, err := client.DescribeAutoScalingNotificationTypes(
				ctx,
				&assdk.DescribeAutoScalingNotificationTypesInput{},
			)
			require.NoError(t, err)
			assert.NotEmpty(t, typesOut.AutoScalingNotificationTypes)

			topicARN := "arn:aws:sns:us-east-1:123456789012:s35-topic"
			_, err = client.PutNotificationConfiguration(ctx, &assdk.PutNotificationConfigurationInput{
				AutoScalingGroupName: aws.String(groupName),
				TopicARN:             aws.String(topicARN),
				NotificationTypes:    []string{"autoscaling:EC2_INSTANCE_LAUNCH"},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeNotificationConfigurations(
				ctx,
				&assdk.DescribeNotificationConfigurationsInput{
					AutoScalingGroupNames: []string{groupName},
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.NotificationConfigurations, 1)
			assert.Equal(t, topicARN, aws.ToString(descOut.NotificationConfigurations[0].TopicARN))

			_, err = client.DeleteNotificationConfiguration(ctx, &assdk.DeleteNotificationConfigurationInput{
				AutoScalingGroupName: aws.String(groupName),
				TopicARN:             aws.String(topicARN),
			})
			require.NoError(t, err)

			descOut2, err := client.DescribeNotificationConfigurations(
				ctx,
				&assdk.DescribeNotificationConfigurationsInput{
					AutoScalingGroupNames: []string{groupName},
				},
			)
			require.NoError(t, err)
			assert.Empty(t, descOut2.NotificationConfigurations)
		}},
		{name: "scheduled actions", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "s35-sched-group"
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(10),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			future := time.Now().Add(24 * time.Hour)

			_, err = client.PutScheduledUpdateGroupAction(ctx, &assdk.PutScheduledUpdateGroupActionInput{
				AutoScalingGroupName: aws.String(groupName),
				ScheduledActionName:  aws.String("s35-sched1"),
				StartTime:            aws.Time(future),
				MinSize:              aws.Int32(1),
				MaxSize:              aws.Int32(5),
				DesiredCapacity:      aws.Int32(3),
			})
			require.NoError(t, err)

			descOut, err := client.DescribeScheduledActions(ctx, &assdk.DescribeScheduledActionsInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			require.Len(t, descOut.ScheduledUpdateGroupActions, 1)
			assert.Equal(t, int32(3), aws.ToInt32(descOut.ScheduledUpdateGroupActions[0].DesiredCapacity))

			batchPutOut, err := client.BatchPutScheduledUpdateGroupAction(
				ctx,
				&assdk.BatchPutScheduledUpdateGroupActionInput{
					AutoScalingGroupName: aws.String(groupName),
					ScheduledUpdateGroupActions: []types.ScheduledUpdateGroupActionRequest{
						{
							ScheduledActionName: aws.String("s35-sched2"),
							StartTime:           aws.Time(future),
							DesiredCapacity:     aws.Int32(2),
						},
						{
							ScheduledActionName: aws.String("s35-sched3"),
							StartTime:           aws.Time(future),
							DesiredCapacity:     aws.Int32(4),
						},
					},
				},
			)
			require.NoError(t, err)
			assert.Empty(t, batchPutOut.FailedScheduledUpdateGroupActions)

			_, err = client.DeleteScheduledAction(ctx, &assdk.DeleteScheduledActionInput{
				AutoScalingGroupName: aws.String(groupName),
				ScheduledActionName:  aws.String("s35-sched1"),
			})
			require.NoError(t, err)

			batchDelOut, err := client.BatchDeleteScheduledAction(ctx, &assdk.BatchDeleteScheduledActionInput{
				AutoScalingGroupName: aws.String(groupName),
				ScheduledActionNames: []string{"s35-sched2", "s35-sched3"},
			})
			require.NoError(t, err)
			assert.Empty(t, batchDelOut.FailedScheduledActions)

			finalOut, err := client.DescribeScheduledActions(ctx, &assdk.DescribeScheduledActionsInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			assert.Empty(t, finalOut.ScheduledUpdateGroupActions)
		}},
		{name: "scaling policies", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "s35-policy-group"
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(10),
				DesiredCapacity:      aws.Int32(1),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			adjOut, err := client.DescribeAdjustmentTypes(ctx, &assdk.DescribeAdjustmentTypesInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, adjOut.AdjustmentTypes)

			_, err = client.PutScalingPolicy(ctx, &assdk.PutScalingPolicyInput{
				AutoScalingGroupName: aws.String(groupName),
				PolicyName:           aws.String("s35-policy"),
				PolicyType:           aws.String("SimpleScaling"),
				AdjustmentType:       aws.String("ChangeInCapacity"),
				ScalingAdjustment:    aws.Int32(2),
			})
			require.NoError(t, err)

			_, err = client.ExecutePolicy(ctx, &assdk.ExecutePolicyInput{
				AutoScalingGroupName: aws.String(groupName),
				PolicyName:           aws.String("s35-policy"),
				HonorCooldown:        aws.Bool(false),
			})
			require.NoError(t, err)

			descGroup, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
			})
			require.NoError(t, err)
			require.Len(t, descGroup.AutoScalingGroups, 1)
			assert.Equal(t, int32(3), aws.ToInt32(descGroup.AutoScalingGroups[0].DesiredCapacity))

			_, err = client.DeletePolicy(ctx, &assdk.DeletePolicyInput{
				AutoScalingGroupName: aws.String(groupName),
				PolicyName:           aws.String("s35-policy"),
			})
			require.NoError(t, err)

			policiesOut, err := client.DescribePolicies(ctx, &assdk.DescribePoliciesInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			assert.Empty(t, policiesOut.ScalingPolicies)
		}},
		{name: "instance refresh cancel and rollback", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "s35-refresh-group"
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(5),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			startOut, err := client.StartInstanceRefresh(ctx, &assdk.StartInstanceRefreshInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(startOut.InstanceRefreshId))

			cancelOut, err := client.CancelInstanceRefresh(ctx, &assdk.CancelInstanceRefreshInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(startOut.InstanceRefreshId), aws.ToString(cancelOut.InstanceRefreshId))

			startOut2, err := client.StartInstanceRefresh(ctx, &assdk.StartInstanceRefreshInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(startOut2.InstanceRefreshId))

			rollbackOut, err := client.RollbackInstanceRefresh(ctx, &assdk.RollbackInstanceRefreshInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(startOut2.InstanceRefreshId), aws.ToString(rollbackOut.InstanceRefreshId))
		}},
		{name: "metrics collection", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "s35-metrics-group"
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(1),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			typesOut, err := client.DescribeMetricCollectionTypes(ctx, &assdk.DescribeMetricCollectionTypesInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, typesOut.Metrics)
			assert.NotEmpty(t, typesOut.Granularities)

			_, err = client.EnableMetricsCollection(ctx, &assdk.EnableMetricsCollectionInput{
				AutoScalingGroupName: aws.String(groupName),
				Metrics:              []string{"GroupMinSize"},
				Granularity:          aws.String("1Minute"),
			})
			require.NoError(t, err)

			descOut, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
			})
			require.NoError(t, err)
			require.Len(t, descOut.AutoScalingGroups, 1)
			assert.NotEmpty(t, descOut.AutoScalingGroups[0].EnabledMetrics)

			_, err = client.DisableMetricsCollection(ctx, &assdk.DisableMetricsCollectionInput{
				AutoScalingGroupName: aws.String(groupName),
				Metrics:              []string{"GroupMinSize"},
			})
			require.NoError(t, err)

			descOut2, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
			})
			require.NoError(t, err)
			require.Len(t, descOut2.AutoScalingGroups, 1)
			assert.Empty(t, descOut2.AutoScalingGroups[0].EnabledMetrics)
		}},
		{name: "process management and describe types", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			groupName := "s35-process-group"
			_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(1),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = client.SuspendProcesses(ctx, &assdk.SuspendProcessesInput{
				AutoScalingGroupName: aws.String(groupName),
				ScalingProcesses:     []string{"Launch"},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
			})
			require.NoError(t, err)
			require.Len(t, descOut.AutoScalingGroups, 1)
			require.Len(t, descOut.AutoScalingGroups[0].SuspendedProcesses, 1)
			assert.Equal(t, "Launch", aws.ToString(descOut.AutoScalingGroups[0].SuspendedProcesses[0].ProcessName))

			_, err = client.ResumeProcesses(ctx, &assdk.ResumeProcessesInput{
				AutoScalingGroupName: aws.String(groupName),
				ScalingProcesses:     []string{"Launch"},
			})
			require.NoError(t, err)

			descOut2, err := client.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{groupName},
			})
			require.NoError(t, err)
			require.Len(t, descOut2.AutoScalingGroups, 1)
			assert.Empty(t, descOut2.AutoScalingGroups[0].SuspendedProcesses)

			_, err = client.PutLifecycleHook(ctx, &assdk.PutLifecycleHookInput{
				AutoScalingGroupName: aws.String(groupName),
				LifecycleHookName:    aws.String("s35-hook"),
				LifecycleTransition:  aws.String("autoscaling:EC2_INSTANCE_LAUNCHING"),
				DefaultResult:        aws.String("CONTINUE"),
			})
			require.NoError(t, err)

			hooksOut, err := client.DescribeLifecycleHooks(ctx, &assdk.DescribeLifecycleHooksInput{
				AutoScalingGroupName: aws.String(groupName),
			})
			require.NoError(t, err)
			require.Len(t, hooksOut.LifecycleHooks, 1)
			assert.Equal(t, "s35-hook", aws.ToString(hooksOut.LifecycleHooks[0].LifecycleHookName))
			assert.Equal(t, "CONTINUE", aws.ToString(hooksOut.LifecycleHooks[0].DefaultResult))

			hookTypesOut, err := client.DescribeLifecycleHookTypes(ctx, &assdk.DescribeLifecycleHookTypesInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, hookTypesOut.LifecycleHookTypes)

			procTypesOut, err := client.DescribeScalingProcessTypes(ctx, &assdk.DescribeScalingProcessTypesInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, procTypesOut.Processes)

			termTypesOut, err := client.DescribeTerminationPolicyTypes(
				ctx,
				&assdk.DescribeTerminationPolicyTypesInput{},
			)
			require.NoError(t, err)
			assert.NotEmpty(t, termTypesOut.TerminationPolicyTypes)

			limitsOut, err := client.DescribeAccountLimits(ctx, &assdk.DescribeAccountLimitsInput{})
			require.NoError(t, err)
			assert.Positive(t, aws.ToInt32(limitsOut.MaxNumberOfAutoScalingGroups))
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
