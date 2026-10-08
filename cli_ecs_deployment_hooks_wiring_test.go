package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newECSBlueGreenService(
	t *testing.T, client *ecs.Client, dc *ecstypes.DeploymentConfiguration,
) ecstypes.ServiceDeployment {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("hooks")})
	require.NoError(t, err)

	td, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family:               aws.String("hooks-td"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{{Name: aws.String("app"), Image: aws.String("nginx")}},
	})
	require.NoError(t, err)

	svc, err := client.CreateService(ctx, &ecs.CreateServiceInput{
		Cluster: aws.String("hooks"), ServiceName: aws.String("svc"),
		TaskDefinition: td.TaskDefinition.TaskDefinitionArn, DeploymentConfiguration: dc,
	})
	require.NoError(t, err)

	list, err := client.ListServiceDeployments(ctx, &ecs.ListServiceDeploymentsInput{
		Cluster: aws.String("hooks"), Service: svc.Service.ServiceArn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.ServiceDeployments)

	desc, err := client.DescribeServiceDeployments(ctx, &ecs.DescribeServiceDeploymentsInput{
		ServiceDeploymentArns: []string{aws.ToString(list.ServiceDeployments[0].ServiceDeploymentArn)},
	})
	require.NoError(t, err)
	require.Len(t, desc.ServiceDeployments, 1)

	return desc.ServiceDeployments[0]
}

func TestECSDeploymentAlarmsReadCloudWatchState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      cwtypes.StateValue
		wantStatus string
		wantAlarms string
	}{
		{name: "alarm_state", state: cwtypes.StateValueAlarm, wantStatus: "STOPPED", wantAlarms: "TRIGGERED"},
		{name: "ok_state", state: cwtypes.StateValueOk, wantStatus: "SUCCESSFUL", wantAlarms: "MONITORING_COMPLETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			cw := cloudwatch.NewFromConfig(fx.cfg)

			_, err := cw.PutMetricAlarm(t.Context(), &cloudwatch.PutMetricAlarmInput{
				AlarmName: aws.String("deploy-5xx"), Namespace: aws.String("App"), MetricName: aws.String("5xx"),
				ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold, Threshold: aws.Float64(1),
				EvaluationPeriods: aws.Int32(1), Period: aws.Int32(60), Statistic: cwtypes.StatisticSum,
			})
			require.NoError(t, err)

			_, err = cw.SetAlarmState(t.Context(), &cloudwatch.SetAlarmStateInput{
				AlarmName: aws.String("deploy-5xx"), StateValue: tt.state, StateReason: aws.String("test"),
			})
			require.NoError(t, err)

			got := newECSBlueGreenService(t, ecs.NewFromConfig(fx.cfg), &ecstypes.DeploymentConfiguration{
				Strategy: ecstypes.DeploymentStrategyBlueGreen,
				Alarms: &ecstypes.DeploymentAlarms{
					AlarmNames: []string{"deploy-5xx"}, Enable: true, Rollback: false,
				},
			})

			assert.Equal(t, tt.wantStatus, string(got.Status))
			require.NotNil(t, got.Alarms)
			assert.Equal(t, tt.wantAlarms, string(got.Alarms.Status))
		})
	}
}

func TestECSLambdaHookIsInvoked(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)

	got := newECSBlueGreenService(t, ecs.NewFromConfig(fx.cfg), &ecstypes.DeploymentConfiguration{
		Strategy: ecstypes.DeploymentStrategyBlueGreen, BakeTimeInMinutes: aws.Int32(0),
		LifecycleHooks: []ecstypes.DeploymentLifecycleHook{{
			TargetType:      ecstypes.DeploymentLifecycleHookTargetTypeAwsLambda,
			HookTargetArn:   aws.String("arn:aws:lambda:us-east-1:000000000000:function:missing-gate"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/hook"),
			LifecycleStages: []ecstypes.DeploymentLifecycleHookStage{ecstypes.DeploymentLifecycleHookStagePreScaleUp},
		}},
	})

	require.Len(t, got.LifecycleHookDetails, 1)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:missing-gate",
		aws.ToString(got.LifecycleHookDetails[0].TargetArn))
	assert.Equal(t, ecstypes.DeploymentLifecycleHookStatusFailed, got.LifecycleHookDetails[0].Status)
}
