package ecs_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func pauseHookConfig(
	stage types.DeploymentLifecycleHookStage,
	timeoutMin int32,
	action types.DeploymentLifecycleHookAction,
) *types.DeploymentConfiguration {
	return &types.DeploymentConfiguration{
		Strategy:          types.DeploymentStrategyBlueGreen,
		BakeTimeInMinutes: aws.Int32(0),
		LifecycleHooks: []types.DeploymentLifecycleHook{{
			TargetType:      types.DeploymentLifecycleHookTargetTypePause,
			LifecycleStages: []types.DeploymentLifecycleHookStage{stage},
			TimeoutConfiguration: &types.DeploymentLifecycleHookTimeoutConfiguration{
				Action: action, TimeoutInMinutes: aws.Int32(timeoutMin),
			},
		}},
	}
}

func lifecycleFixture(t *testing.T, client *ecssdk.Client, dc *types.DeploymentConfiguration) (string, string) {
	t.Helper()

	_, err := client.CreateCluster(t.Context(), &ecssdk.CreateClusterInput{ClusterName: aws.String("lc")})
	require.NoError(t, err)

	td, err := client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
		Family:               aws.String("lc-td"),
		ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("app"), Image: aws.String("nginx")}},
	})
	require.NoError(t, err)

	svc, err := client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
		Cluster: aws.String("lc"), ServiceName: aws.String("svc"),
		TaskDefinition:          td.TaskDefinition.TaskDefinitionArn,
		DeploymentConfiguration: dc,
	})
	require.NoError(t, err)

	return aws.ToString(svc.Service.ServiceArn), aws.ToString(td.TaskDefinition.TaskDefinitionArn)
}

func describeLatestDeployment(t *testing.T, client *ecssdk.Client, serviceArn string) types.ServiceDeployment {
	t.Helper()

	list, err := client.ListServiceDeployments(t.Context(), &ecssdk.ListServiceDeploymentsInput{
		Cluster: aws.String("lc"), Service: aws.String(serviceArn),
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.ServiceDeployments)

	desc, err := client.DescribeServiceDeployments(t.Context(), &ecssdk.DescribeServiceDeploymentsInput{
		ServiceDeploymentArns: []string{aws.ToString(list.ServiceDeployments[0].ServiceDeploymentArn)},
	})
	require.NoError(t, err)
	require.Len(t, desc.ServiceDeployments, 1)

	return desc.ServiceDeployments[0]
}

func TestRealClient_BlueGreenContinue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     types.DeploymentLifecycleHookAction
		wantStatus types.ServiceDeploymentStatus
		wantStage  types.ServiceDeploymentLifecycleStage
	}{
		{
			"continue",
			types.DeploymentLifecycleHookActionContinue,
			types.ServiceDeploymentStatusSuccessful,
			types.ServiceDeploymentLifecycleStageCleanUp,
		},
		{
			"default_action", "",
			types.ServiceDeploymentStatusSuccessful, types.ServiceDeploymentLifecycleStageCleanUp,
		},
		{
			"rollback",
			types.DeploymentLifecycleHookActionRollback,
			types.ServiceDeploymentStatusRollbackSuccessful,
			types.ServiceDeploymentLifecycleStagePostScaleUp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			cfg := pauseHookConfig(
				types.DeploymentLifecycleHookStagePostScaleUp,
				60,
				types.DeploymentLifecycleHookActionRollback,
			)
			svcArn, _ := lifecycleFixture(t, client, cfg)

			if tt.action == types.DeploymentLifecycleHookActionRollback {
				_, err := client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
					Family: aws.String("lc-td"),
					ContainerDefinitions: []types.ContainerDefinition{
						{Name: aws.String("app"), Image: aws.String("nginx:2")},
					},
				})
				require.NoError(t, err)

				first := describeLatestDeployment(t, client, svcArn)
				_, err = client.ContinueServiceDeployment(t.Context(), &ecssdk.ContinueServiceDeploymentInput{
					ServiceDeploymentArn: first.ServiceDeploymentArn,
					HookId:               first.LifecycleHookDetails[0].HookId,
				})
				require.NoError(t, err)

				_, err = client.UpdateService(t.Context(), &ecssdk.UpdateServiceInput{
					Cluster: aws.String("lc"), Service: aws.String(svcArn), ForceNewDeployment: true,
					TaskDefinition: aws.String("lc-td:2"),
				})
				require.NoError(t, err)
			}

			dep := latestInProgress(t, client, svcArn)
			assert.Equal(t, types.ServiceDeploymentLifecycleStagePostScaleUp, dep.LifecycleStage)
			assert.Equal(t, types.DeploymentStrategyBlueGreen, dep.DeploymentConfiguration.Strategy)
			require.Len(t, dep.LifecycleHookDetails, 1)

			hook := dep.LifecycleHookDetails[0]
			assert.Equal(t, types.DeploymentLifecycleHookStatusAwaitingAction, hook.Status)
			assert.Equal(t, types.DeploymentLifecycleHookTargetTypePause, hook.TargetType)
			assert.Equal(t, types.DeploymentLifecycleHookActionRollback, hook.TimeoutAction)
			assert.NotNil(t, hook.ExpiresAt)

			_, err := client.ContinueServiceDeployment(t.Context(), &ecssdk.ContinueServiceDeploymentInput{
				ServiceDeploymentArn: dep.ServiceDeploymentArn, HookId: hook.HookId, Action: tt.action,
			})
			require.NoError(t, err)

			got := describeByArn(t, client, aws.ToString(dep.ServiceDeploymentArn))
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.Equal(t, tt.wantStage, got.LifecycleStage)

			_, err = client.ContinueServiceDeployment(t.Context(), &ecssdk.ContinueServiceDeploymentInput{
				ServiceDeploymentArn: dep.ServiceDeploymentArn, HookId: hook.HookId,
			})
			require.Error(t, err)
		})
	}
}

func latestInProgress(t *testing.T, client *ecssdk.Client, serviceArn string) types.ServiceDeployment {
	t.Helper()

	list, err := client.ListServiceDeployments(t.Context(), &ecssdk.ListServiceDeploymentsInput{
		Cluster: aws.String("lc"), Service: aws.String(serviceArn),
		Status: []types.ServiceDeploymentStatus{types.ServiceDeploymentStatusInProgress},
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.ServiceDeployments)

	return describeByArn(t, client, aws.ToString(list.ServiceDeployments[0].ServiceDeploymentArn))
}

func describeByArn(t *testing.T, client *ecssdk.Client, arn string) types.ServiceDeployment {
	t.Helper()

	desc, err := client.DescribeServiceDeployments(t.Context(), &ecssdk.DescribeServiceDeploymentsInput{
		ServiceDeploymentArns: []string{arn},
	})
	require.NoError(t, err)
	require.Len(t, desc.ServiceDeployments, 1)

	return desc.ServiceDeployments[0]
}

func TestRealClient_DeploymentConfigurationValidation(t *testing.T) {
	t.Parallel()

	bad := map[string]*types.DeploymentConfiguration{
		"strategy":      {Strategy: "NOPE"},
		"pause_at_test": pauseHookConfig(types.DeploymentLifecycleHookStageTestTrafficShift, 5, ""),
		"hook_no_stage": {
			Strategy:       types.DeploymentStrategyBlueGreen,
			LifecycleHooks: []types.DeploymentLifecycleHook{{TargetType: types.DeploymentLifecycleHookTargetTypePause}},
		},
		"lambda_no_target": {
			Strategy: types.DeploymentStrategyBlueGreen,
			LifecycleHooks: []types.DeploymentLifecycleHook{
				{LifecycleStages: []types.DeploymentLifecycleHookStage{types.DeploymentLifecycleHookStagePreScaleUp}},
			},
		},
		"canary_wrong_mode": {
			Strategy:            types.DeploymentStrategyBlueGreen,
			CanaryConfiguration: &types.CanaryConfiguration{},
		},
		"linear_percent": {
			Strategy:            types.DeploymentStrategyLinear,
			LinearConfiguration: &types.LinearConfiguration{StepPercent: aws.Float64(1)},
		},
		"bake_range": {Strategy: types.DeploymentStrategyBlueGreen, BakeTimeInMinutes: aws.Int32(2000)},
	}

	for name, dc := range bad {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			td, err := client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
				Family: aws.String("v"),
				ContainerDefinitions: []types.ContainerDefinition{
					{Name: aws.String("app"), Image: aws.String("nginx")},
				},
			})
			require.NoError(t, err)

			_, err = client.CreateService(t.Context(), &ecssdk.CreateServiceInput{
				ServiceName: aws.String("s"), TaskDefinition: td.TaskDefinition.TaskDefinitionArn,
				DeploymentConfiguration: dc,
			})
			require.Error(t, err)
		})
	}
}

func TestBlueGreen_HookTimeoutAndBakeTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		wantStatus string
	}{
		{name: "timeout_continue", action: "CONTINUE", wantStatus: "SUCCESSFUL"},
		{name: "timeout_rollback_without_previous_revision", action: "ROLLBACK", wantStatus: "ROLLBACK_FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
				_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "c"})
				require.NoError(t, err)

				td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
					Family:               "td",
					ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
				})
				require.NoError(t, err)

				one, bake := 1, 2
				_, err = backend.CreateService(ecs.CreateServiceInput{
					Cluster: "c", ServiceName: "s", TaskDefinition: td.TaskDefinitionArn,
					DeploymentConfiguration: &ecs.DeploymentConfiguration{
						Strategy: "BLUE_GREEN", BakeTimeInMinutes: &bake,
						LifecycleHooks: []ecs.DeploymentLifecycleHook{
							{
								TargetType:      "PAUSE",
								LifecycleStages: []string{"PRE_SCALE_UP"},
								TimeoutConfiguration: &ecs.DeploymentLifecycleHookTimeout{
									TimeoutInMinutes: &one,
									Action:           tt.action,
								},
							},
						},
					},
				})
				require.NoError(t, err)

				list, err := backend.ListServiceDeployments("c", "s")
				require.NoError(t, err)
				require.Len(t, list, 1)

				got, _, err := backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
				require.NoError(t, err)
				assert.Equal(t, "PRE_SCALE_UP", got[0].LifecycleStage)
				assert.Equal(t, "IN_PROGRESS", got[0].Status)

				time.Sleep(61 * time.Second)

				got, _, err = backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
				require.NoError(t, err)

				if tt.action == "ROLLBACK" {
					assert.Equal(t, tt.wantStatus, got[0].Status)
					assert.Equal(t, "TIMED_OUT", got[0].LifecycleHookDetails[0].Status)

					return
				}

				assert.Equal(t, "BAKE_TIME", got[0].LifecycleStage)
				assert.Equal(t, "IN_PROGRESS", got[0].Status)

				time.Sleep(2 * time.Minute)

				got, _, err = backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, got[0].Status)
				assert.Equal(t, "CLEAN_UP", got[0].LifecycleStage)
			})
		})
	}
}

type fakeAlarms struct{ triggered []string }

func (f fakeAlarms) TriggeredAlarms([]string) []string { return f.triggered }

func TestBlueGreen_AlarmsFailDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		rollback   bool
	}{
		{name: "rollback_without_previous_revision", rollback: true, wantStatus: "ROLLBACK_FAILED"},
		{name: "no_rollback", rollback: false, wantStatus: "STOPPED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			backend.SetAlarmStateProvider(fakeAlarms{triggered: []string{"high-5xx"}})

			_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)

			td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
				Family:               "td",
				ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
			})
			require.NoError(t, err)

			_, err = backend.CreateService(ecs.CreateServiceInput{
				Cluster: "c", ServiceName: "s", TaskDefinition: td.TaskDefinitionArn,
				DeploymentConfiguration: &ecs.DeploymentConfiguration{
					Strategy: "BLUE_GREEN",
					Alarms: &ecs.DeploymentAlarms{
						AlarmNames: []string{"high-5xx"},
						Enable:     true,
						Rollback:   tt.rollback,
					},
				},
			})
			require.NoError(t, err)

			list, err := backend.ListServiceDeployments("c", "s")
			require.NoError(t, err)

			got, _, err := backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got[0].Status)
			require.NotNil(t, got[0].Alarms)
			assert.Equal(t, "TRIGGERED", got[0].Alarms.Status)
			assert.Equal(t, []string{"high-5xx"}, got[0].Alarms.TriggeredAlarmNames)
		})
	}
}
