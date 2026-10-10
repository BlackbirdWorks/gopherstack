package ecs_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestTrafficShiftTiming(t *testing.T) {
	t.Parallel()

	two, three, fifty, zero := 2, 3, 50.0, 0

	tests := []struct {
		dc   *ecs.DeploymentConfiguration
		name string
		want time.Duration
	}{
		{
			name: "linear_two_steps",
			dc: &ecs.DeploymentConfiguration{
				Strategy: "LINEAR", BakeTimeInMinutes: &zero,
				LinearConfiguration: &ecs.LinearConfiguration{StepPercent: &fifty, StepBakeTimeInMinutes: &two},
			},
			want: 2 * time.Minute,
		},
		{
			name: "canary_bake",
			dc: &ecs.DeploymentConfiguration{
				Strategy: "CANARY", BakeTimeInMinutes: &zero,
				CanaryConfiguration: &ecs.CanaryConfiguration{CanaryBakeTimeInMinutes: &three},
			},
			want: 3 * time.Minute,
		},
		{
			name: "canary_default_bake",
			dc:   &ecs.DeploymentConfiguration{Strategy: "CANARY", BakeTimeInMinutes: &zero},
			want: 10 * time.Minute,
		},
		{
			name: "linear_defaults",
			dc:   &ecs.DeploymentConfiguration{Strategy: "LINEAR", BakeTimeInMinutes: &zero},
			want: 9 * 6 * time.Minute,
		},
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

				_, err = backend.CreateService(ecs.CreateServiceInput{
					Cluster: "c", ServiceName: "s", TaskDefinition: td.TaskDefinitionArn,
					DeploymentConfiguration: tt.dc,
				})
				require.NoError(t, err)

				list, err := backend.ListServiceDeployments("c", "s")
				require.NoError(t, err)
				require.Len(t, list, 1)

				got, _, err := backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
				require.NoError(t, err)
				assert.Equal(t, "PRODUCTION_TRAFFIC_SHIFT", got[0].LifecycleStage)
				assert.Equal(t, "IN_PROGRESS", got[0].Status)

				time.Sleep(tt.want - time.Second)

				got, _, err = backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
				require.NoError(t, err)
				assert.Equal(t, "IN_PROGRESS", got[0].Status)

				time.Sleep(2 * time.Second)

				got, _, err = backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
				require.NoError(t, err)
				assert.Equal(t, "SUCCESSFUL", got[0].Status)
			})
		})
	}
}
