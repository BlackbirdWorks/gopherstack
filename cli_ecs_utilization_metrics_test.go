package main

import (
	"context"
	"testing"

	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

type fixedUsageRunner struct {
	ecsbackend.TaskRunner
}

func (fixedUsageRunner) TaskUsage(context.Context, string) (float64, float64, bool) {
	return 128, 128 * 1024 * 1024, true
}

func TestServiceMetrics_ECSUtilization(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	byName := serviceByName(fx.services)

	bk := ecsbackend.NewInMemoryBackend("000000000000", "us-east-1", fixedUsageRunner{ecsbackend.NewNoopRunner()})
	byName["ECS"] = ecsbackend.NewHandler(bk)

	wireServiceMetrics(t.Context(), map[string]service.Registerable{
		"CloudWatch": byName["CloudWatch"], "ECS": byName["ECS"],
	})

	_, err := bk.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "util"})
	require.NoError(t, err)

	td, err := bk.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
		Family: "util", RequiresCompatibilities: []string{"FARGATE"}, NetworkMode: "awsvpc", CPU: "256", Memory: "512",
		ContainerDefinitions: []ecsbackend.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, err = bk.CreateService(ecsbackend.CreateServiceInput{
		Cluster:        "util",
		ServiceName:    "web",
		TaskDefinition: td.TaskDefinitionArn,
		LaunchType:     "FARGATE",
		DesiredCount:   1,
	})
	require.NoError(t, err)
	require.NoError(t, bk.StartTaskForService("util", "web", td.TaskDefinitionArn))

	ecsbackend.NewReconciler(bk).RunOnce(t.Context())

	d := map[string]string{"ClusterName": "util", "ServiceName": "web"}

	assertMetricsEmitted(t, fx, []metricWant{
		{
			namespace: "AWS/ECS",
			name:      "CPUUtilization",
			dims:      d,
			unit:      cwtypes.StandardUnitPercent,
			sum:       50,
			minSum:    true,
		},
		{
			namespace: "AWS/ECS",
			name:      "MemoryUtilization",
			dims:      d,
			unit:      cwtypes.StandardUnitPercent,
			sum:       25,
			minSum:    true,
		},
	})
}
