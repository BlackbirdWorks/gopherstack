package ecs_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func benchReconcilerBackend(b *testing.B, services, stopped int) *ecs.Reconciler {
	b.Helper()

	be := ecs.NewInMemoryBackend("000000000000", "us-east-1", nil)
	_, err := be.CreateCluster(ecs.CreateClusterInput{ClusterName: "c"})
	require.NoError(b, err)

	td, err := be.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "f",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(b, err)

	for i := range services {
		_, err = be.CreateService(ecs.CreateServiceInput{
			Cluster: "c", ServiceName: fmt.Sprintf("svc-%d", i),
			TaskDefinition: td.TaskDefinitionArn, DesiredCount: 3,
		})
		require.NoError(b, err)
	}

	tasks, _, err := be.RunTask(ecs.RunTaskInput{
		Cluster: "c", TaskDefinition: td.TaskDefinitionArn, Count: 10, Group: "family:other",
	})
	require.NoError(b, err)

	for range stopped / 10 {
		ts, _, rerr := be.RunTask(ecs.RunTaskInput{
			Cluster: "c", TaskDefinition: td.TaskDefinitionArn, Count: 10, Group: "family:other",
		})
		require.NoError(b, rerr)

		for _, t := range ts {
			_, err = be.StopTask("c", t.TaskArn, "bench")
			require.NoError(b, err)
		}
	}

	_ = tasks

	r := ecs.NewReconciler(be)
	r.RunOnce(context.Background())

	return r
}

func BenchmarkReconcileSteadyState(b *testing.B) {
	r := benchReconcilerBackend(b, 100, 2000)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		r.RunOnce(ctx)
	}
}
