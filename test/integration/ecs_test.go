package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_ECS_CreateCluster(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	clusterName := fmt.Sprintf("test-cluster-%s", uuid.NewString()[:8])

	out, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})

	require.NoError(t, err)
	require.NotNil(t, out.Cluster)
	assert.Equal(t, clusterName, aws.ToString(out.Cluster.ClusterName))
	assert.NotEmpty(t, aws.ToString(out.Cluster.ClusterArn))
	assert.Equal(t, "ACTIVE", aws.ToString(out.Cluster.Status))
}

func TestIntegration_ECS_CreateCluster_AlreadyExists(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	clusterName := fmt.Sprintf("dupe-cluster-%s", uuid.NewString()[:8])

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ClusterAlreadyExistsException")
}

func TestIntegration_ECS_DescribeClusters(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	clusterName := fmt.Sprintf("describe-cluster-%s", uuid.NewString()[:8])

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	out, err := client.DescribeClusters(ctx, &ecs.DescribeClustersInput{
		Clusters: []string{clusterName},
	})
	require.NoError(t, err)
	require.Len(t, out.Clusters, 1)
	assert.Equal(t, clusterName, aws.ToString(out.Clusters[0].ClusterName))
}

func TestIntegration_ECS_DeleteCluster(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	clusterName := fmt.Sprintf("delete-cluster-%s", uuid.NewString()[:8])

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	out, err := client.DeleteCluster(ctx, &ecs.DeleteClusterInput{
		Cluster: aws.String(clusterName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Cluster)
	assert.Equal(t, clusterName, aws.ToString(out.Cluster.ClusterName))
}

func TestIntegration_ECS_RegisterTaskDefinition(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	family := fmt.Sprintf("test-family-%s", uuid.NewString()[:8])

	out, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:      aws.String("nginx"),
				Image:     aws.String("nginx:latest"),
				Essential: aws.Bool(true),
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, out.TaskDefinition)
	assert.Equal(t, family, aws.ToString(out.TaskDefinition.Family))
	assert.NotEmpty(t, aws.ToString(out.TaskDefinition.TaskDefinitionArn))
	assert.Equal(t, int32(1), out.TaskDefinition.Revision)
	assert.Equal(t, ecstypes.TaskDefinitionStatusActive, out.TaskDefinition.Status)
}

func TestIntegration_ECS_RegisterTaskDefinition_MultipleRevisions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	family := fmt.Sprintf("multi-rev-%s", uuid.NewString()[:8])

	for i := int32(1); i <= 3; i++ {
		out, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
			Family: aws.String(family),
			ContainerDefinitions: []ecstypes.ContainerDefinition{
				{Name: aws.String("app"), Image: aws.String("nginx:latest")},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, i, out.TaskDefinition.Revision)
	}
}

func TestIntegration_ECS_DescribeTaskDefinition(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	family := fmt.Sprintf("describe-td-%s", uuid.NewString()[:8])

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	// Describe by family name.
	out, err := client.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String(family),
	})
	require.NoError(t, err)
	require.NotNil(t, out.TaskDefinition)
	assert.Equal(t, family, aws.ToString(out.TaskDefinition.Family))

	// Describe by ARN.
	out2, err := client.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
	})
	require.NoError(t, err)
	require.NotNil(t, out2.TaskDefinition)
}

func TestIntegration_ECS_ListTaskDefinitions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	families := []string{
		fmt.Sprintf("list-td-a-%s", suffix),
		fmt.Sprintf("list-td-b-%s", suffix),
	}

	for _, f := range families {
		_, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
			Family: aws.String(f),
			ContainerDefinitions: []ecstypes.ContainerDefinition{
				{Name: aws.String("app"), Image: aws.String("nginx")},
			},
		})
		require.NoError(t, err)
	}

	out, err := client.ListTaskDefinitions(ctx, &ecs.ListTaskDefinitionsInput{
		FamilyPrefix: aws.String("list-td-"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.TaskDefinitionArns), 2)
}

func TestIntegration_ECS_CreateService(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("svc-cluster-%s", suffix)
	family := fmt.Sprintf("svc-task-%s", suffix)
	serviceName := fmt.Sprintf("my-service-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	out, err := client.CreateService(ctx, &ecs.CreateServiceInput{
		ServiceName:    aws.String(serviceName),
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(2),
	})

	require.NoError(t, err)
	require.NotNil(t, out.Service)
	assert.Equal(t, serviceName, aws.ToString(out.Service.ServiceName))
	assert.Equal(t, int32(2), out.Service.DesiredCount)
	assert.Equal(t, "ACTIVE", aws.ToString(out.Service.Status))
}

func TestIntegration_ECS_DescribeServices(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("dsvc-cluster-%s", suffix)
	family := fmt.Sprintf("dsvc-task-%s", suffix)
	serviceName := fmt.Sprintf("describe-svc-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateService(ctx, &ecs.CreateServiceInput{
		ServiceName:    aws.String(serviceName),
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.DescribeServices(ctx, &ecs.DescribeServicesInput{
		Cluster:  aws.String(clusterName),
		Services: []string{serviceName},
	})
	require.NoError(t, err)
	require.Len(t, out.Services, 1)
	assert.Equal(t, serviceName, aws.ToString(out.Services[0].ServiceName))
}

func TestIntegration_ECS_UpdateService(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("upd-cluster-%s", suffix)
	family := fmt.Sprintf("upd-task-%s", suffix)
	serviceName := fmt.Sprintf("update-svc-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateService(ctx, &ecs.CreateServiceInput{
		ServiceName:    aws.String(serviceName),
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.UpdateService(ctx, &ecs.UpdateServiceInput{
		Cluster:      aws.String(clusterName),
		Service:      aws.String(serviceName),
		DesiredCount: aws.Int32(5),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Service)
	assert.Equal(t, int32(5), out.Service.DesiredCount)
}

func TestIntegration_ECS_DeleteService(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("del-svc-cluster-%s", suffix)
	family := fmt.Sprintf("del-svc-task-%s", suffix)
	serviceName := fmt.Sprintf("delete-svc-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateService(ctx, &ecs.CreateServiceInput{
		ServiceName:    aws.String(serviceName),
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.DeleteService(ctx, &ecs.DeleteServiceInput{
		Cluster: aws.String(clusterName),
		Service: aws.String(serviceName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Service)
	assert.Equal(t, serviceName, aws.ToString(out.Service.ServiceName))
}

func TestIntegration_ECS_RunTask(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("run-cluster-%s", suffix)
	family := fmt.Sprintf("run-task-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("nginx"), Image: aws.String("nginx:latest"), Essential: aws.Bool(true)},
		},
	})
	require.NoError(t, err)

	out, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		Count:          aws.Int32(1),
	})

	require.NoError(t, err)
	require.Len(t, out.Tasks, 1)
	assert.NotEmpty(t, aws.ToString(out.Tasks[0].TaskArn))
	assert.Equal(t, "RUNNING", aws.ToString(out.Tasks[0].LastStatus))
}

func TestIntegration_ECS_RunTask_TransitionToRunning(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("trans-cluster-%s", suffix)
	family := fmt.Sprintf("trans-task-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)

	taskArn := aws.ToString(runOut.Tasks[0].TaskArn)

	// Task should be immediately in RUNNING state (no Docker runtime configured).
	descOut, err := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
		Cluster: aws.String(clusterName),
		Tasks:   []string{taskArn},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	assert.Equal(t, "RUNNING", aws.ToString(descOut.Tasks[0].LastStatus))
}

func TestIntegration_ECS_StopTask(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("stop-cluster-%s", suffix)
	family := fmt.Sprintf("stop-task-family-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)

	taskArn := aws.ToString(runOut.Tasks[0].TaskArn)

	stopOut, err := client.StopTask(ctx, &ecs.StopTaskInput{
		Cluster: aws.String(clusterName),
		Task:    aws.String(taskArn),
		Reason:  aws.String("test stop"),
	})
	require.NoError(t, err)
	require.NotNil(t, stopOut.Task)
	assert.Equal(t, "STOPPED", aws.ToString(stopOut.Task.LastStatus))
	assert.Equal(t, "test stop", aws.ToString(stopOut.Task.StoppedReason))
}

func TestIntegration_ECS_ListTasks(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("list-tasks-cluster-%s", suffix)
	family := fmt.Sprintf("list-tasks-family-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	_, err = client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		Count:          aws.Int32(3),
	})
	require.NoError(t, err)

	out, err := client.ListTasks(ctx, &ecs.ListTasksInput{
		Cluster: aws.String(clusterName),
	})
	require.NoError(t, err)
	assert.Len(t, out.TaskArns, 3)
}
