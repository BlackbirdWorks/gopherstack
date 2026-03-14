package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
		ServiceName:        aws.String(serviceName),
		Cluster:            aws.String(clusterName),
		TaskDefinition:     regOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:       aws.Int32(1),
		SchedulingStrategy: ecstypes.SchedulingStrategyReplica,
	})
	require.NoError(t, err)

	out, err := client.DescribeServices(ctx, &ecs.DescribeServicesInput{
		Cluster:  aws.String(clusterName),
		Services: []string{serviceName},
	})
	require.NoError(t, err)
	require.Len(t, out.Services, 1)
	assert.Equal(t, serviceName, aws.ToString(out.Services[0].ServiceName))
	assert.Equal(t, ecstypes.SchedulingStrategyReplica, out.Services[0].SchedulingStrategy)
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

func TestIntegration_ECS_ListServices(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("list-svc-cluster-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	tdOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(fmt.Sprintf("list-svc-td-%s", suffix)),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	// Create a FARGATE/REPLICA service.
	_, err = client.CreateService(ctx, &ecs.CreateServiceInput{
		Cluster:            aws.String(clusterName),
		ServiceName:        aws.String("svc-a-" + suffix),
		TaskDefinition:     tdOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:       aws.Int32(1),
		LaunchType:         ecstypes.LaunchTypeFargate,
		SchedulingStrategy: ecstypes.SchedulingStrategyReplica,
	})
	require.NoError(t, err)

	// Create an EC2/DAEMON service.
	_, err = client.CreateService(ctx, &ecs.CreateServiceInput{
		Cluster:            aws.String(clusterName),
		ServiceName:        aws.String("svc-b-" + suffix),
		TaskDefinition:     tdOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:       aws.Int32(0),
		LaunchType:         ecstypes.LaunchTypeEc2,
		SchedulingStrategy: ecstypes.SchedulingStrategyDaemon,
	})
	require.NoError(t, err)

	// No filter — all 2 services.
	out, err := client.ListServices(ctx, &ecs.ListServicesInput{
		Cluster: aws.String(clusterName),
	})
	require.NoError(t, err)
	assert.Len(t, out.ServiceArns, 2)

	// Filter by FARGATE — only 1 service.
	outFargate, err := client.ListServices(ctx, &ecs.ListServicesInput{
		Cluster:    aws.String(clusterName),
		LaunchType: ecstypes.LaunchTypeFargate,
	})
	require.NoError(t, err)
	assert.Len(t, outFargate.ServiceArns, 1)

	// Filter by DAEMON scheduling strategy — only 1 service.
	outDaemon, err := client.ListServices(ctx, &ecs.ListServicesInput{
		Cluster:            aws.String(clusterName),
		SchedulingStrategy: ecstypes.SchedulingStrategyDaemon,
	})
	require.NoError(t, err)
	assert.Len(t, outDaemon.ServiceArns, 1)
}

func TestIntegration_ECS_ContainerInstances(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("ci-cluster-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	// Register a container instance.
	regOut, err := client.RegisterContainerInstance(ctx, &ecs.RegisterContainerInstanceInput{
		Cluster: aws.String(clusterName),
	})
	require.NoError(t, err)
	require.NotNil(t, regOut.ContainerInstance)
	assert.NotEmpty(t, aws.ToString(regOut.ContainerInstance.ContainerInstanceArn))
	assert.Equal(t, "ACTIVE", aws.ToString(regOut.ContainerInstance.Status))
	assert.True(t, regOut.ContainerInstance.AgentConnected)

	ciArn := regOut.ContainerInstance.ContainerInstanceArn

	// List container instances.
	listOut, err := client.ListContainerInstances(ctx, &ecs.ListContainerInstancesInput{
		Cluster: aws.String(clusterName),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.ContainerInstanceArns, 1)
	assert.Equal(t, aws.ToString(ciArn), listOut.ContainerInstanceArns[0])

	// Describe container instances.
	descOut, err := client.DescribeContainerInstances(ctx, &ecs.DescribeContainerInstancesInput{
		Cluster:            aws.String(clusterName),
		ContainerInstances: []string{aws.ToString(ciArn)},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ContainerInstances, 1)
	assert.Equal(t, aws.ToString(ciArn), aws.ToString(descOut.ContainerInstances[0].ContainerInstanceArn))

	// Drain the instance.
	drainOut, err := client.UpdateContainerInstancesState(ctx, &ecs.UpdateContainerInstancesStateInput{
		Cluster:            aws.String(clusterName),
		ContainerInstances: []string{aws.ToString(ciArn)},
		Status:             ecstypes.ContainerInstanceStatusDraining,
	})
	require.NoError(t, err)
	require.Len(t, drainOut.ContainerInstances, 1)
	assert.Equal(t, "DRAINING", aws.ToString(drainOut.ContainerInstances[0].Status))

	// Deregister the instance.
	deregOut, err := client.DeregisterContainerInstance(ctx, &ecs.DeregisterContainerInstanceInput{
		Cluster:           aws.String(clusterName),
		ContainerInstance: ciArn,
		Force:             aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, deregOut.ContainerInstance)
	assert.Equal(t, "INACTIVE", aws.ToString(deregOut.ContainerInstance.Status))

	// Confirm it was removed.
	listOut2, err := client.ListContainerInstances(ctx, &ecs.ListContainerInstancesInput{
		Cluster: aws.String(clusterName),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut2.ContainerInstanceArns)
}

func TestIntegration_ECS_TaskSets(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("ts-cluster-%s", suffix)
	serviceName := fmt.Sprintf("ts-service-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	tdOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(fmt.Sprintf("ts-td-%s", suffix)),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateService(ctx, &ecs.CreateServiceInput{
		Cluster:        aws.String(clusterName),
		ServiceName:    aws.String(serviceName),
		TaskDefinition: tdOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)

	// Create two task sets.
	ts1Out, err := client.CreateTaskSet(ctx, &ecs.CreateTaskSetInput{
		Cluster:        aws.String(clusterName),
		Service:        aws.String(serviceName),
		TaskDefinition: tdOut.TaskDefinition.TaskDefinitionArn,
	})
	require.NoError(t, err)
	require.NotNil(t, ts1Out.TaskSet)
	assert.NotEmpty(t, aws.ToString(ts1Out.TaskSet.TaskSetArn))
	assert.Equal(t, "ACTIVE", aws.ToString(ts1Out.TaskSet.Status))

	ts2Out, err := client.CreateTaskSet(ctx, &ecs.CreateTaskSetInput{
		Cluster:        aws.String(clusterName),
		Service:        aws.String(serviceName),
		TaskDefinition: tdOut.TaskDefinition.TaskDefinitionArn,
	})
	require.NoError(t, err)

	ts1Arn := ts1Out.TaskSet.TaskSetArn
	ts2Arn := ts2Out.TaskSet.TaskSetArn

	// Describe task sets.
	descOut, err := client.DescribeTaskSets(ctx, &ecs.DescribeTaskSetsInput{
		Cluster:  aws.String(clusterName),
		Service:  aws.String(serviceName),
		TaskSets: []string{aws.ToString(ts1Arn)},
	})
	require.NoError(t, err)
	require.Len(t, descOut.TaskSets, 1)
	assert.Equal(t, aws.ToString(ts1Arn), aws.ToString(descOut.TaskSets[0].TaskSetArn))

	// Update task set scale.
	updateOut, err := client.UpdateTaskSet(ctx, &ecs.UpdateTaskSetInput{
		Cluster: aws.String(clusterName),
		Service: aws.String(serviceName),
		TaskSet: ts1Arn,
		Scale: &ecstypes.Scale{
			Unit:  ecstypes.ScaleUnitPercent,
			Value: 25.0,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.TaskSet)
	assert.InDelta(t, 25.0, updateOut.TaskSet.Scale.Value, 0.001)

	// Set primary task set.
	primaryOut, err := client.UpdateServicePrimaryTaskSet(ctx, &ecs.UpdateServicePrimaryTaskSetInput{
		Cluster:        aws.String(clusterName),
		Service:        aws.String(serviceName),
		PrimaryTaskSet: ts1Arn,
	})
	require.NoError(t, err)
	require.NotNil(t, primaryOut.TaskSet)
	assert.Equal(t, "PRIMARY", aws.ToString(primaryOut.TaskSet.Status))

	// Verify ts1 is PRIMARY.
	descOut2, err := client.DescribeTaskSets(ctx, &ecs.DescribeTaskSetsInput{
		Cluster:  aws.String(clusterName),
		Service:  aws.String(serviceName),
		TaskSets: []string{aws.ToString(ts1Arn)},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.TaskSets, 1)
	assert.Equal(t, "PRIMARY", aws.ToString(descOut2.TaskSets[0].Status))

	// Delete task set 2.
	delOut, err := client.DeleteTaskSet(ctx, &ecs.DeleteTaskSetInput{
		Cluster: aws.String(clusterName),
		Service: aws.String(serviceName),
		TaskSet: ts2Arn,
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.TaskSet)
}

func TestIntegration_ECS_ExecuteCommand(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterName := fmt.Sprintf("exec-cluster-%s", suffix)

	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	tdOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String(fmt.Sprintf("exec-td-%s", suffix)),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest"), Essential: aws.Bool(true)},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(clusterName),
		TaskDefinition: tdOut.TaskDefinition.TaskDefinitionArn,
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)

	taskArn := runOut.Tasks[0].TaskArn

	// ExecuteCommand on running task.
	execOut, err := client.ExecuteCommand(ctx, &ecs.ExecuteCommandInput{
		Cluster:     aws.String(clusterName),
		Task:        taskArn,
		Container:   aws.String("app"),
		Command:     aws.String("/bin/sh"),
		Interactive: true,
	})
	require.NoError(t, err)
	require.NotNil(t, execOut.Session)
	assert.NotEmpty(t, aws.ToString(execOut.Session.SessionId))
	assert.NotEmpty(t, aws.ToString(execOut.Session.StreamUrl))
	assert.NotEmpty(t, aws.ToString(execOut.Session.TokenValue))
	assert.NotEmpty(t, aws.ToString(execOut.ClusterArn))
	assert.Equal(t, aws.ToString(taskArn), aws.ToString(execOut.TaskArn))
}

// TestIntegration_ECS_DockerRuntime tests ECS task execution when the Docker
// runtime is available. This test is skipped unless GOPHERSTACK_ECS_RUNTIME is
// set to "docker" on the test server and the local environment exposes a
// Docker-enabled Gopherstack endpoint via GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT.
//
// To run:
//
//	GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT=http://localhost:8000 \
//	 go test ./test/integration/... -run TestIntegration_ECS_DockerRuntime
func TestIntegration_ECS_DockerRuntime(t *testing.T) {
	t.Parallel()

	dockerEndpoint := os.Getenv("GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT")
	if dockerEndpoint == "" {
		t.Skip("skipping docker-runtime ECS test: GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT not set")
	}

	ctx := t.Context()
	suffix := uuid.NewString()[:8]

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := ecs.NewFromConfig(cfg, func(o *ecs.Options) {
		o.BaseEndpoint = &dockerEndpoint
	})

	// Create a cluster.
	clusterName := fmt.Sprintf("docker-cluster-%s", suffix)
	_, err = client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	// Register a minimal nginx task definition.
	family := fmt.Sprintf("docker-nginx-%s", suffix)
	regOut, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
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

	// Run the task via the Docker runtime; task should reach RUNNING.
	runOut, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(clusterName),
		TaskDefinition: regOut.TaskDefinition.TaskDefinitionArn,
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)

	taskArn := aws.ToString(runOut.Tasks[0].TaskArn)

	// Verify the task transitioned from PROVISIONING to RUNNING.
	descOut, err := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
		Cluster: aws.String(clusterName),
		Tasks:   []string{taskArn},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	assert.Equal(t, "RUNNING", aws.ToString(descOut.Tasks[0].LastStatus),
		"task should be RUNNING after Docker container start")

	// Clean up: stop the task.
	_, err = client.StopTask(ctx, &ecs.StopTaskInput{
		Cluster: aws.String(clusterName),
		Task:    aws.String(taskArn),
		Reason:  aws.String("integration test cleanup"),
	})
	require.NoError(t, err)
}
