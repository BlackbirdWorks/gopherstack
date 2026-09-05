package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/require"
)

// TestStartTask_UnknownContainerInstance_ReportsFailure proves that StartTask
// reports an unknown container instance ARN in its Failures field (ecs
// v1.90.0 api_op_StartTask.go: StartTaskOutput.Failures) rather than silently
// starting a task on a container instance that was never registered. Before
// the fix, the backend created a task for every ARN in the request
// unconditionally and the handler hardcoded Failures to an empty slice, so a
// client asking to start a task on a stale or mistyped container instance ARN
// got back a task claiming to run there instead of the documented failure.
func TestStartTask_UnknownContainerInstance_ReportsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
		ClusterName: aws.String("start-task-cluster"),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterContainerInstance(ctx, &ecssdk.RegisterContainerInstanceInput{
		Cluster:                  aws.String("start-task-cluster"),
		InstanceIdentityDocument: aws.String(fakeInstanceIdentityDocument("i-known")),
	})
	require.NoError(t, err)
	knownArn := *regOut.ContainerInstance.ContainerInstanceArn

	tdArn := registerTestTaskDef(t, h, "start-task-family")

	out, err := client.StartTask(ctx, &ecssdk.StartTaskInput{
		Cluster:        aws.String("start-task-cluster"),
		TaskDefinition: aws.String(tdArn),
		ContainerInstances: []string{
			knownArn,
			"arn:aws:ecs:us-east-1:000000000000:container-instance/start-task-cluster/does-not-exist",
		},
	})
	require.NoError(t, err)

	require.Len(t, out.Tasks, 1, "only the known container instance should get a task")
	require.Equal(t, knownArn, *out.Tasks[0].ContainerInstanceArn)

	require.Len(t, out.Failures, 1, "the unknown container instance should be reported as a failure")
	require.Contains(t, *out.Failures[0].Arn, "does-not-exist")
	require.Equal(t, "MISSING", *out.Failures[0].Reason)
}

// TestUpdateContainerInstancesState_UnknownInstance_ReportsFailure proves
// that UpdateContainerInstancesState processes the container instances it
// recognizes and reports the rest in its Failures field (ecs v1.90.0
// api_op_UpdateContainerInstancesState.go:
// UpdateContainerInstancesStateOutput.Failures), instead of aborting the
// entire batch because one ARN in it doesn't exist. Before the fix, the
// backend returned a request-level error for the whole call and the wire
// output type had no Failures field at all, so a client draining ten
// instances lost the state change on the nine valid ones because of one
// stale ARN.
func TestUpdateContainerInstancesState_UnknownInstance_ReportsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
		ClusterName: aws.String("update-state-cluster"),
	})
	require.NoError(t, err)

	regOut, err := client.RegisterContainerInstance(ctx, &ecssdk.RegisterContainerInstanceInput{
		Cluster:                  aws.String("update-state-cluster"),
		InstanceIdentityDocument: aws.String(fakeInstanceIdentityDocument("i-known-2")),
	})
	require.NoError(t, err)
	knownArn := *regOut.ContainerInstance.ContainerInstanceArn

	out, err := client.UpdateContainerInstancesState(ctx, &ecssdk.UpdateContainerInstancesStateInput{
		Cluster: aws.String("update-state-cluster"),
		Status:  ecstypes.ContainerInstanceStatusDraining,
		ContainerInstances: []string{
			knownArn,
			"arn:aws:ecs:us-east-1:000000000000:container-instance/update-state-cluster/does-not-exist",
		},
	})
	require.NoError(t, err)

	require.Len(t, out.ContainerInstances, 1, "the known instance should still transition")
	require.Equal(t, knownArn, *out.ContainerInstances[0].ContainerInstanceArn)
	gotStatus := ecstypes.ContainerInstanceStatus(*out.ContainerInstances[0].Status)
	require.Equal(t, ecstypes.ContainerInstanceStatusDraining, gotStatus)

	require.Len(t, out.Failures, 1, "the unknown instance should be reported as a failure")
	require.Contains(t, *out.Failures[0].Arn, "does-not-exist")
	require.Equal(t, "MISSING", *out.Failures[0].Reason)
}
