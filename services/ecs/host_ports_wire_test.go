package ecs_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/require"
)

const (
	wireEphemeralPortRangeMin = 49153
	wireEphemeralPortRangeMax = 65535
)

// TestRunTask_BridgeMode_NetworkBindings_RealClient proves bridge-mode
// dynamic host-port allocation (gopherstack-fpro) round-trips through the
// real ECS SDK client: NetworkBinding is a real member of
// ecs@v1.96.0's types.Container (types/types.go), populated by RunTask and
// echoed identically by DescribeTasks.
func TestRunTask_BridgeMode_NetworkBindings_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	ciRec := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"instanceIdentityDocument": `{"instanceId":"i-wire0001"}`,
	})
	require.Equal(t, http.StatusOK, ciRec.Code)

	tdOut, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
		Family:      aws.String("bridge-wire"),
		NetworkMode: ecstypes.NetworkModeBridge,
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:  aws.String("app"),
				Image: aws.String("nginx"),
				PortMappings: []ecstypes.PortMapping{
					{ContainerPort: aws.Int32(8080)},
				},
			},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &ecssdk.RunTaskInput{
		TaskDefinition: tdOut.TaskDefinition.TaskDefinitionArn,
		LaunchType:     ecstypes.LaunchTypeEc2,
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Empty(t, runOut.Failures)
	require.Len(t, runOut.Tasks, 1)
	require.Len(t, runOut.Tasks[0].Containers, 1)
	require.Len(t, runOut.Tasks[0].Containers[0].NetworkBindings, 1)

	binding := runOut.Tasks[0].Containers[0].NetworkBindings[0]
	require.NotNil(t, binding.ContainerPort)
	require.NotNil(t, binding.HostPort)
	require.EqualValues(t, 8080, *binding.ContainerPort)
	require.GreaterOrEqual(t, *binding.HostPort, int32(wireEphemeralPortRangeMin))
	require.LessOrEqual(t, *binding.HostPort, int32(wireEphemeralPortRangeMax))
	require.NotEqual(t, *binding.ContainerPort, *binding.HostPort)

	describeOut, err := client.DescribeTasks(ctx, &ecssdk.DescribeTasksInput{
		Tasks: []string{*runOut.Tasks[0].TaskArn},
	})
	require.NoError(t, err)
	require.Len(t, describeOut.Tasks, 1)
	require.Len(t, describeOut.Tasks[0].Containers[0].NetworkBindings, 1)
	require.Equal(t, *binding.HostPort, *describeOut.Tasks[0].Containers[0].NetworkBindings[0].HostPort)
}

// TestRunTask_HostMode_NetworkBindings_RealClient proves host-mode hostPort
// always equals containerPort (see host_ports.go's assignHostPortLocked)
// through the real ECS SDK client.
func TestRunTask_HostMode_NetworkBindings_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	ciRec := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"instanceIdentityDocument": `{"instanceId":"i-wire0002"}`,
	})
	require.Equal(t, http.StatusOK, ciRec.Code)

	tdOut, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
		Family:      aws.String("host-wire"),
		NetworkMode: ecstypes.NetworkModeHost,
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:  aws.String("app"),
				Image: aws.String("nginx"),
				PortMappings: []ecstypes.PortMapping{
					{ContainerPort: aws.Int32(9090)},
				},
			},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &ecssdk.RunTaskInput{
		TaskDefinition: tdOut.TaskDefinition.TaskDefinitionArn,
		LaunchType:     ecstypes.LaunchTypeEc2,
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Empty(t, runOut.Failures)
	require.Len(t, runOut.Tasks, 1)
	require.Len(t, runOut.Tasks[0].Containers[0].NetworkBindings, 1)

	binding := runOut.Tasks[0].Containers[0].NetworkBindings[0]
	require.EqualValues(t, 9090, *binding.ContainerPort)
	require.EqualValues(t, 9090, *binding.HostPort)
}
