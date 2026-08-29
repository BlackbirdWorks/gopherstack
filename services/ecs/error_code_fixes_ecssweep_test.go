package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStopTask_UnknownTask_RealClient drives StopTask through the real
// client for a task ARN that was never run. "TaskNotFoundException" is not
// a real ECS exception type at all (no such shape exists in
// ecs@v1.90.0/types/errors.go, and it appears in none of the 77 per-op
// deserializeOpError switches) -- gopherstack emitted it anyway
// (confirmed by hand-reverting). StopTask's own deserializer models only
// InvalidParameterException for this condition.
func TestStopTask_UnknownTask_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("default")})
	require.NoError(t, err)

	_, err = client.StopTask(ctx, &ecssdk.StopTaskInput{
		Task: aws.String("arn:aws:ecs:us-east-1:123456789012:task/default/does-not-exist"),
	})
	require.Error(t, err)

	var ip *ecstypes.InvalidParameterException
	require.ErrorAs(t, err, &ip, "expected a real InvalidParameterException from the SDK deserializer")
}

// TestExecuteCommand_UnknownTask_RealClient covers the same fabricated-code
// bug at ExecuteCommand's other ErrTaskNotFound call site.
func TestExecuteCommand_UnknownTask_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("default")})
	require.NoError(t, err)

	_, err = client.ExecuteCommand(ctx, &ecssdk.ExecuteCommandInput{
		Task:        aws.String("arn:aws:ecs:us-east-1:123456789012:task/default/does-not-exist"),
		Command:     aws.String("/bin/sh"),
		Interactive: true,
	})
	require.Error(t, err)

	var ip *ecstypes.InvalidParameterException
	require.ErrorAs(t, err, &ip, "expected a real InvalidParameterException from the SDK deserializer")
}

// TestDescribeTaskDefinition_UnknownFamily_RealClient drives
// DescribeTaskDefinition through the real client for a family that was
// never registered. "TaskDefinitionNotFoundException" is not a real ECS
// exception type either (same absent-from-SDK shape as TaskNotFoundException
// above) -- gopherstack emitted it anyway (confirmed by hand-reverting).
// DescribeTaskDefinition's own deserializer models only
// InvalidParameterException for this condition.
func TestDescribeTaskDefinition_UnknownFamily_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	_, err := client.DescribeTaskDefinition(ctx, &ecssdk.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("no-such-family"),
	})
	require.Error(t, err)

	var ip *ecstypes.InvalidParameterException
	require.ErrorAs(t, err, &ip, "expected a real InvalidParameterException from the SDK deserializer")
}

// TestCreateCluster_Idempotent_RealClient drives CreateCluster twice with the
// same ClusterName through the real client. Real ECS's CreateCluster is
// idempotent (calling it again with an existing name returns the existing
// cluster, HTTP 200) -- confirmed by the error model: "ClusterAlreadyExistsException"
// is not a real ECS exception type (absent from ecs@v1.90.0/types/errors.go
// and from all 77 per-op deserializeOpError switches, the same 0-of-N shape
// as TaskNotFoundException above). gopherstack raised it as an error anyway
// (confirmed by hand-reverting).
func TestCreateCluster_Idempotent_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	first, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("dup-cluster")})
	require.NoError(t, err)

	second, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("dup-cluster")})
	require.NoError(t, err, "CreateCluster should be idempotent for an existing cluster name")
	require.Equal(t, aws.ToString(first.Cluster.ClusterArn), aws.ToString(second.Cluster.ClusterArn))
}

// TestCreateService_DuplicateName_RealClient drives CreateService twice with
// the same ServiceName. "ServiceAlreadyExistsException" is not a real ECS
// exception type (absent from ecs@v1.90.0/types/errors.go and from all 77
// per-op deserializeOpError switches -- gopherstack emitted it anyway,
// confirmed by hand-reverting). CreateService's own deserializer models
// InvalidParameterException, which is the code real AWS uses for this
// condition ("Creation of service was not idempotent").
func TestCreateService_DuplicateName_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "dup-svc-family")

	_, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("dup-service"),
		TaskDefinition: aws.String(tdArn),
	})
	require.NoError(t, err)

	_, err = client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("dup-service"),
		TaskDefinition: aws.String(tdArn),
	})
	require.Error(t, err)

	var ip *ecstypes.InvalidParameterException
	require.ErrorAs(t, err, &ip, "expected a real InvalidParameterException from the SDK deserializer")
}

// TestFabricatedNotFoundCodes_RealClient covers six more ECS operations whose
// gopherstack handler raised a fabricated "...NotFoundException"/
// "...AlreadyExistsException" code that appears in none of the 77 real
// per-op deserializeOpError switches and has no shape in
// ecs@v1.90.0/types/errors.go at all (same 0-of-N pattern as TaskNotFoundException).
// Each op's own deserializer models only InvalidParameterException for the
// condition being tested here.
func TestFabricatedNotFoundCodes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	_, createClusterErr := client.CreateCluster(
		ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("default")},
	)
	require.NoError(t, createClusterErr)

	t.Run("DeleteAccountSetting unknown name", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeleteAccountSetting(ctx, &ecssdk.DeleteAccountSettingInput{
			Name: ecstypes.SettingNameServiceLongArnFormat,
		})
		require.Error(t, err)

		var ip *ecstypes.InvalidParameterException
		require.ErrorAs(t, err, &ip)
	})

	t.Run("DeregisterContainerInstance unknown instance", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeregisterContainerInstance(ctx, &ecssdk.DeregisterContainerInstanceInput{
			Cluster:           aws.String("default"),
			ContainerInstance: aws.String("no-such-instance"),
		})
		require.Error(t, err)

		var ip *ecstypes.InvalidParameterException
		require.ErrorAs(t, err, &ip)
	})

	t.Run("UpdateContainerInstancesState unknown instance", func(t *testing.T) {
		t.Parallel()

		out, err := client.UpdateContainerInstancesState(ctx, &ecssdk.UpdateContainerInstancesStateInput{
			Cluster:            aws.String("default"),
			ContainerInstances: []string{"no-such-instance"},
			Status:             ecstypes.ContainerInstanceStatusDraining,
		})
		require.NoError(t, err)
		require.Empty(t, out.ContainerInstances)

		require.Len(t, out.Failures, 1)
		assert.Equal(t, "no-such-instance", *out.Failures[0].Arn)
		assert.Equal(t, "MISSING", *out.Failures[0].Reason)
	})

	t.Run("UpdateContainerAgent unknown instance", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateContainerAgent(ctx, &ecssdk.UpdateContainerAgentInput{
			Cluster:           aws.String("default"),
			ContainerInstance: aws.String("no-such-instance"),
		})
		require.Error(t, err)

		var ip *ecstypes.InvalidParameterException
		require.ErrorAs(t, err, &ip)
	})

	t.Run("DeleteCapacityProvider unknown provider", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeleteCapacityProvider(ctx, &ecssdk.DeleteCapacityProviderInput{
			CapacityProvider: aws.String("no-such-cp"),
		})
		require.Error(t, err)

		var ip *ecstypes.InvalidParameterException
		require.ErrorAs(t, err, &ip)
	})

	t.Run("UpdateCapacityProvider unknown provider", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateCapacityProvider(ctx, &ecssdk.UpdateCapacityProviderInput{
			Name: aws.String("no-such-cp"),
			AutoScalingGroupProvider: &ecstypes.AutoScalingGroupProviderUpdate{
				ManagedScaling: &ecstypes.ManagedScaling{},
			},
		})
		require.Error(t, err)

		var ip *ecstypes.InvalidParameterException
		require.ErrorAs(t, err, &ip)
	})
}

// TestExpressGatewayService_ErrorCodes_RealClient covers four more
// fabricated-code call sites in the Express Gateway family. Real ECS's own
// per-op deserializers model different codes for essentially the same
// "service not found" condition on sibling ops -- DeleteExpressGatewayService
// and UpdateExpressGatewayService both model plain "ServiceNotFoundException"
// (the same code regular ECS services use), while
// DescribeExpressGatewayService models "ResourceNotFoundException" instead.
// gopherstack used a single fabricated
// "ExpressGatewayServiceNotFoundException" for all three (absent from
// ecs@v1.90.0/types/errors.go and from every per-op switch), and a
// fabricated "ExpressGatewayServiceAlreadyExistsException" for
// CreateExpressGatewayService, whose own deserializer models no
// "already exists" exception at all.
func TestExpressGatewayService_ErrorCodes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	createInput := func(name string) *ecssdk.CreateExpressGatewayServiceInput {
		return &ecssdk.CreateExpressGatewayServiceInput{
			InfrastructureRoleArn: aws.String("arn:aws:iam::000000000000:role/infra"),
			ExecutionRoleArn:      aws.String("arn:aws:iam::000000000000:role/exec"),
			TaskRoleArn:           aws.String("arn:aws:iam::000000000000:role/task"),
			ServiceName:           aws.String(name),
			NetworkConfiguration: &ecstypes.ExpressGatewayServiceNetworkConfiguration{
				SecurityGroups: []string{"sg-1"},
				Subnets:        []string{"subnet-1"},
			},
			PrimaryContainer: &ecstypes.ExpressGatewayContainer{
				Image:         aws.String("nginx:latest"),
				ContainerPort: aws.Int32(8080),
			},
		}
	}

	t.Run("DeleteExpressGatewayService unknown", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeleteExpressGatewayService(ctx, &ecssdk.DeleteExpressGatewayServiceInput{
			ServiceArn: aws.String("arn:aws:ecs:us-east-1:123456789012:service/default/no-such-service"),
		})
		require.Error(t, err)

		var nf *ecstypes.ServiceNotFoundException
		require.ErrorAs(t, err, &nf, "expected a real ServiceNotFoundException from the SDK deserializer")
	})

	t.Run("DescribeExpressGatewayService unknown", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeExpressGatewayService(ctx, &ecssdk.DescribeExpressGatewayServiceInput{
			ServiceArn: aws.String("arn:aws:ecs:us-east-1:123456789012:service/default/no-such-service"),
		})
		require.Error(t, err)

		var nf *ecstypes.ResourceNotFoundException
		require.ErrorAs(t, err, &nf, "expected a real ResourceNotFoundException from the SDK deserializer")
	})

	t.Run("UpdateExpressGatewayService unknown", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateExpressGatewayService(ctx, &ecssdk.UpdateExpressGatewayServiceInput{
			ServiceArn: aws.String("arn:aws:ecs:us-east-1:123456789012:service/default/no-such-service"),
		})
		require.Error(t, err)

		var nf *ecstypes.ServiceNotFoundException
		require.ErrorAs(t, err, &nf, "expected a real ServiceNotFoundException from the SDK deserializer")
	})

	t.Run("CreateExpressGatewayService duplicate name", func(t *testing.T) {
		t.Parallel()

		_, err := client.CreateExpressGatewayService(ctx, createInput("dup-express-svc"))
		require.NoError(t, err)

		_, err = client.CreateExpressGatewayService(ctx, createInput("dup-express-svc"))
		require.Error(t, err)

		var ip *ecstypes.InvalidParameterException
		require.ErrorAs(t, err, &ip, "expected a real InvalidParameterException from the SDK deserializer")
	})
}
