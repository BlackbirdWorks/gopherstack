package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests prove the zeroguard fix (cmd/zeroguard): several Update*Input
// fields are pointers on the real SDK, so an omitted PATCH member must
// preserve the stored value and an explicit value must apply. Before the
// fix each field below was plain string, so an omitted member could not be
// told apart from an explicit zero value.
func TestUpdateService_PreservesOmittedTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn1 := registerTestTaskDef(t, h, "update-omit-td-family")

	_, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("update-omit-td-svc"),
		TaskDefinition: aws.String(tdArn1),
	})
	require.NoError(t, err)

	desired := int32(2)

	// Omitted TaskDefinition must preserve the prior value.
	preserved, err := client.UpdateService(ctx, &ecssdk.UpdateServiceInput{
		Service:      aws.String("update-omit-td-svc"),
		DesiredCount: &desired,
	})
	require.NoError(t, err)
	assert.Equal(t, tdArn1, aws.ToString(preserved.Service.TaskDefinition), "omitted task definition must survive")
	assert.Equal(t, int32(2), preserved.Service.DesiredCount)

	tdArn2 := registerTestTaskDef(t, h, "update-omit-td-family")

	// An explicit TaskDefinition must apply.
	moved, err := client.UpdateService(ctx, &ecssdk.UpdateServiceInput{
		Service:        aws.String("update-omit-td-svc"),
		TaskDefinition: aws.String(tdArn2),
	})
	require.NoError(t, err)
	assert.Equal(t, tdArn2, aws.ToString(moved.Service.TaskDefinition))
}

// TestUpdateExpressGatewayService_PreservesOmittedMembers proves a more
// serious variant of the same bug: before the fix, UpdateExpressGatewayService
// didn't just fail to distinguish omitted from zero for CPU/Memory/
// HealthCheckPath (it reset them to Create-time defaults) -- and had NO
// fallback at all for ExecutionRoleArn/TaskRoleArn/TaskDefinitionArn, so an
// update that omitted them blanked the running revision's values outright.
func TestUpdateExpressGatewayService_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	created, err := client.CreateExpressGatewayService(ctx, &ecssdk.CreateExpressGatewayServiceInput{
		ServiceName:           aws.String("update-omit-egs"),
		InfrastructureRoleArn: aws.String("arn:aws:iam::000000000000:role/infra-role"),
		ExecutionRoleArn:      aws.String("arn:aws:iam::000000000000:role/exec-role"),
		Cpu:                   aws.String("512"),
		Memory:                aws.String("1024"),
		HealthCheckPath:       aws.String("/healthz"),
	})
	require.NoError(t, err)
	arn := created.Service.ServiceArn

	// Omitted CPU/Memory/HealthCheckPath/ExecutionRoleArn must preserve the
	// prior revision's values, not reset to Create-time defaults or "".
	preserved, err := client.UpdateExpressGatewayService(ctx, &ecssdk.UpdateExpressGatewayServiceInput{
		ServiceArn: arn,
	})
	require.NoError(t, err)
	require.NotNil(t, preserved.Service.TargetConfiguration)
	cfg := preserved.Service.TargetConfiguration
	assert.Equal(t, "512", aws.ToString(cfg.Cpu), "omitted cpu must survive")
	assert.Equal(t, "1024", aws.ToString(cfg.Memory), "omitted memory must survive")
	assert.Equal(t, "/healthz", aws.ToString(cfg.HealthCheckPath), "omitted health check path must survive")
	assert.Equal(t, "arn:aws:iam::000000000000:role/exec-role", aws.ToString(cfg.ExecutionRoleArn),
		"omitted execution role must survive")

	// Explicit values still apply.
	changed, err := client.UpdateExpressGatewayService(ctx, &ecssdk.UpdateExpressGatewayServiceInput{
		ServiceArn: arn,
		Cpu:        aws.String("256"),
	})
	require.NoError(t, err)
	require.NotNil(t, changed.Service.TargetConfiguration)
	assert.Equal(t, "256", aws.ToString(changed.Service.TargetConfiguration.Cpu))
	assert.Equal(t, "1024", aws.ToString(changed.Service.TargetConfiguration.Memory), "unrelated field untouched")
}
