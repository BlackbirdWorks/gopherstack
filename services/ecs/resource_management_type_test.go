package ecs_test

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceManagementType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		filter      ecstypes.ResourceManagementType
		wantCustom  bool
		wantExpress bool
	}{
		{name: "unfiltered", wantCustom: true, wantExpress: true},
		{name: "customer", filter: ecstypes.ResourceManagementTypeCustomer, wantCustom: true},
		{name: "ecs", filter: ecstypes.ResourceManagementTypeEcs, wantExpress: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			ctx := t.Context()

			reg, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
				Family: aws.String("rmt-td"),
				ContainerDefinitions: []ecstypes.ContainerDefinition{
					{Name: aws.String("app"), Image: aws.String("example/app:latest")},
				},
			})
			require.NoError(t, err)

			custom, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
				ServiceName:    aws.String("custom-svc"),
				TaskDefinition: reg.TaskDefinition.TaskDefinitionArn,
				DesiredCount:   aws.Int32(0),
			})
			require.NoError(t, err)

			express, err := client.CreateExpressGatewayService(ctx, &ecssdk.CreateExpressGatewayServiceInput{
				ServiceName:           aws.String("express-svc"),
				ExecutionRoleArn:      aws.String("arn:aws:iam::000000000000:role/exec"),
				InfrastructureRoleArn: aws.String("arn:aws:iam::000000000000:role/infra"),
				PrimaryContainer:      &ecstypes.ExpressGatewayContainer{Image: aws.String("example/app:latest")},
			})
			require.NoError(t, err)

			customArn := aws.ToString(custom.Service.ServiceArn)
			expressArn := aws.ToString(express.Service.ServiceArn)

			listed, err := client.ListServices(ctx, &ecssdk.ListServicesInput{ResourceManagementType: tt.filter})
			require.NoError(t, err)
			assert.Equal(t, tt.wantCustom, slices.Contains(listed.ServiceArns, customArn))
			assert.Equal(t, tt.wantExpress, slices.Contains(listed.ServiceArns, expressArn))

			described, err := client.DescribeServices(ctx, &ecssdk.DescribeServicesInput{
				Services: []string{customArn, expressArn},
			})
			require.NoError(t, err)
			assert.Empty(t, described.Failures)
			require.Len(t, described.Services, 2)

			got := map[string]ecstypes.ResourceManagementType{}
			for _, s := range described.Services {
				got[aws.ToString(s.ServiceArn)] = s.ResourceManagementType
			}

			assert.Equal(t, ecstypes.ResourceManagementTypeCustomer, got[customArn])
			assert.Equal(t, ecstypes.ResourceManagementTypeEcs, got[expressArn])
		})
	}
}

func TestListServices_InvalidResourceManagementType(t *testing.T) {
	t.Parallel()

	client := newTestECSClient(t, newTestHandler(t))
	_, err := client.ListServices(t.Context(), &ecssdk.ListServicesInput{ResourceManagementType: "BOGUS"})
	require.Error(t, err)
}
