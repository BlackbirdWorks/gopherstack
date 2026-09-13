package apprunner_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

func createTestImageService(t *testing.T, client *apprunnersdk.Client, name string) *apprunnersdk.CreateServiceOutput {
	t.Helper()

	out, err := client.CreateService(t.Context(), &apprunnersdk.CreateServiceInput{
		ServiceName: aws.String(name),
		SourceConfiguration: &types.SourceConfiguration{
			ImageRepository: &types.ImageRepository{
				ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
				ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
			},
		},
	})
	require.NoError(t, err)

	return out
}

// TestTypedSlice23RealClient drives apprunner's remaining typed-coverage-
// blind ops (gopherstack-n3zi slice 23) through the real aws-sdk-go-v2
// client: DeleteObservabilityConfiguration, DescribeCustomDomains,
// DescribeVpcConnector, DescribeVpcIngressConnection,
// ListAutoScalingConfigurations, ListOperations,
// ListServicesForAutoScalingConfiguration, ListVpcConnectors,
// ListVpcIngressConnections, ResumeService, TagResource, UntagResource,
// UpdateDefaultAutoScalingConfiguration, UpdateService,
// UpdateVpcIngressConnection.
func TestTypedSlice23RealClient(t *testing.T) {
	t.Parallel()

	t.Run("service pause/resume, operations, tags, update", func(t *testing.T) {
		t.Parallel()

		backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
		client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
		ctx := t.Context()

		svc := createTestImageService(t, client, "s23-lifecycle-service")

		_, err := client.PauseService(ctx, &apprunnersdk.PauseServiceInput{ServiceArn: svc.Service.ServiceArn})
		require.NoError(t, err)

		resumeOut, err := client.ResumeService(
			ctx,
			&apprunnersdk.ResumeServiceInput{ServiceArn: svc.Service.ServiceArn},
		)
		require.NoError(t, err)
		assert.Equal(t, types.ServiceStatusRunning, resumeOut.Service.Status)

		opsOut, err := client.ListOperations(ctx, &apprunnersdk.ListOperationsInput{ServiceArn: svc.Service.ServiceArn})
		require.NoError(t, err)
		require.NotEmpty(t, opsOut.OperationSummaryList)

		opTypes := make([]string, 0, len(opsOut.OperationSummaryList))
		for _, op := range opsOut.OperationSummaryList {
			opTypes = append(opTypes, string(op.Type))
		}

		assert.Contains(t, opTypes, string(types.OperationTypePauseService))
		assert.Contains(t, opTypes, string(types.OperationTypeResumeService))

		_, err = client.TagResource(ctx, &apprunnersdk.TagResourceInput{
			ResourceArn: svc.Service.ServiceArn,
			Tags:        []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTagsForResource(ctx, &apprunnersdk.ListTagsForResourceInput{
			ResourceArn: svc.Service.ServiceArn,
		})
		require.NoError(t, err)
		require.Len(t, tagsOut.Tags, 1)
		assert.Equal(t, "team", aws.ToString(tagsOut.Tags[0].Key))

		_, err = client.UntagResource(ctx, &apprunnersdk.UntagResourceInput{
			ResourceArn: svc.Service.ServiceArn,
			TagKeys:     []string{"team"},
		})
		require.NoError(t, err)

		tagsAfterUntag, err := client.ListTagsForResource(ctx, &apprunnersdk.ListTagsForResourceInput{
			ResourceArn: svc.Service.ServiceArn,
		})
		require.NoError(t, err)
		assert.Empty(t, tagsAfterUntag.Tags)

		updateOut, err := client.UpdateService(ctx, &apprunnersdk.UpdateServiceInput{
			ServiceArn: svc.Service.ServiceArn,
			InstanceConfiguration: &types.InstanceConfiguration{
				Cpu:    aws.String("1024"),
				Memory: aws.String("2048"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, updateOut.Service.InstanceConfiguration)
		assert.Equal(t, "1024", aws.ToString(updateOut.Service.InstanceConfiguration.Cpu))
		assert.Equal(t, "2048", aws.ToString(updateOut.Service.InstanceConfiguration.Memory))
	})

	t.Run("custom domains", func(t *testing.T) {
		t.Parallel()

		backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
		client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
		ctx := t.Context()

		svc := createTestImageService(t, client, "s23-domains-service")

		_, err := client.AssociateCustomDomain(ctx, &apprunnersdk.AssociateCustomDomainInput{
			ServiceArn: svc.Service.ServiceArn,
			DomainName: aws.String("example.com"),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeCustomDomains(ctx, &apprunnersdk.DescribeCustomDomainsInput{
			ServiceArn: svc.Service.ServiceArn,
		})
		require.NoError(t, err)
		require.Len(t, descOut.CustomDomains, 1)
		assert.Equal(t, "example.com", aws.ToString(descOut.CustomDomains[0].DomainName))
	})

	t.Run("VPC connector describe and list", func(t *testing.T) {
		t.Parallel()

		backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
		client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateVpcConnector(ctx, &apprunnersdk.CreateVpcConnectorInput{
			VpcConnectorName: aws.String("s23-vpc-connector"),
			Subnets:          []string{"subnet-12345"},
		})
		require.NoError(t, err)

		descOut, err := client.DescribeVpcConnector(ctx, &apprunnersdk.DescribeVpcConnectorInput{
			VpcConnectorArn: createOut.VpcConnector.VpcConnectorArn,
		})
		require.NoError(t, err)
		assert.Equal(
			t,
			aws.ToString(createOut.VpcConnector.VpcConnectorArn),
			aws.ToString(descOut.VpcConnector.VpcConnectorArn),
		)

		listOut, err := client.ListVpcConnectors(ctx, &apprunnersdk.ListVpcConnectorsInput{})
		require.NoError(t, err)

		arns := make([]string, 0, len(listOut.VpcConnectors))
		for _, vc := range listOut.VpcConnectors {
			arns = append(arns, aws.ToString(vc.VpcConnectorArn))
		}

		assert.Contains(t, arns, aws.ToString(createOut.VpcConnector.VpcConnectorArn))
	})

	t.Run("VPC ingress connection describe, list, update", func(t *testing.T) {
		t.Parallel()

		backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
		client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
		ctx := t.Context()

		svc := createTestImageService(t, client, "s23-vic-service")

		createOut, err := client.CreateVpcIngressConnection(ctx, &apprunnersdk.CreateVpcIngressConnectionInput{
			ServiceArn:               svc.Service.ServiceArn,
			VpcIngressConnectionName: aws.String("s23-vic"),
			IngressVpcConfiguration: &types.IngressVpcConfiguration{
				VpcId:         aws.String("vpc-12345"),
				VpcEndpointId: aws.String("vpce-12345"),
			},
		})
		require.NoError(t, err)
		vicArn := createOut.VpcIngressConnection.VpcIngressConnectionArn

		descOut, err := client.DescribeVpcIngressConnection(ctx, &apprunnersdk.DescribeVpcIngressConnectionInput{
			VpcIngressConnectionArn: vicArn,
		})
		require.NoError(t, err)
		assert.Equal(t, "vpc-12345", aws.ToString(descOut.VpcIngressConnection.IngressVpcConfiguration.VpcId))

		listOut, err := client.ListVpcIngressConnections(ctx, &apprunnersdk.ListVpcIngressConnectionsInput{})
		require.NoError(t, err)

		arns := make([]string, 0, len(listOut.VpcIngressConnectionSummaryList))
		for _, v := range listOut.VpcIngressConnectionSummaryList {
			arns = append(arns, aws.ToString(v.VpcIngressConnectionArn))
		}

		assert.Contains(t, arns, aws.ToString(vicArn))

		_, err = client.UpdateVpcIngressConnection(ctx, &apprunnersdk.UpdateVpcIngressConnectionInput{
			VpcIngressConnectionArn: vicArn,
			IngressVpcConfiguration: &types.IngressVpcConfiguration{
				VpcId:         aws.String("vpc-67890"),
				VpcEndpointId: aws.String("vpce-67890"),
			},
		})
		require.NoError(t, err)

		descAfterUpdate, err := client.DescribeVpcIngressConnection(
			ctx,
			&apprunnersdk.DescribeVpcIngressConnectionInput{
				VpcIngressConnectionArn: vicArn,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "vpc-67890", aws.ToString(descAfterUpdate.VpcIngressConnection.IngressVpcConfiguration.VpcId))
	})

	t.Run("auto scaling configuration default, list, services-for", func(t *testing.T) {
		t.Parallel()

		backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
		client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
		ctx := t.Context()

		ascOut, err := client.CreateAutoScalingConfiguration(ctx, &apprunnersdk.CreateAutoScalingConfigurationInput{
			AutoScalingConfigurationName: aws.String("s23-asc"),
			MinSize:                      aws.Int32(2),
			MaxSize:                      aws.Int32(10),
		})
		require.NoError(t, err)
		ascArn := ascOut.AutoScalingConfiguration.AutoScalingConfigurationArn

		listOut, err := client.ListAutoScalingConfigurations(ctx, &apprunnersdk.ListAutoScalingConfigurationsInput{
			AutoScalingConfigurationName: aws.String("s23-asc"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, listOut.AutoScalingConfigurationSummaryList)

		defaultOut, err := client.UpdateDefaultAutoScalingConfiguration(ctx,
			&apprunnersdk.UpdateDefaultAutoScalingConfigurationInput{AutoScalingConfigurationArn: ascArn},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(defaultOut.AutoScalingConfiguration.IsDefault))

		svc, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
			ServiceName: aws.String("s23-asc-service"),
			SourceConfiguration: &types.SourceConfiguration{
				ImageRepository: &types.ImageRepository{
					ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
					ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
				},
			},
			AutoScalingConfigurationArn: ascArn,
		})
		require.NoError(t, err)

		servicesForOut, err := client.ListServicesForAutoScalingConfiguration(ctx,
			&apprunnersdk.ListServicesForAutoScalingConfigurationInput{AutoScalingConfigurationArn: ascArn},
		)
		require.NoError(t, err)

		assert.Contains(t, servicesForOut.ServiceArnList, aws.ToString(svc.Service.ServiceArn))
	})

	t.Run("DeleteObservabilityConfiguration", func(t *testing.T) {
		t.Parallel()

		backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
		client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateObservabilityConfiguration(ctx,
			&apprunnersdk.CreateObservabilityConfigurationInput{
				ObservabilityConfigurationName: aws.String("s23-observability"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteObservabilityConfiguration(ctx, &apprunnersdk.DeleteObservabilityConfigurationInput{
			ObservabilityConfigurationArn: createOut.ObservabilityConfiguration.ObservabilityConfigurationArn,
		})
		require.NoError(t, err)

		_, err = client.DescribeObservabilityConfiguration(ctx, &apprunnersdk.DescribeObservabilityConfigurationInput{
			ObservabilityConfigurationArn: createOut.ObservabilityConfiguration.ObservabilityConfigurationArn,
		})
		require.Error(t, err, "observability configuration no longer exists after delete")
	})
}
