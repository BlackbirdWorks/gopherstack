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

// TestListServices_SummaryHasUpdatedAt verifies ServiceSummary carries
// UpdatedAt, present on the real types.ServiceSummary (deserializers.go:6939
// in aws-sdk-go-v2/service/apprunner@v1.42.4). Before the fix, gopherstack's
// serviceSummaryOutput had no UpdatedAt key at all, so a real client's
// ServiceSummary.UpdatedAt stayed nil even after UpdateService changed it.
func TestListServices_SummaryHasUpdatedAt(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
		ServiceName: aws.String("summary-updatedat-svc"),
		SourceConfiguration: &types.SourceConfiguration{
			ImageRepository: &types.ImageRepository{
				ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
				ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListServices(ctx, &apprunnersdk.ListServicesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.ServiceSummaryList, 1)
	require.NotNil(t, listOut.ServiceSummaryList[0].UpdatedAt,
		"real ServiceSummary.UpdatedAt must round-trip, matching deserializers.go's UpdatedAt case")
	assert.Equal(t, svc.Service.UpdatedAt.Unix(), listOut.ServiceSummaryList[0].UpdatedAt.Unix())
}

// TestDeleteService_ResponseHasDeletedAt verifies the Service returned by
// DeleteService carries DeletedAt, present on the real types.Service
// (deserializers.go:6615). Before the fix, storedService had no DeletedAt
// field at all, so a real client's Service.DeletedAt stayed nil on every
// DeleteService response.
func TestDeleteService_ResponseHasDeletedAt(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
		ServiceName: aws.String("deletedat-svc"),
		SourceConfiguration: &types.SourceConfiguration{
			ImageRepository: &types.ImageRepository{
				ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
				ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
			},
		},
	})
	require.NoError(t, err)

	delOut, err := client.DeleteService(ctx, &apprunnersdk.DeleteServiceInput{ServiceArn: svc.Service.ServiceArn})
	require.NoError(t, err)
	require.NotNil(t, delOut.Service.DeletedAt,
		"real Service.DeletedAt must round-trip on DeleteService, matching deserializers.go's DeletedAt case")
}

// TestDeleteVpcConnector_ResponseHasDeletedAt verifies DeletedAt round-trips
// on VpcConnector (deserializers.go:7299), which storedVpcConnector and the
// VpcConnector domain type already tracked internally but vpcConnectorOutput
// never surfaced.
func TestDeleteVpcConnector_ResponseHasDeletedAt(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateVpcConnector(ctx, &apprunnersdk.CreateVpcConnectorInput{
		VpcConnectorName: aws.String("deletedat-vpc-connector"),
		Subnets:          []string{"subnet-1"},
	})
	require.NoError(t, err)
	assert.Nil(t, created.VpcConnector.DeletedAt, "a live VpcConnector must not carry a fabricated DeletedAt")

	deleted, err := client.DeleteVpcConnector(ctx, &apprunnersdk.DeleteVpcConnectorInput{
		VpcConnectorArn: created.VpcConnector.VpcConnectorArn,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.VpcConnector.DeletedAt,
		"real VpcConnector.DeletedAt must round-trip on delete, matching deserializers.go's DeletedAt case")
}

// TestDeleteVpcIngressConnection_ResponseHasDeletedAt verifies DeletedAt
// round-trips on VpcIngressConnection (deserializers.go:7547), the same
// already-tracked-but-not-surfaced gap as VpcConnector above.
func TestDeleteVpcIngressConnection_ResponseHasDeletedAt(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
		ServiceName: aws.String("vic-deletedat-svc"),
		SourceConfiguration: &types.SourceConfiguration{
			ImageRepository: &types.ImageRepository{
				ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
				ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
			},
		},
	})
	require.NoError(t, err)

	created, err := client.CreateVpcIngressConnection(ctx, &apprunnersdk.CreateVpcIngressConnectionInput{
		VpcIngressConnectionName: aws.String("deletedat-vic"),
		ServiceArn:               svc.Service.ServiceArn,
		IngressVpcConfiguration: &types.IngressVpcConfiguration{
			VpcId:         aws.String("vpc-1"),
			VpcEndpointId: aws.String("vpce-1"),
		},
	})
	require.NoError(t, err)
	assert.Nil(t, created.VpcIngressConnection.DeletedAt,
		"a live VpcIngressConnection must not carry a fabricated DeletedAt")

	deleted, err := client.DeleteVpcIngressConnection(ctx, &apprunnersdk.DeleteVpcIngressConnectionInput{
		VpcIngressConnectionArn: created.VpcIngressConnection.VpcIngressConnectionArn,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.VpcIngressConnection.DeletedAt,
		"real VpcIngressConnection.DeletedAt must round-trip on delete, matching deserializers.go's DeletedAt case")
}

// TestAutoScalingConfiguration_Latest verifies Latest (deserializers.go:4692)
// is true for the highest revision of a name and flips to the new highest
// revision after the current Latest is deleted, matching the real API's
// documented semantics (types.go: "set to true for the configuration with
// the highest Revision among all configurations that share the same
// AutoScalingConfigurationName").
func TestAutoScalingConfiguration_Latest(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
	ctx := t.Context()

	rev1, err := client.CreateAutoScalingConfiguration(ctx, &apprunnersdk.CreateAutoScalingConfigurationInput{
		AutoScalingConfigurationName: aws.String("latest-asg"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(rev1.AutoScalingConfiguration.Latest), "revision 1 is the only, thus latest, revision")

	rev2, err := client.CreateAutoScalingConfiguration(ctx, &apprunnersdk.CreateAutoScalingConfigurationInput{
		AutoScalingConfigurationName: aws.String("latest-asg"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(rev2.AutoScalingConfiguration.Latest), "revision 2 is now the highest revision")

	rev1Desc, err := client.DescribeAutoScalingConfiguration(ctx, &apprunnersdk.DescribeAutoScalingConfigurationInput{
		AutoScalingConfigurationArn: rev1.AutoScalingConfiguration.AutoScalingConfigurationArn,
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(rev1Desc.AutoScalingConfiguration.Latest), "revision 1 is superseded by revision 2")

	_, err = client.DeleteAutoScalingConfiguration(ctx, &apprunnersdk.DeleteAutoScalingConfigurationInput{
		AutoScalingConfigurationArn: rev2.AutoScalingConfiguration.AutoScalingConfigurationArn,
	})
	require.NoError(t, err)

	rev1Again, err := client.DescribeAutoScalingConfiguration(ctx, &apprunnersdk.DescribeAutoScalingConfigurationInput{
		AutoScalingConfigurationArn: rev1.AutoScalingConfiguration.AutoScalingConfigurationArn,
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(rev1Again.AutoScalingConfiguration.Latest),
		"revision 1 becomes latest again once revision 2 is deleted")
}
