package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	apprunnertypes "github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAppRunnerClient returns an App Runner client pointed at the shared test container.
func createAppRunnerClient(t *testing.T) *apprunnersdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return apprunnersdk.NewFromConfig(cfg, func(o *apprunnersdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_AppRunner_ServiceLifecycle drives create→describe→list→delete of a service
// backed by an image repository source.
func TestIntegration_AppRunner_ServiceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name        string
		serviceName string
		image       string
	}{
		{name: "image_service", serviceName: "integ-svc", image: "public.ecr.aws/nginx/nginx:latest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppRunnerClient(t)

			createOut, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
				ServiceName: aws.String(tt.serviceName),
				SourceConfiguration: &apprunnertypes.SourceConfiguration{
					ImageRepository: &apprunnertypes.ImageRepository{
						ImageIdentifier:     aws.String(tt.image),
						ImageRepositoryType: apprunnertypes.ImageRepositoryTypeEcrPublic,
					},
				},
			})
			require.NoError(t, err, "CreateService should succeed")
			require.NotNil(t, createOut.Service)
			serviceArn := aws.ToString(createOut.Service.ServiceArn)
			require.NotEmpty(t, serviceArn, "service ARN must be returned")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteService(
					cleanupCtx,
					&apprunnersdk.DeleteServiceInput{ServiceArn: aws.String(serviceArn)},
				)
			})

			descOut, err := client.DescribeService(ctx, &apprunnersdk.DescribeServiceInput{
				ServiceArn: aws.String(serviceArn),
			})
			require.NoError(t, err, "DescribeService should succeed")
			require.NotNil(t, descOut.Service)
			assert.Equal(t, tt.serviceName, aws.ToString(descOut.Service.ServiceName))
			assert.NotEmpty(t, aws.ToString(descOut.Service.ServiceUrl), "service URL must be set")

			listOut, err := client.ListServices(ctx, &apprunnersdk.ListServicesInput{})
			require.NoError(t, err, "ListServices should succeed")

			found := false
			for _, s := range listOut.ServiceSummaryList {
				if aws.ToString(s.ServiceArn) == serviceArn {
					found = true

					break
				}
			}

			assert.True(t, found, "created service should appear in list")

			_, err = client.DeleteService(ctx, &apprunnersdk.DeleteServiceInput{ServiceArn: aws.String(serviceArn)})
			require.NoError(t, err, "DeleteService should succeed")
		})
	}
}

// TestIntegration_AppRunner_ConnectionLifecycle drives create→list→delete of a source connection.
func TestIntegration_AppRunner_ConnectionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name           string
		connectionName string
	}{
		{name: "github_connection", connectionName: "integ-conn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppRunnerClient(t)

			createOut, err := client.CreateConnection(ctx, &apprunnersdk.CreateConnectionInput{
				ConnectionName: aws.String(tt.connectionName),
				ProviderType:   apprunnertypes.ProviderTypeGithub,
			})
			require.NoError(t, err, "CreateConnection should succeed")
			require.NotNil(t, createOut.Connection)
			assert.Equal(t, tt.connectionName, aws.ToString(createOut.Connection.ConnectionName))
			connArn := aws.ToString(createOut.Connection.ConnectionArn)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteConnection(
					cleanupCtx,
					&apprunnersdk.DeleteConnectionInput{ConnectionArn: aws.String(connArn)},
				)
			})

			listOut, err := client.ListConnections(ctx, &apprunnersdk.ListConnectionsInput{})
			require.NoError(t, err, "ListConnections should succeed")

			found := false
			for _, c := range listOut.ConnectionSummaryList {
				if aws.ToString(c.ConnectionName) == tt.connectionName {
					found = true

					break
				}
			}

			assert.True(t, found, "created connection should appear in list")

			_, err = client.DeleteConnection(
				ctx,
				&apprunnersdk.DeleteConnectionInput{ConnectionArn: aws.String(connArn)},
			)
			require.NoError(t, err, "DeleteConnection should succeed")
		})
	}
}
