package apprunner_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

const apprunnerTagsRTRegion = "us-east-1"

// newTestAppRunnerClient stands up the real aws-sdk-go-v2 App Runner client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestAppRunnerClient(t *testing.T, h *apprunner.Handler) *apprunnersdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(apprunnerTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return apprunnersdk.NewFromConfig(cfg, func(o *apprunnersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every apprunner Create* op whose real
// Input struct accepts Tags (apprunner@v1.42.4: api_op_CreateAutoScalingConfiguration.go,
// api_op_CreateConnection.go, api_op_CreateObservabilityConfiguration.go,
// api_op_CreateService.go, api_op_CreateVpcConnector.go,
// api_op_CreateVpcIngressConnection.go, all `Tags []types.Tag`) through the
// real SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *apprunnersdk.Client) string
		name  string
	}{
		{
			name: "auto scaling configuration",
			setup: func(t *testing.T, client *apprunnersdk.Client) string {
				t.Helper()
				out, err := client.CreateAutoScalingConfiguration(
					t.Context(),
					&apprunnersdk.CreateAutoScalingConfigurationInput{
						AutoScalingConfigurationName: aws.String("tagged-asg"),
						Tags:                         []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.AutoScalingConfiguration.AutoScalingConfigurationArn)
			},
		},
		{
			name: "connection",
			setup: func(t *testing.T, client *apprunnersdk.Client) string {
				t.Helper()
				out, err := client.CreateConnection(t.Context(), &apprunnersdk.CreateConnectionInput{
					ConnectionName: aws.String("tagged-conn"),
					ProviderType:   types.ProviderTypeGithub,
					Tags:           []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.Connection.ConnectionArn)
			},
		},
		{
			name: "observability configuration",
			setup: func(t *testing.T, client *apprunnersdk.Client) string {
				t.Helper()
				out, err := client.CreateObservabilityConfiguration(
					t.Context(),
					&apprunnersdk.CreateObservabilityConfigurationInput{
						ObservabilityConfigurationName: aws.String("tagged-obs"),
						Tags: []types.Tag{
							{Key: aws.String("env"), Value: aws.String("prod")},
						},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.ObservabilityConfiguration.ObservabilityConfigurationArn)
			},
		},
		{
			name: "service",
			setup: func(t *testing.T, client *apprunnersdk.Client) string {
				t.Helper()
				out, err := client.CreateService(t.Context(), &apprunnersdk.CreateServiceInput{
					ServiceName: aws.String("tagged-service"),
					SourceConfiguration: &types.SourceConfiguration{
						ImageRepository: &types.ImageRepository{
							ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
							ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
						},
					},
					Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.Service.ServiceArn)
			},
		},
		{
			name: "vpc connector",
			setup: func(t *testing.T, client *apprunnersdk.Client) string {
				t.Helper()
				out, err := client.CreateVpcConnector(t.Context(), &apprunnersdk.CreateVpcConnectorInput{
					VpcConnectorName: aws.String("tagged-vpcconn"),
					Subnets:          []string{"subnet-12345"},
					Tags:             []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.VpcConnector.VpcConnectorArn)
			},
		},
		{
			name: "vpc ingress connection",
			setup: func(t *testing.T, client *apprunnersdk.Client) string {
				t.Helper()
				svc, err := client.CreateService(t.Context(), &apprunnersdk.CreateServiceInput{
					ServiceName: aws.String("tagged-service-for-vic"),
					SourceConfiguration: &types.SourceConfiguration{
						ImageRepository: &types.ImageRepository{
							ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
							ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
						},
					},
				})
				require.NoError(t, err)

				out, err := client.CreateVpcIngressConnection(
					t.Context(),
					&apprunnersdk.CreateVpcIngressConnectionInput{
						ServiceArn:               svc.Service.ServiceArn,
						VpcIngressConnectionName: aws.String("tagged-vic"),
						IngressVpcConfiguration: &types.IngressVpcConfiguration{
							VpcId:         aws.String("vpc-12345"),
							VpcEndpointId: aws.String("vpce-12345"),
						},
						Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.VpcIngressConnection.VpcIngressConnectionArn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
			client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &apprunnersdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "prod", aws.ToString(got.Tags[0].Value))
		})
	}
}
