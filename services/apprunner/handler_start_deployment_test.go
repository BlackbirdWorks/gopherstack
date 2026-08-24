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
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestStartDeployment_NonRunning_SDKRoundTrip proves StartDeployment on a
// non-running service types as InvalidRequestException through the real
// client, not InvalidStateException. StartDeployment's own
// deserializeOpErrorStartDeployment switch in the vendored SDK
// (apprunner@v1.42.4) types only InternalServiceErrorException,
// InvalidRequestException, and ResourceNotFoundException -- unlike
// UpdateService/PauseService/ResumeService, it has no InvalidStateException
// case, so an InvalidStateException body would fail errors.As here.
func TestStartDeployment_NonRunning_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("123456789012", "us-east-1")
	h := apprunner.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := apprunnersdk.NewFromConfig(cfg, func(o *apprunnersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	created, err := client.CreateService(t.Context(), &apprunnersdk.CreateServiceInput{
		ServiceName: aws.String("start-deployment-paused"),
		SourceConfiguration: &types.SourceConfiguration{
			ImageRepository: &types.ImageRepository{
				ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
				ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
			},
		},
	})
	require.NoError(t, err)

	serviceArn := created.Service.ServiceArn

	_, err = client.PauseService(t.Context(), &apprunnersdk.PauseServiceInput{ServiceArn: serviceArn})
	require.NoError(t, err)

	_, err = client.StartDeployment(t.Context(), &apprunnersdk.StartDeploymentInput{ServiceArn: serviceArn})
	require.Error(t, err)

	var invalidRequest *types.InvalidRequestException
	require.ErrorAs(t, err, &invalidRequest,
		"a paused service must type as InvalidRequestException, the only 4xx StartDeployment's own switch can type")
}
