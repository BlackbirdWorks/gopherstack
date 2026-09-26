package apprunner_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// postAppRunnerJSON sends an empty-body App Runner JSON-protocol request for
// target, returning the raw response for wire-shape assertions.
func postAppRunnerJSON(t *testing.T, h *apprunner.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AppRunner."+target)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack-dv4s, 2026-09-19) for apprunner's six flagged List ops:
// ListAutoScalingConfigurations, ListConnections,
// ListObservabilityConfigurations, ListOperations, ListServices and
// ListVpcIngressConnections were all ALREADY narrow, member-for-member
// exact matches of their real *Summary types (verified via
// cmd/structfielddiff against apprunner@v1.42.4) -- no leak or gap found in
// any of the six. This test locks that in via the real aws-sdk-go-v2
// client and a raw-body check that no Describe-only member ever leaks in.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("auto scaling configurations exact", func(t *testing.T) {
		t.Parallel()

		h := apprunner.NewHandler(apprunner.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppRunnerClient(t, h)
		ctx := t.Context()

		_, err := client.CreateAutoScalingConfiguration(ctx, &apprunnersdk.CreateAutoScalingConfigurationInput{
			AutoScalingConfigurationName: aws.String("lss-asc"),
			MinSize:                      aws.Int32(1),
			MaxSize:                      aws.Int32(5),
		})
		require.NoError(t, err)

		out, err := client.ListAutoScalingConfigurations(ctx, &apprunnersdk.ListAutoScalingConfigurationsInput{
			AutoScalingConfigurationName: aws.String("lss-asc"),
		})
		require.NoError(t, err)
		require.Len(t, out.AutoScalingConfigurationSummaryList, 1)
		s := out.AutoScalingConfigurationSummaryList[0]
		assert.Equal(t, "lss-asc", aws.ToString(s.AutoScalingConfigurationName))
		assert.NotNil(t, s.CreatedAt)
		assert.NotNil(t, s.IsDefault)
		assert.NotNil(t, s.HasAssociatedService)

		rec := postAppRunnerJSON(t, h, "ListAutoScalingConfigurations")
		assert.NotContains(t, rec.Body.String(), "MaxConcurrency")
		assert.NotContains(t, rec.Body.String(), "MaxSize")
		assert.NotContains(t, rec.Body.String(), "MinSize")
	})

	t.Run("connections exact", func(t *testing.T) {
		t.Parallel()

		h := apprunner.NewHandler(apprunner.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppRunnerClient(t, h)
		ctx := t.Context()

		_, err := client.CreateConnection(ctx, &apprunnersdk.CreateConnectionInput{
			ConnectionName: aws.String("lss-conn"),
			ProviderType:   types.ProviderTypeGithub,
		})
		require.NoError(t, err)

		out, err := client.ListConnections(ctx, &apprunnersdk.ListConnectionsInput{})
		require.NoError(t, err)
		require.Len(t, out.ConnectionSummaryList, 1)
		assert.Equal(t, "lss-conn", aws.ToString(out.ConnectionSummaryList[0].ConnectionName))
		assert.Equal(t, types.ProviderTypeGithub, out.ConnectionSummaryList[0].ProviderType)
	})

	t.Run("observability configurations exact", func(t *testing.T) {
		t.Parallel()

		h := apprunner.NewHandler(apprunner.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppRunnerClient(t, h)
		ctx := t.Context()

		_, err := client.CreateObservabilityConfiguration(ctx, &apprunnersdk.CreateObservabilityConfigurationInput{
			ObservabilityConfigurationName: aws.String("lss-obs"),
		})
		require.NoError(t, err)

		out, err := client.ListObservabilityConfigurations(ctx, &apprunnersdk.ListObservabilityConfigurationsInput{})
		require.NoError(t, err)
		require.Len(t, out.ObservabilityConfigurationSummaryList, 1)
		gotName := out.ObservabilityConfigurationSummaryList[0].ObservabilityConfigurationName
		assert.Equal(t, "lss-obs", aws.ToString(gotName))

		rec := postAppRunnerJSON(t, h, "ListObservabilityConfigurations")
		assert.NotContains(t, rec.Body.String(), "TraceConfiguration")
		assert.NotContains(t, rec.Body.String(), `"Latest"`)
		assert.NotContains(t, rec.Body.String(), `"Status"`)
	})

	t.Run("operations exact", func(t *testing.T) {
		t.Parallel()

		h := apprunner.NewHandler(apprunner.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppRunnerClient(t, h)
		ctx := t.Context()

		svc, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
			ServiceName: aws.String("lss-svc-ops"),
			SourceConfiguration: &types.SourceConfiguration{
				ImageRepository: &types.ImageRepository{
					ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
					ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListOperations(ctx, &apprunnersdk.ListOperationsInput{
			ServiceArn: svc.Service.ServiceArn,
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.OperationSummaryList)
		op := out.OperationSummaryList[0]
		assert.NotEmpty(t, aws.ToString(op.Id))
		assert.Equal(t, types.OperationTypeCreateService, op.Type)
	})

	t.Run("services exact", func(t *testing.T) {
		t.Parallel()

		h := apprunner.NewHandler(apprunner.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppRunnerClient(t, h)
		ctx := t.Context()

		_, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
			ServiceName: aws.String("lss-svc"),
			SourceConfiguration: &types.SourceConfiguration{
				ImageRepository: &types.ImageRepository{
					ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
					ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListServices(ctx, &apprunnersdk.ListServicesInput{})
		require.NoError(t, err)
		require.Len(t, out.ServiceSummaryList, 1)
		s := out.ServiceSummaryList[0]
		assert.Equal(t, "lss-svc", aws.ToString(s.ServiceName))
		assert.NotNil(t, s.UpdatedAt)

		rec := postAppRunnerJSON(t, h, "ListServices")
		assert.NotContains(t, rec.Body.String(), "SourceConfiguration")
		assert.NotContains(t, rec.Body.String(), "InstanceConfiguration")
	})

	t.Run("vpc ingress connections exact", func(t *testing.T) {
		t.Parallel()

		h := apprunner.NewHandler(apprunner.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestAppRunnerClient(t, h)
		ctx := t.Context()

		svc, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
			ServiceName: aws.String("lss-svc-vic"),
			SourceConfiguration: &types.SourceConfiguration{
				ImageRepository: &types.ImageRepository{
					ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
					ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
				},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateVpcIngressConnection(ctx, &apprunnersdk.CreateVpcIngressConnectionInput{
			VpcIngressConnectionName: aws.String("lss-vic"),
			ServiceArn:               svc.Service.ServiceArn,
			IngressVpcConfiguration: &types.IngressVpcConfiguration{
				VpcId:         aws.String("vpc-1"),
				VpcEndpointId: aws.String("vpce-1"),
			},
		})
		require.NoError(t, err)

		out, err := client.ListVpcIngressConnections(ctx, &apprunnersdk.ListVpcIngressConnectionsInput{})
		require.NoError(t, err)
		require.Len(t, out.VpcIngressConnectionSummaryList, 1)
		s := out.VpcIngressConnectionSummaryList[0]
		assert.Equal(t, aws.ToString(svc.Service.ServiceArn), aws.ToString(s.ServiceArn))
		assert.NotEmpty(t, aws.ToString(s.VpcIngressConnectionArn))

		rec := postAppRunnerJSON(t, h, "ListVpcIngressConnections")
		assert.NotContains(t, rec.Body.String(), "IngressVpcConfiguration")
		assert.NotContains(t, rec.Body.String(), "DomainName")
	})
}
