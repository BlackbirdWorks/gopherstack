package servicediscovery_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	sdtypes "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

const sdTagsRTRegion = "us-east-1"

// newTestServiceDiscoveryClient stands up the real aws-sdk-go-v2
// servicediscovery client against an httptest server running this
// package's Handler, wired through the same pkgs/service registry/router
// used in production.
func newTestServiceDiscoveryClient(t *testing.T, h *servicediscovery.Handler) *sdsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(sdTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sdsdk.NewFromConfig(cfg, func(o *sdsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every servicediscovery Create op
// whose real Input struct accepts Tags (servicediscovery@v1.43.4:
// api_op_CreateHttpNamespace.go:55, api_op_CreatePublicDnsNamespace.go:66,
// api_op_CreatePrivateDnsNamespace.go:67, api_op_CreateService.go:132)
// through the real SDK client and asserts ListTagsForResource sees what
// was supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *sdsdk.Client) string
		name  string
	}{
		{
			name: "http namespace",
			setup: func(t *testing.T, client *sdsdk.Client) string {
				t.Helper()

				out, err := client.CreateHttpNamespace(t.Context(), &sdsdk.CreateHttpNamespaceInput{
					Name: aws.String("tagged-http-ns"),
					Tags: []sdtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return waitForOperationResource(t, client, aws.ToString(out.OperationId))
			},
		},
		{
			name: "public dns namespace",
			setup: func(t *testing.T, client *sdsdk.Client) string {
				t.Helper()

				out, err := client.CreatePublicDnsNamespace(t.Context(), &sdsdk.CreatePublicDnsNamespaceInput{
					Name: aws.String("tagged-public-ns.com"),
					Tags: []sdtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return waitForOperationResource(t, client, aws.ToString(out.OperationId))
			},
		},
		{
			name: "private dns namespace",
			setup: func(t *testing.T, client *sdsdk.Client) string {
				t.Helper()

				out, err := client.CreatePrivateDnsNamespace(t.Context(), &sdsdk.CreatePrivateDnsNamespaceInput{
					Name: aws.String("tagged-private-ns.com"),
					Vpc:  aws.String("vpc-12345"),
					Tags: []sdtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return waitForOperationResource(t, client, aws.ToString(out.OperationId))
			},
		},
		{
			name: "service",
			setup: func(t *testing.T, client *sdsdk.Client) string {
				t.Helper()

				out, err := client.CreateService(t.Context(), &sdsdk.CreateServiceInput{
					Name: aws.String("tagged-service"),
					Type: sdtypes.ServiceTypeOptionHttp,
					Tags: []sdtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.Service.Arn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion)
			client := newTestServiceDiscoveryClient(t, servicediscovery.NewHandler(backend))

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			got, err := client.ListTagsForResource(
				t.Context(),
				&sdsdk.ListTagsForResourceInput{ResourceARN: aws.String(resourceARN)},
			)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}

// waitForOperationResource fetches the operation and returns its created
// resource's ARN. This backend completes namespace operations synchronously,
// so a single GetOperation call suffices without polling.
func waitForOperationResource(t *testing.T, client *sdsdk.Client, opID string) string {
	t.Helper()

	op, err := client.GetOperation(t.Context(), &sdsdk.GetOperationInput{OperationId: aws.String(opID)})
	require.NoError(t, err)

	nsID, ok := op.Operation.Targets[string(sdtypes.OperationTargetTypeNamespace)]
	require.True(t, ok, "operation must target a namespace")

	ns, err := client.GetNamespace(t.Context(), &sdsdk.GetNamespaceInput{Id: aws.String(nsID)})
	require.NoError(t, err)

	return aws.ToString(ns.Namespace.Arn)
}
