package apigatewayv2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

const tagsRTRegion = "us-east-1"

// newTestAPIGatewayV2Client stands up the real aws-sdk-go-v2 apigatewayv2
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestAPIGatewayV2Client(t *testing.T, h *apigatewayv2.Handler) *apigatewayv2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return apigatewayv2sdk.NewFromConfig(cfg, func(o *apigatewayv2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// testPortalContent is the minimal valid PortalContent CreatePortal's client-side
// validation requires (apigatewayv2@v1.37.4 types.go:917-932, types.go:1034-1045,
// types.go:295-325): DisplayName, a Theme, and every CustomColors field.
func testPortalContent() *apigatewayv2types.PortalContent {
	return &apigatewayv2types.PortalContent{
		DisplayName: aws.String("test-portal"),
		Theme: &apigatewayv2types.PortalTheme{
			CustomColors: &apigatewayv2types.CustomColors{
				AccentColor:          aws.String("#000000"),
				BackgroundColor:      aws.String("#000000"),
				ErrorValidationColor: aws.String("#000000"),
				HeaderColor:          aws.String("#000000"),
				NavigationColor:      aws.String("#000000"),
				TextColor:            aws.String("#000000"),
			},
		},
	}
}

// TestCreateOpsWithTags_RoundTrip drives every apigatewayv2 Create op whose
// real Input struct accepts Tags (apigatewayv2@v1.37.4:
// api_op_CreateApi.go:44, api_op_CreateStage.go:41, api_op_CreateDomainName.go:37,
// api_op_CreateVpcLink.go:37, api_op_CreatePortal.go:47,
// api_op_CreatePortalProduct.go:41) through the real SDK client and asserts
// GetTags sees what was supplied at creation (gopherstack-2mwl).
//
// Before the fix: CreateStage decoded no Tags field at all (Stage.Tags existed
// but CreateStageInput did not); CreatePortal/CreatePortalProduct stored tags
// on the right struct but TagResource/UntagResource/GetTags's ARN-type switch
// had no "portals"/"portalproducts" case, so those tags could never be read
// back -- the same missing-case shape as memorydb -- and neither Portal nor
// PortalProduct even exposed an ARN in their Create response for a client to
// construct a GetTags call against.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *apigatewayv2sdk.Client) string
		name  string
	}{
		{
			name: "api",
			setup: func(t *testing.T, client *apigatewayv2sdk.Client) string {
				t.Helper()

				out, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
					Name:         aws.String("tagged-api"),
					ProtocolType: apigatewayv2types.ProtocolTypeHttp,
					Tags:         map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/apis/" + aws.ToString(out.ApiId)
			},
		},
		{
			name: "stage",
			setup: func(t *testing.T, client *apigatewayv2sdk.Client) string {
				t.Helper()

				api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
					Name:         aws.String("stage-host-api"),
					ProtocolType: apigatewayv2types.ProtocolTypeHttp,
				})
				require.NoError(t, err)

				_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
					ApiId:     api.ApiId,
					StageName: aws.String("tagged-stage"),
					Tags:      map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/apis/" + aws.ToString(
					api.ApiId,
				) + "/stages/tagged-stage"
			},
		},
		{
			name: "domain name",
			setup: func(t *testing.T, client *apigatewayv2sdk.Client) string {
				t.Helper()

				out, err := client.CreateDomainName(
					t.Context(),
					&apigatewayv2sdk.CreateDomainNameInput{
						DomainName: aws.String("tagged.example.com"),
						Tags:       map[string]string{"env": "test"},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.DomainNameArn)
			},
		},
		{
			name: "vpc link",
			setup: func(t *testing.T, client *apigatewayv2sdk.Client) string {
				t.Helper()

				out, err := client.CreateVpcLink(t.Context(), &apigatewayv2sdk.CreateVpcLinkInput{
					Name:      aws.String("tagged-link"),
					SubnetIds: []string{"subnet-abc123"},
					Tags:      map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/vpclinks/" + aws.ToString(
					out.VpcLinkId,
				)
			},
		},
		{
			name: "portal",
			setup: func(t *testing.T, client *apigatewayv2sdk.Client) string {
				t.Helper()

				out, err := client.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
					Authorization: &apigatewayv2types.Authorization{
						None: &apigatewayv2types.None{},
					},
					EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{
						None: &apigatewayv2types.None{},
					},
					PortalContent: testPortalContent(),
					Tags:          map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.PortalArn)
			},
		},
		{
			name: "portal product",
			setup: func(t *testing.T, client *apigatewayv2sdk.Client) string {
				t.Helper()

				out, err := client.CreatePortalProduct(
					t.Context(),
					&apigatewayv2sdk.CreatePortalProductInput{
						DisplayName: aws.String("tagged-product"),
						Tags:        map[string]string{"env": "test"},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.PortalProductArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigatewayv2.NewInMemoryBackend()
			client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

			arn := tt.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.GetTags(
				t.Context(),
				&apigatewayv2sdk.GetTagsInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test"}, got.Tags)
		})
	}
}
