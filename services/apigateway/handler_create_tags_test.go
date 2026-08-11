package apigateway_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

const tagsRTRegion = "us-east-1"

// newTestAPIGatewayClient stands up the real aws-sdk-go-v2 apigateway client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestAPIGatewayClient(t *testing.T, h *apigateway.Handler) *apigatewaysdk.Client {
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

	return apigatewaysdk.NewFromConfig(cfg, func(o *apigatewaysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every apigateway Create/Generate op
// whose real Input struct accepts Tags (apigateway@v1.42.4:
// api_op_CreateRestApi.go:85, api_op_CreateApiKey.go:55,
// api_op_CreateDomainName.go:106, api_op_CreateUsagePlan.go:51,
// api_op_CreateVpcLink.go:55, api_op_GenerateClientCertificate.go:37,
// api_op_CreateStage.go:70) through the real SDK client and asserts GetTags
// sees what was supplied at creation (gopherstack-2mwl).
//
// Before the fix, GetResourceTags/TagResource/UntagResource (tags.go) only
// recognised "/restapis/{id}" ARNs -- ApiKey, DomainName, UsagePlan, VpcLink
// each stored tags on their own struct but the shared read path could never
// reach them, and ClientCertificate/Stage dropped input.Tags entirely since
// neither their struct nor CreateStageInput even had a Tags field.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *apigatewaysdk.Client) string
		name  string
	}{
		{
			name: "rest api",
			setup: func(t *testing.T, client *apigatewaysdk.Client) string {
				t.Helper()

				out, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{
					Name: aws.String("tagged-api"),
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/restapis/" + aws.ToString(out.Id)
			},
		},
		{
			name: "api key",
			setup: func(t *testing.T, client *apigatewaysdk.Client) string {
				t.Helper()

				out, err := client.CreateApiKey(t.Context(), &apigatewaysdk.CreateApiKeyInput{
					Name: aws.String("tagged-key"),
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/apikeys/" + aws.ToString(out.Id)
			},
		},
		{
			name: "domain name",
			setup: func(t *testing.T, client *apigatewaysdk.Client) string {
				t.Helper()

				out, err := client.CreateDomainName(
					t.Context(),
					&apigatewaysdk.CreateDomainNameInput{
						DomainName: aws.String("tagged.example.com"),
						Tags:       map[string]string{"env": "test"},
					},
				)
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/domainnames/" + aws.ToString(
					out.DomainName,
				)
			},
		},
		{
			name: "usage plan",
			setup: func(t *testing.T, client *apigatewaysdk.Client) string {
				t.Helper()

				out, err := client.CreateUsagePlan(t.Context(), &apigatewaysdk.CreateUsagePlanInput{
					Name: aws.String("tagged-plan"),
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/usageplans/" + aws.ToString(
					out.Id,
				)
			},
		},
		{
			name: "vpc link",
			setup: func(t *testing.T, client *apigatewaysdk.Client) string {
				t.Helper()

				out, err := client.CreateVpcLink(t.Context(), &apigatewaysdk.CreateVpcLinkInput{
					Name: aws.String("tagged-link"),
					TargetArns: []string{
						"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/nlb/abc",
					},
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/vpclinks/" + aws.ToString(out.Id)
			},
		},
		{
			name: "client certificate",
			setup: func(t *testing.T, client *apigatewaysdk.Client) string {
				t.Helper()

				out, err := client.GenerateClientCertificate(
					t.Context(),
					&apigatewaysdk.GenerateClientCertificateInput{
						Tags: map[string]string{"env": "test"},
					},
				)
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/clientcertificates/" + aws.ToString(
					out.ClientCertificateId,
				)
			},
		},
		{
			name: "stage",
			setup: func(t *testing.T, client *apigatewaysdk.Client) string {
				t.Helper()

				api, err := client.CreateRestApi(
					t.Context(),
					&apigatewaysdk.CreateRestApiInput{Name: aws.String("stage-host-api")},
				)
				require.NoError(t, err)

				depl, err := client.CreateDeployment(
					t.Context(),
					&apigatewaysdk.CreateDeploymentInput{
						RestApiId: api.Id,
					},
				)
				require.NoError(t, err)

				_, err = client.CreateStage(t.Context(), &apigatewaysdk.CreateStageInput{
					RestApiId:    api.Id,
					DeploymentId: depl.Id,
					StageName:    aws.String("tagged-stage"),
					Tags:         map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:" + tagsRTRegion + "::/restapis/" + aws.ToString(
					api.Id,
				) + "/stages/tagged-stage"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			client := newTestAPIGatewayClient(t, apigateway.NewHandler(backend))

			arn := tt.setup(t, client)

			got, err := client.GetTags(
				t.Context(),
				&apigatewaysdk.GetTagsInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test"}, got.Tags)
		})
	}
}
