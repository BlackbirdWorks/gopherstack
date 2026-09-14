package apigateway_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// captureTransport records the raw bytes of the last HTTP response, then
// replays them so the real SDK deserializer still sees the full body.
type captureTransport struct {
	body []byte
}

func (c *captureTransport) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	b, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	resp.Body.Close()

	c.body = b
	resp.Body = io.NopCloser(bytes.NewReader(b))

	return resp, nil
}

// newCapturingTestAPIGatewayClient is newTestAPIGatewayClient (see
// handler_create_tags_test.go) plus a transport that stashes each response's
// raw bytes on capture.body, so a test can assert on the wire JSON while
// also proving the real client decodes it.
func newCapturingTestAPIGatewayClient(t *testing.T, h *apigateway.Handler) (*apigatewaysdk.Client, *captureTransport) {
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

	capture := &captureTransport{}

	client := apigatewaysdk.NewFromConfig(cfg, func(o *apigatewaysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = capture
	})

	return client, capture
}

// seedResourceCORS sets CorsConfiguration on a resource via a direct backend
// call, the only way this emulator ever populates it: real AWS's
// UpdateResource PATCH table documents only /parentId and /pathPart (see
// applyResourceEntityPatchOp in patch.go), so no real client request can set
// corsConfiguration at all. proxy_validation_test.go's CORS-preflight tests
// seed it the same way.
func seedResourceCORS(t *testing.T, b *apigateway.InMemoryBackend, restAPIID, resourceID string) {
	t.Helper()

	_, err := b.UpdateResource(restAPIID, resourceID, apigateway.UpdateResourceInput{
		CorsConfiguration: &apigateway.CorsConfiguration{
			AllowOrigins: []string{"https://example.com"},
			AllowMethods: []string{"GET", "POST"},
		},
	})
	require.NoError(t, err)
}

// TestResourceWire_CorsConfigurationStripped covers every wire path that
// marshals Resource directly (gopherstack-z887j): real types.Resource
// (apigateway v1.42.4 deserializers.go:27457, 5-case list: id/parentId/
// path/pathPart/resourceMethods) has no corsConfiguration member -- CORS is
// an HTTP-API (apigatewayv2) concept only.
//
// get_resource, get_resources and update_resource seed CorsConfiguration via
// seedResourceCORS first, so each proves an actual leak would show up if the
// wire twin regressed. create_resource cannot carry a non-nil
// CorsConfiguration at all -- CreateResourceInput has no such field, so a
// freshly created resource is always nil here -- its subtest is
// defensive-only (the key is trivially absent either way) and exists to keep
// all four emission paths on the same wire-safe helper and to prove decode
// still succeeds.
func TestResourceWire_CorsConfigurationStripped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(t *testing.T, client *apigatewaysdk.Client, apiID, resourceID string)
		name     string
		seedCORS bool
	}{
		{
			name:     "get_resource",
			seedCORS: true,
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, resourceID string) {
				t.Helper()

				out, err := client.GetResource(t.Context(), &apigatewaysdk.GetResourceInput{
					RestApiId: aws.String(apiID), ResourceId: aws.String(resourceID),
				})
				require.NoError(t, err)
				assert.Equal(t, resourceID, aws.ToString(out.Id))
			},
		},
		{
			name:     "get_resources",
			seedCORS: true,
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, _ string) {
				t.Helper()

				out, err := client.GetResources(t.Context(), &apigatewaysdk.GetResourcesInput{
					RestApiId: aws.String(apiID),
				})
				require.NoError(t, err)
				require.NotEmpty(t, out.Items)
			},
		},
		{
			name:     "update_resource",
			seedCORS: true,
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, resourceID string) {
				t.Helper()

				out, err := client.UpdateResource(t.Context(), &apigatewaysdk.UpdateResourceInput{
					RestApiId: aws.String(apiID), ResourceId: aws.String(resourceID),
					PatchOperations: []apigwtypes.PatchOperation{
						{Op: apigwtypes.OpReplace, Path: aws.String("/pathPart"), Value: aws.String("renamed")},
					},
				})
				require.NoError(t, err)
				assert.Equal(t, "renamed", aws.ToString(out.PathPart))
			},
		},
		{
			name:     "create_resource",
			seedCORS: false,
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, resourceID string) {
				t.Helper()

				out, err := client.CreateResource(t.Context(), &apigatewaysdk.CreateResourceInput{
					RestApiId: aws.String(apiID), ParentId: aws.String(resourceID), PathPart: aws.String("child"),
				})
				require.NoError(t, err)
				assert.Equal(t, "child", aws.ToString(out.PathPart))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)
			client, capture := newCapturingTestAPIGatewayClient(t, h)

			api, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{
				Name: aws.String("cors-wire-" + tt.name),
			})
			require.NoError(t, err)

			resources, err := client.GetResources(t.Context(), &apigatewaysdk.GetResourcesInput{RestApiId: api.Id})
			require.NoError(t, err)
			require.NotEmpty(t, resources.Items)
			rootID := aws.ToString(resources.Items[0].Id)

			if tt.seedCORS {
				seedResourceCORS(t, b, aws.ToString(api.Id), rootID)
			}

			tt.run(t, client, aws.ToString(api.Id), rootID)

			require.NotEmpty(t, capture.body)
			assert.NotContains(t, string(capture.body), `"corsConfiguration"`,
				"response must not carry the persisted-only corsConfiguration field on the wire")
		})
	}
}

// TestStageWire_InvokeURLStripped covers every wire path that marshals Stage
// directly (gopherstack-z887j): real types.Stage (apigateway v1.42.4
// deserializers.go:27905, 17-case list) has no invokeUrl member.
//
// get_stage and get_stages are the genuine leaks: InMemoryBackend.GetStage/
// GetStages (stages.go) compute and set InvokeURL on every read via
// stageInvokeURL. create_stage and update_stage never populate InvokeURL at
// all (CreateStage/UpdateStage never call stageInvokeURL), so their
// subtests are defensive-only -- the omitempty tag already suppresses the
// always-empty string there -- kept for the same reason as
// create_resource above: same wire-safe helper on every path, decode proven.
func TestStageWire_InvokeURLStripped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *apigatewaysdk.Client, apiID, deploymentID string)
		name string
	}{
		{
			name: "create_stage",
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, deploymentID string) {
				t.Helper()

				out, err := client.CreateStage(t.Context(), &apigatewaysdk.CreateStageInput{
					RestApiId: aws.String(apiID), DeploymentId: aws.String(deploymentID),
					StageName: aws.String("prod"),
				})
				require.NoError(t, err)
				assert.Equal(t, "prod", aws.ToString(out.StageName))
			},
		},
		{
			name: "update_stage",
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, deploymentID string) {
				t.Helper()

				_, err := client.CreateStage(t.Context(), &apigatewaysdk.CreateStageInput{
					RestApiId: aws.String(apiID), DeploymentId: aws.String(deploymentID),
					StageName: aws.String("prod"),
				})
				require.NoError(t, err)

				out, err := client.UpdateStage(t.Context(), &apigatewaysdk.UpdateStageInput{
					RestApiId: aws.String(apiID), StageName: aws.String("prod"),
					PatchOperations: []apigwtypes.PatchOperation{
						{Op: apigwtypes.OpReplace, Path: aws.String("/description"), Value: aws.String("updated")},
					},
				})
				require.NoError(t, err)
				assert.Equal(t, "updated", aws.ToString(out.Description))
			},
		},
		{
			name: "get_stage",
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, deploymentID string) {
				t.Helper()

				_, err := client.CreateStage(t.Context(), &apigatewaysdk.CreateStageInput{
					RestApiId: aws.String(apiID), DeploymentId: aws.String(deploymentID),
					StageName: aws.String("prod"),
				})
				require.NoError(t, err)

				out, err := client.GetStage(t.Context(), &apigatewaysdk.GetStageInput{
					RestApiId: aws.String(apiID), StageName: aws.String("prod"),
				})
				require.NoError(t, err)
				assert.Equal(t, "prod", aws.ToString(out.StageName))
			},
		},
		{
			name: "get_stages",
			run: func(t *testing.T, client *apigatewaysdk.Client, apiID, deploymentID string) {
				t.Helper()

				_, err := client.CreateStage(t.Context(), &apigatewaysdk.CreateStageInput{
					RestApiId: aws.String(apiID), DeploymentId: aws.String(deploymentID),
					StageName: aws.String("prod"),
				})
				require.NoError(t, err)

				out, err := client.GetStages(t.Context(), &apigatewaysdk.GetStagesInput{RestApiId: aws.String(apiID)})
				require.NoError(t, err)
				require.NotEmpty(t, out.Item)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)
			client, capture := newCapturingTestAPIGatewayClient(t, h)

			api, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{
				Name: aws.String("invokeurl-wire-" + tt.name),
			})
			require.NoError(t, err)

			depl, err := client.CreateDeployment(t.Context(), &apigatewaysdk.CreateDeploymentInput{RestApiId: api.Id})
			require.NoError(t, err)

			tt.run(t, client, aws.ToString(api.Id), aws.ToString(depl.Id))

			require.NotEmpty(t, capture.body)
			assert.NotContains(t, string(capture.body), `"invokeUrl"`,
				"response must not carry the persisted-only invokeUrl field on the wire")
		})
	}
}
