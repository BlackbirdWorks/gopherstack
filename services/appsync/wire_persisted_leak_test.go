package appsync_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appsync"
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

// newCapturingTestAppsyncClient is newTestAppsyncClient (see
// handler_create_tags_test.go) plus a transport that stashes each response's
// raw bytes on capture.body, so a test can assert on the wire JSON while
// also proving the real client decodes it.
func newCapturingTestAppsyncClient(t *testing.T, h *appsync.Handler) (*appsyncsdk.Client, *captureTransport) {
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

	client := appsyncsdk.NewFromConfig(cfg, func(o *appsyncsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = capture
	})

	return client, capture
}

// TestGraphqlAPIWire_RegionCreatedAtUpdatedAtStripped covers every wire path
// that marshals GraphqlAPI directly (gopherstack-z887j): real
// types.GraphqlApi (appsync v1.60.0 deserializers.go:15221, 22-case list)
// has no region, createdAt or updatedAt member. All three are genuinely
// persisted and set on every CreateGraphqlAPI/UpdateGraphqlAPI call
// (graphql_apis.go), so every path below actually carries non-zero values
// to leak, unlike apigateway's create/update-only-empty defensive cases.
//
// The control assertions prove the fix stripped the right keys and nothing
// else: apiId/name/arn/authenticationType, which real GraphqlApi does
// declare, must still be present and must still decode through the real
// typed client.
func TestGraphqlAPIWire_RegionCreatedAtUpdatedAtStripped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *appsyncsdk.Client) *appsynctypes.GraphqlApi
		name string
	}{
		{
			name: "create_graphql_api",
			run: func(t *testing.T, client *appsyncsdk.Client) *appsynctypes.GraphqlApi {
				t.Helper()

				out, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
					Name:               aws.String("wire-leak-create"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				})
				require.NoError(t, err)

				return out.GraphqlApi
			},
		},
		{
			name: "get_graphql_api",
			run: func(t *testing.T, client *appsyncsdk.Client) *appsynctypes.GraphqlApi {
				t.Helper()

				created, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
					Name:               aws.String("wire-leak-get"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				})
				require.NoError(t, err)

				out, err := client.GetGraphqlApi(t.Context(), &appsyncsdk.GetGraphqlApiInput{
					ApiId: created.GraphqlApi.ApiId,
				})
				require.NoError(t, err)

				return out.GraphqlApi
			},
		},
		{
			name: "update_graphql_api",
			run: func(t *testing.T, client *appsyncsdk.Client) *appsynctypes.GraphqlApi {
				t.Helper()

				created, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
					Name:               aws.String("wire-leak-update"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				})
				require.NoError(t, err)

				out, err := client.UpdateGraphqlApi(t.Context(), &appsyncsdk.UpdateGraphqlApiInput{
					ApiId:              created.GraphqlApi.ApiId,
					Name:               aws.String("wire-leak-update-renamed"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				})
				require.NoError(t, err)

				return out.GraphqlApi
			},
		},
		{
			name: "list_graphql_apis",
			run: func(t *testing.T, client *appsyncsdk.Client) *appsynctypes.GraphqlApi {
				t.Helper()

				_, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
					Name:               aws.String("wire-leak-list"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				})
				require.NoError(t, err)

				out, err := client.ListGraphqlApis(t.Context(), &appsyncsdk.ListGraphqlApisInput{})
				require.NoError(t, err)
				require.NotEmpty(t, out.GraphqlApis)

				return &out.GraphqlApis[len(out.GraphqlApis)-1]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appsync.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
			handler := appsync.NewHandler(b)
			client, capture := newCapturingTestAppsyncClient(t, handler)

			api := tt.run(t, client)
			require.NotNil(t, api)
			assert.NotEmpty(t, aws.ToString(api.ApiId), "control: apiId must still be present")
			assert.NotEmpty(t, aws.ToString(api.Name), "control: name must still be present")
			assert.NotEmpty(t, aws.ToString(api.Arn), "control: arn must still be present")
			assert.Equal(t, appsynctypes.AuthenticationTypeApiKey, api.AuthenticationType,
				"control: authenticationType must still be present")

			require.NotEmpty(t, capture.body)
			body := string(capture.body)
			assert.NotContains(t, body, `"region"`, "response must not carry the persisted-only region field")
			assert.NotContains(t, body, `"createdAt"`, "response must not carry the persisted-only createdAt field")
			assert.NotContains(t, body, `"updatedAt"`, "response must not carry the persisted-only updatedAt field")
			assert.Contains(t, body, `"apiId"`, "control: apiId must still appear on the wire")
			assert.Contains(t, body, `"arn"`, "control: arn must still appear on the wire")
		})
	}
}
