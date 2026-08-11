package appsync_test

import (
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

const tagsRTRegion = "us-east-1"

// newTestAppsyncClient stands up the real aws-sdk-go-v2 appsync client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestAppsyncClient(t *testing.T, h *appsync.Handler) *appsyncsdk.Client {
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

	return appsyncsdk.NewFromConfig(cfg, func(o *appsyncsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives CreateGraphqlApi and CreateApi (the
// only two appsync Create ops whose real ARN is a valid TagResource target --
// appsync@v1.56.4's TagResource/UntagResource/ListTagsForResource all document
// ResourceArn as "The GraphqlApi Amazon Resource Name (ARN)"
// (api_op_TagResource.go:29, api_op_UntagResource.go:29,
// api_op_ListTagsForResource.go:29), and gopherstack's handler_tags.go already
// documents that GraphqlApi (v1) and Api (v2 Event API) share that ARN space)
// through the real SDK client and asserts ListTagsForResource sees what was
// supplied at creation (gopherstack-2mwl).
//
// CreateDataSource takes no Tags in the real SDK at all (no bug). CreateDomainName
// and CreateChannelNamespace DO accept Tags (api_op_CreateDomainName.go:46,
// api_op_CreateChannelNamespace.go:57) and gopherstack correctly stores and
// returns them on the resource's own struct (domain_names.go:49,
// channel_namespaces.go:37) -- verified by reading types.DomainNameConfig and
// types.ChannelNamespace, which both carry a Tags field in their own Describe
// response, not by ListTagsForResource. Neither resource kind is a documented
// TagResource target, so they are correctly excluded from cross-resource tag
// dispatch (handler_tags.go's apiIDFromResourceARN deliberately truncates any
// deeper ARN path to just the api ID).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *appsyncsdk.Client) string
		name  string
	}{
		{
			name: "graphql api",
			setup: func(t *testing.T, client *appsyncsdk.Client) string {
				t.Helper()

				out, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
					Name:               aws.String("tagged-graphql-api"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
					Tags:               map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.GraphqlApi.Arn)
			},
		},
		{
			name: "event api",
			setup: func(t *testing.T, client *appsyncsdk.Client) string {
				t.Helper()

				out, err := client.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
					Name: aws.String("tagged-event-api"),
					EventConfig: &appsynctypes.EventConfig{
						AuthProviders: []appsynctypes.AuthProvider{
							{AuthType: appsynctypes.AuthenticationTypeApiKey},
						},
						ConnectionAuthModes: []appsynctypes.AuthMode{
							{AuthType: appsynctypes.AuthenticationTypeApiKey},
						},
						DefaultPublishAuthModes: []appsynctypes.AuthMode{
							{AuthType: appsynctypes.AuthenticationTypeApiKey},
						},
						DefaultSubscribeAuthModes: []appsynctypes.AuthMode{
							{AuthType: appsynctypes.AuthenticationTypeApiKey},
						},
					},
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Api.ApiArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
			client := newTestAppsyncClient(t, appsync.NewHandler(backend))

			arn := tt.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(
				t.Context(),
				&appsyncsdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test"}, got.Tags)
		})
	}
}

// TestDomainNameAndChannelNamespace_OwnTagPath verifies CreateDomainName and
// CreateChannelNamespace store the Tags they accept on the resource's own
// struct, surfaced through GetDomainName/GetChannelNamespace -- their real
// read path, since neither is a documented TagResource target (see the
// package doc above).
func TestDomainNameAndChannelNamespace_OwnTagPath(t *testing.T) {
	t.Parallel()

	t.Run("domain name", func(t *testing.T) {
		t.Parallel()

		backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
		client := newTestAppsyncClient(t, appsync.NewHandler(backend))

		_, err := client.CreateDomainName(t.Context(), &appsyncsdk.CreateDomainNameInput{
			DomainName:     aws.String("tagged.example.com"),
			CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/test"),
			Tags:           map[string]string{"env": "test"},
		})
		require.NoError(t, err)

		got, err := client.GetDomainName(t.Context(), &appsyncsdk.GetDomainNameInput{
			DomainName: aws.String("tagged.example.com"),
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"env": "test"}, got.DomainNameConfig.Tags)
	})

	t.Run("channel namespace", func(t *testing.T) {
		t.Parallel()

		backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
		client := newTestAppsyncClient(t, appsync.NewHandler(backend))

		api, err := client.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
			Name: aws.String("ns-host-api"),
			EventConfig: &appsynctypes.EventConfig{
				AuthProviders: []appsynctypes.AuthProvider{
					{AuthType: appsynctypes.AuthenticationTypeApiKey},
				},
				ConnectionAuthModes: []appsynctypes.AuthMode{
					{AuthType: appsynctypes.AuthenticationTypeApiKey},
				},
				DefaultPublishAuthModes: []appsynctypes.AuthMode{
					{AuthType: appsynctypes.AuthenticationTypeApiKey},
				},
				DefaultSubscribeAuthModes: []appsynctypes.AuthMode{
					{AuthType: appsynctypes.AuthenticationTypeApiKey},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateChannelNamespace(t.Context(), &appsyncsdk.CreateChannelNamespaceInput{
			ApiId: api.Api.ApiId,
			Name:  aws.String("tagged-ns"),
			Tags:  map[string]string{"env": "test"},
		})
		require.NoError(t, err)

		got, err := client.GetChannelNamespace(t.Context(), &appsyncsdk.GetChannelNamespaceInput{
			ApiId: api.Api.ApiId,
			Name:  aws.String("tagged-ns"),
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"env": "test"}, got.ChannelNamespace.Tags)
	})
}
