package codestarconnections_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codestarconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	codestarconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

const tagsRTRegion = "us-east-1"

// newTestCodeStarConnectionsClient stands up the real aws-sdk-go-v2
// codestarconnections client against an httptest server running this
// package's Handler, wired through the same pkgs/service registry/router
// used in production.
func newTestCodeStarConnectionsClient(t *testing.T, h *codestarconnections.Handler) *codestarconnectionssdk.Client {
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

	return codestarconnectionssdk.NewFromConfig(cfg, func(o *codestarconnectionssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every codestarconnections Create op
// whose real Input struct accepts Tags (codestarconnections@v1.38.4:
// api_op_CreateConnection.go, api_op_CreateHost.go,
// api_op_CreateRepositoryLink.go) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *codestarconnectionssdk.Client) string
		name  string
	}{
		{
			name: "connection",
			setup: func(t *testing.T, client *codestarconnectionssdk.Client) string {
				t.Helper()

				out, err := client.CreateConnection(t.Context(), &codestarconnectionssdk.CreateConnectionInput{
					ConnectionName: aws.String("tagged-connection"),
					ProviderType:   codestarconnectionstypes.ProviderTypeGithub,
					Tags:           []codestarconnectionstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ConnectionArn)
			},
		},
		{
			name: "host",
			setup: func(t *testing.T, client *codestarconnectionssdk.Client) string {
				t.Helper()

				out, err := client.CreateHost(t.Context(), &codestarconnectionssdk.CreateHostInput{
					Name:             aws.String("tagged-host"),
					ProviderEndpoint: aws.String("https://ghe.example.com"),
					ProviderType:     codestarconnectionstypes.ProviderTypeGithubEnterpriseServer,
					Tags: []codestarconnectionstypes.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)

				return aws.ToString(out.HostArn)
			},
		},
		{
			name: "repository link",
			setup: func(t *testing.T, client *codestarconnectionssdk.Client) string {
				t.Helper()

				conn, err := client.CreateConnection(t.Context(), &codestarconnectionssdk.CreateConnectionInput{
					ConnectionName: aws.String("link-host-connection"),
					ProviderType:   codestarconnectionstypes.ProviderTypeGithub,
				})
				require.NoError(t, err)

				out, err := client.CreateRepositoryLink(t.Context(), &codestarconnectionssdk.CreateRepositoryLinkInput{
					ConnectionArn:  conn.ConnectionArn,
					OwnerId:        aws.String("some-owner"),
					RepositoryName: aws.String("some-repo"),
					Tags:           []codestarconnectionstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.RepositoryLinkInfo.RepositoryLinkArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := codestarconnections.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestCodeStarConnectionsClient(t, codestarconnections.NewHandler(backend))

			resourceArn := tt.setup(t, client)
			require.NotEmpty(t, resourceArn)

			got, err := client.ListTagsForResource(t.Context(), &codestarconnectionssdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceArn),
			})
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}
