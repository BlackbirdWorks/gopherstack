package amplify_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/amplify"
)

const tagsRTRegion = "us-east-1"

// newTestAmplifyClient stands up the real aws-sdk-go-v2 amplify client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestAmplifyClient(t *testing.T, h *amplify.Handler) *amplifysdk.Client {
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

	return amplifysdk.NewFromConfig(cfg, func(o *amplifysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every amplify Create op whose real
// Input struct accepts Tags (amplify@v1.41.4: api_op_CreateApp.go:148,
// api_op_CreateBranch.go:121 -- CreateDomainAssociation and CreateWebhook take
// no Tags in the pinned SDK) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *amplifysdk.Client) string
		name  string
	}{
		{
			name: "app",
			setup: func(t *testing.T, client *amplifysdk.Client) string {
				t.Helper()

				out, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{
					Name: aws.String("tagged-app"),
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.App.AppArn)
			},
		},
		{
			name: "branch",
			setup: func(t *testing.T, client *amplifysdk.Client) string {
				t.Helper()

				app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{
					Name: aws.String("branch-host-app"),
				})
				require.NoError(t, err)

				out, err := client.CreateBranch(t.Context(), &amplifysdk.CreateBranchInput{
					AppId:      app.App.AppId,
					BranchName: aws.String("main"),
					Tags:       map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Branch.BranchArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))

			arn := tt.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(
				t.Context(),
				&amplifysdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test"}, got.Tags)
		})
	}
}
