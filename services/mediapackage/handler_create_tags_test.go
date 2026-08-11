package mediapackage_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediapackagesdk "github.com/aws/aws-sdk-go-v2/service/mediapackage"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

const mediapackageTagsRTRegion = "us-east-1"

// newTestMediaPackageClient stands up the real aws-sdk-go-v2 MediaPackage
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestMediaPackageClient(t *testing.T, h *mediapackage.Handler) *mediapackagesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(mediapackageTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return mediapackagesdk.NewFromConfig(cfg, func(o *mediapackagesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every mediapackage Create* op whose
// real Input struct accepts Tags (mediapackage@v1.42.4: api_op_CreateChannel.go,
// api_op_CreateOriginEndpoint.go, both `Tags map[string]string`) through the
// real SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl). CreateHarvestJob takes no Tags field in the
// real SDK and is excluded.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *mediapackagesdk.Client) string
		name  string
	}{
		{
			name: "channel",
			setup: func(t *testing.T, client *mediapackagesdk.Client) string {
				t.Helper()
				out, err := client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
					Id:   aws.String("tagged-channel"),
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "origin endpoint",
			setup: func(t *testing.T, client *mediapackagesdk.Client) string {
				t.Helper()
				_, err := client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
					Id: aws.String("channel-for-endpoint"),
				})
				require.NoError(t, err)

				out, err := client.CreateOriginEndpoint(t.Context(), &mediapackagesdk.CreateOriginEndpointInput{
					ChannelId: aws.String("channel-for-endpoint"),
					Id:        aws.String("tagged-endpoint"),
					Tags:      map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediapackage.NewInMemoryBackend("000000000000", mediapackageTagsRTRegion)
			client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &mediapackagesdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got.Tags)
		})
	}
}
