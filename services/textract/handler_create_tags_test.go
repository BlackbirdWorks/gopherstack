package textract_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	textracttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

const (
	tagsRTRegion    = "us-east-1"
	tagsRTAccountID = "000000000000"
)

// newTestTextractClient stands up the real aws-sdk-go-v2 textract client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestTextractClient(t *testing.T, h *textract.Handler) *textractsdk.Client {
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

	return textractsdk.NewFromConfig(cfg, func(o *textractsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every textract Create op whose real
// Input struct accepts Tags (textract@v1.43.4: api_op_CreateAdapter.go:59,
// api_op_CreateAdapterVersion.go:83) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
//
// Neither CreateAdapter nor CreateAdapterVersion return an ARN (only
// AdapterId/AdapterVersion, matching the real SDK's Output structs), so the
// resource ARN is constructed the same way gopherstack's own
// adapters.go/adapter_versions.go do: arn:aws:textract:{region}:{account}:adapter/{id}
// and .../version/{version}.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *textractsdk.Client) string
		name  string
	}{
		{
			name: "adapter",
			setup: func(t *testing.T, client *textractsdk.Client) string {
				t.Helper()

				out, err := client.CreateAdapter(t.Context(), &textractsdk.CreateAdapterInput{
					AdapterName:  aws.String("tagged-adapter"),
					FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeQueries},
					Tags:         map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return "arn:aws:textract:" + tagsRTRegion + ":" + tagsRTAccountID + ":adapter/" + aws.ToString(
					out.AdapterId,
				)
			},
		},
		{
			name: "adapter version",
			setup: func(t *testing.T, client *textractsdk.Client) string {
				t.Helper()

				adapter, err := client.CreateAdapter(t.Context(), &textractsdk.CreateAdapterInput{
					AdapterName:  aws.String("version-host-adapter"),
					FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeQueries},
				})
				require.NoError(t, err)

				out, err := client.CreateAdapterVersion(
					t.Context(),
					&textractsdk.CreateAdapterVersionInput{
						AdapterId: adapter.AdapterId,
						DatasetConfig: &textracttypes.AdapterVersionDatasetConfig{
							ManifestS3Object: &textracttypes.S3Object{
								Bucket: aws.String("manifests"),
								Name:   aws.String("m.json"),
							},
						},
						OutputConfig: &textracttypes.OutputConfig{S3Bucket: aws.String("out")},
						Tags:         map[string]string{"env": "test"},
					},
				)
				require.NoError(t, err)

				return "arn:aws:textract:" + tagsRTRegion + ":" + tagsRTAccountID + ":adapter/" +
					aws.ToString(adapter.AdapterId) + "/version/" + aws.ToString(out.AdapterVersion)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := textract.NewInMemoryBackend(tagsRTAccountID, tagsRTRegion)
			client := newTestTextractClient(t, textract.NewHandler(backend))

			arn := tt.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(
				t.Context(),
				&textractsdk.ListTagsForResourceInput{ResourceARN: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "test"}, got.Tags)
		})
	}
}
