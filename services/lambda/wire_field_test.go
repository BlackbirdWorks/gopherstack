package lambda_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

const lambdaWireTestRegion = "us-east-1"

func newWireTestLambdaClient(t *testing.T, h *lambda.Handler) *lambdasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(lambdaWireTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return lambdasdk.NewFromConfig(cfg, func(o *lambdasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func TestLambdaListOps_NarrowSummaryParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		test func(t *testing.T, client *lambdasdk.Client)
		name string
	}{
		{
			name: "list_layers_summary_parity",
			test: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()
				ctx := t.Context()

				pubOut, err := client.PublishLayerVersion(ctx, &lambdasdk.PublishLayerVersionInput{
					LayerName:   aws.String("my-narrow-layer"),
					Description: aws.String("layer for wire test"),
					Content: &lambdatypes.LayerVersionContentInput{
						ZipFile: []byte(
							"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
						),
					},
					CompatibleRuntimes: []lambdatypes.Runtime{
						lambdatypes.RuntimeNodejs18x,
					},
				})
				require.NoError(t, err)
				require.NotNil(t, pubOut)

				listOut, err := client.ListLayers(ctx, &lambdasdk.ListLayersInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listOut.Layers)

				var found *lambdatypes.LayersListItem
				for i := range listOut.Layers {
					if aws.ToString(listOut.Layers[i].LayerName) == "my-narrow-layer" {
						found = &listOut.Layers[i]

						break
					}
				}
				require.NotNil(t, found)
				assert.Equal(t, "my-narrow-layer", aws.ToString(found.LayerName))
				require.NotNil(t, found.LatestMatchingVersion)
				assert.Equal(t, int64(1), found.LatestMatchingVersion.Version)
				assert.Equal(t, "layer for wire test", aws.ToString(found.LatestMatchingVersion.Description))
			},
		},
		{
			name: "list_layer_versions_summary_parity",
			test: func(t *testing.T, client *lambdasdk.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.PublishLayerVersion(ctx, &lambdasdk.PublishLayerVersionInput{
					LayerName:   aws.String("my-versioned-layer"),
					Description: aws.String("version 1"),
					Content: &lambdatypes.LayerVersionContentInput{
						ZipFile: []byte(
							"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
						),
					},
				})
				require.NoError(t, err)

				_, err = client.PublishLayerVersion(ctx, &lambdasdk.PublishLayerVersionInput{
					LayerName:   aws.String("my-versioned-layer"),
					Description: aws.String("version 2"),
					Content: &lambdatypes.LayerVersionContentInput{
						ZipFile: []byte(
							"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
						),
					},
				})
				require.NoError(t, err)

				listOut, err := client.ListLayerVersions(ctx, &lambdasdk.ListLayerVersionsInput{
					LayerName: aws.String("my-versioned-layer"),
				})
				require.NoError(t, err)
				require.Len(t, listOut.LayerVersions, 2)
				assert.Equal(t, int64(2), listOut.LayerVersions[0].Version)
				assert.Equal(t, "version 2", aws.ToString(listOut.LayerVersions[0].Description))
				assert.Equal(t, int64(1), listOut.LayerVersions[1].Version)
				assert.Equal(t, "version 1", aws.ToString(listOut.LayerVersions[1].Description))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newWireTestLambdaClient(t, h)
			tt.test(t, client)
		})
	}
}
