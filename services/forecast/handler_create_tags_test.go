package forecast_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/forecast"
)

const tagsRTRegion = "us-east-1"

// newTestForecastClient stands up the real aws-sdk-go-v2 Forecast client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. forecast's only
// creation path (InMemoryBackend.create) is unexported and reachable solely
// through the handler's JSON operation dispatch (gopherstack-23ti), so this
// full HTTP round trip is the only way to drive creation from _test.go.
func newTestForecastClient(t *testing.T, h *forecast.Handler) *forecastsdk.Client {
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

	return forecastsdk.NewFromConfig(cfg, func(o *forecastsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives representative forecast Create* ops
// through the real SDK client and asserts ListTagsForResource sees what was
// supplied at creation (gopherstack-2mwl). All 14 forecast Create* ops accept
// Tags in the real SDK (forecast@v1.44.4: api_op_CreateDatasetGroup.go:98,
// api_op_CreateAutoPredictor.go:180, and eleven more) and are routed through
// one shared, kind-generic InMemoryBackend.create (store.go) via
// forecastOperations()'s addCRUD table (handler.go) -- so unlike a
// per-resource-kind handler, a single fix point covers every kind, and
// testing a representative few kinds (one plain addCRUD entry, one
// special-cased outside addCRUD) is sufficient to prove or disprove the whole
// family at once.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *forecastsdk.Client) string
		name  string
	}{
		{
			name: "dataset group",
			setup: func(t *testing.T, client *forecastsdk.Client) string {
				t.Helper()
				out, err := client.CreateDatasetGroup(t.Context(), &forecastsdk.CreateDatasetGroupInput{
					DatasetGroupName: aws.String("tagged_dataset_group"),
					Domain:           types.DomainRetail,
					Tags:             []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DatasetGroupArn)
			},
		},
		{
			name: "auto predictor",
			setup: func(t *testing.T, client *forecastsdk.Client) string {
				t.Helper()
				out, err := client.CreateAutoPredictor(t.Context(), &forecastsdk.CreateAutoPredictorInput{
					PredictorName: aws.String("tagged_predictor"),
					Tags:          []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.PredictorArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := forecast.NewInMemoryBackend("000000000000", tagsRTRegion)
			h := forecast.NewHandler(backend)
			client := newTestForecastClient(t, h)

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			out, err := client.ListTagsForResource(t.Context(), &forecastsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceARN),
			})
			require.NoError(t, err)

			require.Len(t, out.Tags, 1)
			assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
			assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
		})
	}
}
