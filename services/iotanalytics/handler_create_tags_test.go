package iotanalytics_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics"
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

const tagsRTRegion = "us-east-1"

// newTestIoTAnalyticsClient stands up the real aws-sdk-go-v2 iotanalytics
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestIoTAnalyticsClient(t *testing.T, h *iotanalytics.Handler) *iotanalyticssdk.Client {
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

	return iotanalyticssdk.NewFromConfig(cfg, func(o *iotanalyticssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every iotanalytics Create op (all
// four resource kinds this deprecated service exposes) whose real Input
// struct accepts Tags (iotanalytics@v1.32.0: api_op_CreateChannel.go:58,
// api_op_CreateDataset.go, api_op_CreateDatastore.go:75,
// api_op_CreatePipeline.go:64) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
//
// gopherstack stores each Create's tags in two places: the resource's own
// cached .Tags field (used only internally -- verified against the real SDK
// that Describe* responses carry no Tags field at all, e.g. types.Channel has
// none) and the shared arn-keyed b.tags map ListTagsForResource actually
// reads. Both writes happen atomically under the same lock at creation
// (channels.go:76-79 and siblings), so there is no reachable divergence for a
// real client despite the duplication.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *iotanalyticssdk.Client) string
		name  string
	}{
		{
			name: "channel",
			setup: func(t *testing.T, client *iotanalyticssdk.Client) string {
				t.Helper()

				out, err := client.CreateChannel(t.Context(), &iotanalyticssdk.CreateChannelInput{
					ChannelName: aws.String("tagged_channel"),
					Tags:        []iotanalyticstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ChannelArn)
			},
		},
		{
			name: "datastore",
			setup: func(t *testing.T, client *iotanalyticssdk.Client) string {
				t.Helper()

				out, err := client.CreateDatastore(t.Context(), &iotanalyticssdk.CreateDatastoreInput{
					DatastoreName: aws.String("tagged_datastore"),
					Tags:          []iotanalyticstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DatastoreArn)
			},
		},
		{
			name: "dataset",
			setup: func(t *testing.T, client *iotanalyticssdk.Client) string {
				t.Helper()

				out, err := client.CreateDataset(t.Context(), &iotanalyticssdk.CreateDatasetInput{
					DatasetName: aws.String("tagged_dataset"),
					Actions: []iotanalyticstypes.DatasetAction{
						{
							ActionName: aws.String("action1"),
							QueryAction: &iotanalyticstypes.SqlQueryDatasetAction{
								SqlQuery: aws.String("SELECT * FROM tagged_datastore"),
							},
						},
					},
					Tags: []iotanalyticstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.DatasetArn)
			},
		},
		{
			name: "pipeline",
			setup: func(t *testing.T, client *iotanalyticssdk.Client) string {
				t.Helper()

				_, err := client.CreateChannel(t.Context(), &iotanalyticssdk.CreateChannelInput{
					ChannelName: aws.String("pipeline_host_channel"),
				})
				require.NoError(t, err)

				_, err = client.CreateDatastore(t.Context(), &iotanalyticssdk.CreateDatastoreInput{
					DatastoreName: aws.String("pipeline_host_datastore"),
				})
				require.NoError(t, err)

				out, err := client.CreatePipeline(t.Context(), &iotanalyticssdk.CreatePipelineInput{
					PipelineName: aws.String("tagged_pipeline"),
					PipelineActivities: []iotanalyticstypes.PipelineActivity{
						{Channel: &iotanalyticstypes.ChannelActivity{
							Name:        aws.String("channelActivity"),
							ChannelName: aws.String("pipeline_host_channel"),
							Next:        aws.String("datastoreActivity"),
						}},
						{Datastore: &iotanalyticstypes.DatastoreActivity{
							Name:          aws.String("datastoreActivity"),
							DatastoreName: aws.String("pipeline_host_datastore"),
						}},
					},
					Tags: []iotanalyticstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.PipelineArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := iotanalytics.NewInMemoryBackend()
			client := newTestIoTAnalyticsClient(t, iotanalytics.NewHandler(backend))

			arn := tt.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(
				t.Context(),
				&iotanalyticssdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}
