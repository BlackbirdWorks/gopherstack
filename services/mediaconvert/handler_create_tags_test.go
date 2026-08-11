package mediaconvert_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// newTestMediaConvertClient stands up the real aws-sdk-go-v2 MediaConvert
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestMediaConvertClient(t *testing.T, h *mediaconvert.Handler) *mediaconvertsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return mediaconvertsdk.NewFromConfig(cfg, func(o *mediaconvertsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every mediaconvert Create* op whose
// real Input struct accepts Tags (mediaconvert@v1.97.1: api_op_CreateJob.go,
// api_op_CreateJobTemplate.go, api_op_CreatePreset.go, api_op_CreateQueue.go,
// all `Tags map[string]string`) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
// CreateResourceShare takes no Tags field in the real SDK and is excluded.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *mediaconvertsdk.Client) string
		name  string
	}{
		{
			name: "queue",
			setup: func(t *testing.T, client *mediaconvertsdk.Client) string {
				t.Helper()
				out, err := client.CreateQueue(t.Context(), &mediaconvertsdk.CreateQueueInput{
					Name: aws.String("tagged-queue"),
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Queue.Arn)
			},
		},
		{
			name: "preset",
			setup: func(t *testing.T, client *mediaconvertsdk.Client) string {
				t.Helper()
				out, err := client.CreatePreset(t.Context(), &mediaconvertsdk.CreatePresetInput{
					Name:     aws.String("tagged-preset"),
					Settings: &types.PresetSettings{},
					Tags:     map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Preset.Arn)
			},
		},
		{
			name: "job template",
			setup: func(t *testing.T, client *mediaconvertsdk.Client) string {
				t.Helper()
				out, err := client.CreateJobTemplate(t.Context(), &mediaconvertsdk.CreateJobTemplateInput{
					Name:     aws.String("tagged-jobtemplate"),
					Settings: &types.JobTemplateSettings{},
					Tags:     map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.JobTemplate.Arn)
			},
		},
		{
			name: "job",
			setup: func(t *testing.T, client *mediaconvertsdk.Client) string {
				t.Helper()
				out, err := client.CreateJob(t.Context(), &mediaconvertsdk.CreateJobInput{
					Role:     aws.String("arn:aws:iam::" + testAccountID + ":role/MediaConvertRole"),
					Settings: &types.JobSettings{},
					Tags:     map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Job.Arn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
			client := newTestMediaConvertClient(t, mediaconvert.NewHandler(backend))

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &mediaconvertsdk.ListTagsForResourceInput{
				Arn: aws.String(arn),
			})
			require.NoError(t, err)
			require.NotNil(t, got.ResourceTags)
			assert.Equal(t, map[string]string{"env": "prod"}, got.ResourceTags.Tags)
		})
	}
}
