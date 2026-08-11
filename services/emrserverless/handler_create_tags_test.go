package emrserverless_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

const tagsRTRegion = "us-east-1"

// newTestEMRServerlessClient stands up the real aws-sdk-go-v2 emrserverless
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestEMRServerlessClient(t *testing.T, h *emrserverless.Handler) *emrserverlesssdk.Client {
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

	return emrserverlesssdk.NewFromConfig(cfg, func(o *emrserverlesssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every emrserverless op whose real
// Input struct accepts Tags (emrserverless@v1.44.4: api_op_CreateApplication.go,
// api_op_StartJobRun.go, api_op_StartSession.go) through the real SDK client
// and asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *emrserverlesssdk.Client) string
		name  string
	}{
		{
			name: "application",
			setup: func(t *testing.T, client *emrserverlesssdk.Client) string {
				t.Helper()

				out, err := client.CreateApplication(t.Context(), &emrserverlesssdk.CreateApplicationInput{
					Name:         aws.String("tagged-app"),
					ReleaseLabel: aws.String("emr-6.6.0"),
					Type:         aws.String("SPARK"),
					Tags:         map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "job run",
			setup: func(t *testing.T, client *emrserverlesssdk.Client) string {
				t.Helper()

				app, err := client.CreateApplication(t.Context(), &emrserverlesssdk.CreateApplicationInput{
					Name:         aws.String("jr-host-app"),
					ReleaseLabel: aws.String("emr-6.6.0"),
					Type:         aws.String("SPARK"),
				})
				require.NoError(t, err)

				out, err := client.StartJobRun(t.Context(), &emrserverlesssdk.StartJobRunInput{
					ApplicationId:    app.ApplicationId,
					ClientToken:      aws.String("jr-token-1"),
					ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
					Tags:             map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "session",
			setup: func(t *testing.T, client *emrserverlesssdk.Client) string {
				t.Helper()

				app, err := client.CreateApplication(t.Context(), &emrserverlesssdk.CreateApplicationInput{
					Name:         aws.String("sess-host-app"),
					ReleaseLabel: aws.String("emr-6.6.0"),
					Type:         aws.String("SPARK"),
				})
				require.NoError(t, err)

				_, err = client.StartApplication(t.Context(), &emrserverlesssdk.StartApplicationInput{
					ApplicationId: app.ApplicationId,
				})
				require.NoError(t, err)

				out, err := client.StartSession(t.Context(), &emrserverlesssdk.StartSessionInput{
					ApplicationId:    app.ApplicationId,
					ClientToken:      aws.String("sess-token-1"),
					ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
					Tags:             map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := emrserverless.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestEMRServerlessClient(t, emrserverless.NewHandler(backend))

			resourceArn := tt.setup(t, client)
			require.NotEmpty(t, resourceArn)

			got, err := client.ListTagsForResource(t.Context(), &emrserverlesssdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceArn),
			})
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "test", got.Tags["env"])
		})
	}
}
