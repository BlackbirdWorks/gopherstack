package emrserverless_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrserverlesstypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

const (
	emrRTRegion    = "us-east-1"
	emrRTAccountID = "000000000000"
)

// newTestEMRServerlessSDKClient stands up the real aws-sdk-go-v2
// emrserverless client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production -- so a fix is verified by the real client's own deserializer,
// not gopherstack's own JSON tags.
func newTestEMRServerlessSDKClient(t *testing.T, h *emrserverless.Handler) *emrserverlesssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(emrRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return emrserverlesssdk.NewFromConfig(cfg, func(o *emrserverlesssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListJobRunAttempts_Mode_SDKRoundTrip proves the synthesized job run
// attempt carries Mode through the real SDK client. Confirmed against
// aws-sdk-go-v2/service/emrserverless@v1.44.4's
// awsRestjson1_deserializeDocumentJobRunAttemptSummary (deserializers.go),
// which recognises a "mode" key that jobRunAttemptToMap (handler.go) never
// emitted -- a real typed client would see a zero-value "" Mode instead of
// the job run's actual "BATCH"/"STREAMING" mode.
func TestListJobRunAttempts_Mode_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
	client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
	ctx := t.Context()

	createOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
		Name:         aws.String("attempt-mode-app"),
		ReleaseLabel: aws.String("emr-6.6.0"),
		Type:         aws.String("SPARK"),
	})
	require.NoError(t, err)

	jobOut, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
		ApplicationId:    createOut.ApplicationId,
		ExecutionRoleArn: aws.String("arn:aws:iam::" + emrRTAccountID + ":role/test-role"),
		Mode:             emrserverlesstypes.JobRunModeStreaming,
	})
	require.NoError(t, err)

	attemptsOut, err := client.ListJobRunAttempts(ctx, &emrserverlesssdk.ListJobRunAttemptsInput{
		ApplicationId: createOut.ApplicationId,
		JobRunId:      jobOut.JobRunId,
	})
	require.NoError(t, err)
	require.Len(t, attemptsOut.JobRunAttempts, 1)
	require.Equal(
		t,
		emrserverlesstypes.JobRunModeStreaming,
		attemptsOut.JobRunAttempts[0].Mode,
		"a real client must see the job run's actual mode, not a zero value",
	)
}
