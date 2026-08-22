package detective_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	detectivetypes "github.com/aws/aws-sdk-go-v2/service/detective/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/detective"
)

const detectiveRTRegion = "us-east-1"

// newTestDetectiveSDKClient stands up the real aws-sdk-go-v2 detective client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- so a fix is
// verified by the real client's own deserializer, not gopherstack's own JSON
// tags.
func newTestDetectiveSDKClient(t *testing.T, h *detective.Handler) *detectivesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(detectiveRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return detectivesdk.NewFromConfig(cfg, func(o *detectivesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListIndicators_TTPsObservedDetail_Technique_SDKRoundTrip proves the
// real detective SDK client sees TTPsObservedDetail.Technique populated.
// aws-sdk-go-v2/service/detective@v1.41.4's types.TTPsObservedDetail has a
// real (non-deprecated) Technique member, deserialized from the "Technique"
// wire key (deserializers.go's
// awsRestjson1_deserializeDocumentTTPsObservedDetail) alongside Tactic and
// Procedure -- gopherstack populated Tactic/Procedure/APIName but never
// Technique, so a real client always saw a nil Technique on every
// TTP_OBSERVED indicator. This test pins the fix using the typed SDK field,
// not a raw-body check, since the real type has a field to observe.
func TestListIndicators_TTPsObservedDetail_Technique_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := detective.NewInMemoryBackend("000000000000", detectiveRTRegion)
	client := newTestDetectiveSDKClient(t, detective.NewHandler(backend))
	ctx := t.Context()

	graphOut, err := client.CreateGraph(ctx, &detectivesdk.CreateGraphInput{})
	require.NoError(t, err)

	startOut, err := client.StartInvestigation(ctx, &detectivesdk.StartInvestigationInput{
		GraphArn:       graphOut.GraphArn,
		EntityArn:      aws.String("arn:aws:iam::000000000000:user/testuser"),
		ScopeStartTime: aws.Time(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
		ScopeEndTime:   aws.Time(time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)),
	})
	require.NoError(t, err)

	listOut, err := client.ListIndicators(ctx, &detectivesdk.ListIndicatorsInput{
		GraphArn:        graphOut.GraphArn,
		InvestigationId: startOut.InvestigationId,
		IndicatorType:   detectivetypes.IndicatorTypeTtpObserved,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.Indicators)

	for _, ind := range listOut.Indicators {
		ttp := ind.IndicatorDetail.TTPsObservedDetail
		require.NotNil(t, ttp, "TTP_OBSERVED indicator must carry TTPsObservedDetail")
		require.NotNil(t, ttp.Technique, "real client must see a Technique value")
		require.NotEmpty(t, *ttp.Technique)
	}
}
