package lakeformation_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// disableDataHostPrefix stops GetWorkUnitResults (real AWS routes this to a
// "data-lakeformation.<region>..." subdomain, see
// endpointPrefix_opGetWorkUnitResultsMiddleware) from prepending "data-"
// onto this test's httptest server host, which has no such DNS entry.
func disableDataHostPrefix(stack *middleware.Stack) error {
	return stack.Initialize.Add(
		middleware.InitializeMiddlewareFunc(
			"DisableDataHostPrefix",
			func(
				ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
			) (middleware.InitializeOutput, middleware.Metadata, error) {
				ctx = smithyhttp.DisableEndpointHostPrefix(ctx, true)

				return next.HandleInitialize(ctx, in)
			},
		),
		middleware.Before,
	)
}

func newTestLakeFormationClient(t *testing.T, h *lakeformation.Handler) *lakeformationsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return lakeformationsdk.NewFromConfig(cfg, func(o *lakeformationsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, disableDataHostPrefix)
	})
}

// TestGetWorkUnitResults_WorkUnitID_RoundTrip drives StartQueryPlanning ->
// GetWorkUnits -> GetWorkUnitResults through a real SDK client and proves
// WorkUnitId is actually checked, instead of being silently dropped so that
// any WorkUnitId value (even one addressing a work unit that was never
// generated) returned the same result (gopherstack-h910).
func TestGetWorkUnitResults_WorkUnitID_RoundTrip(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
	client := newTestLakeFormationClient(t, h)

	planned, err := client.StartQueryPlanning(t.Context(), &lakeformationsdk.StartQueryPlanningInput{
		QueryString: aws.String("SELECT * FROM t"),
		QueryPlanningContext: &types.QueryPlanningContext{
			DatabaseName: aws.String("db1"),
		},
	})
	require.NoError(t, err)
	queryID := aws.ToString(planned.QueryId)

	units, err := client.GetWorkUnits(t.Context(), &lakeformationsdk.GetWorkUnitsInput{
		QueryId: aws.String(queryID),
	})
	require.NoError(t, err)
	require.Len(t, units.WorkUnitRanges, 1)
	workUnitToken := aws.ToString(units.WorkUnitRanges[0].WorkUnitToken)

	t.Run("unknown_work_unit_id_errors", func(t *testing.T) {
		t.Parallel()

		_, invalidErr := client.GetWorkUnitResults(t.Context(), &lakeformationsdk.GetWorkUnitResultsInput{
			QueryId:       aws.String(queryID),
			WorkUnitId:    999,
			WorkUnitToken: aws.String(workUnitToken),
		})
		require.Error(t, invalidErr)
		assert.Contains(t, invalidErr.Error(), "InvalidInputException")
	})

	t.Run("the_one_generated_work_unit_id_succeeds", func(t *testing.T) {
		t.Parallel()

		out, validErr := client.GetWorkUnitResults(t.Context(), &lakeformationsdk.GetWorkUnitResultsInput{
			QueryId:       aws.String(queryID),
			WorkUnitId:    0,
			WorkUnitToken: aws.String(workUnitToken),
		})
		require.NoError(t, validErr)
		require.NotNil(t, out.ResultStream)
		defer out.ResultStream.Close()
	})
}
