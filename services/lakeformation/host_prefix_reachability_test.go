package lakeformation_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// gopherstack-3gbe: Lake Formation's query-planning family carries the same
// client-side host-prefix rewrite Omics has (gopherstack-keee). Five ops,
// two literal prefixes -- "query-" (StartQueryPlanning, GetQueryState,
// GetQueryStatistics, GetWorkUnits) and "data-" (GetWorkUnitResults) --
// confirmed by grepping lakeformation@v1.50.4's api_op_*.go for
// `req.URL.Host = "..." + req.URL.Host`, matching gopherstack-3gbe's filing
// exactly.
//
// Handler.RouteMatcher (handler.go:193) matches on URL.Path alone, gated on
// the SigV4 service name (services/_ROUTE_COLLISIONS.md already lists
// lakeformation as SigV4-scoped and confirmed clean), and ExtractOperation
// (handler.go:208) is just the path with its leading slash stripped -- the
// prefix only ever touches Host, never Path, so it can't create a
// route-table collision. Same conclusion as Omics: no gopherstack
// routing/auth code needs to change, the gap is a pure client-side DNS/dial
// failure.
//
// Unlike mwaa, lakeformation's test suite already has a real-SDK-client
// round trip for this family (handler_work_unit_results_sdk_test.go's
// TestGetWorkUnitResults_WorkUnitID_RoundTrip) -- but it works around this
// exact problem with disableDataHostPrefix, applied to every operation the
// client issues (StartQueryPlanning and GetWorkUnits included, both
// "query-", not just GetWorkUnitResults's "data-"). That test is not
// exercising a real, unmodified client: swap
// smithyhttp.DisableEndpointHostPrefix out and it fails to dial exactly like
// mwaa/omics did. This test drives the same op sequence with a
// redial-to-the-real-listener transport instead, leaving the SDK's real,
// un-disabled rewrite intact on the wire for both prefixes.
func dialToRealAddr(realAddr string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, network, realAddr)
			},
		},
	}
}

func newLakeFormationHostPrefixTestClient(t *testing.T, redialFix bool) *lakeformationsdk.Client {
	t.Helper()

	backend := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(backend)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfgOpts := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	}
	if redialFix {
		cfgOpts = append(cfgOpts, awscfg.WithHTTPClient(dialToRealAddr(srv.Listener.Addr().String())))
	}

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), cfgOpts...)
	require.NoError(t, err)

	return lakeformationsdk.NewFromConfig(cfg, func(o *lakeformationsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix drives an unmodified SDK
// client through one op per prefix family and proves it can't dial.
func TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		probe  func(ctx context.Context, client *lakeformationsdk.Client) error
		name   string
		prefix string
	}{
		{
			name:   "query",
			prefix: "query-",
			probe: func(ctx context.Context, client *lakeformationsdk.Client) error {
				_, err := client.StartQueryPlanning(ctx, &lakeformationsdk.StartQueryPlanningInput{
					QueryString: aws.String("SELECT * FROM t"),
					QueryPlanningContext: &types.QueryPlanningContext{
						DatabaseName: aws.String("unreachable-probe"),
					},
				})

				return err
			},
		},
		{
			name:   "data",
			prefix: "data-",
			probe: func(ctx context.Context, client *lakeformationsdk.Client) error {
				_, err := client.GetWorkUnitResults(ctx, &lakeformationsdk.GetWorkUnitResultsInput{
					QueryId:       aws.String("unreachable-probe"),
					WorkUnitId:    0,
					WorkUnitToken: aws.String("unreachable-probe"),
				})

				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newLakeFormationHostPrefixTestClient(t, false)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := tc.probe(ctx, client)
			require.Error(t, err, "prefix=%s: expected the unmodified client to fail to dial the rewritten host",
				tc.prefix)
			t.Logf("prefix=%s unmodified-client error (expected): %v", tc.prefix, err)
		})
	}
}

// TestSDKRoundTrip_HostPrefix_Reachable_AfterFix drives StartQueryPlanning
// ("query-") -> GetWorkUnits ("query-") -> GetWorkUnitResults ("data-")
// through the real SDK client with a redial-to-the-real-listener transport,
// proving gopherstack survives the real, un-disabled rewrite on both
// prefixes and the full query-planning round trip succeeds.
func TestSDKRoundTrip_HostPrefix_Reachable_AfterFix(t *testing.T) {
	t.Parallel()

	client := newLakeFormationHostPrefixTestClient(t, true)

	planned, err := client.StartQueryPlanning(t.Context(), &lakeformationsdk.StartQueryPlanningInput{
		QueryString: aws.String("SELECT * FROM t"),
		QueryPlanningContext: &types.QueryPlanningContext{
			DatabaseName: aws.String("db1"),
		},
	})
	require.NoError(t, err)
	queryID := aws.ToString(planned.QueryId)

	stats, err := client.GetQueryStatistics(t.Context(), &lakeformationsdk.GetQueryStatisticsInput{
		QueryId: aws.String(queryID),
	})
	require.NoError(t, err)
	require.NotNil(t, stats.ExecutionStatistics)

	units, err := client.GetWorkUnits(t.Context(), &lakeformationsdk.GetWorkUnitsInput{
		QueryId: aws.String(queryID),
	})
	require.NoError(t, err)
	require.Len(t, units.WorkUnitRanges, 1)

	out, err := client.GetWorkUnitResults(t.Context(), &lakeformationsdk.GetWorkUnitResultsInput{
		QueryId:       aws.String(queryID),
		WorkUnitId:    0,
		WorkUnitToken: units.WorkUnitRanges[0].WorkUnitToken,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ResultStream)
	defer out.ResultStream.Close()
}
