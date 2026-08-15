package mwaa_test

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
	mwaasdk "github.com/aws/aws-sdk-go-v2/service/mwaa"
	"github.com/aws/aws-sdk-go-v2/service/mwaa/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// gopherstack-3gbe: every one of MWAA's real operations except
// InvokeRestApi/CreateCliToken/CreateWebLoginToken/PublishMetrics -- which is
// to say all 12, nearly the entire surface -- carries a client-side
// host-prefix rewrite from a per-operation Smithy Finalize middleware (e.g.
// mwaa@v1.43.4 api_op_ListEnvironments.go:219's
// endpointPrefix_opListEnvironmentsMiddleware). Three literal prefixes, using
// "." rather than "-": "api." (8 ops: CreateEnvironment, GetEnvironment,
// DeleteEnvironment, UpdateEnvironment, ListEnvironments, TagResource,
// UntagResource, ListTagsForResource), "env." (3: CreateCliToken,
// CreateWebLoginToken, InvokeRestApi), "ops." (1: PublishMetrics) --
// confirmed by grepping every api_op_*.go for `req.URL.Host = "..." +
// req.URL.Host`, matching gopherstack-3gbe's filing exactly.
//
// Handler.RouteMatcher (handler.go:82) matches on URL.Path alone (gated on
// the SigV4 service name "airflow", not Host) and ExtractOperation
// (handler.go:108) resolves the operation from path+method; every one of
// these 12 ops already has a distinct path/method pair by construction
// (services/_ROUTE_COLLISIONS.md's "hand-read this pass, confirmed clean"
// list already covers mwaa as SigV4-scoped). Same conclusion as Omics: no
// gopherstack routing/auth code needs to change here, and the reachability
// gap is a pure client-side DNS/dial failure.
//
// This test follows the same before/after pattern as
// services/omics/host_prefix_reachability_test.go: drive the real,
// unmodified aws-sdk-go-v2/service/mwaa client through one op per prefix
// family, proving it can't dial (before), then that gopherstack correctly
// handles the real, un-disabled rewrite once the dial problem is solved
// (after). mwaa has no existing disableXHostPrefix-style workaround in its
// test suite at all -- in fact, before this pass, no mwaa test used a real
// SDK client (NewFromConfig/BaseEndpoint) for any operation; every existing
// test drives the handler directly over a raw httptest.Recorder, so the
// reachability of mwaa's near-entire surface via a real client had never
// been exercised.

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

func newMWAAHostPrefixTestClient(t *testing.T, redialFix bool) *mwaasdk.Client {
	t.Helper()

	backend := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(backend)
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

	return mwaasdk.NewFromConfig(cfg, func(o *mwaasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func mwaaNetworkConfig() *types.NetworkConfiguration {
	return &types.NetworkConfiguration{
		SubnetIds:        []string{"subnet-aaaa1111", "subnet-bbbb2222"},
		SecurityGroupIds: []string{"sg-cccc3333"},
	}
}

type mwaaHostPrefixCase struct {
	probe  func(ctx context.Context, client *mwaasdk.Client) error
	call   func(t *testing.T, ctx context.Context, client *mwaasdk.Client)
	name   string
	prefix string
}

func mwaaHostPrefixCases() []mwaaHostPrefixCase {
	return []mwaaHostPrefixCase{
		{
			name:   "api",
			prefix: "api.",
			probe: func(ctx context.Context, client *mwaasdk.Client) error {
				_, err := client.GetEnvironment(ctx, &mwaasdk.GetEnvironmentInput{
					Name: aws.String("unreachable-probe"),
				})

				return err
			},
			call: func(t *testing.T, ctx context.Context, client *mwaasdk.Client) {
				t.Helper()

				envName := "keee-api-env"
				_, err := client.CreateEnvironment(ctx, &mwaasdk.CreateEnvironmentInput{
					Name:                 aws.String(envName),
					DagS3Path:            aws.String("dags/"),
					ExecutionRoleArn:     aws.String("arn:aws:iam::" + testAccountID + ":role/mwaa-role"),
					SourceBucketArn:      aws.String("arn:aws:s3:::keee-bucket"),
					NetworkConfiguration: mwaaNetworkConfig(),
				})
				require.NoError(t, err)

				got, err := client.GetEnvironment(ctx, &mwaasdk.GetEnvironmentInput{Name: aws.String(envName)})
				require.NoError(t, err)
				require.NotNil(t, got.Environment)
				assert.Equal(t, envName, aws.ToString(got.Environment.Name))
			},
		},
		{
			name:   "env",
			prefix: "env.",
			probe: func(ctx context.Context, client *mwaasdk.Client) error {
				_, err := client.CreateCliToken(ctx, &mwaasdk.CreateCliTokenInput{
					Name: aws.String("unreachable-probe"),
				})

				return err
			},
			call: func(t *testing.T, ctx context.Context, client *mwaasdk.Client) {
				t.Helper()

				envName := "keee-env-env"
				_, err := client.CreateEnvironment(ctx, &mwaasdk.CreateEnvironmentInput{
					Name:                 aws.String(envName),
					DagS3Path:            aws.String("dags/"),
					ExecutionRoleArn:     aws.String("arn:aws:iam::" + testAccountID + ":role/mwaa-role"),
					SourceBucketArn:      aws.String("arn:aws:s3:::keee-bucket"),
					NetworkConfiguration: mwaaNetworkConfig(),
				})
				require.NoError(t, err)

				// GetEnvironment promotes CREATING -> AVAILABLE (mwaa's
				// deliberate test-friendly lifecycle simulation, see
				// environments.go:192); CreateCliToken 404s on any
				// non-AVAILABLE state.
				_, err = client.GetEnvironment(ctx, &mwaasdk.GetEnvironmentInput{Name: aws.String(envName)})
				require.NoError(t, err)

				tok, err := client.CreateCliToken(ctx, &mwaasdk.CreateCliTokenInput{Name: aws.String(envName)})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(tok.CliToken))
				assert.NotEmpty(t, aws.ToString(tok.WebServerHostname))
			},
		},
		{
			name:   "ops",
			prefix: "ops.",
			probe: func(ctx context.Context, client *mwaasdk.Client) error {
				//nolint:staticcheck // deliberately exercising the "ops." prefix, which is unique to this deprecated-but-real op
				_, err := client.PublishMetrics(ctx, &mwaasdk.PublishMetricsInput{
					EnvironmentName: aws.String("unreachable-probe"),
					MetricData: []types.MetricDatum{
						{MetricName: aws.String("probe"), Timestamp: aws.Time(time.Now())},
					},
				})

				return err
			},
			call: func(t *testing.T, ctx context.Context, client *mwaasdk.Client) {
				t.Helper()

				envName := "keee-ops-env"
				_, err := client.CreateEnvironment(ctx, &mwaasdk.CreateEnvironmentInput{
					Name:                 aws.String(envName),
					DagS3Path:            aws.String("dags/"),
					ExecutionRoleArn:     aws.String("arn:aws:iam::" + testAccountID + ":role/mwaa-role"),
					SourceBucketArn:      aws.String("arn:aws:s3:::keee-bucket"),
					NetworkConfiguration: mwaaNetworkConfig(),
				})
				require.NoError(t, err)

				//nolint:staticcheck // deliberately exercising the "ops." prefix, which is unique to this deprecated-but-real op
				_, err = client.PublishMetrics(ctx, &mwaasdk.PublishMetricsInput{
					EnvironmentName: aws.String(envName),
					MetricData: []types.MetricDatum{
						{MetricName: aws.String("keee-metric"), Timestamp: aws.Time(time.Now())},
					},
				})
				require.NoError(t, err)
			},
		},
	}
}

// TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix drives an unmodified SDK
// client through one op per prefix family and proves it can't dial: the SDK
// rewrites the request host to "<prefix>127.0.0.1:NNNN" before ever opening a
// TCP connection.
func TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix(t *testing.T) {
	t.Parallel()

	for _, tc := range mwaaHostPrefixCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newMWAAHostPrefixTestClient(t, false)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := tc.probe(ctx, client)
			require.Error(t, err, "prefix=%s: expected the unmodified client to fail to dial the rewritten host",
				tc.prefix)
			t.Logf("prefix=%s unmodified-client error (expected): %v", tc.prefix, err)
		})
	}
}

// TestSDKRoundTrip_HostPrefix_Reachable_AfterFix drives the real SDK client
// with a redial-to-the-real-listener transport (leaving the SDK's real,
// un-disabled host-prefix rewrite intact on the wire -- gopherstack still
// receives "Host: api.127.0.0.1:NNNN" etc.) and asserts the op succeeds with
// correctly decoded values.
func TestSDKRoundTrip_HostPrefix_Reachable_AfterFix(t *testing.T) {
	t.Parallel()

	for _, tc := range mwaaHostPrefixCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newMWAAHostPrefixTestClient(t, true)
			tc.call(t, t.Context(), client)
		})
	}
}
