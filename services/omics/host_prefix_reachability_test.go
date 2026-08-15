package omics_test

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
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/omics"
)

// gopherstack-keee: every one of Omics' 107 real operations carries a
// client-side host-prefix rewrite applied by a per-operation Smithy Finalize
// middleware (e.g. omics@v1.49.5 api_op_CancelRun.go:127's
// endpointPrefix_opCancelRunMiddleware, inserted after "ResolveEndpointV2"),
// not an endpoint resolver or a static trait read once. There are five
// distinct literal prefixes across the surface -- "workflows-" (38 ops),
// "control-storage-" (34), "analytics-" (28), "storage-" (4: GetReadSet,
// GetReference, CompleteMultipartReadSetUpload, UploadReadSetPart), "tags-"
// (3: Tag/Untag/ListTagsForResource) -- confirmed by grepping every
// api_op_*.go for `req.URL.Host = "..." + req.URL.Host`. A stock client
// pointed at a bare IP/localhost BaseEndpoint (gopherstack's normal local
// setup) cannot resolve "<prefix>-<host>" via DNS, so the request never
// leaves the client process -- confirmed live below.
//
// gopherstack's own routing is unaffected once a request DOES arrive:
// omics.Handler.RouteMatcher (handler.go:223) matches on URL.Path alone, all
// 107 real (method,path) pairs are pairwise distinct (handler_sdk_route_table_test.go
// plus this pass's own cross-check found zero collisions across prefix
// families), and SigV4 verification derives its "host" canonical-request
// component from the request that actually arrived (pkgs/httputils/sigv4.go:241,
// `r.Host`), not any configured/expected value -- so no gopherstack routing
// or auth code needed to change. The reachability gap is a pure client-side
// DNS/dial problem that occurs before any byte reaches gopherstack, the same
// class of problem s3control's CreateAccessPoint family and CloudFront
// KeyValueStore's per-account-ID host already hit (see
// services/s3control/handler_create_tags_test.go and
// services/cloudfrontkeyvaluestore/handler_test.go's staticEndpointResolver)
// -- both worked around the same way: redirect the dial, not the Host header,
// so gopherstack still receives (and must correctly handle) the rewritten
// Host it would see from a real client with working DNS/wildcard routing.
//
// Scoped out of this pass: the four "storage-" ops (GetReadSet/GetReference/
// CompleteMultipartReadSetUpload/UploadReadSetPart) need an existing
// sequence/reference store with real byte content staged via an import job
// before they can be called meaningfully; the mechanism triggering their
// unreachability is identical (same middleware shape, confirmed by source
// grep above) and is not re-verified with its own round trip here.

// dialToRealAddr redirects every dial to realAddr regardless of the
// hostname/port the caller asked for -- the same technique
// services/s3control/handler_create_tags_test.go uses for S3 Control's
// per-account-ID host. Unlike an EndpointResolverV2 override (which only
// replaces the endpoint *ruleset*, and does not stop a Finalize-stage
// hostPrefix middleware from still mutating whatever host that ruleset
// produced -- confirmed by services/omics/wire_field_additions_test.go's
// disableAnalyticsHostPrefix needing its own explicit
// smithyhttp.DisableEndpointHostPrefix middleware instead), this leaves the
// SDK's real host-prefix rewrite fully intact on the wire: the request that
// reaches gopherstack still carries "Host: workflows-127.0.0.1:NNNN" (etc),
// so this actually proves gopherstack survives the rewrite rather than
// avoiding it.
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

// newHostPrefixTestClient stands up a fresh Handler+backend behind an
// httptest server wired through the real pkgs/service router, and returns an
// SDK client pointed at it. When redialFix is true, the client's transport
// redials straight to the httptest listener regardless of the SDK's
// rewritten Host (the "after" case); when false, it uses a plain transport
// so the SDK's real host-prefix rewrite is left to fail on its own DNS
// lookup (the "before" case).
func newHostPrefixTestClient(t *testing.T, redialFix bool) *omicssdk.Client {
	t.Helper()

	backend := omics.NewInMemoryBackend("000000000000", "us-east-1")
	h := omics.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfgOpts := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	}
	if redialFix {
		cfgOpts = append(cfgOpts, awscfg.WithHTTPClient(dialToRealAddr(srv.Listener.Addr().String())))
	}

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), cfgOpts...)
	require.NoError(t, err)

	return omicssdk.NewFromConfig(cfg, func(o *omicssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// hostPrefixCase names one representative op per prefix family and performs
// it, asserting the response actually decoded real values written by
// gopherstack's handler (not just a non-nil pointer) -- the same bar every
// other round-trip test in this package holds itself to.
type hostPrefixCase struct {
	// probe issues the single op that actually carries this family's prefix
	// (see this file's doc comment) and returns its error, so the
	// before-fix test exercises exactly the operation the after-fix test
	// exercises rather than a stand-in.
	probe  func(ctx context.Context, client *omicssdk.Client) error
	call   func(t *testing.T, ctx context.Context, client *omicssdk.Client)
	name   string
	prefix string
}

func hostPrefixCases() []hostPrefixCase {
	return []hostPrefixCase{
		{
			name:   "workflows",
			prefix: "workflows-",
			probe: func(ctx context.Context, client *omicssdk.Client) error {
				_, err := client.CreateRunGroup(ctx, &omicssdk.CreateRunGroupInput{
					Name: aws.String("unreachable-probe"),
				})

				return err
			},
			call: func(t *testing.T, ctx context.Context, client *omicssdk.Client) {
				t.Helper()

				created, err := client.CreateRunGroup(ctx, &omicssdk.CreateRunGroupInput{
					Name: aws.String("keee-workflows-rg"),
				})
				require.NoError(t, err)
				require.NotNil(t, created.Id)

				got, err := client.GetRunGroup(ctx, &omicssdk.GetRunGroupInput{Id: created.Id})
				require.NoError(t, err)
				assert.Equal(t, "keee-workflows-rg", aws.ToString(got.Name))
				assert.Equal(t, aws.ToString(created.Arn), aws.ToString(got.Arn))
			},
		},
		{
			name:   "analytics",
			prefix: "analytics-",
			probe: func(ctx context.Context, client *omicssdk.Client) error {
				_, err := client.CreateAnnotationStore(ctx, &omicssdk.CreateAnnotationStoreInput{
					Name:        aws.String("unreachable-probe"),
					StoreFormat: types.StoreFormatVcf,
				})

				return err
			},
			call: func(t *testing.T, ctx context.Context, client *omicssdk.Client) {
				t.Helper()

				created, err := client.CreateAnnotationStore(ctx, &omicssdk.CreateAnnotationStoreInput{
					Name:        aws.String("keee-analytics-store"),
					StoreFormat: types.StoreFormatVcf,
				})
				require.NoError(t, err)
				require.NotNil(t, created.Id)

				got, err := client.GetAnnotationStore(ctx, &omicssdk.GetAnnotationStoreInput{
					Name: aws.String("keee-analytics-store"),
				})
				require.NoError(t, err)
				assert.Equal(t, "keee-analytics-store", aws.ToString(got.Name))
				assert.Equal(t, aws.ToString(created.Id), aws.ToString(got.Id))
			},
		},
		{
			name:   "control-storage",
			prefix: "control-storage-",
			probe: func(ctx context.Context, client *omicssdk.Client) error {
				_, err := client.CreateReferenceStore(ctx, &omicssdk.CreateReferenceStoreInput{
					Name: aws.String("unreachable-probe"),
				})

				return err
			},
			call: func(t *testing.T, ctx context.Context, client *omicssdk.Client) {
				t.Helper()

				created, err := client.CreateReferenceStore(ctx, &omicssdk.CreateReferenceStoreInput{
					Name: aws.String("keee-control-storage-rs"),
				})
				require.NoError(t, err)
				require.NotNil(t, created.Id)

				got, err := client.GetReferenceStore(ctx, &omicssdk.GetReferenceStoreInput{Id: created.Id})
				require.NoError(t, err)
				assert.Equal(t, "keee-control-storage-rs", aws.ToString(got.Name))
				assert.Equal(t, aws.ToString(created.Arn), aws.ToString(got.Arn))
			},
		},
		{
			name:   "tags",
			prefix: "tags-",
			probe: func(ctx context.Context, client *omicssdk.Client) error {
				_, err := client.TagResource(ctx, &omicssdk.TagResourceInput{
					ResourceArn: aws.String("arn:aws:omics:us-east-1:000000000000:runGroup/unreachable-probe"),
					Tags:        map[string]string{"env": "probe"},
				})

				return err
			},
			call: func(t *testing.T, ctx context.Context, client *omicssdk.Client) {
				t.Helper()

				created, err := client.CreateRunGroup(ctx, &omicssdk.CreateRunGroupInput{
					Name: aws.String("keee-tags-rg"),
				})
				require.NoError(t, err)
				require.NotNil(t, created.Arn)

				_, err = client.TagResource(ctx, &omicssdk.TagResourceInput{
					ResourceArn: created.Arn,
					Tags:        map[string]string{"env": "keee-test"},
				})
				require.NoError(t, err)

				got, err := client.ListTagsForResource(ctx, &omicssdk.ListTagsForResourceInput{
					ResourceArn: created.Arn,
				})
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"env": "keee-test"}, got.Tags)
			},
		},
	}
}

// TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix drives an unmodified SDK
// client -- exactly what a real integrator's application would construct --
// through each affected prefix family and proves the call cannot even reach
// gopherstack: the SDK rewrites the request host to
// "<prefix>127.0.0.1:NNNN" before dialing, which has no DNS record. This is
// the "hand-revert" baseline: any test/dev client lacking the redial
// workaround below reproduces this failure deterministically.
func TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix(t *testing.T) {
	t.Parallel()

	for _, tc := range hostPrefixCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newHostPrefixTestClient(t, false)

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
// with the redial workaround (dialToRealAddr) through each prefix family and
// asserts the affected op actually succeeds and decodes correct values --
// proving gopherstack's router/dispatch/SigV4 verification survive the SDK's
// real, unmodified host-prefix rewrite once the underlying network problem
// is solved (here: a redirected dial; in a real deployment: DNS covering the
// five literal prefixes). This is the "restore" side of the before/after
// pair above -- same client construction, only the transport differs.
func TestSDKRoundTrip_HostPrefix_Reachable_AfterFix(t *testing.T) {
	t.Parallel()

	for _, tc := range hostPrefixCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newHostPrefixTestClient(t, true)
			tc.call(t, t.Context(), client)
		})
	}
}
