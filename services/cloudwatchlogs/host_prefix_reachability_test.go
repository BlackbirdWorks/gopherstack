package cloudwatchlogs_test

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
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// gopherstack-3gbe: CloudWatch Logs' large-object/live-tail family carries
// the same client-side host-prefix rewrite Omics has (gopherstack-keee).
// Two ops, one literal prefix -- "stream-" (GetLogObject, StartLiveTail) --
// confirmed by grepping cloudwatchlogs@v1.81.1's api_op_*.go for
// `req.URL.Host = "..." + req.URL.Host`, matching gopherstack-3gbe's filing
// exactly.
//
// Handler.RouteMatcher (handler.go:228) matches on the X-Amz-Target header
// prefix "Logs_20140328.", never Host or Path, so the rewrite can't create a
// routing collision here -- header-based dispatch is inherently immune to
// the path-collision class this bug family could otherwise cause. Same
// conclusion as Omics: no gopherstack routing/auth code needs to change,
// the gap is a pure client-side DNS/dial failure.
//
// This family is NOT the same shape as mwaa/lakeformation/servicediscovery/
// stepfunctions, though: both real GetLogObject and StartLiveTail responses
// are Smithy event streams (GetLogObjectEventStream /
// StartLiveTailResponseStream), and gopherstack's handlers -- deliberately,
// per handler_log_events.go's handleStartLiveTail doc comment -- return a
// plain unary JSON body instead of real event-stream framing, "a streaming
// (HTTP/2 event-stream) operation that cannot be meaningfully emulated over
// the standard unary JSON response". Confirmed live during this pass: an
// unmodified client's happy-path StartLiveTail call fails client-side with
// "unexpected output result type: <nil>" once the dial problem is solved,
// because the SDK's event-stream deserializer has nothing to unpack -- a
// pre-existing, separately-documented gap, not a host-prefix-reachability
// bug, and out of scope for gopherstack-3gbe to fix.
//
// So the "after" case here proves what host-prefix-reachability actually
// grants: the real, un-disabled "stream-" rewrite reaches gopherstack, gets
// authenticated and routed, and gopherstack's validation runs and returns a
// correctly-typed AWS error (which the SDK decodes via the ordinary
// unary-JSON error path, unaffected by the success shape's event-stream
// gap) -- as opposed to the "before" case's dial failure, which never
// reaches gopherstack at all.
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

func newCWLHostPrefixTestClient(t *testing.T, redialFix bool) *cwlsdk.Client {
	t.Helper()

	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

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

	return cwlsdk.NewFromConfig(cfg, func(o *cwlsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix drives an unmodified SDK
// client through both "stream-" prefixed ops and proves neither can dial.
func TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		probe func(ctx context.Context, client *cwlsdk.Client) error
		name  string
	}{
		{
			name: "get_log_object",
			probe: func(ctx context.Context, client *cwlsdk.Client) error {
				_, err := client.GetLogObject(ctx, &cwlsdk.GetLogObjectInput{
					LogObjectPointer: aws.String("unreachable-probe"),
				})

				return err
			},
		},
		{
			name: "start_live_tail",
			probe: func(ctx context.Context, client *cwlsdk.Client) error {
				_, err := client.StartLiveTail(ctx, &cwlsdk.StartLiveTailInput{
					LogGroupIdentifiers: []string{"unreachable-probe"},
				})

				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newCWLHostPrefixTestClient(t, false)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := tc.probe(ctx, client)
			require.Error(t, err, "expected the unmodified client to fail to dial the stream- rewritten host")
			t.Logf("stream- unmodified-client error (expected): %v", err)
		})
	}
}

// TestSDKRoundTrip_HostPrefix_Reachable_AfterFix drives the real SDK client
// with a redial-to-the-real-listener transport, leaving the SDK's real,
// un-disabled "stream-" rewrite intact on the wire, and proves the request
// reaches gopherstack and is correctly authenticated/routed/validated: both
// ops return the AWS-shaped error for a nonexistent log group/object,
// decoded by the SDK's ordinary unary-JSON error path. See this file's
// top-of-file comment for why a happy-path decode assertion is out of scope
// here (a separate, pre-existing event-stream emulation gap).
func TestSDKRoundTrip_HostPrefix_Reachable_AfterFix(t *testing.T) {
	t.Parallel()

	client := newCWLHostPrefixTestClient(t, true)

	t.Run("get_log_object", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetLogObject(t.Context(), &cwlsdk.GetLogObjectInput{
			LogObjectPointer: aws.String("bm90LWEtcmVhbC1wb2ludGVy"),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InvalidParameterException")
	})

	t.Run("start_live_tail", func(t *testing.T) {
		t.Parallel()

		_, err := client.StartLiveTail(t.Context(), &cwlsdk.StartLiveTailInput{
			LogGroupIdentifiers: []string{"does-not-exist"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ResourceNotFoundException")
	})
}
