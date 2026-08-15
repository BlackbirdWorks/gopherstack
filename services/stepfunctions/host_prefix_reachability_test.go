package stepfunctions_test

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
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// gopherstack-3gbe: Step Functions' sync family carries the same
// client-side host-prefix rewrite Omics has (gopherstack-keee). Two ops,
// one literal prefix -- "sync-" (TestState, StartSyncExecution) -- confirmed
// by grepping sfn@v1.45.4's api_op_*.go for `req.URL.Host = "..." +
// req.URL.Host`, matching gopherstack-3gbe's filing exactly.
//
// Handler.RouteMatcher (handler.go:160) matches on the X-Amz-Target header
// prefix ("AmazonStates." or "AWSStepFunctions."), never Host or Path, so
// the rewrite can't create a routing collision here -- header-based
// dispatch is inherently immune to the path-collision class this bug
// family could otherwise cause. Same conclusion as Omics: no gopherstack
// routing/auth code needs to change, the gap is a pure client-side DNS/dial
// failure.
//
// stepfunctions already has a real-SDK-client round trip
// (wire_updatedate_test.go), but it never exercises TestState or
// StartSyncExecution, so the host-prefix reachability of this family
// specifically had never been proven either way before this test.
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

func newSFNHostPrefixTestClient(t *testing.T, redialFix bool) *sfnsdk.Client {
	t.Helper()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)

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

	return sfnsdk.NewFromConfig(cfg, func(o *sfnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

const sfnHostPrefixDefinition = `{"MyState":{"Type":"Pass","End":true}}`

// TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix drives an unmodified SDK
// client through both "sync-" prefixed ops and proves neither can dial.
func TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		probe func(ctx context.Context, client *sfnsdk.Client) error
		name  string
	}{
		{
			name: "test_state",
			probe: func(ctx context.Context, client *sfnsdk.Client) error {
				_, err := client.TestState(ctx, &sfnsdk.TestStateInput{
					Definition: aws.String(sfnHostPrefixDefinition),
				})

				return err
			},
		},
		{
			name: "start_sync_execution",
			probe: func(ctx context.Context, client *sfnsdk.Client) error {
				_, err := client.StartSyncExecution(ctx, &sfnsdk.StartSyncExecutionInput{
					StateMachineArn: aws.String("arn:aws:states:us-east-1:000000000000:stateMachine:unreachable-probe"),
				})

				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newSFNHostPrefixTestClient(t, false)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := tc.probe(ctx, client)
			require.Error(t, err, "expected the unmodified client to fail to dial the sync- rewritten host")
			t.Logf("sync- unmodified-client error (expected): %v", err)
		})
	}
}

// TestSDKRoundTrip_HostPrefix_Reachable_AfterFix drives TestState and
// (via CreateStateMachine of an EXPRESS state machine) StartSyncExecution
// through the real SDK client with a redial-to-the-real-listener transport,
// proving gopherstack survives the real, un-disabled "sync-" rewrite and
// decodes correct values.
func TestSDKRoundTrip_HostPrefix_Reachable_AfterFix(t *testing.T) {
	t.Parallel()

	client := newSFNHostPrefixTestClient(t, true)

	t.Run("test_state", func(t *testing.T) {
		t.Parallel()

		out, err := client.TestState(t.Context(), &sfnsdk.TestStateInput{
			Definition: aws.String(sfnHostPrefixDefinition),
			Input:      aws.String(`{"x":1}`),
		})
		require.NoError(t, err)
		assert.Equal(t, "SUCCEEDED", string(out.Status))
	})

	t.Run("start_sync_execution", func(t *testing.T) {
		t.Parallel()

		sm, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
			Name:       aws.String("keee-sync-sm"),
			Definition: aws.String(`{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`),
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-test"),
			Type:       "EXPRESS",
		})
		require.NoError(t, err)

		out, err := client.StartSyncExecution(t.Context(), &sfnsdk.StartSyncExecutionInput{
			StateMachineArn: sm.StateMachineArn,
			Input:           aws.String(`{"hello":"world"}`),
		})
		require.NoError(t, err)
		assert.Equal(t, "SUCCEEDED", string(out.Status))
		assert.JSONEq(t, `{"hello":"world"}`, aws.ToString(out.Input))
	})
}
