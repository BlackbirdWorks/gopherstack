package cloudwatch_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// The pinned aws-sdk-go-v2 cloudwatch client (v1.66.3) sets
// options.Protocol = rpcv2.NewCBOR(...) unconditionally (api_client.go:214),
// so every operation it issues uses the rpc-v2-cbor protocol -- there is no
// way to drive the legacy form-urlencoded (Query/XML) RouteMatcher branch
// through this client. That branch still matters: it is what a browser
// dashboard, the AWS CLI, or an older (<1.55) SDK sends, and RouteMatcher's
// own httputils.ReadBody call there used to swallow a read failure as a
// plain "false", 404ing a request that legitimately belongs to CloudWatch.
// This test drives that branch directly with a raw http.Client POST rather
// than the SDK client, through the same service.NewRegistry/NewServiceRouter
// used in production, and sets the real aws-sdk-go-v2 User-Agent marker
// (api_client.go's AddSDKAgentKeyValue -- "api/cloudwatch") that
// RouteMatcher now falls back to when the body can't be read.
func TestRouteMatcher_OversizedFormBodyRoutesInsteadOf404(t *testing.T) {
	t.Parallel()

	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	huge := bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))
	body := "Action=ListMetrics&NextToken=" + string(huge)

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, srv.URL+"/", bytes.NewReader([]byte(body)),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "aws-sdk-go2/1.30.0 api/cloudwatch#1.66.3")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotEqual(t, http.StatusNotFound, resp.StatusCode,
		"an unreadable body must not 404 -- it should reach Handler() and surface a typed error")
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// TestHandler_CBOROversizedBodyAlreadyTyped confirms the CBOR path a real
// aws-sdk-go-v2 cloudwatch client actually uses already surfaces a typed
// error (not a 404, not silently-empty input) for an oversized body: it
// reads the body itself (handleCBOR, capped at maxCBORBodyBytes) rather
// than through RouteMatcher, and was unaffected by (and unbroken by) the
// RouteMatcher fix above.
func TestHandler_CBOROversizedBodyAlreadyTyped(t *testing.T) {
	t.Parallel()

	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	huge := aws.String(string(bytes.Repeat([]byte("x"), 2*1024*1024)))

	_, err = client.ListMetrics(t.Context(), &cwsdk.ListMetricsInput{
		NextToken: huge,
	}, func(o *cwsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.NotEmpty(t, apiErr.ErrorCode())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard: a normal
// request through the real SDK client (rpc-v2-cbor) must still reach
// Handler() and succeed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	out, err := client.ListMetrics(t.Context(), &cwsdk.ListMetricsInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
