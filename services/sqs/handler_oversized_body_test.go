package sqs_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func newTestSQSClientForOversized(t *testing.T) *sqssdk.Client {
	t.Helper()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)
	h := sqs.NewHandler(backend)
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

	return sqssdk.NewFromConfig(cfg, func(o *sqssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestRouteMatcher_OversizedQueryProtocolBodyRoutesInsteadOf404 exercises
// RouteMatcher's Query-protocol (form-urlencoded) branch directly. The
// pinned aws-sdk-go-v2 sqs client (v1.46.4) always sends JSON-RPC
// (X-Amz-Target: AmazonSQS.*) requests, which RouteMatcher matches on the
// header alone without ever reading the body -- so a real client can't
// drive this branch. The Query-protocol branch still matters: it is what
// the AWS CLI, boto3, and other non-JSON SQS callers send, and its
// httputils.ReadBody call used to swallow a read failure as a plain
// "false", 404ing a request that legitimately belongs to SQS. This sends a
// raw form-urlencoded POST, with the real aws-sdk-go-v2 User-Agent marker
// (api_client.go's AddSDKAgentKeyValue -- "api/sqs") that RouteMatcher now
// falls back to when the body can't be read, through the same
// service.NewRegistry/NewServiceRouter used in production.
func TestRouteMatcher_OversizedQueryProtocolBodyRoutesInsteadOf404(t *testing.T) {
	t.Parallel()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)
	h := sqs.NewHandler(backend)
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	huge := bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))
	body := "Action=ListQueues&NextToken=" + string(huge)

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, srv.URL+"/", bytes.NewReader([]byte(body)),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "aws-sdk-go2/1.30.0 api/sqs#1.46.4")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotEqual(t, http.StatusNotFound, resp.StatusCode,
		"an unreadable body must not 404 -- it should reach the handler and surface a typed error")
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// TestHandler_OversizedBodySurfacesInternalFailure drives a real SQS
// client's ListQueues with a NextToken large enough to push the request
// body past httputils.MaxRequestBodyBytes, through the same registry/router
// used in production (service.NewRegistry + service.NewServiceRouter). The
// real client sends JSON-RPC, so this exercises pkgs/service.HandleTarget's
// (already-correct) ReadBody-failure handling rather than the
// RouteMatcher fix above -- kept as the real-client regression check.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	client := newTestSQSClientForOversized(t)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.ListQueues(t.Context(), &sqssdk.ListQueuesInput{
		NextToken: huge,
	}, func(o *sqssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard: a normal
// request must still reach Handler() and succeed now that RouteMatcher's
// read-failure branch has changed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	client := newTestSQSClientForOversized(t)

	out, err := client.ListQueues(t.Context(), &sqssdk.ListQueuesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
