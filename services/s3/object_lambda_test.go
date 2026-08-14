package s3_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

const objectLambdaTransformedContent = "lambda-transformed-content"

// staticObjectLambda is a test LambdaInvoker that calls WriteGetObjectResponse
// with a hardcoded body, bypassing the original object entirely.
type staticObjectLambda struct {
	serverURL    string
	responseBody string
	// fwdStatus, when non-empty, is sent as X-Amz-Fwd-Status on the
	// WriteGetObjectResponse call, letting the Lambda choose the status code
	// GetObject's caller ultimately sees (e.g. an access-control Lambda
	// returning 403).
	fwdStatus string
}

func (l *staticObjectLambda) InvokeFunction(
	_ context.Context,
	_, _ string,
	payload []byte,
) ([]byte, int, error) {
	var event struct {
		GetObjectContext struct {
			OutputToken string `json:"outputToken"`
		} `json:"getObjectContext"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, 0, err
	}

	wgorURL := l.serverURL + "/?writeGetObjectResponse"
	wgorReq, err := http.NewRequest(http.MethodPost, wgorURL, strings.NewReader(l.responseBody))
	if err != nil {
		return nil, 0, err
	}
	wgorReq.Header.Set("X-Amz-Request-Token", event.GetObjectContext.OutputToken)
	wgorReq.Header.Set("Content-Type", "application/octet-stream")
	if l.fwdStatus != "" {
		wgorReq.Header.Set("X-Amz-Fwd-Status", l.fwdStatus)
	}

	wgorResp, err := http.DefaultClient.Do(wgorReq)
	if err != nil {
		return nil, 0, err
	}
	wgorResp.Body.Close()

	return nil, 200, nil
}

// TestS3ObjectLambda_WriteGetObjectResponse verifies the full Object Lambda
// WriteGetObjectResponse pipeline: GetObject invokes the configured Lambda,
// which calls back into WriteGetObjectResponse to substitute the response
// body, without a recursive GetObject call.
func TestS3ObjectLambda_WriteGetObjectResponse(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	bucket := "object-lambda-test-bucket"
	key := "hello.txt"

	// Create bucket and put an object (content doesn't matter; lambda replaces it).
	req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"/"+key,
		strings.NewReader("original content"),
	)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Start an HTTP server so the lambda can call WriteGetObjectResponse via HTTP.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveS3Handler(handler, w, r)
	}))
	defer srv.Close()

	handler.Endpoint = srv.URL

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:transformer"
	handler.SetObjectLambdaConfig(bucket, lambdaARN)

	// The lambda ignores the original object and writes back a hardcoded body.
	// This tests the full WriteGetObjectResponse pipeline without recursive GetObject.
	lambdaFn := &staticObjectLambda{
		serverURL:    srv.URL,
		responseBody: objectLambdaTransformedContent,
	}
	targets := &s3.NotificationTargets{LambdaInvoker: lambdaFn}
	handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(targets, "us-east-1"))

	// GetObject → lambda invoked → WriteGetObjectResponse → response returned to caller.
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, objectLambdaTransformedContent, rec.Body.String())
}

// TestS3ObjectLambda_WriteGetObjectResponse_ForwardsStatus is a regression
// test: real S3's WriteGetObjectResponseInput.StatusCode is header-bound to
// X-Amz-Fwd-Status (confirmed against aws-sdk-go-v2/service/s3@v1.106.5
// serializers.go's awsRestxml_serializeOpHttpBindingsWriteGetObjectResponseInput,
// locationName "X-Amz-Fwd-Status") -- a Lambda can use it to signal e.g. a 403
// from an access-control check. The handler previously hardcoded 200 for every
// WriteGetObjectResponse call regardless of what the Lambda sent, silently
// discarding this header, so GetObject always reported success even when the
// Lambda intended to reject the request.
func TestS3ObjectLambda_WriteGetObjectResponse_ForwardsStatus(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	bucket := "object-lambda-status-bucket"
	key := "hello.txt"

	req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, strings.NewReader("original content"))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveS3Handler(handler, w, r)
	}))
	defer srv.Close()

	handler.Endpoint = srv.URL

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:denier"
	handler.SetObjectLambdaConfig(bucket, lambdaARN)

	lambdaFn := &staticObjectLambda{
		serverURL:    srv.URL,
		responseBody: "access denied by lambda",
		fwdStatus:    "403",
	}
	targets := &s3.NotificationTargets{LambdaInvoker: lambdaFn}
	handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(targets, "us-east-1"))

	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, "access denied by lambda", rec.Body.String())
}

// TestS3ObjectLambda_ConfigClearedOnBucketDelete locks that DeleteBucket
// drops any registered Object Lambda config for that bucket name. Without
// this, a bucket recreated under the same name would silently inherit the
// previous incarnation's Lambda wiring on GetObject.
func TestS3ObjectLambda_ConfigClearedOnBucketDelete(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "object-lambda-recreate-bucket"
	key := "hello.txt"

	req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	handler.SetObjectLambdaConfig(bucket, "arn:aws:lambda:us-east-1:000000000000:function:transformer")

	// Empty the bucket (required for DeleteBucket to succeed) and delete it.
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Run the janitor so the pending-delete bucket is fully removed from the
	// table (DeleteBucket only marks it pending; removal is asynchronous).
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s3.NewJanitor(backend, s3.Settings{JanitorInterval: 5 * time.Millisecond}).Run(ctx)

	// Recreate a bucket with the same name and put a plain object.
	require.Eventually(t, func() bool {
		req = httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
		rec = httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		return rec.Code == http.StatusOK
	}, time.Second, 10*time.Millisecond, "recreated bucket should succeed once janitor drains the pending delete")

	req = httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"/"+key,
		strings.NewReader("plain content"),
	)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// GetObject must return the plain object directly, NOT attempt to invoke
	// the stale Lambda config (which would hang/fail since no notifier or
	// LambdaInvoker is wired up for this handler).
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "plain content", rec.Body.String())
}
