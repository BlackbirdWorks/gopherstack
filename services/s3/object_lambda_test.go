package s3_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
