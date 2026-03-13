package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pipesRequest(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, endpoint+path, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/pipes/aws4_request, "+
		"SignedHeaders=host;x-amz-date, Signature=fakesignature")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func pipesReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_Pipes_CreatePipe(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := pipesRequest(t, http.MethodPost, "/v1/pipes/integ-pipe", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/pipe-role",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:source-queue",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:target-fn",
	})
	body := pipesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "integ-pipe")
}

func TestIntegration_Pipes_ListPipes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	pipesRequest(t, http.MethodPost, "/v1/pipes/list-pipe-integ", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
	})

	resp := pipesRequest(t, http.MethodGet, "/v1/pipes", nil)
	body := pipesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Pipes")
}

func TestIntegration_Pipes_DescribePipe(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	pipesRequest(t, http.MethodPost, "/v1/pipes/describe-pipe-integ", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
	})

	resp := pipesRequest(t, http.MethodGet, "/v1/pipes/describe-pipe-integ", nil)
	body := pipesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "describe-pipe-integ")
}

func TestIntegration_Pipes_UpdatePipe(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	pipesRequest(t, http.MethodPost, "/v1/pipes/update-pipe-integ", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
	})

	resp := pipesRequest(t, http.MethodPut, "/v1/pipes/update-pipe-integ", map[string]any{
		"Target":      "arn:aws:lambda:us-east-1:000000000000:function:new-fn",
		"Description": "updated",
	})
	body := pipesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "update-pipe-integ")
}

func TestIntegration_Pipes_StartStopPipe(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	pipesRequest(t, http.MethodPost, "/v1/pipes/startstop-pipe-integ", map[string]any{
		"RoleArn":      "arn:aws:iam::000000000000:role/r",
		"Source":       "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":       "arn:aws:lambda:us-east-1:000000000000:function:fn",
		"DesiredState": "RUNNING",
	})

	stopResp := pipesRequest(t, http.MethodPost, "/v1/pipes/startstop-pipe-integ/stop", nil)
	stopBody := pipesReadBody(t, stopResp)
	assert.Equal(t, http.StatusOK, stopResp.StatusCode, "body: %s", stopBody)
	assert.Contains(t, stopBody, "STOPPED")

	startResp := pipesRequest(t, http.MethodPost, "/v1/pipes/startstop-pipe-integ/start", nil)
	startBody := pipesReadBody(t, startResp)
	assert.Equal(t, http.StatusOK, startResp.StatusCode, "body: %s", startBody)
	assert.Contains(t, startBody, "RUNNING")
}

func TestIntegration_Pipes_DeletePipe(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	pipesRequest(t, http.MethodPost, "/v1/pipes/delete-pipe-integ", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
	})

	resp := pipesRequest(t, http.MethodDelete, "/v1/pipes/delete-pipe-integ", nil)
	body := pipesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}
