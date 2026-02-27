package integration_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func firehosePost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Firehose_20150804."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func firehoseReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_Firehose_CreateDeliveryStream(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := firehosePost(t, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "integ-firehose-stream",
	})
	body := firehoseReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DeliveryStreamARN")
}

func TestIntegration_Firehose_ListDeliveryStreams(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	firehosePost(t, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "fh-list-test"})

	resp := firehosePost(t, "ListDeliveryStreams", map[string]any{})
	body := firehoseReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DeliveryStreamNames")
}

func TestIntegration_Firehose_DescribeDeliveryStream(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	firehosePost(t, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "fh-describe-test"})

	resp := firehosePost(t, "DescribeDeliveryStream", map[string]any{
		"DeliveryStreamName": "fh-describe-test",
	})
	body := firehoseReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DeliveryStreamDescription")
}

func TestIntegration_Firehose_PutRecord(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	firehosePost(t, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "fh-put-test"})

	data := base64.StdEncoding.EncodeToString([]byte("hello firehose"))
	resp := firehosePost(t, "PutRecord", map[string]any{
		"DeliveryStreamName": "fh-put-test",
		"Record":             map[string]string{"Data": data},
	})
	body := firehoseReadBody(t, resp)

	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "RecordId")
}

func TestIntegration_Firehose_DeleteDeliveryStream(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	firehosePost(t, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "fh-delete-test"})

	resp := firehosePost(t, "DeleteDeliveryStream", map[string]any{
		"DeliveryStreamName": "fh-delete-test",
	})
	body := firehoseReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}
