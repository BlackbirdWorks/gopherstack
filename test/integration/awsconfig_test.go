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

func awsconfigPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "StarlingDoveService."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func awsconfigReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_AWSConfig_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := awsconfigPost(t, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{
			"name":    "default",
			"roleARN": "arn:aws:iam::000000000000:role/config",
		},
	})
	body := awsconfigReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_AWSConfig_DescribeConfigurationRecorders(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	awsconfigPost(t, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{
			"name":    "describe-test",
			"roleARN": "arn:aws:iam::000000000000:role/config",
		},
	})

	resp := awsconfigPost(t, "DescribeConfigurationRecorders", map[string]any{})
	body := awsconfigReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ConfigurationRecorders")
}

func TestIntegration_AWSConfig_PutDeliveryChannel(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := awsconfigPost(t, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{
			"name":         "default",
			"s3BucketName": "my-config-bucket",
			"snsTopicARN":  "arn:aws:sns:us-east-1:000000000000:config-topic",
		},
	})
	body := awsconfigReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_AWSConfig_DescribeDeliveryChannels(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	awsconfigPost(t, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{
			"name":         "dc-list-test",
			"s3BucketName": "test-bucket",
		},
	})

	resp := awsconfigPost(t, "DescribeDeliveryChannels", map[string]any{})
	body := awsconfigReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "DeliveryChannels")
}
