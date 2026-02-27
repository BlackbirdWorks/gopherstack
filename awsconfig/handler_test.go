package awsconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/awsconfig"
)

func newTestAWSConfigHandler(t *testing.T) *awsconfig.Handler {
	t.Helper()

	return awsconfig.NewHandler(awsconfig.NewInMemoryBackend(), slog.Default())
}

func doAWSConfigRequest(t *testing.T, h *awsconfig.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "StarlingDoveService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestAWSConfig_Handler_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	rec := doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{
			"name":    "default",
			"roleARN": "arn:aws:iam::000000000000:role/config",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAWSConfig_Handler_DescribeConfigurationRecorders(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::000000000000:role/config"},
	})

	rec := doAWSConfigRequest(t, h, "DescribeConfigurationRecorders", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "ConfigurationRecorders")
}

func TestAWSConfig_Handler_StartConfigurationRecorder(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::000000000000:role/config"},
	})

	rec := doAWSConfigRequest(t, h, "StartConfigurationRecorder", map[string]any{
		"ConfigurationRecorderName": "default",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAWSConfig_Handler_StartConfigurationRecorder_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	rec := doAWSConfigRequest(t, h, "StartConfigurationRecorder", map[string]any{
		"ConfigurationRecorderName": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAWSConfig_Handler_PutDeliveryChannel(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	rec := doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{
			"name":         "default",
			"s3BucketName": "my-bucket",
			"snsTopicARN":  "arn:aws:sns:us-east-1:000000000000:my-topic",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAWSConfig_Handler_DescribeDeliveryChannels(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{"name": "default", "s3BucketName": "my-bucket", "snsTopicARN": ""},
	})

	rec := doAWSConfigRequest(t, h, "DescribeDeliveryChannels", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "DeliveryChannels")
}

func TestAWSConfig_Handler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	rec := doAWSConfigRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAWSConfig_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "StarlingDoveService.PutConfigurationRecorder")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))
}

func TestAWSConfig_Handler_RouteMatcher_NoMatch(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.CreateStream")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.False(t, matcher(c))
}

func TestAWSConfig_Provider(t *testing.T) {
	t.Parallel()

	p := &awsconfig.Provider{}
	assert.Equal(t, "AWSConfig", p.Name())
}
