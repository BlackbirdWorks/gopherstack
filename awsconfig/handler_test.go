package awsconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/awsconfig"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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

func TestAWSConfig_Handler_Name(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	assert.Equal(t, "AWSConfig", h.Name())
}

func TestAWSConfig_Handler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "PutConfigurationRecorder")
	assert.Contains(t, ops, "DescribeConfigurationRecorders")
	assert.Contains(t, ops, "StartConfigurationRecorder")
	assert.Contains(t, ops, "PutDeliveryChannel")
	assert.Contains(t, ops, "DescribeDeliveryChannels")
}

func TestAWSConfig_Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestAWSConfig_Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "StarlingDoveService.PutConfigurationRecorder")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "PutConfigurationRecorder", h.ExtractOperation(c))

	// No target → "Unknown"
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))
}

func TestAWSConfig_Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	e := echo.New()

	newCtx := func(target, body string) *echo.Context {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("X-Amz-Target", "StarlingDoveService."+target)

		return e.NewContext(req, httptest.NewRecorder())
	}

	// PutConfigurationRecorder
	c := newCtx("PutConfigurationRecorder", `{"ConfigurationRecorder":{"name":"my-recorder"}}`)
	assert.Equal(t, "my-recorder", h.ExtractResource(c))

	// StartConfigurationRecorder
	c = newCtx("StartConfigurationRecorder", `{"ConfigurationRecorderName":"my-recorder"}`)
	assert.Equal(t, "my-recorder", h.ExtractResource(c))

	// DescribeConfigurationRecorders with names
	c = newCtx("DescribeConfigurationRecorders", `{"ConfigurationRecorderNames":["r1"]}`)
	assert.Equal(t, "r1", h.ExtractResource(c))

	// DescribeConfigurationRecorders without names
	c = newCtx("DescribeConfigurationRecorders", `{}`)
	assert.Empty(t, h.ExtractResource(c))

	// PutDeliveryChannel
	c = newCtx("PutDeliveryChannel", `{"DeliveryChannel":{"name":"my-channel"}}`)
	assert.Equal(t, "my-channel", h.ExtractResource(c))

	// DescribeDeliveryChannels with names
	c = newCtx("DescribeDeliveryChannels", `{"DeliveryChannelNames":["ch1"]}`)
	assert.Equal(t, "ch1", h.ExtractResource(c))

	// DescribeDeliveryChannels without names
	c = newCtx("DescribeDeliveryChannels", `{}`)
	assert.Empty(t, h.ExtractResource(c))

	// Default fallback
	c = newCtx("UnknownOp", `{"ConfigurationRecorderName":"fallback"}`)
	assert.Equal(t, "fallback", h.ExtractResource(c))
}

func TestAWSConfig_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &awsconfig.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "AWSConfig", svc.Name())
}
