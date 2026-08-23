package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAccuracy_LoggingConfig_RoundTrip proves the real
// PutModelInvocationLoggingConfigurationInput wire shape round-trips: the config is wrapped
// under a top-level "loggingConfig" key (api_op_PutModelInvocationLoggingConfiguration.go),
// itself shaped like types.LoggingConfig (cloudWatchConfig/s3Config/*DataDeliveryEnabled) --
// NOT the flat {loggingEnabled,s3BucketName} shape this handler used to accept, which has no
// counterpart anywhere in the real API.
func TestAccuracy_LoggingConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(
		t, h, http.MethodPut, "/logging/modelinvocations",
		map[string]any{
			"loggingConfig": map[string]any{
				"s3Config":                 map[string]any{"bucketName": "my-log-bucket", "keyPrefix": "logs/"},
				"textDataDeliveryEnabled":  true,
				"imageDataDeliveryEnabled": false,
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	cfg, ok := out["loggingConfig"].(map[string]any)
	require.True(t, ok, "loggingConfig must be present after Put")

	s3, ok := cfg["s3Config"].(map[string]any)
	require.True(t, ok, "s3Config must round-trip under the real key")
	assert.Equal(t, "my-log-bucket", s3["bucketName"])
	assert.Equal(t, "logs/", s3["keyPrefix"])
	assert.Equal(t, true, cfg["textDataDeliveryEnabled"])
	assert.Equal(t, false, cfg["imageDataDeliveryEnabled"])
}

// TestAccuracy_LoggingConfig_CloudWatchConfigRoundTrip covers the other real nested shape,
// cloudWatchConfig (logGroupName/roleArn/largeDataDeliveryS3Config).
func TestAccuracy_LoggingConfig_CloudWatchConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(
		t, h, http.MethodPut, "/logging/modelinvocations",
		map[string]any{
			"loggingConfig": map[string]any{
				"cloudWatchConfig": map[string]any{
					"logGroupName": "/aws/bedrock/model-invocations",
					"roleArn":      "arn:aws:iam::000000000000:role/BedrockLoggingRole",
				},
				"audioDataDeliveryEnabled": true,
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	cfg, ok := out["loggingConfig"].(map[string]any)
	require.True(t, ok)

	cw, ok := cfg["cloudWatchConfig"].(map[string]any)
	require.True(t, ok, "cloudWatchConfig must round-trip under the real key")
	assert.Equal(t, "/aws/bedrock/model-invocations", cw["logGroupName"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/BedrockLoggingRole", cw["roleArn"])
	assert.Equal(t, true, cfg["audioDataDeliveryEnabled"])
}

func TestAccuracy_LoggingConfig_PutRequiresLoggingConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPut, "/logging/modelinvocations", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_LoggingConfig_DeleteClearsConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recPut := doRequest(t, h, http.MethodPut, "/logging/modelinvocations",
		map[string]any{"loggingConfig": map[string]any{"textDataDeliveryEnabled": true}})
	require.Equal(t, http.StatusOK, recPut.Code)

	recDel := doRequest(t, h, http.MethodDelete, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, recDel.Code)

	recGet := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	assert.Nil(t, out["loggingConfig"], "Delete must clear the config back to absent, not an empty object")
}

// TestAccuracy_LoggingConfig_GetBeforePutOmitsLoggingConfig: LoggingConfig is optional on
// GetModelInvocationLoggingConfigurationOutput; before any Put has ever happened there is no
// configuration to report, so the key must be absent rather than a fabricated empty object.
func TestAccuracy_LoggingConfig_GetBeforePutOmitsLoggingConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Nil(t, out["loggingConfig"])
}
