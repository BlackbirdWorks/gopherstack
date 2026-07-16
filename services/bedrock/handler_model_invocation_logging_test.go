package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccuracy_LoggingConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bucket  string
		enabled bool
	}{
		{
			name:    "logging enabled with bucket",
			enabled: true,
			bucket:  "my-log-bucket",
		},
		{
			name:    "logging disabled no bucket",
			enabled: false,
			bucket:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(
				t, h, http.MethodPut, "/logging/modelinvocations",
				map[string]any{
					"loggingEnabled": tt.enabled,
					"s3BucketName":   tt.bucket,
				},
			)
			assert.Equal(t, http.StatusOK, rec.Code)

			recGet := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			cfg := out["loggingConfig"].(map[string]any)
			assert.Equal(t, tt.enabled, cfg["loggingEnabled"])
			if tt.bucket != "" {
				assert.Equal(t, tt.bucket, cfg["s3BucketName"])
			}
		})
	}
}

func TestAccuracy_LoggingConfig_DeleteClearsConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Put a config.
	recPut := doRequest(t, h, http.MethodPut, "/logging/modelinvocations",
		map[string]any{"loggingEnabled": true, "s3BucketName": "bucket"})
	require.Equal(t, http.StatusOK, recPut.Code)

	// Delete it.
	recDel := doRequest(t, h, http.MethodDelete, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, recDel.Code)

	// Get should return an empty config.
	recGet := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	cfg := out["loggingConfig"].(map[string]any)
	enabled, _ := cfg["loggingEnabled"].(bool)
	assert.False(t, enabled)
}

func TestAccuracy_LoggingConfig_GetBeforePutReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["loggingConfig"])
}

func TestHandler_ModelInvocationLoggingConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Get (returns empty by default).
	rec := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Put.
	rec2 := doRequest(t, h, http.MethodPut, "/logging/modelinvocations", map[string]any{
		"loggingConfig": map[string]any{
			"cloudWatchConfig": map[string]any{
				"logGroupName": "/aws/bedrock/model-invocations",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Get again — should have config now.
	rec3 := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &getOut))
	assert.NotNil(t, getOut["loggingConfig"])

	// Delete.
	rec4 := doRequest(t, h, http.MethodDelete, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}
