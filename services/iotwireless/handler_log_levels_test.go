package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_LogLevels(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Get log levels (default)
	rec := doIoTWRequest(t, h, http.MethodGet, "/log-levels", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "INFO", getResp["DefaultLogLevel"])

	// Update log levels
	rec = doIoTWRequest(t, h, http.MethodPost, "/log-levels", `{"DefaultLogLevel":"DEBUG"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify updated
	rec = doIoTWRequest(t, h, http.MethodGet, "/log-levels", "")
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "DEBUG", getResp["DefaultLogLevel"])

	// Put resource log level
	rec = doIoTWRequest(
		t,
		h,
		http.MethodPut,
		"/log-levels/my-device",
		`{"LogLevel":"ERROR","ResourceType":"WirelessDevice"}`,
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get resource log level
	rec = doIoTWRequest(t, h, http.MethodGet, "/log-levels/my-device", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "ERROR", getResp["LogLevel"])

	// Reset resource log level
	rec = doIoTWRequest(t, h, http.MethodDelete, "/log-levels/my-device", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// After reset, should return INFO default
	rec = doIoTWRequest(t, h, http.MethodGet, "/log-levels/my-device", "")
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "INFO", getResp["LogLevel"])

	// Reset all resource log levels
	rec = doIoTWRequest(t, h, http.MethodDelete, "/log-levels", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// TestHandler_LogLevels_StatusOnly covers log level stub ops.
func TestHandler_LogLevels_StatusOnly(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// GetLogLevelsByResourceTypes.
	rec := doIoTWRequest(t, h, http.MethodGet, "/log-levels", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// UpdateLogLevelsByResourceTypes.
	rec = doIoTWRequest(t, h, http.MethodPost, "/log-levels",
		`{"DefaultLogLevel":"INFO"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ResetAllResourceLogLevels.
	rec = doIoTWRequest(t, h, http.MethodDelete, "/log-levels", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}
