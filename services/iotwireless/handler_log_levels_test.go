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

// TestHandler_LogLevels_OptionListsRoundTrip locks in that
// FuotaTaskLogOptions/WirelessDeviceLogOptions/WirelessGatewayLogOptions
// submitted via UpdateLogLevelsByResourceTypes are stored and echoed back on
// Get, instead of always reporting empty arrays regardless of input (see
// PARITY.md deferred item on LogLevels).
func TestHandler_LogLevels_OptionListsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	body := `{
		"DefaultLogLevel": "ERROR",
		"WirelessDeviceLogOptions": [{"Type":"LoRaWAN","LogLevel":"INFO"}],
		"WirelessGatewayLogOptions": [{"Type":"LoRaWAN","LogLevel":"DEBUG"}],
		"FuotaTaskLogOptions": [{"Type":"LoRaWAN","LogLevel":"ERROR"}]
	}`

	rec := doIoTWRequest(t, h, http.MethodPost, "/log-levels", body)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doIoTWRequest(t, h, http.MethodGet, "/log-levels", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		DefaultLogLevel           string           `json:"DefaultLogLevel"`
		FuotaTaskLogOptions       []map[string]any `json:"FuotaTaskLogOptions"`
		WirelessDeviceLogOptions  []map[string]any `json:"WirelessDeviceLogOptions"`
		WirelessGatewayLogOptions []map[string]any `json:"WirelessGatewayLogOptions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, "ERROR", resp.DefaultLogLevel)
	require.Len(t, resp.WirelessDeviceLogOptions, 1)
	assert.Equal(t, "INFO", resp.WirelessDeviceLogOptions[0]["LogLevel"])
	require.Len(t, resp.WirelessGatewayLogOptions, 1)
	assert.Equal(t, "DEBUG", resp.WirelessGatewayLogOptions[0]["LogLevel"])
	require.Len(t, resp.FuotaTaskLogOptions, 1)
	assert.Equal(t, "ERROR", resp.FuotaTaskLogOptions[0]["LogLevel"])
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
