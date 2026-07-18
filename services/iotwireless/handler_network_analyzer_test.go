package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_NetworkAnalyzerCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create
	body := `{"Name":"na1","Description":"test config","WirelessDevices":["dev1"],"WirelessGateways":["gw1"]}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/network-analyzer-configurations", body)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	name, _ := createResp["Name"].(string)
	assert.Equal(t, "na1", name)

	// Get
	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations/na1", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "na1", getResp["Name"])
	assert.Equal(t, "test config", getResp["Description"])

	// List
	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok := listResp["NetworkAnalyzerConfigurationList"].([]any)
	require.True(t, ok)
	assert.Len(t, configs, 1)

	// Update
	rec = doIoTWRequest(t, h, http.MethodPatch, "/network-analyzer-configurations/na1",
		`{"Description":"updated","WirelessDevices":["dev2"]}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify update
	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations/na1", "")
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "updated", getResp["Description"])

	// Delete
	rec = doIoTWRequest(t, h, http.MethodDelete, "/network-analyzer-configurations/na1", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get after delete should 404
	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations/na1", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_NetworkAnalyzerConfiguration_FullLifecycle covers network analyzer stub ops.
func TestHandler_NetworkAnalyzerConfiguration_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// CreateNetworkAnalyzerConfiguration.
	rec := doIoTWRequest(t, h, http.MethodPost, "/network-analyzer-configurations",
		`{"Name":"test-nac","Description":"test"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ListNetworkAnalyzerConfigurations.
	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// GetNetworkAnalyzerConfiguration by name.
	rec = doIoTWRequest(t, h, http.MethodGet, "/network-analyzer-configurations/test-nac", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// UpdateNetworkAnalyzerConfiguration.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/network-analyzer-configurations/test-nac",
		`{"Description":"updated"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// DeleteNetworkAnalyzerConfiguration.
	rec = doIoTWRequest(t, h, http.MethodDelete, "/network-analyzer-configurations/test-nac", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}
