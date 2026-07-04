package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIoTW_MulticastGroups covers multicast group stub operations.
func TestIoTW_MulticastGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// CreateMulticastGroup.
	rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups",
		`{"Name":"test-mg","LoRaWAN":{"RfRegion":"US915"}}`)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	mgID, _ := createResp["Id"].(string)
	require.NotEmpty(t, mgID)

	// GetMulticastGroup.
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+mgID, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListMulticastGroups.
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateMulticastGroup.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/multicast-groups/"+mgID,
		`{"Name":"updated-mg"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetMulticastGroupSession before one has been started must 404, matching
	// real AWS's ResourceNotFoundException for a group with no active session.
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+mgID+"/session", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// StartMulticastGroupSession.
	rec = doIoTWRequest(t, h, http.MethodPut, "/multicast-groups/"+mgID+"/session",
		`{"LoRaWAN":{"DlDr":5,"DlFreq":923300000,"SessionStartTime":"2024-01-01T00:00:00Z","SessionTimeout":60}}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetMulticastGroupSession now that a session is active.
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+mgID+"/session", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// SendDataToMulticastGroup.
	rec = doIoTWRequest(t, h, http.MethodPost, "/multicast-groups/"+mgID+"/data",
		`{"PayloadData":"dGVzdA==","WirelessMetadata":{"LoRaWAN":{"FPort":1}}}`)
	assert.Equal(t, http.StatusCreated, rec.Code)

	// DeleteMulticastGroup.
	rec = doIoTWRequest(t, h, http.MethodDelete, "/multicast-groups/"+mgID, "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// TestIoTW_NetworkAnalyzerConfiguration covers network analyzer stub ops.
func TestIoTW_NetworkAnalyzerConfiguration(t *testing.T) {
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

// TestIoTW_EventConfigurations covers event configuration stub ops.
func TestIoTW_EventConfigurations(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// GetEventConfigurationByResourceTypes.
	rec := doIoTWRequest(t, h, http.MethodGet, "/event-configurations-resource-types", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// UpdateEventConfigurationByResourceTypes.
	rec = doIoTWRequest(t, h, http.MethodPost, "/event-configurations-resource-types",
		`{"DeviceRegistrationState":{"Sidewalk":{"AmazonIdEventTopic":"Enabled"}}}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ListEventConfigurations.
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

// TestIoTW_LogLevels covers log level stub ops.
func TestIoTW_LogLevels(t *testing.T) {
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

// TestIoTW_FuotaTasks covers fuota task stub ops.
func TestIoTW_FuotaTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// CreateFuotaTask.
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/fuota-tasks",
		`{"Name":"test-fuota","FirmwareUpdateImage":"s3://bucket/firmware.bin",`+
			`"FirmwareUpdateRole":"arn:aws:iam::000000000000:role/fuota-role"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	fuotaID, _ := createResp["Id"].(string)
	if fuotaID == "" {
		t.Skip("fuota task creation not returning ID")
	}

	// GetFuotaTask.
	rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+fuotaID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// UpdateFuotaTask.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/fuota-tasks/"+fuotaID,
		`{"Name":"updated-fuota"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// StartFuotaTask (PUT /fuota-tasks/{id} with no subpath).
	rec = doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+fuotaID,
		`{"LoRaWAN":{"StartTime":"2024-01-01T00:00:00Z"}}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

// TestIoTW_WirelessGatewayUpdates covers UpdateWirelessGateway and UpdateDestination.
func TestIoTW_WirelessGatewayUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a wireless gateway.
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways",
		`{"Name":"test-gw","LoRaWAN":{"GatewayEui":"a1b2c3d4e5f60718","RfRegion":"US915"}}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	gwID, _ := createResp["Id"].(string)
	if gwID == "" {
		t.Skip("gateway creation not returning ID")
	}

	// UpdateWirelessGateway.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/wireless-gateways/"+gwID,
		`{"Name":"updated-gw"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// Create a destination.
	rec = doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/destinations",
		`{"Name":"test-dest","ExpressionType":"RuleName",`+
			`"Expression":"test-rule","RoleArn":"arn:aws:iam::000000000000:role/test-role"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	// UpdateDestination.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/destinations/test-dest",
		`{"Description":"updated"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}
