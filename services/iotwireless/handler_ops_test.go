package iotwireless_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// ============================================================
// Group 1 — Multicast Group operations
// ============================================================

func TestHandlerOps_MulticastGroupCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create
	body := `{"Name":"mg1","Description":"test group"}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", body)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id, ok := createResp["Id"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, id)

	arn, _ := createResp["Arn"].(string)
	assert.Contains(t, arn, "MulticastGroup")

	// Get
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+id, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "mg1", getResp["Name"])
	assert.Equal(t, id, getResp["Id"])

	// List
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	groups, ok := listResp["MulticastGroupList"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 1)

	// Update
	rec = doIoTWRequest(t, h, http.MethodPatch, "/multicast-groups/"+id, `{"Name":"mg1-updated"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify update
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+id, "")
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "mg1-updated", getResp["Name"])

	// Delete
	rec = doIoTWRequest(t, h, http.MethodDelete, "/multicast-groups/"+id, "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get after delete should 404
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+id, "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerOps_MulticastGroupSession(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a multicast group first
	rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", `{"Name":"sg1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Id"].(string)

	// Get session (should return empty LoRaWAN)
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+id+"/session", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var sessionResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sessionResp))
	_, hasLoRaWAN := sessionResp["LoRaWAN"]
	assert.True(t, hasLoRaWAN)

	// Start session
	rec = doIoTWRequest(t, h, http.MethodPut, "/multicast-groups/"+id+"/session", `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandlerOps_ListMulticastGroupsByFuotaTask(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a FUOTA task
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/fuota-tasks",
		`{"Name":"ft1","FirmwareUpdateImage":"s3://bucket/image","FirmwareUpdateRole":"arn:aws:iam::123:role/r"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var ftResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ftResp))
	ftID := ftResp["Id"].(string)

	// Create a multicast group
	rec = doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", `{"Name":"mg1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var mgResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mgResp))
	mgID := mgResp["Id"].(string)

	// Associate multicast group with FUOTA task
	rec = doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+ftID+"/multicast-groups",
		fmt.Sprintf(`{"MulticastGroupId":%q}`, mgID))
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List multicast groups by fuota task
	rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+ftID+"/multicast-groups", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	groups, ok := listResp["MulticastGroupList"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 1)

	// Disassociate multicast group from fuota task
	rec = doIoTWRequest(t, h, http.MethodDelete, "/fuota-tasks/"+ftID+"/multicast-groups/"+mgID, "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List again should return empty
	rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+ftID+"/multicast-groups", "")
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	groups = listResp["MulticastGroupList"].([]any)
	assert.Empty(t, groups)
}

func TestHandlerOps_DisassociateWirelessDeviceFromMulticastGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create multicast group
	rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", `{"Name":"mg1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var mgResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mgResp))
	mgID := mgResp["Id"].(string)

	// Create wireless device
	rec = doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var devResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &devResp))
	devID := devResp["Id"].(string)

	// Associate device with multicast group
	rec = doIoTWRequest(t, h, http.MethodPut, "/multicast-groups/"+mgID+"/wireless-devices",
		fmt.Sprintf(`{"WirelessDeviceId":%q}`, devID))
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Disassociate device from multicast group
	rec = doIoTWRequest(
		t,
		h,
		http.MethodDelete,
		"/multicast-groups/"+mgID+"/wireless-devices/"+devID,
		"",
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// Group 2 — Network Analyzer operations
// ============================================================

func TestHandlerOps_NetworkAnalyzerCRUD(t *testing.T) {
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

// ============================================================
// Group 3 — FuotaTask operations
// ============================================================

func TestHandlerOps_FuotaTaskUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a FUOTA task
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/fuota-tasks",
		`{"Name":"ft1","FirmwareUpdateImage":"s3://bucket/image","FirmwareUpdateRole":"arn:aws:iam::123:role/r"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Id"].(string)

	// Update
	rec = doIoTWRequest(t, h, http.MethodPatch, "/fuota-tasks/"+id,
		`{"Name":"ft1-updated","Description":"updated desc"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify
	rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "ft1-updated", getResp["Name"])
}

func TestHandlerOps_StartFuotaTask(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a FUOTA task
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/fuota-tasks",
		`{"Name":"ft1","FirmwareUpdateImage":"s3://bucket/image","FirmwareUpdateRole":"arn:aws:iam::123:role/r"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Id"].(string)

	// Start the task
	rec = doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+id, `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandlerOps_DisassociateWirelessDeviceFromFuotaTask(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a FUOTA task
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/fuota-tasks",
		`{"Name":"ft1","FirmwareUpdateImage":"s3://bucket/image","FirmwareUpdateRole":"arn:aws:iam::123:role/r"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	ftID := createResp["Id"].(string)

	// Create a device
	rec = doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var devResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &devResp))
	devID := devResp["Id"].(string)

	// Associate device with FUOTA task
	rec = doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+ftID+"/wireless-devices",
		fmt.Sprintf(`{"WirelessDeviceId":%q}`, devID))
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Disassociate device
	rec = doIoTWRequest(
		t,
		h,
		http.MethodDelete,
		"/fuota-tasks/"+ftID+"/wireless-devices/"+devID,
		"",
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// Group 4 — WirelessGateway misc operations
// ============================================================

func TestHandlerOps_UpdateWirelessGateway(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create gateway
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/wireless-gateways",
		`{"Name":"gw1","Description":"original"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Id"].(string)

	// Update
	rec = doIoTWRequest(
		t,
		h,
		http.MethodPatch,
		"/wireless-gateways/"+id,
		`{"Name":"gw1-updated","Description":"new desc"}`,
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+id, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "gw1-updated", getResp["Name"])
}

func TestHandlerOps_GatewayCertificateLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create gateway
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	gwID := createResp["Id"].(string)

	// Associate certificate
	rec = doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+gwID+"/certificate",
		`{"IotCertificateId":"cert-123"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get certificate
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+gwID+"/certificate", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var certResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &certResp))
	assert.Equal(t, "cert-123", certResp["IotCertificateId"])

	// Disassociate certificate
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateways/"+gwID+"/certificate", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandlerOps_DisassociateWirelessGatewayFromThing(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create gateway
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	gwID := createResp["Id"].(string)

	// Associate with thing
	rec = doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+gwID+"/thing",
		`{"ThingArn":"arn:aws:iot:us-east-1:123:thing/my-thing"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Disassociate from thing
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateways/"+gwID+"/thing", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandlerOps_GetWirelessGatewayStatistics(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create gateway
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	gwID := createResp["Id"].(string)

	// Get statistics
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+gwID+"/statistics", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var statsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statsResp))
	assert.Equal(t, "Connected", statsResp["ConnectionStatus"])
}

// ============================================================
// Group 5 — WirelessDevice misc operations
// ============================================================

func TestHandlerOps_UpdateWirelessDevice(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create device
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN","DestinationName":"dest1","Description":"original"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Id"].(string)

	// Update
	rec = doIoTWRequest(t, h, http.MethodPatch, "/wireless-devices/"+id,
		`{"Name":"dev1-updated","Description":"new desc"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "dev1-updated", getResp["Name"])
}

func TestHandlerOps_DeregisterWirelessDevice(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create device
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Id"].(string)

	// Deregister (should delete the device)
	rec = doIoTWRequest(t, h, http.MethodPatch, "/wireless-devices/"+id+"/deregister", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get should 404
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerOps_DisassociateWirelessDeviceFromThing(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create device
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	devID := createResp["Id"].(string)

	// Associate with thing
	rec = doIoTWRequest(t, h, http.MethodPut, "/wireless-devices/"+devID+"/thing",
		`{"ThingArn":"arn:aws:iot:us-east-1:123:thing/my-thing"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Disassociate from thing
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+devID+"/thing", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandlerOps_GetWirelessDeviceStatistics(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/some-id/statistics", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerOps_SendDataToWirelessDevice(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices/some-id/data",
		`{"PayloadData":"aGVsbG8=","WirelessMetadata":{}}`)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["MessageId"])
}

func TestHandlerOps_TestWirelessDevice(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices/some-id/test", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "PASS", resp["Result"])
}

// ============================================================
// Group 6 — Destination update
// ============================================================

func TestHandlerOps_UpdateDestination(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create destination
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/destinations",
		`{"Name":"dest1","Expression":"rule/test","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::123:role/r"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Update
	rec = doIoTWRequest(
		t,
		h,
		http.MethodPatch,
		"/destinations/dest1",
		`{"Expression":"rule/updated","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::123:role/r2","Description":"desc"}`,
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify
	rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/dest1", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "rule/updated", getResp["Expression"])
}

// ============================================================
// Group 7 — Log level operations
// ============================================================

func TestHandlerOps_LogLevels(t *testing.T) {
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

// ============================================================
// Group 8 — Event configuration operations
// ============================================================

func TestHandlerOps_EventConfigurations(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Get event config by resource types
	rec := doIoTWRequest(t, h, http.MethodGet, "/event-configurations-resource-types", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update event config by resource types
	rec = doIoTWRequest(t, h, http.MethodPost, "/event-configurations-resource-types", `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List event configurations
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok := listResp["EventConfigurationsList"].([]any)
	require.True(t, ok)
	assert.Empty(t, configs)

	// Get resource event configuration
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations/some-resource", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update resource event configuration
	rec = doIoTWRequest(t, h, http.MethodPatch, "/event-configurations/some-resource", `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// Group 9 — Partner account operations
// ============================================================

func TestHandlerOps_PartnerAccounts(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Associate partner account
	rec := doIoTWRequest(t, h, http.MethodPut, "/partner-accounts/partner-123", `{"Tags":{}}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp))
	assert.NotEmpty(t, assocResp["Arn"])

	// Get partner account
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts/partner-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "partner-123", getResp["AccountId"])

	// List partner accounts
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update partner account (no-op)
	rec = doIoTWRequest(t, h, http.MethodPatch, "/partner-accounts/partner-123", `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Disassociate partner account
	rec = doIoTWRequest(t, h, http.MethodDelete, "/partner-accounts/partner-123", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// Group 10 — Gateway task operations
// ============================================================

func TestHandlerOps_GatewayTaskLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create gateway
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var gwResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gwResp))
	gwID := gwResp["Id"].(string)

	// Create task definition
	rec = doIoTWRequest(t, h, http.MethodPost, "/wireless-gateway-task-definitions",
		`{"Name":"taskdef1","AutoCreateTasks":false}`)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var defResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &defResp))
	defID := defResp["Id"].(string)
	assert.NotEmpty(t, defID)

	// Create task on gateway
	rec = doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways/"+gwID+"/tasks",
		fmt.Sprintf(`{"WirelessGatewayTaskDefinitionId":%q}`, defID))
	assert.Equal(t, http.StatusCreated, rec.Code)

	var taskResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &taskResp))
	assert.Equal(t, "PENDING", taskResp["Status"])

	// Get task
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+gwID+"/tasks", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getTaskResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getTaskResp))
	assert.Equal(t, "PENDING", getTaskResp["Status"])

	// List task definitions
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get task definition
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateway-task-definitions/"+defID, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete task
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateways/"+gwID+"/tasks", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Delete task definition
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateway-task-definitions/"+defID, "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// Group 11 — Position operations
// ============================================================

func TestHandlerOps_Position(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Get position (empty)
	rec := doIoTWRequest(t, h, http.MethodGet, "/positions/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	pos, ok := getResp["Position"].([]any)
	require.True(t, ok)
	assert.Empty(t, pos)

	// Update position
	rec = doIoTWRequest(t, h, http.MethodPut, "/positions/resource-123",
		`{"Position":[47.6,-122.3,100.0]}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get position configuration (empty struct)
	rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Put position configuration
	rec = doIoTWRequest(t, h, http.MethodPut, "/position-configurations/resource-123", `{"SolverType":"GNSS"}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List position configurations — must include the stored config.
	rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok := listResp["PositionConfigurationList"].([]any)
	require.True(t, ok)
	require.Len(t, configs, 1, "stored position config must appear in list")
	cfg0, ok := configs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "resource-123", cfg0["ResourceIdentifier"])
}

// ============================================================
// Group 12 — Queued messages operations
// ============================================================

func TestHandlerOps_QueuedMessages(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a device
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var devResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &devResp))
	devID := devResp["Id"].(string)

	// List queued messages (empty)
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+devID+"/data", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	msgs, ok := listResp["DownlinkQueueMessagesList"].([]any)
	require.True(t, ok)
	assert.Empty(t, msgs)

	// Delete queued messages
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+devID+"/data", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ============================================================
// Group 13-14 — Misc operations
// ============================================================

func TestHandlerOps_GetMetricConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerOps_GetServiceEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/service-endpoint", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "CUPS", resp["ServiceType"])
	assert.NotEmpty(t, resp["ServiceEndpoint"])
}

func TestHandlerOps_GetWirelessGatewayFirmwareInformation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/gw-id/firmware-information", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerOps_NotFoundErrors verifies that not-found errors return 404.
func TestHandlerOps_NotFoundErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	tests := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/multicast-groups/nonexistent", ""},
		{http.MethodDelete, "/multicast-groups/nonexistent", ""},
		{http.MethodPatch, "/multicast-groups/nonexistent", `{"Name":"x"}`},
		{http.MethodGet, "/network-analyzer-configurations/nonexistent", ""},
		{http.MethodDelete, "/network-analyzer-configurations/nonexistent", ""},
		{http.MethodGet, "/fuota-tasks/nonexistent", ""},
		{http.MethodPatch, "/fuota-tasks/nonexistent", `{"Name":"x"}`},
		{http.MethodPut, "/fuota-tasks/nonexistent", `{}`},
		{http.MethodGet, "/wireless-devices/nonexistent", ""},
		{http.MethodDelete, "/wireless-devices/nonexistent", ""},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_"+tt.path, func(t *testing.T) {
			t.Parallel()
			rec := doIoTWRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestHandlerOps_BackendReset verifies that Reset() clears all new backend state.
func TestHandlerOps_BackendReset(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	// Add some data
	_, err := bk.CreateMulticastGroup(testAccountID, testRegion, "mg1", "", nil)
	require.NoError(t, err)

	err = bk.PutResourceLogLevel("res1", "DEBUG")
	require.NoError(t, err)

	_, err = bk.CreateWirelessGatewayTaskDefinition("000000000000", "us-east-1", "def1", false)
	require.NoError(t, err)

	// Reset
	bk.Reset()

	// Verify cleared
	groups := bk.ListMulticastGroups(testAccountID, testRegion)
	assert.Empty(t, groups)

	level := bk.GetResourceLogLevel("res1")
	assert.Equal(t, "INFO", level)

	taskDefs := bk.ListWirelessGatewayTaskDefinitions()
	assert.Empty(t, taskDefs)
}
