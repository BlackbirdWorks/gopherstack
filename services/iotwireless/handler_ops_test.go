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

	// Getting the session before one has been started must 404, matching real
	// AWS's ResourceNotFoundException for a group with no active session.
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+id+"/session", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Start session
	rec = doIoTWRequest(t, h, http.MethodPut, "/multicast-groups/"+id+"/session", `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Now the session should be visible with a real LoRaWAN payload.
	rec = doIoTWRequest(t, h, http.MethodGet, "/multicast-groups/"+id+"/session", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var sessionResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sessionResp))
	_, hasLoRaWAN := sessionResp["LoRaWAN"]
	assert.True(t, hasLoRaWAN)
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
	assert.Equal(t, gwID, statsResp["WirelessGatewayId"])
}

func TestHandlerOps_GetWirelessGatewayStatistics_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/does-not-exist/statistics", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
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

	createResp := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN"}`)
	require.Equal(t, http.StatusCreated, createResp.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &created))
	devID := created["Id"].(string)

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+devID+"/statistics", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var stats map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stats))
	assert.Equal(t, devID, stats["WirelessDeviceId"])
}

func TestHandlerOps_GetWirelessDeviceStatistics_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/does-not-exist/statistics", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
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

	createResp := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN"}`)
	require.Equal(t, http.StatusCreated, createResp.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &created))
	devID := created["Id"].(string)

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices/"+devID+"/test", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "PASS", resp["Result"])
}

func TestHandlerOps_TestWirelessDevice_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices/does-not-exist/test", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
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

	// Get event config by resource types (never set): correct empty shape.
	rec := doIoTWRequest(t, h, http.MethodGet, "/event-configurations-resource-types", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var defaultResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &defaultResp))
	assert.NotContains(t, defaultResp, "DeviceRegistrationState")

	// Update event config by resource types
	rec = doIoTWRequest(t, h, http.MethodPost, "/event-configurations-resource-types",
		`{"DeviceRegistrationState":{"Sidewalk":{"AmazonIdEventTopic":"Enabled"}}}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get event config by resource types must now reflect the update, not a
	// fabricated always-empty response.
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations-resource-types", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &defaultResp))
	drs, ok := defaultResp["DeviceRegistrationState"].(map[string]any)
	require.True(t, ok)
	sidewalk, ok := drs["Sidewalk"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Enabled", sidewalk["AmazonIdEventTopic"])

	// List event configurations (none set yet)
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok := listResp["EventConfigurationsList"].([]any)
	require.True(t, ok)
	assert.Empty(t, configs)

	// Get resource event configuration (never set): correct empty shape.
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations/some-resource", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.NotContains(t, getResp, "Join")

	// Update resource event configuration
	rec = doIoTWRequest(t, h, http.MethodPatch,
		"/event-configurations/some-resource?identifierType=WirelessDeviceId",
		`{"Join":{"LoRaWAN":{"DevEuiEventTopic":"Enabled"}}}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get resource event configuration must now reflect the stored config.
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations/some-resource", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	join, ok := getResp["Join"].(map[string]any)
	require.True(t, ok)
	loRaWAN, ok := join["LoRaWAN"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Enabled", loRaWAN["DevEuiEventTopic"])

	// List event configurations must now reflect the stored resource config.
	rec = doIoTWRequest(
		t,
		h,
		http.MethodGet,
		"/event-configurations?resourceType=WirelessDevice",
		"",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok = listResp["EventConfigurationsList"].([]any)
	require.True(t, ok)
	require.Len(t, configs, 1)
	assert.Equal(t, "some-resource", configs[0].(map[string]any)["Identifier"])
}

// ============================================================
// Group 9 — Partner account operations
// ============================================================

func TestHandlerOps_PartnerAccounts(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Associate partner account. Real AWS binds this op to the bare
	// collection path (POST /partner-accounts): the partner account ID
	// travels as Sidewalk.AmazonId in the body, never as a path parameter.
	rec := doIoTWRequest(t, h, http.MethodPost, "/partner-accounts", `{"Sidewalk":{"AmazonId":"partner-123"}}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp))
	assert.NotEmpty(t, assocResp["Arn"])

	// Get partner account
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts/partner-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, true, getResp["AccountLinked"])
	sidewalk, ok := getResp["Sidewalk"].(map[string]any)
	require.True(t, ok, "AccountLinked accounts must include Sidewalk info")
	assert.Equal(t, "partner-123", sidewalk["AmazonId"])
	assert.NotEmpty(t, sidewalk["Arn"])

	// Get an account that was never associated: AccountLinked must be false,
	// not a fabricated success.
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts/never-linked", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var unlinkedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &unlinkedResp))
	assert.Equal(t, false, unlinkedResp["AccountLinked"])

	// List partner accounts
	rec = doIoTWRequest(t, h, http.MethodGet, "/partner-accounts", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sidewalkList, ok := listResp["Sidewalk"].([]any)
	require.True(t, ok)
	require.Len(
		t,
		sidewalkList,
		1,
		"list must reflect the associated account, not a hardcoded empty list",
	)
	assert.Equal(t, "partner-123", sidewalkList[0].(map[string]any)["AmazonId"])

	// Update partner account for a linked account succeeds.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/partner-accounts/partner-123", `{}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Update partner account for an account that was never linked must be a
	// real 404, not a fabricated success.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/partner-accounts/never-linked", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

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

	// Get task after deletion must be a real 404, not a fabricated PENDING
	// placeholder.
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+gwID+"/tasks", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var notFoundResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &notFoundResp))
	msg, _ := notFoundResp["Message"].(string)
	assert.Contains(t, msg, "ResourceNotFoundException")
	assert.NotContains(t, notFoundResp, "Status", "a stub response would fabricate a Status field")

	// Delete task definition
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateway-task-definitions/"+defID, "")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// TestHandlerOps_GetWirelessGatewayTask_NeverCreated verifies that a gateway
// which never had a task returns a real 404 rather than a fabricated PENDING
// task, for a gateway ID that was never associated with any task at all.
func TestHandlerOps_GetWirelessGatewayTask_NeverCreated(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/never-had-a-task/tasks", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ============================================================
// Group 11 — Position operations
// ============================================================

func TestHandlerOps_Position(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Get position (empty): no data ever submitted for this resource.
	rec := doIoTWRequest(t, h, http.MethodGet, "/positions/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	pos, ok := getResp["Position"].([]any)
	require.True(t, ok)
	assert.Empty(t, pos)
	assert.NotContains(
		t,
		getResp,
		"Accuracy",
		"Accuracy must be absent when no position data exists",
	)

	// Update position
	rec = doIoTWRequest(t, h, http.MethodPut, "/positions/resource-123",
		`{"Position":[47.6,-122.3,100.0]}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get position must now reflect the real, previously-submitted
	// coordinates, not a fabricated empty response.
	rec = doIoTWRequest(t, h, http.MethodGet, "/positions/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	pos, ok = getResp["Position"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{47.6, -122.3, 100.0}, pos)
	assert.InDelta(t, 0.0, getResp["Accuracy"], 0.0001,
		"Accuracy 0.0 signals position data is available, per AWS docs")

	// Get position configuration (never set): correct empty shape.
	rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getCfgResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCfgResp))
	assert.NotContains(t, getCfgResp, "Destination")

	// Put position configuration with a real ResourceType and Destination.
	rec = doIoTWRequest(t, h, http.MethodPut,
		"/position-configurations/resource-123?resourceType=WirelessDevice",
		`{"Destination":"my-position-dest","Solvers":{"SemtechGnss":{"Status":"Enabled"}}}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get position configuration must now reflect the stored destination.
	rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations/resource-123", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCfgResp))
	assert.Equal(t, "my-position-dest", getCfgResp["Destination"])
	solvers, ok := getCfgResp["Solvers"].(map[string]any)
	require.True(t, ok)
	semtech, ok := solvers["SemtechGnss"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Semtech", semtech["Provider"])
	assert.Equal(t, "GNSS", semtech["Type"])
	assert.Equal(t, "Enabled", semtech["Status"])

	// List position configurations, filtered by the matching resource type,
	// must reflect the stored entry rather than a hardcoded empty list.
	rec = doIoTWRequest(
		t,
		h,
		http.MethodGet,
		"/position-configurations?resourceType=WirelessDevice",
		"",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok := listResp["PositionConfigurationList"].([]any)
	require.True(t, ok)
	require.Len(t, configs, 1)
	assert.Equal(t, "resource-123", configs[0].(map[string]any)["ResourceIdentifier"])

	// Filtering by a non-matching resource type must exclude the entry.
	rec = doIoTWRequest(
		t,
		h,
		http.MethodGet,
		"/position-configurations?resourceType=WirelessGateway",
		"",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	configs, ok = listResp["PositionConfigurationList"].([]any)
	require.True(t, ok)
	assert.Empty(t, configs)
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

	// Send data to the device: this must enqueue a real downlink message,
	// not just return a message ID that vanishes into the void.
	rec = doIoTWRequest(t, h, http.MethodPost, "/wireless-devices/"+devID+"/data",
		`{"PayloadData":"aGVsbG8="}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var sendResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sendResp))
	sentMessageID, _ := sendResp["MessageId"].(string)
	require.NotEmpty(t, sentMessageID)

	// List queued messages must now reflect the sent message.
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+devID+"/data", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	msgs, ok = listResp["DownlinkQueueMessagesList"].([]any)
	require.True(t, ok)
	require.Len(t, msgs, 1)
	assert.Equal(t, sentMessageID, msgs[0].(map[string]any)["MessageId"])

	// Delete queued messages
	rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+devID+"/data", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// After deletion, the queue must be empty again.
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+devID+"/data", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	msgs, ok = listResp["DownlinkQueueMessagesList"].([]any)
	require.True(t, ok)
	assert.Empty(t, msgs)
}

// ============================================================
// Group 13-14 — Misc operations
// ============================================================

func TestHandlerOps_GetMetricConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Defaults to Enabled per AWS's documented default, not a bare "{}".
	rec := doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	summaryMetric, ok := getResp["SummaryMetric"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Enabled", summaryMetric["Status"])

	// Update must persist, not silently no-op.
	rec = doIoTWRequest(t, h, http.MethodPut, "/metric-configuration",
		`{"SummaryMetric":{"Status":"Disabled"}}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	summaryMetric, ok = getResp["SummaryMetric"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Disabled", summaryMetric["Status"])
}

// TestHandlerOps_GetMetrics_MapsQueriesToResults verifies that each requested
// query produces a corresponding result entry, rather than a
// query-count-independent hardcoded empty list.
func TestHandlerOps_GetMetrics_MapsQueriesToResults(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	body := `{"SummaryMetricQueries":[` +
		`{"QueryId":"q1","MetricName":"ConnectionCount"},` +
		`{"QueryId":"q2","MetricName":"UplinkCount"}]}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/metrics", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results, ok := resp["SummaryMetricQueryResults"].([]any)
	require.True(t, ok)
	require.Len(t, results, 2)
	assert.Equal(t, "q1", results[0].(map[string]any)["QueryId"])
	assert.Equal(t, "Succeeded", results[0].(map[string]any)["QueryStatus"])
	assert.Equal(t, "q2", results[1].(map[string]any)["QueryId"])
}

// TestHandlerOps_ListDevicesForWirelessDeviceImportTask verifies that
// supplying a real task ID reflects the task's DestinationName, and an
// unknown ID returns a real 404.
func TestHandlerOps_ListDevicesForWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task",
		`{"DestinationName":"dest-import"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id, _ := createResp["Id"].(string)
	require.NotEmpty(t, id)

	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task?id="+id, "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Equal(t, "dest-import", listResp["DestinationName"])

	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task?id=does-not-exist", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerOps_GetServiceEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Default (no serviceType query param) must be CUPS.
	rec := doIoTWRequest(t, h, http.MethodGet, "/service-endpoint", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "CUPS", resp["ServiceType"])
	assert.Equal(t, "https://cups.lorawan.us-east-1.amazonaws.com", resp["ServiceEndpoint"])

	// LNS must return a distinct, correctly-shaped endpoint, not the CUPS one.
	rec = doIoTWRequest(t, h, http.MethodGet, "/service-endpoint?serviceType=LNS", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var lnsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lnsResp))
	assert.Equal(t, "LNS", lnsResp["ServiceType"])
	assert.Equal(t, "https://lns.lorawan.us-east-1.amazonaws.com", lnsResp["ServiceEndpoint"])

	// An invalid serviceType must be rejected, not silently accepted.
	rec = doIoTWRequest(t, h, http.MethodGet, "/service-endpoint?serviceType=BOGUS", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerOps_GetWirelessGatewayFirmwareInformation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	createResp := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, createResp.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &created))
	gwID := created["Id"].(string)

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+gwID+"/firmware-information", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var info map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &info))
	loRaWAN, ok := info["LoRaWAN"].(map[string]any)
	require.True(t, ok)
	currentVersion, ok := loRaWAN["CurrentVersion"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, currentVersion["PackageVersion"])
}

func TestHandlerOps_GetWirelessGatewayFirmwareInformation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/does-not-exist/firmware-information", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
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
