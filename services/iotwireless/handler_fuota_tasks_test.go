package iotwireless_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_FuotaTask_LoRaWANRoundTrip verifies the LoRaWANFuotaTask
// (types.go:844) request field echoes correctly through GetFuotaTask's
// LoRaWANFuotaTaskGetInfo (types.go:853) response shape, and that Update
// replaces it wholesale (api_op_UpdateFuotaTask.go:64 uses the same
// LoRaWANFuotaTask type as create, not a merge-style narrower shape).
func TestHandler_FuotaTask_LoRaWANRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/fuota-tasks",
		`{"Name":"ft-rt","FirmwareUpdateImage":"s3://bucket/fw.bin",`+
			`"FirmwareUpdateRole":"arn:aws:iam::000000000000:role/fuota-role",`+
			`"LoRaWAN":{"RfRegion":"US915"}}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id, _ := createResp["Id"].(string)
	require.NotEmpty(t, id)

	rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	loRaWAN, ok := getResp["LoRaWAN"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "US915", loRaWAN["RfRegion"])
	_, hasStartTime := loRaWAN["StartTime"]
	assert.False(t, hasStartTime, "StartTime is never fabricated when unset")

	rec = doIoTWRequest(t, h, http.MethodPatch, "/fuota-tasks/"+id,
		`{"LoRaWAN":{"RfRegion":"EU868"}}`)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	loRaWAN, ok = getResp["LoRaWAN"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "EU868", loRaWAN["RfRegion"])
}

// TestHandler_UpdateFuotaTask_Fields verifies each of the six
// UpdateFuotaTaskInput members gopherstack previously dropped silently
// (api_op_UpdateFuotaTask.go:28: Descriptor, FirmwareUpdateImage,
// FirmwareUpdateRole, FragmentIntervalMS, FragmentSizeBytes,
// RedundancyPercent) actually lands in the stored FuotaTask and is returned
// by GetFuotaTask.
func TestHandler_UpdateFuotaTask_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantValue any
		name      string
		wantField string
		patchBody string
	}{
		{name: "descriptor", patchBody: `{"Descriptor":"AQID"}`, wantField: "Descriptor", wantValue: "AQID"},
		{
			name:      "firmware_update_image",
			patchBody: `{"FirmwareUpdateImage":"s3://bucket/v2.bin"}`,
			wantField: "FirmwareUpdateImage",
			wantValue: "s3://bucket/v2.bin",
		},
		{
			name:      "firmware_update_role",
			patchBody: `{"FirmwareUpdateRole":"arn:aws:iam::000000000000:role/r2"}`,
			wantField: "FirmwareUpdateRole",
			wantValue: "arn:aws:iam::000000000000:role/r2",
		},
		{
			name:      "fragment_interval_ms",
			patchBody: `{"FragmentIntervalMS":2000}`,
			wantField: "FragmentIntervalMS",
			wantValue: float64(2000),
		},
		{
			name:      "fragment_size_bytes",
			patchBody: `{"FragmentSizeBytes":256}`,
			wantField: "FragmentSizeBytes",
			wantValue: float64(256),
		},
		{
			name:      "redundancy_percent",
			patchBody: `{"RedundancyPercent":75}`,
			wantField: "RedundancyPercent",
			wantValue: float64(75),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			rec := doIoTWRequest(t, h, http.MethodPost, "/fuota-tasks",
				`{"Name":"ft-update","FirmwareUpdateImage":"s3://bucket/fw.bin",`+
					`"FirmwareUpdateRole":"arn:aws:iam::000000000000:role/r"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, _ := createResp["Id"].(string)
			require.NotEmpty(t, id)

			rec = doIoTWRequest(t, h, http.MethodPatch, "/fuota-tasks/"+id, tt.patchBody)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.wantValue, getResp[tt.wantField])
		})
	}
}

// TestHandler_StartFuotaTask_StartTime verifies StartFuotaTaskInput.LoRaWAN
// (types.LoRaWANStartFuotaTask, types.go:1202) sets GetFuotaTask's
// LoRaWANFuotaTaskGetInfo.StartTime (types.go:853), and that omitting it
// leaves StartTime unset rather than fabricated.
func TestHandler_StartFuotaTask_StartTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		startBody     string
		wantStartTime string
	}{
		{
			name:          "with_start_time",
			startBody:     `{"LoRaWAN":{"StartTime":"2026-01-01T12:00:00Z"}}`,
			wantStartTime: "2026-01-01T12:00:00Z",
		},
		{name: "without_start_time", startBody: `{}`, wantStartTime: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			rec := doIoTWRequest(t, h, http.MethodPost, "/fuota-tasks",
				`{"Name":"ft-start","FirmwareUpdateImage":"s3://bucket/fw.bin",`+
					`"FirmwareUpdateRole":"arn:aws:iam::000000000000:role/r"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, _ := createResp["Id"].(string)
			require.NotEmpty(t, id)

			rec = doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+id, tt.startBody)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			loRaWAN, hasLoRaWAN := getResp["LoRaWAN"].(map[string]any)

			if tt.wantStartTime == "" {
				if hasLoRaWAN {
					_, hasStart := loRaWAN["StartTime"]
					assert.False(t, hasStart, "StartTime must not be fabricated when omitted")
				}

				return
			}

			require.True(t, hasLoRaWAN)
			assert.Equal(t, tt.wantStartTime, loRaWAN["StartTime"])
		})
	}
}

func TestHandler_CreateFuotaTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskName    string
		description string
		wantStatus  int
	}{
		{
			name:        "create_fuota_task",
			taskName:    "my-fuota-task",
			description: "firmware update task",
			wantStatus:  http.StatusCreated,
		},
		{
			name:       "create_without_description",
			taskName:   "bare-fuota-task",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"Name":"` + tt.taskName + `","Description":"` + tt.description +
				`","FirmwareUpdateImage":"s3://bucket/fw.bin","FirmwareUpdateRole":"arn:aws:iam::000000000000:role/r"}`
			rec := doIoTWRequest(t, h, http.MethodPost, "/fuota-tasks", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["Id"])
			assert.NotEmpty(t, resp["Arn"])
		})
	}
}

func TestHandler_AssociateMulticastGroupWithFuotaTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		fuotaTaskID      string
		multicastGroupID string
		wantStatus       int
	}{
		{
			name:             "associate_multicast_group",
			fuotaTaskID:      "fuota-task-001",
			multicastGroupID: "multicast-group-001",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"MulticastGroupId":"` + tt.multicastGroupID + `"}`
			// AssociateMulticastGroupWithFuotaTask PUTs the singular path;
			// iotwireless@v1.59.4 serializers.go:140.
			rec := doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+tt.fuotaTaskID+"/multicast-group", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateWirelessDeviceWithFuotaTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		fuotaTaskID      string
		wirelessDeviceID string
		wantStatus       int
	}{
		{
			name:             "associate_wireless_device",
			fuotaTaskID:      "fuota-task-002",
			wirelessDeviceID: "dev-001",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"WirelessDeviceId":"` + tt.wirelessDeviceID + `"}`
			// AssociateWirelessDeviceWithFuotaTask PUTs the singular path;
			// iotwireless@v1.59.4 serializers.go:234.
			rec := doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+tt.fuotaTaskID+"/wireless-device", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListMulticastGroupsByFuotaTask(t *testing.T) {
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

	// Associate multicast group with FUOTA task; AssociateMulticastGroupWithFuotaTask
	// PUTs the singular path (iotwireless@v1.59.4 serializers.go:140).
	rec = doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+ftID+"/multicast-group",
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

func TestHandler_FuotaTaskUpdate(t *testing.T) {
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

func TestHandler_StartFuotaTask(t *testing.T) {
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

func TestHandler_DisassociateWirelessDeviceFromFuotaTask(t *testing.T) {
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

	// Associate device with FUOTA task; AssociateWirelessDeviceWithFuotaTask
	// PUTs the singular path (iotwireless@v1.59.4 serializers.go:234).
	rec = doIoTWRequest(t, h, http.MethodPut, "/fuota-tasks/"+ftID+"/wireless-device",
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

// TestHandler_FuotaTask_RoundTrip verifies full FuotaTask CRUD lifecycle.
func TestHandler_FuotaTask_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskName    string
		description string
	}{
		{name: "basic_task", taskName: "fw-update-task", description: "firmware update"},
		{name: "no_description", taskName: "fw-task-minimal", description: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.taskName + `","Description":"` + tt.description +
				`","FirmwareUpdateImage":"s3://bucket/fw.bin","FirmwareUpdateRole":"arn:aws:iam::000000000000:role/r"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/fuota-tasks", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id := createResp["Id"].(string)
			require.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.taskName, getResp["Name"])
			assert.Equal(t, id, getResp["Id"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			list, ok := listResp["FuotaTaskList"].([]any)
			require.True(t, ok)
			assert.Len(t, list, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/fuota-tasks/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestHandler_DeleteFuotaTask_NotFound verifies 404 is returned for non-existent FUOTA tasks.
func TestHandler_DeleteFuotaTask_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodDelete, "/fuota-tasks/no-such-id", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_FuotaTasks_FullLifecycle covers fuota task stub ops.
func TestHandler_FuotaTasks_FullLifecycle(t *testing.T) {
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
