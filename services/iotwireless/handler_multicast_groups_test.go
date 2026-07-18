package iotwireless_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AssociateWirelessDeviceWithMulticastGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		multicastGroupID string
		wirelessDeviceID string
		wantStatus       int
	}{
		{
			name:             "associate_wireless_device",
			multicastGroupID: "multicast-group-002",
			wirelessDeviceID: "dev-002",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"WirelessDeviceId":"` + tt.wirelessDeviceID + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut,
				"/multicast-groups/"+tt.multicastGroupID+"/wireless-devices", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CancelMulticastGroupSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		multicastGroupID string
		wantStatus       int
	}{
		{
			name:             "cancel_existing_session",
			multicastGroupID: "multicast-group-session-01",
			wantStatus:       http.StatusNoContent,
		},
		{
			name:             "cancel_nonexistent_session_is_idempotent",
			multicastGroupID: "nonexistent-group",
			wantStatus:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodDelete, "/multicast-groups/"+tt.multicastGroupID+"/session", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_MulticastGroupCRUD(t *testing.T) {
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

func TestHandler_MulticastGroupSession(t *testing.T) {
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

func TestHandler_DisassociateWirelessDeviceFromMulticastGroup(t *testing.T) {
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

func TestHandler_SendDataToMulticastGroup_UniqueMessageID(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a multicast group first.
	rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", `{"Name":"mg-send"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	mgID := createResp["Id"].(string)

	seen := make(map[string]bool)

	for range 5 {
		rec = doIoTWRequest(t, h, http.MethodPost,
			"/multicast-groups/"+mgID+"/data",
			`{"PayloadData":"aGVsbG8="}`)
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		msgID, _ := resp["MessageId"].(string)
		assert.NotEmpty(t, msgID)
		assert.False(t, seen[msgID], "MessageId should be unique: %s", msgID)
		seen[msgID] = true
	}
}

func TestHandler_MulticastGroup_BulkOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "bulk_associate",
			method: http.MethodPatch,
			path:   "/multicast-groups/{id}/bulk",
		},
		{
			name:   "bulk_disassociate",
			method: http.MethodPost,
			path:   "/multicast-groups/{id}/bulk",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Create a multicast group.
			rec := doIoTWRequest(t, h, http.MethodPost, "/multicast-groups", `{"Name":"mg-bulk"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			mgID := createResp["Id"].(string)

			path := "/multicast-groups/" + mgID + "/bulk"
			rec = doIoTWRequest(t, h, tt.method, path, `{}`)
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

// TestHandler_MulticastGroups_FullLifecycle covers multicast group stub operations.
func TestHandler_MulticastGroups_FullLifecycle(t *testing.T) {
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
