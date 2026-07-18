package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateDeviceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantStatus  int
	}{
		{
			name:        "create_device_profile",
			profileName: "my-device-profile",
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "create_with_empty_name",
			profileName: "",
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			body := `{"Name":"` + tt.profileName + `"}`
			rec := doIoTWRequest(t, h, http.MethodPost, "/device-profiles", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["Id"])
			assert.NotEmpty(t, resp["Arn"])
		})
	}
}

// TestHandler_DeviceProfile_RoundTrip verifies full DeviceProfile CRUD lifecycle.
func TestHandler_DeviceProfile_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
	}{
		{name: "lorawan_profile", profileName: "lorawan-dp"},
		{name: "sidewalk_profile", profileName: "sidewalk-dp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Create. Tags travel as []Tag{Key,Value}, matching real IoT
			// Wireless's wire shape (never a bare {"k":"v"} map).
			body := `{"Name":"` + tt.profileName + `","Tags":[{"Key":"env","Value":"test"}]}`
			rec := doIoTWRequest(t, h, http.MethodPost, "/device-profiles", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id := createResp["Id"].(string)
			require.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/device-profiles/"+id, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.profileName, getResp["Name"])
			assert.Equal(t, id, getResp["Id"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/device-profiles", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			list, ok := listResp["DeviceProfileList"].([]any)
			require.True(t, ok)
			assert.Len(t, list, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/device-profiles/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/device-profiles/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestHandler_DeleteDeviceProfile_NotFound verifies 404 is returned for non-existent device profiles.
func TestHandler_DeleteDeviceProfile_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodDelete, "/device-profiles/no-such-id", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateGetListDeleteServiceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantStatus  int
	}{
		{
			name:        "full_lifecycle",
			profileName: "my-profile",
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.profileName + `"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/service-profiles", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.profileName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			profiles, ok := listResp["ServiceProfileList"].([]any)
			require.True(t, ok)
			assert.Len(t, profiles, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
