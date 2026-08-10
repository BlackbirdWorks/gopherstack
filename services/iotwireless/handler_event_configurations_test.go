package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_EventConfigurations(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Get event config by resource types (never set): correct empty shape.
	rec := doIoTWRequest(t, h, http.MethodGet, "/event-configurations-resource-types", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var defaultResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &defaultResp))
	assert.NotContains(t, defaultResp, "DeviceRegistrationState")

	// Update event config by resource types (PATCH, not POST -- iotwireless@v1.59.4
	// serializers.go:8146).
	rec = doIoTWRequest(t, h, http.MethodPatch, "/event-configurations-resource-types",
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

// TestHandler_EventConfigurations_StatusOnly covers event configuration stub ops.
func TestHandler_EventConfigurations_StatusOnly(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// GetEventConfigurationByResourceTypes.
	rec := doIoTWRequest(t, h, http.MethodGet, "/event-configurations-resource-types", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// UpdateEventConfigurationByResourceTypes.
	rec = doIoTWRequest(t, h, http.MethodPatch, "/event-configurations-resource-types",
		`{"DeviceRegistrationState":{"Sidewalk":{"AmazonIdEventTopic":"Enabled"}}}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ListEventConfigurations.
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestHandler_EventConfigurationByResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Update global event config (PATCH /event-configurations-resource-types).
	rec := doIoTWRequest(t, h, http.MethodPatch, "/event-configurations-resource-types",
		`{"DeviceRegistrationState":{"Sidewalk":{"AmazonIdEventTopic":"Enabled"}}}`)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Retrieve (GET /event-configurations-resource-types).
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations-resource-types", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["DeviceRegistrationState"]
	assert.True(t, ok, "persisted event config must be returned")
}

func TestHandler_ResourceEventConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		body       string
		wantField  string
	}{
		{
			name:       "device_event_config",
			resourceID: "device-aaa",
			body:       `{"DeviceRegistrationState":{"Sidewalk":{"AmazonIdEventTopic":"Enabled"}}}`,
			wantField:  "DeviceRegistrationState",
		},
		{
			name:       "gateway_event_config",
			resourceID: "gateway-bbb",
			body:       `{"ConnectionStatus":{"LoRaWAN":{}}}`,
			wantField:  "ConnectionStatus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Update per-resource event config (PATCH /event-configurations/{id}).
			rec := doIoTWRequest(t, h, http.MethodPatch,
				"/event-configurations/"+tt.resourceID, tt.body)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// Get it back (GET /event-configurations/{id}).
			rec = doIoTWRequest(t, h, http.MethodGet,
				"/event-configurations/"+tt.resourceID, "")
			require.Equal(t, http.StatusOK, rec.Code)

			// AWS's GetResourceEventConfiguration response has no Identifier
			// field (it echoes only the event-type configuration); confirm the
			// stored per-resource event config round-trips instead.
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, ok := resp[tt.wantField]
			assert.True(t, ok, "persisted event config field %q must be returned", tt.wantField)
		})
	}
}

func TestHandler_ListEventConfigurations(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Before any updates: list is empty.
	rec := doIoTWRequest(t, h, http.MethodGet, "/event-configurations", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var pre map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pre))
	assert.Empty(t, pre["EventConfigurationsList"])

	// Update config for two resources.
	for _, id := range []string{"res-1", "res-2"} {
		r := doIoTWRequest(t, h, http.MethodPatch, "/event-configurations/"+id,
			`{"ConnectionStatus":{}}`)
		require.Equal(t, http.StatusNoContent, r.Code)
	}

	// List must now contain two entries.
	rec = doIoTWRequest(t, h, http.MethodGet, "/event-configurations", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var post map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &post))
	list, ok := post["EventConfigurationsList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)
}
