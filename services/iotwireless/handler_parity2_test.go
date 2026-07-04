package iotwireless_test

// handler_parity2_test.go — table-driven parity tests for go-41k71 fixes.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// MetricConfiguration — persist and retrieve
// ============================================================

func TestParity2_MetricConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantKV string // top-level key to check in GET response
	}{
		{
			name:   "enable_summary_metric",
			body:   `{"SummaryMetric":{"Status":"Enable"}}`,
			wantKV: "SummaryMetric",
		},
		{
			name:   "disable_summary_metric",
			body:   `{"SummaryMetric":{"Status":"Disable"}}`,
			wantKV: "SummaryMetric",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Update metric configuration (PUT /metric-configuration).
			rec := doIoTWRequest(t, h, http.MethodPut, "/metric-configuration", tt.body)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// Retrieve and verify persisted.
			rec = doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, ok := resp[tt.wantKV]
			assert.True(t, ok, "expected key %q in response", tt.wantKV)
		})
	}
}

// ============================================================
// EventConfiguration — global and per-resource
// ============================================================

func TestParity2_EventConfigurationByResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Update global event config (POST /event-configurations-resource-types).
	rec := doIoTWRequest(t, h, http.MethodPost, "/event-configurations-resource-types",
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

func TestParity2_ResourceEventConfiguration(t *testing.T) {
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

func TestParity2_ListEventConfigurations(t *testing.T) {
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

// ============================================================
// PositionConfiguration — persist and list
// ============================================================

func TestParity2_PositionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		body       string
		wantKey    string
	}{
		{
			name:       "gnss_solver",
			resourceID: "loc-aaa",
			body:       `{"Destination":"dest-1","Solvers":{"SemtechGnss":{"Status":"Enabled"}}}`,
			wantKey:    "Solvers",
		},
		{
			name:       "combined_solver",
			resourceID: "loc-bbb",
			body:       `{"Destination":"dest-2","Solvers":{"SemtechGnss":{"Status":"Enabled"}}}`,
			wantKey:    "Solvers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Put position config (PUT /position-configurations/{id}).
			rec := doIoTWRequest(t, h, http.MethodPut,
				"/position-configurations/"+tt.resourceID, tt.body)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// Get it back.
			rec = doIoTWRequest(t, h, http.MethodGet,
				"/position-configurations/"+tt.resourceID, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, ok := resp[tt.wantKey]
			assert.True(t, ok, "expected key %q in position config response", tt.wantKey)

			// List — must include this resource.
			rec = doIoTWRequest(t, h, http.MethodGet, "/position-configurations", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			cfgs, ok := listResp["PositionConfigurationList"].([]any)
			require.True(t, ok)
			require.Len(t, cfgs, 1)
			cfg0 := cfgs[0].(map[string]any)
			assert.Equal(t, tt.resourceID, cfg0["ResourceIdentifier"])
		})
	}
}

// ============================================================
// QueuedMessages — enqueue via SendData, list per device
// ============================================================

func TestParity2_QueuedMessages(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create device (POST /wireless-devices).
	createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev-q","Type":"LoRaWAN","DestinationName":"dst"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	devID, ok := createResp["Id"].(string)
	require.True(t, ok)

	// Before any send: queue is empty.
	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+devID+"/data", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var pre map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pre))
	assert.Empty(t, pre["DownlinkQueueMessagesList"])

	// Send two messages (POST /wireless-devices/{id}/data).
	for _, payload := range []string{"AQID", "BAUG"} {
		r := doIoTWRequest(t, h, http.MethodPost,
			"/wireless-devices/"+devID+"/data",
			`{"PayloadData":"`+payload+`","WirelessMetadata":{}}`)
		require.Equal(t, http.StatusCreated, r.Code)

		var sr map[string]any
		require.NoError(t, json.Unmarshal(r.Body.Bytes(), &sr))
		assert.NotEmpty(t, sr["MessageId"], "SendData must return a MessageId")
	}

	// List — must return 2 queued messages.
	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+devID+"/data", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var post map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &post))
	msgs, ok := post["DownlinkQueueMessagesList"].([]any)
	require.True(t, ok)
	assert.Len(t, msgs, 2, "two enqueued messages must appear in list")

	msg0 := msgs[0].(map[string]any)
	assert.NotEmpty(t, msg0["MessageId"])
}

// ============================================================
// GatewayFirmwareInformation — real LoRaWAN version fields
// ============================================================

func TestParity2_GatewayFirmwareInformation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create a gateway (POST /wireless-gateways).
	gwRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways",
		`{"Name":"gw-fw","LoRaWAN":{}}`)
	require.Equal(t, http.StatusCreated, gwRec.Code)

	var gwResp map[string]any
	require.NoError(t, json.Unmarshal(gwRec.Body.Bytes(), &gwResp))
	gwID, ok := gwResp["Id"].(string)
	require.True(t, ok)

	// Get firmware information (GET /wireless-gateways/{id}/firmware-information).
	rec := doIoTWRequest(t, h, http.MethodGet,
		"/wireless-gateways/"+gwID+"/firmware-information", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	loRaWAN, ok := resp["LoRaWAN"].(map[string]any)
	require.True(t, ok, "response must have LoRaWAN field")
	_, ok = loRaWAN["CurrentVersion"]
	assert.True(t, ok, "LoRaWAN.CurrentVersion must be present")
}

// ============================================================
// GatewayTaskDefinition — 404 on unknown ID (not stub fallback)
// ============================================================

func TestParity2_GatewayTaskDefinitionNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// GET /wireless-gateway-task-definitions/{id} with unknown ID must 404.
	rec := doIoTWRequest(t, h, http.MethodGet,
		"/wireless-gateway-task-definitions/no-such-id", "")
	assert.Equal(t, http.StatusNotFound, rec.Code, "unknown task definition must return 404")
}

// ============================================================
// GetServiceEndpoint — region-aware, service-type-aware
// ============================================================

func TestParity2_GetServiceEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		queryParam      string
		wantServiceType string
		wantSubstr      string
	}{
		{
			name:            "cups",
			queryParam:      "?serviceType=CUPS",
			wantServiceType: "CUPS",
			wantSubstr:      "cups.lorawan",
		},
		{
			name:            "lns",
			queryParam:      "?serviceType=LNS",
			wantServiceType: "LNS",
			wantSubstr:      "lns.lorawan",
		},
		{
			name:            "default_no_param",
			queryParam:      "",
			wantServiceType: "CUPS",
			wantSubstr:      "cups.lorawan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			rec := doIoTWRequest(t, h, http.MethodGet, "/service-endpoint"+tt.queryParam, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantServiceType, resp["ServiceType"])
			endpoint, _ := resp["ServiceEndpoint"].(string)
			assert.Contains(t, endpoint, tt.wantSubstr)
			assert.Contains(t, endpoint, testRegion)
		})
	}
}
