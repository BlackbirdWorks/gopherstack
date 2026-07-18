package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateGetListDeleteWirelessGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		gatewayName string
		description string
		wantStatus  int
	}{
		{
			name:        "full_lifecycle",
			gatewayName: "my-gateway",
			description: "test gateway",
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "gateway_with_empty_description",
			gatewayName: "bare-gateway",
			description: "",
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.gatewayName + `","Description":"` + tt.description + `"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.gatewayName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			gateways, ok := listResp["WirelessGatewayList"].([]any)
			require.True(t, ok)
			assert.Len(t, gateways, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-gateways/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_AssociateWirelessGatewayWithThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		thingArn      string
		createGateway bool
		wantStatus    int
	}{
		{
			name:          "associate_existing_gateway",
			thingArn:      "arn:aws:iot:us-east-1:000000000000:thing/gw-thing",
			createGateway: true,
			wantStatus:    http.StatusNoContent,
		},
		{
			name:          "gateway_not_found",
			thingArn:      "arn:aws:iot:us-east-1:000000000000:thing/other-thing",
			createGateway: false,
			wantStatus:    http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			gwID := "no-such-gateway"

			if tt.createGateway {
				createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways",
					`{"Name":"gw-thing","Description":"thing gw"}`)
				require.Equal(t, http.StatusCreated, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				gwID = createResp["Id"].(string)
			}

			body := `{"ThingArn":"` + tt.thingArn + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+gwID+"/thing", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateWirelessGateway(t *testing.T) {
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

func TestHandler_DisassociateWirelessGatewayFromThing(t *testing.T) {
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

func TestHandler_GetWirelessGatewayStatistics(t *testing.T) {
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

func TestHandler_GetWirelessGatewayStatistics_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/does-not-exist/statistics", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetWirelessGatewayFirmwareInformation(t *testing.T) {
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

func TestHandler_GetWirelessGatewayFirmwareInformation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/does-not-exist/firmware-information", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GatewayFirmwareInformation(t *testing.T) {
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

// Test_GetWirelessGateway_ReflectsThingAssociation is the WirelessGateway
// analogue of Test_GetWirelessDevice_ReflectsThingAssociation.
func Test_GetWirelessGateway_ReflectsThingAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	id := created["Id"].(string)

	thingArn := "arn:aws:iot:us-east-1:000000000000:thing/gw-thing"
	assocRec := doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+id+"/thing",
		`{"ThingArn":"`+thingArn+`"}`)
	require.Equal(t, http.StatusNoContent, assocRec.Code)

	getRec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &after))
	assert.Equal(t, thingArn, after["ThingArn"])
	assert.Equal(t, "gw-thing", after["ThingName"])
}

// TestHandler_WirelessGatewayAndDestinationUpdates covers UpdateWirelessGateway and UpdateDestination.
func TestHandler_WirelessGatewayAndDestinationUpdates(t *testing.T) {
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
