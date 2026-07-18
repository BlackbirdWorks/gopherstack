package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestHandler_CreateGetListDeleteWirelessDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceName string
		devType    string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			deviceName: "my-device",
			devType:    "LoRaWAN",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.deviceName + `","Type":"` + tt.devType + `","DestinationName":"d1"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.deviceName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			devices, ok := listResp["WirelessDeviceList"].([]any)
			require.True(t, ok)
			assert.Len(t, devices, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_GetWirelessDevice_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "no_such_device", id: "does-not-exist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+tt.id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_AssociateWirelessDeviceWithThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		thingArn   string
		createDev  bool
		wantStatus int
	}{
		{
			name:       "associate_existing_device",
			thingArn:   "arn:aws:iot:us-east-1:000000000000:thing/my-thing",
			createDev:  true,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "device_not_found",
			thingArn:   "arn:aws:iot:us-east-1:000000000000:thing/other-thing",
			createDev:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			devID := "no-such-device"

			if tt.createDev {
				createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
					`{"Name":"dev-thing","Type":"LoRaWAN","DestinationName":"d"}`)
				require.Equal(t, http.StatusCreated, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				devID = createResp["Id"].(string)
			}

			body := `{"ThingArn":"` + tt.thingArn + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/wireless-devices/"+devID+"/thing", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateWirelessDevice(t *testing.T) {
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

func TestHandler_DeregisterWirelessDevice(t *testing.T) {
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

func TestHandler_DisassociateWirelessDeviceFromThing(t *testing.T) {
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

func TestHandler_GetWirelessDeviceStatistics(t *testing.T) {
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

func TestHandler_GetWirelessDeviceStatistics_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/does-not-exist/statistics", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_SendDataToWirelessDevice(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices/some-id/data",
		`{"PayloadData":"aGVsbG8=","WirelessMetadata":{}}`)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["MessageId"])
}

func TestHandler_TestWirelessDevice(t *testing.T) {
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

func TestHandler_TestWirelessDevice_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices/does-not-exist/test", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_QueuedMessages_SendListDelete(t *testing.T) {
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

func TestHandler_SendDataToWirelessDevice_UniqueMessageID(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	dev := setupWirelessDevice(t, h, "test-device", "LoRaWAN", "dest")

	seen := make(map[string]bool)

	for range 5 {
		rec := doIoTWRequest(t, h, http.MethodPost,
			"/wireless-devices/"+dev+"/data",
			`{"PayloadData":"aGVsbG8=","WirelessMetadata":{}}`)
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		msgID, _ := resp["MessageId"].(string)
		assert.NotEmpty(t, msgID, "MessageId should not be empty")
		assert.False(t, seen[msgID], "MessageId should be unique: %s", msgID)
		seen[msgID] = true
	}
}

// setupWirelessDevice creates a wireless device and returns its ID.
func setupWirelessDevice(t *testing.T, h *iotwireless.Handler, name, devType, dest string) string {
	t.Helper()

	body := `{"Name":"` + name + `","Type":"` + devType + `","DestinationName":"` + dest + `"}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	id, _ := resp["Id"].(string)
	require.NotEmpty(t, id)

	return id
}

func TestHandler_QueuedMessages_MultipleSends(t *testing.T) {
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

// Test_GetWirelessDevice_ReflectsThingAssociation verifies
// AssociateWirelessDeviceWithThing / DisassociateWirelessDeviceFromThing are
// real state mutations reflected by GetWirelessDevice's ThingArn/ThingName —
// not a disguised no-op. Real AWS derives ThingName from the last path
// segment of ThingArn (arn:...:thing/<name>) since the association request
// never carries ThingName directly.
func Test_GetWirelessDevice_ReflectsThingAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN","DestinationName":"d"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	id := created["Id"].(string)

	// Before association, ThingArn/ThingName must be absent.
	getRec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var before map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &before))
	assert.Empty(t, before["ThingArn"])
	assert.Empty(t, before["ThingName"])

	thingArn := "arn:aws:iot:us-east-1:000000000000:thing/my-thing"
	assocRec := doIoTWRequest(t, h, http.MethodPut, "/wireless-devices/"+id+"/thing",
		`{"ThingArn":"`+thingArn+`"}`)
	require.Equal(t, http.StatusNoContent, assocRec.Code)

	getRec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &after))
	assert.Equal(t, thingArn, after["ThingArn"])
	assert.Equal(t, "my-thing", after["ThingName"])

	disassocRec := doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+id+"/thing", "")
	require.Equal(t, http.StatusNoContent, disassocRec.Code)

	getRec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var afterDisassoc map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &afterDisassoc))
	assert.Empty(t, afterDisassoc["ThingArn"])
	assert.Empty(t, afterDisassoc["ThingName"])
}
