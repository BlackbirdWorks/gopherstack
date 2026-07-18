package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AssociateWirelessGatewayWithCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		iotCertificateID string
		createGateway    bool
		wantStatus       int
	}{
		{
			name:             "associate_existing_gateway",
			iotCertificateID: "cert-abc123",
			createGateway:    true,
			wantStatus:       http.StatusOK,
		},
		{
			name:             "gateway_not_found",
			iotCertificateID: "cert-xyz789",
			createGateway:    false,
			wantStatus:       http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			gwID := "no-such-gateway"

			if tt.createGateway {
				createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways",
					`{"Name":"gw-cert","Description":"cert gw"}`)
				require.Equal(t, http.StatusCreated, createRec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				gwID = createResp["Id"].(string)
			}

			body := `{"IotCertificateId":"` + tt.iotCertificateID + `"}`
			rec := doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+gwID+"/certificate", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["IotCertificateArn"])
			}
		})
	}
}

func TestHandler_GatewayCertificateLifecycle(t *testing.T) {
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

// TestHandler_ListDevicesForWirelessDeviceImportTask_ByID verifies that
// supplying a real task ID reflects the task's DestinationName, and an
// unknown ID returns a real 404.
func TestHandler_ListDevicesForWirelessDeviceImportTask_ByID(t *testing.T) {
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

func TestHandler_StartWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantDestination string
		wantStatus      int
	}{
		{
			name:            "with_destination",
			body:            `{"DestinationName":"dest-a"}`,
			wantStatus:      http.StatusCreated,
			wantDestination: "dest-a",
		},
		{
			name:       "empty_destination",
			body:       `{}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:            "full_body",
			body:            `{"DestinationName":"my-dest","Sidewalk":{}}`,
			wantStatus:      http.StatusCreated,
			wantDestination: "my-dest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arn, _ := resp["Arn"].(string)
			id, _ := resp["Id"].(string)
			assert.NotEmpty(t, arn)
			assert.NotEmpty(t, id)
			assert.Contains(t, arn, "ImportTask")
		})
	}
}

func TestHandler_GetWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupDest  string
		id         string
		wantStatus int
		wantFound  bool
	}{
		{
			name:       "existing_task",
			setupDest:  "my-dest",
			wantStatus: http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "not_found",
			id:         "nonexistent-id",
			wantStatus: http.StatusNotFound,
			wantFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			taskID := tt.id
			if tt.setupDest != "" {
				rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task",
					`{"DestinationName":"`+tt.setupDest+`"}`)
				require.Equal(t, http.StatusCreated, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				taskID = createResp["Id"].(string)
			}

			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+taskID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFound {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.setupDest, resp["DestinationName"])
				assert.Equal(t, "Initialized", resp["Status"])
			}
		})
	}
}

func TestHandler_DeleteWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupTask  bool
		wantStatus int
	}{
		{
			name:       "existing_task",
			setupTask:  true,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			setupTask:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			taskID := "nonexistent-task-id"
			if tt.setupTask {
				rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task", `{"DestinationName":"d"}`)
				require.Equal(t, http.StatusCreated, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				taskID = createResp["Id"].(string)
			}

			rec := doIoTWRequest(t, h, http.MethodDelete, "/wireless_device_import_task/"+taskID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.setupTask {
				// Get after delete returns 404.
				rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+taskID, "")
				assert.Equal(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}

func TestHandler_UpdateWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialDest string
		updatedDest string
		wantNewDest string
		wantStatus  int
	}{
		{
			name:        "update_destination",
			initialDest: "dest-old",
			updatedDest: "dest-new",
			wantStatus:  http.StatusNoContent,
			wantNewDest: "dest-new",
		},
		{
			name:        "empty_update_preserves",
			initialDest: "dest-initial",
			updatedDest: "",
			wantStatus:  http.StatusNoContent,
			wantNewDest: "dest-initial",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task",
				`{"DestinationName":"`+tt.initialDest+`"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			taskID := createResp["Id"].(string)

			var body string
			if tt.updatedDest != "" {
				body = `{"DestinationName":"` + tt.updatedDest + `"}`
			} else {
				body = `{}`
			}

			rec = doIoTWRequest(t, h, http.MethodPatch, "/wireless_device_import_task/"+taskID, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify via Get.
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task/"+taskID, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.wantNewDest, getResp["DestinationName"])
		})
	}
}

func TestHandler_ListWirelessDeviceImportTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		taskCount int
	}{
		{name: "empty", taskCount: 0},
		{name: "one_task", taskCount: 1},
		{name: "multiple_tasks", taskCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			for i := range tt.taskCount {
				body := `{"DestinationName":"dest-` + string(rune('a'+i)) + `"}`
				rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_device_import_task", body)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_tasks", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			list, ok := resp["WirelessDeviceImportTaskList"].([]any)
			require.True(t, ok, "WirelessDeviceImportTaskList should be array")
			assert.Len(t, list, tt.taskCount)
		})
	}
}

func TestHandler_StartSingleWirelessDeviceImportTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "basic_create",
			body:       `{"DestinationName":"d1"}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "no_destination",
			body:       `{}`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless_single_device_import_task", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arn, _ := resp["Arn"].(string)
			devID, _ := resp["WirelessDeviceId"].(string)
			assert.NotEmpty(t, arn)
			assert.NotEmpty(t, devID)
			assert.Contains(t, arn, "ImportTask")
		})
	}
}

func TestHandler_ListDevicesForWirelessDeviceImportTask_NoIDReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodGet, "/wireless_device_import_task", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	list, ok := resp["ImportedWirelessDeviceList"].([]any)
	require.True(t, ok, "ImportedWirelessDeviceList should be array")
	assert.Empty(t, list)
}
