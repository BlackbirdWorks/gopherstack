package medialive_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

func claimTestDevice(t *testing.T, h *medialive.Handler, id string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/prod/claimDevice", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerClaimDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			body:       map[string]any{"Id": "hd-abc123"},
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			body:       map[string]any{"Id": ""},
			name:       "missing id returns bad request",
			wantStatus: http.StatusBadRequest,
		},
		{
			body:       map[string]any{"Id": "hd-dup"},
			name:       "duplicate claim returns conflict",
			wantStatus: http.StatusOK,
		},
	}

	h := newTestHandler(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			localH := newTestHandler(t)
			if tt.name == "duplicate claim returns conflict" {
				claimTestDevice(t, localH, "hd-dup")
				rec := doRequest(t, localH, http.MethodPost, "/prod/claimDevice", tt.body)
				assert.Equal(t, http.StatusConflict, rec.Code)

				return
			}

			rec := doRequest(t, localH, http.MethodPost, "/prod/claimDevice", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}

	_ = h
}

func TestHandlerListInputDevices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		devicesAdded []string
		wantCount    int
	}{
		{
			name:         "empty list",
			devicesAdded: nil,
			wantCount:    0,
		},
		{
			name:         "two devices",
			devicesAdded: []string{"hd-1111", "hd-2222"},
			wantCount:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, id := range tt.devicesAdded {
				claimTestDevice(t, h, id)
			}

			rec := doRequest(t, h, http.MethodGet, "/prod/inputDevices", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			devices := resp["InputDevices"].([]any)
			assert.Len(t, devices, tt.wantCount)
		})
	}
}

func TestHandlerDescribeInputDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *medialive.Handler)
		name       string
		deviceID   string
		wantName   string
		wantStatus int
	}{
		{
			setup: func(h *medialive.Handler) {
				claimTestDevice(t, h, "hd-desc1")
			},
			name:       "existing device",
			deviceID:   "hd-desc1",
			wantName:   "hd-desc1",
			wantStatus: http.StatusOK,
		},
		{
			setup:      func(_ *medialive.Handler) {},
			name:       "not found",
			deviceID:   "hd-notfound",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doRequest(t, h, http.MethodGet, "/prod/inputDevices/"+tt.deviceID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantName, resp["Name"])
			}
		})
	}
}

func TestHandlerUpdateInputDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		deviceID   string
		wantName   string
		wantStatus int
	}{
		{
			body:       map[string]any{"name": "renamed-device"},
			name:       "rename device",
			deviceID:   "hd-upd1",
			wantName:   "renamed-device",
			wantStatus: http.StatusOK,
		},
		{
			body:       map[string]any{"name": "x"},
			name:       "not found",
			deviceID:   "hd-notfound",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.wantStatus == http.StatusOK {
				claimTestDevice(t, h, tt.deviceID)
			}

			rec := doRequest(t, h, http.MethodPut, "/prod/inputDevices/"+tt.deviceID, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantName, resp["Name"])
			}
		})
	}
}

func TestHandlerRebootInputDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceID   string
		claim      bool
		wantStatus int
	}{
		{
			name:       "success",
			deviceID:   "hd-reboot1",
			claim:      true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			deviceID:   "hd-notfound",
			claim:      false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.claim {
				claimTestDevice(t, h, tt.deviceID)
			}

			rec := doRequest(t, h, http.MethodPost, "/prod/inputDevices/"+tt.deviceID+"/reboot", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandlerInputDeviceTransferLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		action             string
		wantTransferStatus int
		wantActionStatus   int
	}{
		{
			name:               "transfer then accept",
			action:             "accept",
			wantTransferStatus: http.StatusOK,
			wantActionStatus:   http.StatusOK,
		},
		{
			name:               "transfer then cancel",
			action:             "cancel",
			wantTransferStatus: http.StatusOK,
			wantActionStatus:   http.StatusOK,
		},
		{
			name:               "transfer then reject",
			action:             "reject",
			wantTransferStatus: http.StatusOK,
			wantActionStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			deviceID := fmt.Sprintf("hd-%s", tt.action)
			claimTestDevice(t, h, deviceID)

			rec := doRequest(t, h, http.MethodPost, "/prod/inputDevices/"+deviceID+"/transfer", map[string]any{
				"TargetCustomerId": "123456789012",
				"TargetRegion":     "us-west-2",
				"TransferMessage":  "please accept",
			})
			assert.Equal(t, tt.wantTransferStatus, rec.Code)

			rec2 := doRequest(t, h, http.MethodPost, "/prod/inputDevices/"+deviceID+"/"+tt.action, nil)
			assert.Equal(t, tt.wantActionStatus, rec2.Code)
		})
	}
}

func TestHandlerTransferInputDevice_NoDevice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/inputDevices/hd-notfound/transfer", map[string]any{
		"TargetCustomerId": "123456789012",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerListInputDeviceTransfers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		transferType string
		wantStatus   int
		wantCount    int
	}{
		{
			name:         "outgoing transfers",
			transferType: "OUTGOING",
			wantStatus:   http.StatusOK,
			wantCount:    2,
		},
		{
			name:         "incoming transfers",
			transferType: "INCOMING",
			wantStatus:   http.StatusOK,
			wantCount:    2,
		},
		{
			name:         "invalid transfer type",
			transferType: "INVALID",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantCount > 0 {
				for i := range tt.wantCount {
					id := fmt.Sprintf("hd-tr%d", i)
					claimTestDevice(t, h, id)
					doRequest(t, h, http.MethodPost, "/prod/inputDevices/"+id+"/transfer", map[string]any{
						"TargetCustomerId": "123456789012",
					})
				}
			}

			rec := doRequest(t, h, http.MethodGet, "/prod/inputDeviceTransfers?transferType="+tt.transferType, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				transfers := resp["InputDeviceTransfers"].([]any)
				assert.Len(t, transfers, tt.wantCount)
			}
		})
	}
}

func TestStartInputDeviceMaintenanceWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceID   string
		claim      bool
		wantStatus int
		checkFlag  bool
	}{
		{
			name:       "not found returns 404",
			deviceID:   "hd-notfound-mw",
			claim:      false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "found sets maintenance window active",
			deviceID:   "hd-mw1",
			claim:      true,
			wantStatus: http.StatusOK,
			checkFlag:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.claim {
				claimTestDevice(t, h, tt.deviceID)
			}

			rec := doRequest(t, h, http.MethodPost, "/prod/inputDevices/"+tt.deviceID+"/startInputDeviceMaintenanceWindow", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkFlag {
				rec2 := doRequest(t, h, http.MethodGet, "/prod/inputDevices/"+tt.deviceID, nil)
				require.Equal(t, http.StatusOK, rec2.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Equal(t, true, resp["MaintenanceWindowActive"])
			}
		})
	}
}

func TestBackendInputDeviceCount(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, medialive.InputDeviceCount(backend))

	_, err := backend.ClaimDevice("hd-count1")
	require.NoError(t, err)
	assert.Equal(t, 1, medialive.InputDeviceCount(backend))
}
