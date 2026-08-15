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

	rec := doRequest(t, h, http.MethodPost, "/prod/claimDevice", map[string]any{"id": id})
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
			body:       map[string]any{"id": "hd-abc123"},
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			body:       map[string]any{"id": ""},
			name:       "missing id returns bad request",
			wantStatus: http.StatusBadRequest,
		},
		{
			body:       map[string]any{"id": "hd-dup"},
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
			devices := resp["inputDevices"].([]any)
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
				assert.Equal(t, tt.wantName, resp["name"])
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
				assert.Equal(t, tt.wantName, resp["name"])
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

			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/prod/inputDevices/"+tt.deviceID+"/reboot",
				nil,
			)
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

			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/prod/inputDevices/"+deviceID+"/transfer",
				map[string]any{
					"targetCustomerId": "123456789012",
					"targetRegion":     "us-west-2",
					"transferMessage":  "please accept",
				},
			)
			assert.Equal(t, tt.wantTransferStatus, rec.Code)

			rec2 := doRequest(
				t,
				h,
				http.MethodPost,
				"/prod/inputDevices/"+deviceID+"/"+tt.action,
				nil,
			)
			assert.Equal(t, tt.wantActionStatus, rec2.Code)
		})
	}
}

// TestHandlerTransferInputDevice_WireCasing locks in the fix for a bug
// where handleTransferInputDevice read PascalCase request-body keys
// ("TargetCustomerId"/"TargetRegion"/"TransferMessage") that never match a
// real client's lowerCamel TransferInputDeviceInput body (verified against
// aws-sdk-go-v2/service/medialive's
// awsRestjson1_serializeOpDocumentTransferInputDeviceInput), silently
// dropping every real caller's target/message fields. Asserts the fields
// actually reach the stored pending transfer and are echoed back by
// ListInputDeviceTransfers.
func TestHandlerTransferInputDevice_WireCasing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	claimTestDevice(t, h, "hd-casing1")

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/inputDevices/hd-casing1/transfer",
		map[string]any{
			"targetCustomerId": "999900001111",
			"targetRegion":     "eu-west-1",
			"transferMessage":  "casing regression check",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodGet, "/prod/inputDeviceTransfers?transferType=OUTGOING", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	transfers := resp["inputDeviceTransfers"].([]any)
	require.Len(t, transfers, 1)

	transfer := transfers[0].(map[string]any)
	assert.Equal(t, "999900001111", transfer["targetCustomerId"])
	assert.Equal(t, "casing regression check", transfer["message"])
}

func TestHandlerTransferInputDevice_NoDevice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/inputDevices/hd-notfound/transfer",
		map[string]any{
			"targetCustomerId": "123456789012",
		},
	)
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
					doRequest(
						t,
						h,
						http.MethodPost,
						"/prod/inputDevices/"+id+"/transfer",
						map[string]any{
							"targetCustomerId": "123456789012",
						},
					)
				}
			}

			rec := doRequest(
				t,
				h,
				http.MethodGet,
				"/prod/inputDeviceTransfers?transferType="+tt.transferType,
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				transfers := resp["inputDeviceTransfers"].([]any)
				assert.Len(t, transfers, tt.wantCount)
			}
		})
	}
}

func TestStartInputDeviceMaintenanceWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deviceID    string
		wantStatus  int
		claim       bool
		checkAbsent bool
	}{
		{
			name:       "not found returns 404",
			deviceID:   "hd-notfound-mw",
			claim:      false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:        "found does not fabricate maintenanceWindowActive",
			deviceID:    "hd-mw1",
			claim:       true,
			wantStatus:  http.StatusOK,
			checkAbsent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.claim {
				claimTestDevice(t, h, tt.deviceID)
			}

			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/prod/inputDevices/"+tt.deviceID+"/startInputDeviceMaintenanceWindow",
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkAbsent {
				rec2 := doRequest(t, h, http.MethodGet, "/prod/inputDevices/"+tt.deviceID, nil)
				require.Equal(t, http.StatusOK, rec2.Code)

				// gopherstack-7ux2: neither types.InputDeviceSummary nor
				// DescribeInputDeviceOutput carries maintenanceWindowActive
				// (medialive@v1.101.4 types/types.go:4498-4556). Asserted on
				// the raw body, since an SDK client discards unrecognised
				// keys and would pass this test even with the phantom field
				// still present.
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.NotContains(t, resp, "maintenanceWindowActive")
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

func TestInputDeviceLifecycleExtras(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/claimDevice",
		map[string]any{"id": "hd-device-1"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	for _, action := range []string{"start", "stop", "startInputDeviceMaintenanceWindow"} {
		rec = doRequest(t, h, http.MethodPost, "/prod/inputDevices/hd-device-1/"+action, nil)
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	rec = doRequest(t, h, http.MethodGet, "/prod/inputDevices/hd-device-1/thumbnailData", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/prod/inputDevices/missing/start", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/inputDevices/missing/thumbnailData", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
