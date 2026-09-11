package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestListMFADevices_Dispatch pins the survivor of the gopherstack-qmcud
// dispatch-table collision: iamMFALinkDispatch's opListMFADevices wins over
// iamMFADeviceDispatch's now-deleted "ListMFADevices" entry. The deleted
// entry swallowed NoSuchEntity into a bogus empty 200, unlike real AWS.
func TestListMFADevices_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		name        string
		userName    string
		wantErrCode string
		wantContain string
		wantCode    int
	}{
		{
			name: "existing_user_with_device",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				dev, _ := b.CreateVirtualMFADeviceFull("alice-mfa", "/")
				require.NoError(t, b.EnableMFADevice("alice", dev.SerialNumber, "123456", "789012"))
			},
			userName:    "alice",
			wantCode:    http.StatusOK,
			wantContain: "alice-mfa",
		},
		{
			name: "existing_user_no_devices",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("bob", "/", "")
			},
			userName: "bob",
			wantCode: http.StatusOK,
		},
		{
			name:        "unknown_user_returns_nosuchentity",
			setup:       func(_ *iam.InMemoryBackend) {},
			userName:    "ghost",
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

			req := iamRequest("ListMFADevices", map[string]string{"UserName": tt.userName})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

func TestCreateVirtualMFADevice_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *iam.InMemoryBackend)
		name       string
		deviceName string
		path       string
		wantErr    bool
	}{
		{
			name:       "create_device_success",
			setup:      func(_ *iam.InMemoryBackend) {},
			deviceName: "MyMFADevice",
			path:       "/",
		},
		{
			name:       "device_with_path",
			setup:      func(_ *iam.InMemoryBackend) {},
			deviceName: "TeamMFA",
			path:       "/team/",
		},
		{
			name:       "empty_device_name_returns_error",
			setup:      func(_ *iam.InMemoryBackend) {},
			deviceName: "",
			wantErr:    true,
		},
		{
			name: "duplicate_device_returns_error",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateVirtualMFADevice("MyMFADevice", "/")
			},
			deviceName: "MyMFADevice",
			path:       "/",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			device, err := b.CreateVirtualMFADevice(tt.deviceName, tt.path)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, device)
			assert.NotEmpty(t, device.SerialNumber)
			assert.Contains(t, device.SerialNumber, tt.deviceName)
		})
	}
}

func TestHandler_VirtualMFA_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	// CreateVirtualMFADevice.
	req := iamRequest("CreateVirtualMFADevice", map[string]string{
		"VirtualMFADeviceName": "alice-mfa",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, "CreateVirtualMFADevice must succeed")
	assert.Contains(t, rec.Body.String(), "SerialNumber")

	// ListVirtualMFADevices.
	req2 := iamRequest("ListVirtualMFADevices", map[string]string{})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "alice-mfa")

	// EnableMFADevice using the backend serial number directly.
	dev, err := b.CreateVirtualMFADeviceFull("alice-mfa2", "/mfa/")
	require.NoError(t, err)

	req3 := iamRequest("EnableMFADevice", map[string]string{
		"UserName":            "alice",
		"SerialNumber":        dev.SerialNumber,
		"AuthenticationCode1": "123456",
		"AuthenticationCode2": "789012",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code, "EnableMFADevice must succeed")

	// ListMFADevices.
	req4 := iamRequest("ListMFADevices", map[string]string{"UserName": "alice"})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
	assert.Contains(t, rec4.Body.String(), dev.SerialNumber)

	// DeactivateMFADevice.
	req5 := iamRequest("DeactivateMFADevice", map[string]string{
		"UserName":     "alice",
		"SerialNumber": dev.SerialNumber,
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)

	// DeleteVirtualMFADevice.
	req6 := iamRequest("DeleteVirtualMFADevice", map[string]string{
		"SerialNumber": dev.SerialNumber,
	})
	rec6 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req6, rec6)))
	assert.Equal(t, http.StatusOK, rec6.Code)
}
