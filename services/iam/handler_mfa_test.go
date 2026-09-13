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

// TestCreateVirtualMFADevice_Dispatch pins the survivor of the
// gopherstack-f185i dispatch-table collision: iamVirtualMFAFullDispatch's
// opCreateVirtualMFADevice entry (backed by CreateVirtualMFADeviceFull) wins
// over iamNewOpsRoleAndCredentialActions's now-deleted entry (backed by the
// plain CreateVirtualMFADevice), which never populated Base32StringSeed or
// QRCodePNG -- both required for a client to actually enroll the device
// (types.VirtualMFADevice, aws-sdk-go-v2/service/iam/types/types.go:2580-2594)
// -- and ignored request tags.
func TestCreateVirtualMFADevice_Dispatch(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("carol", "/", "")

	req := iamRequest("CreateVirtualMFADevice", map[string]string{
		"VirtualMFADeviceName": "carol-mfa",
		"Tags.member.1.Key":    "team",
		"Tags.member.1.Value":  "platform",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Device iam.VirtualMFADeviceXML `xml:"VirtualMFADevice"`
		} `xml:"CreateVirtualMFADeviceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.Device.Base32StringSeed)
	assert.NotEmpty(t, resp.Result.Device.QRCodePNG)

	// Tags aren't echoed in the create response, but the plain
	// CreateVirtualMFADevice backend method the deleted entry called never
	// stores them at all -- confirm they landed via ListVirtualMFADevices.
	listReq := iamRequest("ListVirtualMFADevices", nil)
	listRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(listReq, listRec)))
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Result struct {
			Devices []iam.VirtualMFADeviceXML `xml:"VirtualMFADevices>member"`
		} `xml:"ListVirtualMFADevicesResult"`
	}
	require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Result.Devices, 1)
	require.Len(t, listResp.Result.Devices[0].Tags, 1)
	assert.Equal(t, "team", listResp.Result.Devices[0].Tags[0].Key)
	assert.Equal(t, "platform", listResp.Result.Devices[0].Tags[0].Value)
}

// TestEnableMFADevice_Dispatch pins the surviving iamMFALinkDispatch entry.
// It is a behaviour-neutral guard: the deleted iamMFADeviceDispatch
// duplicate called the same backend method with the same parameters and
// produced an identical response, so this test does not distinguish winner
// from loser -- it only guards against EnableMFADevice losing its dispatch
// entry entirely.
func TestEnableMFADevice_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend) (userName, serial string)
		name        string
		wantErrCode string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("dave", "/", "")
				dev, _ := b.CreateVirtualMFADeviceFull("dave-mfa", "/")

				return "dave", dev.SerialNumber
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown_user_returns_nosuchentity",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				dev, _ := b.CreateVirtualMFADeviceFull("ghost-mfa", "/")

				return "ghost", dev.SerialNumber
			},
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
		{
			name: "unknown_device_returns_nosuchentity",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("erin", "/", "")

				return "erin", "arn:aws:iam::123456789012:mfa/missing"
			},
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			userName, serial := tt.setup(b)

			req := iamRequest("EnableMFADevice", map[string]string{
				"UserName":            userName,
				"SerialNumber":        serial,
				"AuthenticationCode1": "123456",
				"AuthenticationCode2": "789012",
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

// TestDeactivateMFADevice_Dispatch pins the surviving iamMFALinkDispatch
// entry. Behaviour-neutral guard: see TestEnableMFADevice_Dispatch --
// the deleted iamMFADeviceDispatch duplicate was byte-identical.
func TestDeactivateMFADevice_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend) (userName, serial string)
		name        string
		wantErrCode string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("frank", "/", "")
				dev, _ := b.CreateVirtualMFADeviceFull("frank-mfa", "/")
				_ = b.EnableMFADevice("frank", dev.SerialNumber, "123456", "789012")

				return "frank", dev.SerialNumber
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown_user_returns_nosuchentity",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				dev, _ := b.CreateVirtualMFADeviceFull("ghost2-mfa", "/")

				return "ghost2", dev.SerialNumber
			},
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
		{
			name: "not_enabled_returns_invalidaction",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("gina", "/", "")
				dev, _ := b.CreateVirtualMFADeviceFull("gina-mfa", "/")

				return "gina", dev.SerialNumber
			},
			wantCode:    http.StatusBadRequest,
			wantErrCode: "InvalidAction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			userName, serial := tt.setup(b)

			req := iamRequest("DeactivateMFADevice", map[string]string{
				"UserName":     userName,
				"SerialNumber": serial,
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}
