package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestRefinement1_Reset verifies that Reset() clears all backend state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*iotwireless.InMemoryBackend)
		check func(*testing.T, *iotwireless.InMemoryBackend)
		name  string
	}{
		{
			name: "devices_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", nil)
				_, _ = b.CreateWirelessDevice(testAccountID, testRegion, "d2", "LoRaWAN", "", "", nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.DeviceCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "gateways_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateWirelessGateway(testAccountID, testRegion, "gw1", "", nil)
				_, _ = b.CreateWirelessGateway(testAccountID, testRegion, "gw2", "", nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.GatewayCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "service_profiles_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateServiceProfile(testAccountID, testRegion, "sp1", nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.ServiceProfileCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "device_profiles_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateDeviceProfile(testAccountID, testRegion, "dp1", nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.DeviceProfileCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "fuota_tasks_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateFuotaTask(testAccountID, testRegion, "ft1", "", "", "", nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.FuotaTaskCount(b, testAccountID, testRegion))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			tt.setup(b)
			b.Reset()
			tt.check(t, b)
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies that repeated Reset() calls are safe.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for range 3 {
		_, err := b.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", nil)
		require.NoError(t, err)

		b.Reset()

		assert.Equal(t, 0, iotwireless.DeviceCount(b, testAccountID, testRegion))
	}
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN","DestinationName":"d"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	h.Reset()

	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	list, ok := resp["WirelessDeviceList"].([]any)
	require.True(t, ok)
	assert.Empty(t, list)
}

// TestRefinement1_ProviderInit_NilCtx verifies that Provider.Init returns ErrNilAppContext for nil ctx.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &iotwireless.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrNilAppContext)
}

// TestRefinement1_GetSupportedOperations_AllOps verifies that all expected ops are included.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	ops := h.GetSupportedOperations()

	expected := []string{
		"GetDeviceProfile",
		"ListDeviceProfiles",
		"DeleteDeviceProfile",
		"GetFuotaTask",
		"ListFuotaTasks",
		"DeleteFuotaTask",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "GetSupportedOperations should contain %q", op)
	}
}

// TestRefinement1_SeedHelpers verifies all AddXInternal seed helpers work correctly.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(*testing.T, *iotwireless.InMemoryBackend)
		name  string
	}{
		{
			name: "add_wireless_device",
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				id := "seed-dev-001"
				b.AddWirelessDeviceInternal(testAccountID, testRegion, &iotwireless.WirelessDevice{
					ID: id, ARN: "arn:aws:iotwireless:us-east-1:000000000000:WirelessDevice/" + id,
					Name: "seed-dev", Tags: map[string]string{"src": "seed"},
				})
				assert.Equal(t, 1, iotwireless.DeviceCount(b, testAccountID, testRegion))
				d, err := b.GetWirelessDevice(testAccountID, testRegion, id)
				require.NoError(t, err)
				assert.Equal(t, "seed-dev", d.Name)
			},
		},
		{
			name: "add_wireless_gateway",
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				id := "seed-gw-001"
				b.AddWirelessGatewayInternal(testAccountID, testRegion, &iotwireless.WirelessGateway{
					ID: id, ARN: "arn:aws:iotwireless:us-east-1:000000000000:WirelessGateway/" + id,
					Name: "seed-gw",
				})
				assert.Equal(t, 1, iotwireless.GatewayCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "add_service_profile",
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				id := "seed-sp-001"
				b.AddServiceProfileInternal(testAccountID, testRegion, &iotwireless.ServiceProfile{
					ID: id, ARN: "arn:aws:iotwireless:us-east-1:000000000000:ServiceProfile/" + id,
					Name: "seed-sp",
				})
				assert.Equal(t, 1, iotwireless.ServiceProfileCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "add_destination",
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				b.AddDestinationInternal(testAccountID, testRegion, &iotwireless.Destination{
					Name: "seed-dest",
					ARN:  "arn:aws:iotwireless:us-east-1:000000000000:Destination/seed-dest",
				})
				assert.Equal(t, 1, iotwireless.DestinationCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "add_device_profile",
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				id := "seed-dp-001"
				b.AddDeviceProfileInternal(testAccountID, testRegion, &iotwireless.DeviceProfile{
					ID: id, ARN: "arn:aws:iotwireless:us-east-1:000000000000:DeviceProfile/" + id,
					Name: "seed-dp",
				})
				assert.Equal(t, 1, iotwireless.DeviceProfileCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "add_fuota_task",
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				id := "seed-ft-001"
				b.AddFuotaTaskInternal(testAccountID, testRegion, &iotwireless.FuotaTask{
					ID: id, ARN: "arn:aws:iotwireless:us-east-1:000000000000:FuotaTask/" + id,
					Name: "seed-ft",
				})
				assert.Equal(t, 1, iotwireless.FuotaTaskCount(b, testAccountID, testRegion))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			tt.check(t, b)
		})
	}
}

// TestRefinement1_ExportCountHelpers verifies all export count helpers.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	assert.Equal(t, 0, iotwireless.DeviceCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.GatewayCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.ServiceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.DestinationCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.DeviceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.FuotaTaskCount(b, testAccountID, testRegion))

	_, _ = b.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", nil)
	_, _ = b.CreateWirelessGateway(testAccountID, testRegion, "gw1", "", nil)
	_, _ = b.CreateServiceProfile(testAccountID, testRegion, "sp1", nil)
	_, _ = b.CreateDestination(testAccountID, testRegion, "dest1", "", "", "", "", nil)
	_, _ = b.CreateDeviceProfile(testAccountID, testRegion, "dp1", nil)
	_, _ = b.CreateFuotaTask(testAccountID, testRegion, "ft1", "", "", "", nil)

	assert.Equal(t, 1, iotwireless.DeviceCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.GatewayCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.ServiceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.DestinationCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.DeviceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.FuotaTaskCount(b, testAccountID, testRegion))
}

// TestRefinement1_DeviceProfile_RoundTrip verifies full DeviceProfile CRUD lifecycle.
func TestRefinement1_DeviceProfile_RoundTrip(t *testing.T) {
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

			// Create
			body := `{"Name":"` + tt.profileName + `","Tags":{"env":"test"}}`
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

// TestRefinement1_FuotaTask_RoundTrip verifies full FuotaTask CRUD lifecycle.
func TestRefinement1_FuotaTask_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskName    string
		description string
	}{
		{name: "basic_task", taskName: "fw-update-task", description: "firmware update"},
		{name: "no_description", taskName: "fw-task-minimal", description: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.taskName + `","Description":"` + tt.description +
				`","FirmwareUpdateImage":"s3://bucket/fw.bin","FirmwareUpdateRole":"arn:aws:iam::000000000000:role/r"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/fuota-tasks", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id := createResp["Id"].(string)
			require.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.taskName, getResp["Name"])
			assert.Equal(t, id, getResp["Id"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			list, ok := listResp["FuotaTaskList"].([]any)
			require.True(t, ok)
			assert.Len(t, list, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/fuota-tasks/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/fuota-tasks/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestRefinement1_DeleteDeviceProfile_NotFound verifies 404 is returned for non-existent device profiles.
func TestRefinement1_DeleteDeviceProfile_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodDelete, "/device-profiles/no-such-id", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_DeleteFuotaTask_NotFound verifies 404 is returned for non-existent FUOTA tasks.
func TestRefinement1_DeleteFuotaTask_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	rec := doIoTWRequest(t, h, http.MethodDelete, "/fuota-tasks/no-such-id", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_SortedListWirelessDevices verifies deterministic sort order.
func TestRefinement1_SortedListWirelessDevices(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"zebra", "alpha", "mango"} {
		_, err := b.CreateWirelessDevice(testAccountID, testRegion, name, "LoRaWAN", "", "", nil)
		require.NoError(t, err)
	}

	devices := b.ListWirelessDevices(testAccountID, testRegion)
	require.Len(t, devices, 3)
	assert.Equal(t, "alpha", devices[0].Name)
	assert.Equal(t, "mango", devices[1].Name)
	assert.Equal(t, "zebra", devices[2].Name)
}

// TestRefinement1_SortedListWirelessGateways verifies deterministic sort order.
func TestRefinement1_SortedListWirelessGateways(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"z-gw", "a-gw", "m-gw"} {
		_, err := b.CreateWirelessGateway(testAccountID, testRegion, name, "", nil)
		require.NoError(t, err)
	}

	gws := b.ListWirelessGateways(testAccountID, testRegion)
	require.Len(t, gws, 3)
	assert.Equal(t, "a-gw", gws[0].Name)
	assert.Equal(t, "m-gw", gws[1].Name)
	assert.Equal(t, "z-gw", gws[2].Name)
}

// TestRefinement1_SortedListDeviceProfiles verifies deterministic sort order for device profiles.
func TestRefinement1_SortedListDeviceProfiles(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"dp-z", "dp-a", "dp-m"} {
		_, err := b.CreateDeviceProfile(testAccountID, testRegion, name, nil)
		require.NoError(t, err)
	}

	profiles := b.ListDeviceProfiles(testAccountID, testRegion)
	require.Len(t, profiles, 3)
	assert.Equal(t, "dp-a", profiles[0].Name)
	assert.Equal(t, "dp-m", profiles[1].Name)
	assert.Equal(t, "dp-z", profiles[2].Name)
}

// TestRefinement1_SortedListFuotaTasks verifies deterministic sort order for FUOTA tasks.
func TestRefinement1_SortedListFuotaTasks(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"ft-z", "ft-a", "ft-m"} {
		_, err := b.CreateFuotaTask(testAccountID, testRegion, name, "", "", "", nil)
		require.NoError(t, err)
	}

	tasks := b.ListFuotaTasks(testAccountID, testRegion)
	require.Len(t, tasks, 3)
	assert.Equal(t, "ft-a", tasks[0].Name)
	assert.Equal(t, "ft-m", tasks[1].Name)
	assert.Equal(t, "ft-z", tasks[2].Name)
}

// TestRefinement1_NonNilEmptySlices verifies that list methods return non-nil empty slices.
func TestRefinement1_NonNilEmptySlices(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	assert.NotNil(t, b.ListWirelessDevices(testAccountID, testRegion))
	assert.NotNil(t, b.ListWirelessGateways(testAccountID, testRegion))
	assert.NotNil(t, b.ListServiceProfiles(testAccountID, testRegion))
	assert.NotNil(t, b.ListDestinations(testAccountID, testRegion))
	assert.NotNil(t, b.ListDeviceProfiles(testAccountID, testRegion))
	assert.NotNil(t, b.ListFuotaTasks(testAccountID, testRegion))
}

// TestRefinement1_PartnerAccountARN_UsesRegion verifies that partner account ARN uses the handler region.
func TestRefinement1_PartnerAccountARN_UsesRegion(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()
	arn, err := b.AssociateAwsAccountWithPartnerAccount(testAccountID, testRegion, "partner-001", nil)
	require.NoError(t, err)
	assert.Contains(t, arn, testRegion, "ARN should include the handler region")
	assert.Contains(t, arn, "partner-001")
}

// TestRefinement1_CertificateARN_UsesRegion verifies that certificate ARN uses the gateway region.
func TestRefinement1_CertificateARN_UsesRegion(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	gw, err := b.CreateWirelessGateway(testAccountID, testRegion, "gw-cert", "", nil)
	require.NoError(t, err)

	certARN, err := b.AssociateWirelessGatewayWithCertificate(testAccountID, testRegion, gw.ID, "cert-abc")
	require.NoError(t, err)
	assert.Contains(t, certARN, testRegion, "Certificate ARN should include the region")
	assert.Contains(t, certARN, "cert-abc")
}

// TestRefinement1_ErrValidation_Sentinel verifies that ErrValidation is a distinct non-nil error.
func TestRefinement1_ErrValidation_Sentinel(t *testing.T) {
	t.Parallel()

	assert.Error(t, iotwireless.ErrValidation)
}

// TestRefinement1_HandleError_NotFound verifies that handleError maps not-found errors to 404.
func TestRefinement1_HandleError_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "device_profile_not_found",
			path:       "/device-profiles/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "fuota_task_not_found",
			path:       "/fuota-tasks/no-such-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore for all resource types.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	dp, err := b.CreateDeviceProfile(testAccountID, testRegion, "dp-persist", map[string]string{"env": "test"})
	require.NoError(t, err)

	ft, err := b.CreateFuotaTask(testAccountID, testRegion, "ft-persist", "desc", "s3://bucket/fw.bin", "arn:role", nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	gotDP, err := b2.GetDeviceProfile(testAccountID, testRegion, dp.ID)
	require.NoError(t, err)
	assert.Equal(t, dp.Name, gotDP.Name)
	assert.Equal(t, "test", gotDP.Tags["env"])

	gotFT, err := b2.GetFuotaTask(testAccountID, testRegion, ft.ID)
	require.NoError(t, err)
	assert.Equal(t, ft.Name, gotFT.Name)
	assert.Equal(t, "desc", gotFT.Description)
}

// TestRefinement1_ListEmpty_NonNil verifies that HTTP list responses return an empty array, not null.
func TestRefinement1_ListEmpty_NonNil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		listKey string
	}{
		{name: "device_profiles", path: "/device-profiles", listKey: "DeviceProfileList"},
		{name: "fuota_tasks", path: "/fuota-tasks", listKey: "FuotaTaskList"},
		{name: "wireless_devices", path: "/wireless-devices", listKey: "WirelessDeviceList"},
		{name: "wireless_gateways", path: "/wireless-gateways", listKey: "WirelessGatewayList"},
		{name: "service_profiles", path: "/service-profiles", listKey: "ServiceProfileList"},
		{name: "destinations", path: "/destinations", listKey: "DestinationList"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			list, ok := resp[tt.listKey].([]any)
			require.True(t, ok, "key %q should be a JSON array, got %T", tt.listKey, resp[tt.listKey])
			assert.Empty(t, list)
		})
	}
}

// TestRefinement1_DeepCopy_Tags verifies that returned structs have independent tag maps.
func TestRefinement1_DeepCopy_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runTest func(*testing.T, *iotwireless.InMemoryBackend)
		name    string
	}{
		{
			name: "device_profile",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()

				dp, err := b.CreateDeviceProfile(testAccountID, testRegion, "dp", map[string]string{"k": "v"})
				require.NoError(t, err)

				dp.Tags["injected"] = "yes"

				got, err := b.GetDeviceProfile(testAccountID, testRegion, dp.ID)
				require.NoError(t, err)
				assert.NotContains(t, got.Tags, "injected")
			},
		},
		{
			name: "fuota_task",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()

				ft, err := b.CreateFuotaTask(testAccountID, testRegion, "ft", "", "", "", map[string]string{"k": "v"})
				require.NoError(t, err)

				ft.Tags["injected"] = "yes"

				got, err := b.GetFuotaTask(testAccountID, testRegion, ft.ID)
				require.NoError(t, err)
				assert.NotContains(t, got.Tags, "injected")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			tt.runTest(t, b)
		})
	}
}
