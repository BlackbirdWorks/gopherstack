package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestInMemoryBackend_GetReturnsIsolatedCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "wireless_device"},
		{name: "wireless_gateway"},
		{name: "service_profile"},
		{name: "destination"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			switch tt.name {
			case "wireless_device":
				d, err := bk.CreateWirelessDevice(
					testAccountID,
					testRegion,
					"dev",
					"LoRaWAN",
					"",
					"",
					"",
					nil,
					nil,
					map[string]string{"k": "v"},
				)
				require.NoError(t, err)

				d.Tags["injected"] = "yes"

				got, err := bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
				require.NoError(t, err)
				assert.NotContains(
					t,
					got.Tags,
					"injected",
					"mutation of returned pointer must not affect backend",
				)

			case "wireless_gateway":
				gw, err := bk.CreateWirelessGateway(
					testAccountID,
					testRegion,
					"gw",
					"",
					nil,
					map[string]string{"k": "v"},
				)
				require.NoError(t, err)

				gw.Tags["injected"] = "yes"

				got, err := bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
				require.NoError(t, err)
				assert.NotContains(
					t,
					got.Tags,
					"injected",
					"mutation of returned pointer must not affect backend",
				)

			case "service_profile":
				sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp", nil, map[string]string{"k": "v"})
				require.NoError(t, err)

				sp.Tags["injected"] = "yes"

				got, err := bk.GetServiceProfile(testAccountID, testRegion, sp.ID)
				require.NoError(t, err)
				assert.NotContains(
					t,
					got.Tags,
					"injected",
					"mutation of returned pointer must not affect backend",
				)

			case "destination":
				dest, err := bk.CreateDestination(
					testAccountID,
					testRegion,
					"dest",
					"",
					"",
					"",
					"",
					map[string]string{"k": "v"},
				)
				require.NoError(t, err)

				dest.Tags["injected"] = "yes"

				got, err := bk.GetDestination(testAccountID, testRegion, dest.Name)
				require.NoError(t, err)
				assert.NotContains(
					t,
					got.Tags,
					"injected",
					"mutation of returned pointer must not affect backend",
				)
			}
		})
	}
}

// TestInMemoryBackend_Reset verifies that Reset() clears all backend state.
func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*iotwireless.InMemoryBackend)
		check func(*testing.T, *iotwireless.InMemoryBackend)
		name  string
	}{
		{
			name: "devices_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", "", nil, nil, nil)
				_, _ = b.CreateWirelessDevice(testAccountID, testRegion, "d2", "LoRaWAN", "", "", "", nil, nil, nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.DeviceCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "gateways_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateWirelessGateway(testAccountID, testRegion, "gw1", "", nil, nil)
				_, _ = b.CreateWirelessGateway(testAccountID, testRegion, "gw2", "", nil, nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.GatewayCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "service_profiles_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateServiceProfile(testAccountID, testRegion, "sp1", nil, nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.ServiceProfileCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "device_profiles_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateDeviceProfile(testAccountID, testRegion, "dp1", nil, nil, nil)
			},
			check: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, iotwireless.DeviceProfileCount(b, testAccountID, testRegion))
			},
		},
		{
			name: "fuota_tasks_cleared",
			setup: func(b *iotwireless.InMemoryBackend) {
				_, _ = b.CreateFuotaTask(testAccountID, testRegion, "ft1", "", "", "", "", 0, 0, 0, nil, nil)
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

// TestInMemoryBackend_MultipleResetCycle verifies that repeated Reset() calls are safe.
func TestInMemoryBackend_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for range 3 {
		_, err := b.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", "", nil, nil, nil)
		require.NoError(t, err)

		b.Reset()

		assert.Equal(t, 0, iotwireless.DeviceCount(b, testAccountID, testRegion))
	}
}

// TestInMemoryBackend_ExportCountHelpers verifies all export count helpers.
func TestInMemoryBackend_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	assert.Equal(t, 0, iotwireless.DeviceCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.GatewayCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.ServiceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.DestinationCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.DeviceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.FuotaTaskCount(b, testAccountID, testRegion))

	_, _ = b.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", "", nil, nil, nil)
	_, _ = b.CreateWirelessGateway(testAccountID, testRegion, "gw1", "", nil, nil)
	_, _ = b.CreateServiceProfile(testAccountID, testRegion, "sp1", nil, nil)
	_, _ = b.CreateDestination(testAccountID, testRegion, "dest1", "", "", "", "", nil)
	_, _ = b.CreateDeviceProfile(testAccountID, testRegion, "dp1", nil, nil, nil)
	_, _ = b.CreateFuotaTask(testAccountID, testRegion, "ft1", "", "", "", "", 0, 0, 0, nil, nil)

	assert.Equal(t, 1, iotwireless.DeviceCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.GatewayCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.ServiceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.DestinationCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.DeviceProfileCount(b, testAccountID, testRegion))
	assert.Equal(t, 1, iotwireless.FuotaTaskCount(b, testAccountID, testRegion))
}

// TestInMemoryBackend_SeedHelpers verifies all AddXInternal seed helpers work correctly.
func TestInMemoryBackend_SeedHelpers(t *testing.T) {
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

// TestInMemoryBackend_NonNilEmptySlices verifies that list methods return non-nil empty slices.
func TestInMemoryBackend_NonNilEmptySlices(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	assert.NotNil(t, b.ListWirelessDevices(testAccountID, testRegion))
	assert.NotNil(t, b.ListWirelessGateways(testAccountID, testRegion))
	assert.NotNil(t, b.ListServiceProfiles(testAccountID, testRegion))
	assert.NotNil(t, b.ListDestinations(testAccountID, testRegion))
	assert.NotNil(t, b.ListDeviceProfiles(testAccountID, testRegion))
	assert.NotNil(t, b.ListFuotaTasks(testAccountID, testRegion))
}

// TestInMemoryBackend_DeepCopy_Tags verifies that returned structs have independent tag maps.
func TestInMemoryBackend_DeepCopy_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runTest func(*testing.T, *iotwireless.InMemoryBackend)
		name    string
	}{
		{
			name: "device_profile",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()

				dp, err := b.CreateDeviceProfile(testAccountID, testRegion, "dp", nil, nil, map[string]string{"k": "v"})
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

				ft, err := b.CreateFuotaTask(
					testAccountID,
					testRegion,
					"ft",
					"",
					"",
					"",
					"",
					0,
					0,
					0,
					nil,
					map[string]string{"k": "v"},
				)
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
