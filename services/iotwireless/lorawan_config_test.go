package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestInMemoryBackend_LoRaWANSidewalkConfig_RoundTrips locks in that
// CreateWirelessDevice/CreateWirelessGateway/CreateDeviceProfile/
// CreateServiceProfile/CreateMulticastGroup/CreateFuotaTask no longer
// silently drop the request's nested LoRaWAN/Sidewalk configuration object
// (see PARITY.md gap: "CreateWirelessDevice/CreateWirelessGateway silently
// drop the request's LoRaWAN/Sidewalk nested config objects").
func TestInMemoryBackend_LoRaWANSidewalkConfig_RoundTrips(t *testing.T) {
	t.Parallel()

	t.Run("wireless_device", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()
		dp1, sp1, sn1 := "dp-1", "sp-1", "sn-1"
		loRaWAN := &iotwireless.LoRaWANDevice{DeviceProfileID: &dp1, ServiceProfileID: &sp1}
		sidewalk := &iotwireless.SidewalkCreateWirelessDevice{SidewalkManufacturingSn: &sn1}

		d, err := bk.CreateWirelessDevice(
			testAccountID, testRegion, "dev-1", "LoRaWAN", "dest-1", "", "Enabled",
			loRaWAN, sidewalk, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, d.LoRaWAN)
		assert.Equal(t, "dp-1", *d.LoRaWAN.DeviceProfileID)
		assert.Equal(t, "Enabled", d.Positioning)

		got, err := bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
		require.NoError(t, err)
		require.NotNil(t, got.Sidewalk)
		assert.Equal(t, "dp-1", *got.LoRaWAN.DeviceProfileID)
		assert.Equal(t, "sn-1", *got.Sidewalk.SidewalkManufacturingSn)

		// GetWirelessDevice's returned LoRaWAN must be independent of the
		// backend's stored one -- mutating it must not corrupt state.
		mutated := "mutated"
		got.LoRaWAN.DeviceProfileID = &mutated
		got2, err := bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
		require.NoError(t, err)
		assert.Equal(t, "dp-1", *got2.LoRaWAN.DeviceProfileID)

		// UpdateWirelessDevice merges rather than replaces.
		sp2 := "sp-2"
		err = bk.UpdateWirelessDevice(
			testAccountID, testRegion, d.ID, "", "", "", "",
			&iotwireless.LoRaWANUpdateDevice{ServiceProfileID: &sp2}, nil,
		)
		require.NoError(t, err)

		updated, err := bk.GetWirelessDevice(testAccountID, testRegion, d.ID)
		require.NoError(t, err)
		assert.Equal(t, "dp-1", *updated.LoRaWAN.DeviceProfileID, "merge must keep untouched fields")
		assert.Equal(t, "sp-2", *updated.LoRaWAN.ServiceProfileID, "merge must apply new fields")
	})

	t.Run("wireless_gateway", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()
		eui, rfRegion := "eui-1", "US915"
		loRaWAN := &iotwireless.LoRaWANGateway{GatewayEui: &eui, RfRegion: &rfRegion}

		gw, err := bk.CreateWirelessGateway(testAccountID, testRegion, "gw-1", "", loRaWAN, nil)
		require.NoError(t, err)
		require.NotNil(t, gw.LoRaWAN)
		assert.Equal(t, "eui-1", *gw.LoRaWAN.GatewayEui)

		got, err := bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
		require.NoError(t, err)
		assert.Equal(t, "US915", *got.LoRaWAN.RfRegion)

		// UpdateWirelessGateway's JoinEuiFilters/MaxEirp/NetIDFilters merge
		// into the stored LoRaWANGateway under their real (top-level-on-update) names.
		maxEirp := float32(15)
		err = bk.UpdateWirelessGateway(testAccountID, testRegion, gw.ID, "", "", nil, nil, &maxEirp)
		require.NoError(t, err)

		updated, err := bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
		require.NoError(t, err)
		assert.Equal(t, "eui-1", *updated.LoRaWAN.GatewayEui, "merge must keep untouched fields")
		assert.InDelta(t, float32(15), *updated.LoRaWAN.MaxEirp, 0.001)
	})

	t.Run("device_profile", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()

		dp, err := bk.CreateDeviceProfile(
			testAccountID, testRegion, "dp-1",
			map[string]any{"MacVersion": "1.0.3"}, map[string]any{"Model": "sidewalk-model"}, nil,
		)
		require.NoError(t, err)

		got, err := bk.GetDeviceProfile(testAccountID, testRegion, dp.ID)
		require.NoError(t, err)
		assert.Equal(t, "1.0.3", got.LoRaWAN["MacVersion"])
		assert.Equal(t, "sidewalk-model", got.Sidewalk["Model"])
	})

	t.Run("service_profile", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()

		sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp-1", map[string]any{"UlRate": float64(1)}, nil)
		require.NoError(t, err)

		got, err := bk.GetServiceProfile(testAccountID, testRegion, sp.ID)
		require.NoError(t, err)
		assert.InDelta(t, float64(1), got.LoRaWAN["UlRate"], 0.001)
	})

	t.Run("multicast_group", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()

		mg, err := bk.CreateMulticastGroup(
			testAccountID, testRegion, "mg-1", "", map[string]any{"RfRegion": "EU868"}, nil,
		)
		require.NoError(t, err)

		got, err := bk.GetMulticastGroup(testAccountID, testRegion, mg.ID)
		require.NoError(t, err)
		assert.Equal(t, "EU868", got.LoRaWAN["RfRegion"])
		assert.False(t, got.CreatedAt.IsZero(), "CreatedAt must be populated")
	})

	t.Run("fuota_task", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()

		ft, err := bk.CreateFuotaTask(
			testAccountID, testRegion, "ft-1", "", "s3://img", "role-arn", "ZGVzYw==",
			1000, 128, 50,
			map[string]any{"RfRegion": "US915"},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "Pending", ft.Status, "new FUOTA task must start Pending")

		got, err := bk.GetFuotaTask(testAccountID, testRegion, ft.ID)
		require.NoError(t, err)
		assert.Equal(t, "US915", got.LoRaWAN["RfRegion"])
		assert.Equal(t, "ZGVzYw==", got.Descriptor)
		assert.Equal(t, int32(1000), got.FragmentIntervalMS)
		assert.Equal(t, int32(128), got.FragmentSizeBytes)
		assert.Equal(t, int32(50), got.RedundancyPercent)

		// StartFuotaTask must transition Status without corrupting
		// FirmwareUpdateRole (a prior bug reused that field to fake status).
		require.NoError(t, bk.StartFuotaTask(testAccountID, testRegion, ft.ID))

		started, err := bk.GetFuotaTask(testAccountID, testRegion, ft.ID)
		require.NoError(t, err)
		assert.Equal(t, "FuotaSession_Waiting", started.Status)
		assert.Equal(t, "role-arn", started.FirmwareUpdateRole, "FirmwareUpdateRole must survive StartFuotaTask")
	})
}
