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
		macVersion := "1.0.3"

		dp, err := bk.CreateDeviceProfile(
			testAccountID, testRegion, "dp-1",
			&iotwireless.LoRaWANDeviceProfile{MacVersion: &macVersion},
			&iotwireless.SidewalkCreateDeviceProfile{},
			nil,
		)
		require.NoError(t, err)

		got, err := bk.GetDeviceProfile(testAccountID, testRegion, dp.ID)
		require.NoError(t, err)
		require.NotNil(t, got.LoRaWAN)
		assert.Equal(t, "1.0.3", *got.LoRaWAN.MacVersion)
		// SidewalkCreateDeviceProfile carries no fields of its own (real AWS
		// wire shape, types.go:1715); its presence alone marks the profile
		// as Sidewalk, so Get must return a non-nil-but-empty object, not a
		// fabricated field.
		require.NotNil(t, got.Sidewalk)
		assert.Nil(t, got.Sidewalk.ApplicationServerPublicKey)

		// GetDeviceProfile's returned LoRaWAN must be independent of the
		// backend's stored one.
		mutated := "mutated"
		got.LoRaWAN.MacVersion = &mutated
		got2, err := bk.GetDeviceProfile(testAccountID, testRegion, dp.ID)
		require.NoError(t, err)
		assert.Equal(t, "1.0.3", *got2.LoRaWAN.MacVersion)
	})

	t.Run("device_profile_lorawan_only", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()

		dp, err := bk.CreateDeviceProfile(testAccountID, testRegion, "dp-2", nil, nil, nil)
		require.NoError(t, err)

		got, err := bk.GetDeviceProfile(testAccountID, testRegion, dp.ID)
		require.NoError(t, err)
		assert.Nil(t, got.Sidewalk, "no Sidewalk key in the request must leave Sidewalk nil, not an empty object")
	})

	t.Run("service_profile", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()
		drMax := int32(15)

		sp, err := bk.CreateServiceProfile(
			testAccountID, testRegion, "sp-1",
			&iotwireless.LoRaWANServiceProfile{DrMax: &drMax, AddGwMetadata: true},
			nil,
		)
		require.NoError(t, err)

		got, err := bk.GetServiceProfile(testAccountID, testRegion, sp.ID)
		require.NoError(t, err)
		require.NotNil(t, got.LoRaWAN)
		// InMemoryBackend stores the create-shape LoRaWANServiceProfile
		// (types.go:1161) as-is; the wider LoRaWANGetServiceProfileInfo
		// response shape (types.go:933) is built by the HTTP handler layer
		// (see TestHandler_ServiceProfile_LoRaWANGetShape) via
		// loRaWANGetServiceProfileInfoFrom.
		assert.Equal(t, int32(15), *got.LoRaWAN.DrMax)
		assert.True(t, got.LoRaWAN.AddGwMetadata)
	})

	t.Run("multicast_group", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()

		mg, err := bk.CreateMulticastGroup(
			testAccountID, testRegion, "mg-1", "",
			&iotwireless.LoRaWANMulticast{RfRegion: "EU868"},
			nil,
		)
		require.NoError(t, err)

		got, err := bk.GetMulticastGroup(testAccountID, testRegion, mg.ID)
		require.NoError(t, err)
		require.NotNil(t, got.LoRaWAN)
		assert.Equal(t, "EU868", got.LoRaWAN.RfRegion)
		assert.False(t, got.CreatedAt.IsZero(), "CreatedAt must be populated")

		// UpdateMulticastGroup's LoRaWAN uses the same LoRaWANMulticast
		// shape as create (api_op_UpdateMulticastGroup.go:39) and replaces
		// the stored value wholesale.
		require.NoError(t, bk.UpdateMulticastGroup(
			testAccountID, testRegion, mg.ID, "", "",
			&iotwireless.LoRaWANMulticast{RfRegion: "US915"},
		))

		updated, err := bk.GetMulticastGroup(testAccountID, testRegion, mg.ID)
		require.NoError(t, err)
		assert.Equal(t, "US915", updated.LoRaWAN.RfRegion)
	})

	t.Run("fuota_task", func(t *testing.T) {
		t.Parallel()

		bk := iotwireless.NewInMemoryBackend()

		ft, err := bk.CreateFuotaTask(
			testAccountID, testRegion, "ft-1", "", "s3://img", "role-arn", "ZGVzYw==",
			1000, 128, 50,
			&iotwireless.LoRaWANFuotaTask{RfRegion: "US915"},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "Pending", ft.Status, "new FUOTA task must start Pending")

		got, err := bk.GetFuotaTask(testAccountID, testRegion, ft.ID)
		require.NoError(t, err)
		require.NotNil(t, got.LoRaWAN)
		assert.Equal(t, "US915", got.LoRaWAN.RfRegion)
		assert.Equal(t, "ZGVzYw==", got.Descriptor)
		assert.Equal(t, int32(1000), got.FragmentIntervalMS)
		assert.Equal(t, int32(128), got.FragmentSizeBytes)
		assert.Equal(t, int32(50), got.RedundancyPercent)

		// StartFuotaTask must transition Status without corrupting
		// FirmwareUpdateRole (a prior bug reused that field to fake status).
		require.NoError(t, bk.StartFuotaTask(testAccountID, testRegion, ft.ID, nil))

		started, err := bk.GetFuotaTask(testAccountID, testRegion, ft.ID)
		require.NoError(t, err)
		assert.Equal(t, "FuotaSession_Waiting", started.Status)
		assert.Equal(t, "role-arn", started.FirmwareUpdateRole, "FirmwareUpdateRole must survive StartFuotaTask")

		// UpdateFuotaTask's LoRaWAN uses the same LoRaWANFuotaTask shape as
		// create/update (api_op_UpdateFuotaTask.go:64) and replaces the
		// stored value wholesale; Descriptor/FirmwareUpdateImage/
		// FirmwareUpdateRole/fragment fields are also part of
		// UpdateFuotaTaskInput (api_op_UpdateFuotaTask.go:28) and must apply too.
		require.NoError(t, bk.UpdateFuotaTask(
			testAccountID, testRegion, ft.ID, "", "",
			"AQID", "s3://img2", "role-arn-2",
			2000, 256, 75,
			&iotwireless.LoRaWANFuotaTask{RfRegion: "EU868"},
		))

		updated, err := bk.GetFuotaTask(testAccountID, testRegion, ft.ID)
		require.NoError(t, err)
		assert.Equal(t, "EU868", updated.LoRaWAN.RfRegion)
		assert.Equal(t, "AQID", updated.Descriptor)
		assert.Equal(t, "s3://img2", updated.FirmwareUpdateImage)
		assert.Equal(t, "role-arn-2", updated.FirmwareUpdateRole)
		assert.Equal(t, int32(2000), updated.FragmentIntervalMS)
		assert.Equal(t, int32(256), updated.FragmentSizeBytes)
		assert.Equal(t, int32(75), updated.RedundancyPercent)
	})
}
