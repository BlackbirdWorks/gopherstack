package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *iotwireless.InMemoryBackend) string
		verify func(t *testing.T, b *iotwireless.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *iotwireless.InMemoryBackend) string {
				d, err := b.CreateWirelessDevice(testAccountID, testRegion, "dev-1", "LoRaWAN", "", "", nil)
				require.NoError(t, err)

				return d.ID
			},
			verify: func(t *testing.T, b *iotwireless.InMemoryBackend, id string) {
				t.Helper()

				got, err := b.GetWirelessDevice(testAccountID, testRegion, id)
				require.NoError(t, err)
				assert.Equal(t, "dev-1", got.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *iotwireless.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *iotwireless.InMemoryBackend, _ string) {
				t.Helper()

				assert.Empty(t, b.ListWirelessDevices(testAccountID, testRegion))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := iotwireless.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := iotwireless.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend := iotwireless.NewInMemoryBackend()
	h := iotwireless.NewHandler(backend)

	_, err := backend.CreateWirelessGateway(testAccountID, testRegion, "gw-delegate", "", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	freshBackend := iotwireless.NewInMemoryBackend()
	h2 := iotwireless.NewHandler(freshBackend)
	require.NoError(t, h2.Restore(t.Context(), snap))

	gws := freshBackend.ListWirelessGateways(testAccountID, testRegion)
	require.Len(t, gws, 1)
	assert.Equal(t, "gw-delegate", gws[0].Name)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched, including the 8 "dirty" tables (devices, gateways,
// serviceProfiles, destinations, deviceProfiles, fuotaTasks, multicastGroups,
// networkAnalyzerConfigs) whose live value type deliberately excludes its own
// AccountID/Region fields from JSON (json:"-") and instead relies on a DTO
// type in persistence.go to carry them through the round trip -- exactly the
// class of bug (identity silently dropped on restore, resources reappearing
// under the wrong account/region, or vanishing) a same-named-fields-only test
// would miss. It also covers the 6 "clean" store.Table-backed fields
// (gatewayTasks, gatewayTaskDefs, importTasks, singleImportTasks,
// positionConfigs, resourceEventConfigs) and every raw map left untouched by
// the conversion.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := iotwireless.NewInMemoryBackend()

	dev, err := original.CreateWirelessDevice(
		testAccountID, testRegion, "dev-1", "LoRaWAN", "dest-1", "a device", map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	gw, err := original.CreateWirelessGateway(testAccountID, testRegion, "gw-1", "a gateway", nil)
	require.NoError(t, err)

	sp, err := original.CreateServiceProfile(testAccountID, testRegion, "sp-1", nil)
	require.NoError(t, err)

	dest, err := original.CreateDestination(
		testAccountID, testRegion, "dest-1", "expr", "RuleName", "role-arn", "a destination", nil,
	)
	require.NoError(t, err)

	dp, err := original.CreateDeviceProfile(testAccountID, testRegion, "dp-1", nil)
	require.NoError(t, err)

	ft, err := original.CreateFuotaTask(testAccountID, testRegion, "ft-1", "a fuota task", "image", "role", nil)
	require.NoError(t, err)

	mg, err := original.CreateMulticastGroup(testAccountID, testRegion, "mg-1", "a multicast group", nil)
	require.NoError(t, err)

	nc, err := original.CreateNetworkAnalyzerConfig(
		testAccountID, testRegion, "nc-1", "a config", []string{dev.ID}, []string{gw.ID}, nil,
	)
	require.NoError(t, err)

	// Cross-reference / association state (raw maps, untouched by the
	// conversion) -- must survive the round trip unchanged.
	require.NoError(t, original.AssociateMulticastGroupWithFuotaTask(ft.ID, mg.ID))
	require.NoError(t, original.AssociateWirelessDeviceWithFuotaTask(ft.ID, dev.ID))
	require.NoError(t, original.AssociateWirelessDeviceWithMulticastGroup(mg.ID, dev.ID))
	require.NoError(t, original.AssociateWirelessDeviceWithThing(testAccountID, testRegion, dev.ID, "thing-arn"))
	_, err = original.AssociateWirelessGatewayWithCertificate(testAccountID, testRegion, gw.ID, "cert-1")
	require.NoError(t, err)
	require.NoError(t, original.AssociateWirelessGatewayWithThing(testAccountID, testRegion, gw.ID, "gw-thing-arn"))
	require.NoError(t, original.StartMulticastGroupSession(mg.ID))
	require.NoError(t, original.UpdateLogLevelsByResourceTypes("DEBUG"))
	require.NoError(t, original.PutResourceLogLevel(dev.ID, "ERROR"))
	require.NoError(t, original.UpdatePosition(dev.ID, map[string]any{"latitude": 1.5}))
	original.EnqueueMessage(dev.ID, iotwireless.QueuedMessage{MessageID: "msg-1", PayloadBase64: "cGF5bG9hZA=="})

	_, err = original.AssociateAwsAccountWithPartnerAccount(testAccountID, testRegion, "partner-1", nil)
	require.NoError(t, err)

	require.NoError(t, original.TagResource(dest.ARN, map[string]string{"env": "prod"}))
	require.NoError(t, original.UpdateMetricConfigurationStatus("Disabled"))
	original.UpdateEventConfigurationByResourceTypes(&iotwireless.EventConfigDoc{
		Join: map[string]any{"Sidewalk": map[string]any{"WirelessDeviceIdEventTopic": "Enabled"}},
	})
	original.UpdateResourceEventConfiguration(dev.ID, "WirelessDeviceId", "Sidewalk", &iotwireless.EventConfigDoc{
		Join: map[string]any{"Sidewalk": map[string]any{"WirelessDeviceIdEventTopic": "Enabled"}},
	})

	// The 6 "clean" store.Table-backed fields.
	_, err = original.CreateWirelessGatewayTask(gw.ID, "taskdef-1")
	require.NoError(t, err)

	taskDef, err := original.CreateWirelessGatewayTaskDefinition(testAccountID, testRegion, "taskdef-1", true)
	require.NoError(t, err)

	_, err = original.StartWirelessDeviceImportTask(testAccountID, testRegion, dest.Name)
	require.NoError(t, err)

	_, err = original.StartSingleWirelessDeviceImportTask(testAccountID, testRegion, dest.Name)
	require.NoError(t, err)

	require.NoError(t, original.PutPositionConfiguration(dev.ID, "WirelessDevice", dest.Name, map[string]any{
		"SemtechGnss": map[string]any{"Status": "Enabled"},
	}))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := iotwireless.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	verifyRestoredResources(t, fresh, dev, gw, sp, dest, dp, ft, mg, nc)
	verifyRestoredAssociations(t, fresh, dev, gw, mg, ft, dest)
	verifyRestoredCleanTables(t, fresh, gw, taskDef, dest)
}

func verifyRestoredResources(
	t *testing.T,
	fresh *iotwireless.InMemoryBackend,
	dev *iotwireless.WirelessDevice,
	gw *iotwireless.WirelessGateway,
	sp *iotwireless.ServiceProfile,
	dest *iotwireless.Destination,
	dp *iotwireless.DeviceProfile,
	ft *iotwireless.FuotaTask,
	mg *iotwireless.MulticastGroup,
	nc *iotwireless.NetworkAnalyzerConfig,
) {
	t.Helper()

	gotDev, err := fresh.GetWirelessDevice(testAccountID, testRegion, dev.ID)
	require.NoError(t, err)
	assert.Equal(t, "dev-1", gotDev.Name)
	assert.Equal(t, map[string]string{"k": "v"}, gotDev.Tags)

	gotGW, err := fresh.GetWirelessGateway(testAccountID, testRegion, gw.ID)
	require.NoError(t, err)
	assert.Equal(t, "gw-1", gotGW.Name)

	gotSP, err := fresh.GetServiceProfile(testAccountID, testRegion, sp.ID)
	require.NoError(t, err)
	assert.Equal(t, "sp-1", gotSP.Name)

	gotDest, err := fresh.GetDestination(testAccountID, testRegion, dest.Name)
	require.NoError(t, err)
	assert.Equal(t, "expr", gotDest.Expression)

	gotDP, err := fresh.GetDeviceProfile(testAccountID, testRegion, dp.ID)
	require.NoError(t, err)
	assert.Equal(t, "dp-1", gotDP.Name)

	gotFT, err := fresh.GetFuotaTask(testAccountID, testRegion, ft.ID)
	require.NoError(t, err)
	assert.Equal(t, "ft-1", gotFT.Name)

	gotMG, err := fresh.GetMulticastGroup(testAccountID, testRegion, mg.ID)
	require.NoError(t, err)
	assert.Equal(t, "mg-1", gotMG.Name)

	gotNC, err := fresh.GetNetworkAnalyzerConfig(testAccountID, testRegion, nc.Name)
	require.NoError(t, err)
	assert.Equal(t, []string{dev.ID}, gotNC.WirelessDevices)
	assert.Equal(t, []string{gw.ID}, gotNC.WirelessGateways)

	// A resource created for a different account/region must never surface
	// under testAccountID/testRegion -- this is the exact bug class a
	// dropped-identity round trip would produce.
	assert.Equal(t, 1, iotwireless.DeviceCount(fresh, testAccountID, testRegion))
	assert.Equal(t, 0, iotwireless.DeviceCount(fresh, "other-account", testRegion))
}

func verifyRestoredAssociations(
	t *testing.T,
	fresh *iotwireless.InMemoryBackend,
	dev *iotwireless.WirelessDevice,
	gw *iotwireless.WirelessGateway,
	mg *iotwireless.MulticastGroup,
	ft *iotwireless.FuotaTask,
	dest *iotwireless.Destination,
) {
	t.Helper()

	groups := fresh.ListMulticastGroupsByFuotaTask(testAccountID, testRegion, ft.ID)
	require.Len(t, groups, 1)
	assert.Equal(t, mg.ID, groups[0].ID)

	_, err := fresh.GetMulticastGroupSession(mg.ID)
	require.NoError(t, err)

	certID, err := fresh.GetWirelessGatewayCertificate(testAccountID, testRegion, gw.ID)
	require.NoError(t, err)
	assert.Equal(t, "cert-1", certID)

	assert.Equal(t, "DEBUG", fresh.GetLogLevelsByResourceTypes())
	assert.Equal(t, "ERROR", fresh.GetResourceLogLevel(dev.ID))

	pos := fresh.GetPosition(dev.ID)
	assert.InEpsilon(t, 1.5, pos["latitude"], 0.0001)

	msgs := fresh.ListQueuedMessages(dev.ID)
	require.Len(t, msgs, 1)
	assert.Equal(t, "msg-1", msgs[0].MessageID)

	partnerARN, err := fresh.GetPartnerAccount("partner-1")
	require.NoError(t, err)
	assert.NotEmpty(t, partnerARN)

	tags, err := fresh.ListTagsForResource(dest.ARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])

	assert.Equal(t, "Disabled", fresh.GetMetricConfigurationStatus())

	eventCfg := fresh.GetEventConfigurationByResourceTypes()
	require.NotNil(t, eventCfg.Join)

	resourceEventCfg, ok := fresh.GetResourceEventConfiguration(dev.ID)
	require.True(t, ok)
	assert.Equal(t, "Sidewalk", resourceEventCfg.PartnerType)
}

func verifyRestoredCleanTables(
	t *testing.T,
	fresh *iotwireless.InMemoryBackend,
	gw *iotwireless.WirelessGateway,
	taskDef *iotwireless.GatewayTaskDefinition,
	dest *iotwireless.Destination,
) {
	t.Helper()

	task, err := fresh.GetWirelessGatewayTask(gw.ID)
	require.NoError(t, err)
	assert.Equal(t, "taskdef-1", task.TaskDefID)

	gotTaskDef, err := fresh.GetWirelessGatewayTaskDefinition(taskDef.ID)
	require.NoError(t, err)
	assert.True(t, gotTaskDef.AutoCreateTasks)

	importTasks := fresh.ListWirelessDeviceImportTasks()
	require.Len(t, importTasks, 1)
	assert.Equal(t, dest.Name, importTasks[0].DestinationName)
	assert.Equal(t, 1, iotwireless.ImportTaskCount(fresh))

	posCfgs := fresh.ListPositionConfigurations("WirelessDevice")
	require.Len(t, posCfgs, 1)
	assert.Equal(t, dest.Name, posCfgs[0].Destination)
}
