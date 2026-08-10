package iotwireless_test

import (
	"context"
	"testing"
	"time"

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
				d, err := b.CreateWirelessDevice(
					testAccountID,
					testRegion,
					"dev-1",
					"LoRaWAN",
					"",
					"",
					"",
					nil,
					nil,
					nil,
				)
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

				assert.Empty(
					t,
					b.ListWirelessDevices(testAccountID, testRegion, iotwireless.ListWirelessDevicesFilter{}),
				)
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

	_, err := backend.CreateWirelessGateway(testAccountID, testRegion, "gw-delegate", "", nil, nil)
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
		testAccountID,
		testRegion,
		"dev-1",
		"LoRaWAN",
		"dest-1",
		"a device",
		"",
		nil,
		nil,
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	gw, err := original.CreateWirelessGateway(testAccountID, testRegion, "gw-1", "a gateway", nil, nil)
	require.NoError(t, err)

	sp, err := original.CreateServiceProfile(testAccountID, testRegion, "sp-1", nil, nil)
	require.NoError(t, err)

	dest, err := original.CreateDestination(
		testAccountID, testRegion, "dest-1", "expr", "RuleName", "role-arn", "a destination", nil,
	)
	require.NoError(t, err)

	dp, err := original.CreateDeviceProfile(testAccountID, testRegion, "dp-1", nil, nil, nil)
	require.NoError(t, err)

	ft, err := original.CreateFuotaTask(
		testAccountID,
		testRegion,
		"ft-1",
		"a fuota task",
		"image",
		"role",
		"",
		0,
		0,
		0,
		nil,
		nil,
	)
	require.NoError(t, err)

	mg, err := original.CreateMulticastGroup(testAccountID, testRegion, "mg-1", "a multicast group", nil, nil)
	require.NoError(t, err)

	nc, err := original.CreateNetworkAnalyzerConfig(
		testAccountID,
		testRegion,
		"nc-1",
		"a config",
		[]string{dev.ID},
		[]string{gw.ID},
		nil,
		nil,
		nil,
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
	require.NoError(t, original.UpdateLogLevelsByResourceTypes(iotwireless.LogLevelsConfig{DefaultLogLevel: "DEBUG"}))
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

	taskDef, err := original.CreateWirelessGatewayTaskDefinition(testAccountID, testRegion, "taskdef-1", true, nil)
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

	assert.Equal(t, "DEBUG", fresh.GetLogLevelsByResourceTypes().DefaultLogLevel)
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

func TestInMemoryBackend_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	// Create one of each resource type.
	dev, err := bk.CreateWirelessDevice(
		testAccountID,
		testRegion,
		"dev-snap",
		"LoRaWAN",
		"dest-snap",
		"desc",
		"",
		nil,
		nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	gw, err := bk.CreateWirelessGateway(
		testAccountID,
		testRegion,
		"gw-snap",
		"gateway",
		nil,
		map[string]string{"tier": "free"},
	)
	require.NoError(t, err)

	sp, err := bk.CreateServiceProfile(testAccountID, testRegion, "sp-snap", nil, map[string]string{"role": "iot"})
	require.NoError(t, err)

	dest, err := bk.CreateDestination(
		testAccountID,
		testRegion,
		"dest-snap",
		"rule",
		"RuleName",
		"arn:role",
		"desc",
		nil,
	)
	require.NoError(t, err)

	// Snapshot.
	snap := bk.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into a fresh backend.
	bk2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, bk2.Restore(t.Context(), snap))

	// Verify all resources are present with correct fields.
	gotDev, err := bk2.GetWirelessDevice(testAccountID, testRegion, dev.ID)
	require.NoError(t, err)
	assert.Equal(t, dev.Name, gotDev.Name)
	assert.Equal(t, "test", gotDev.Tags["env"])

	gotGW, err := bk2.GetWirelessGateway(testAccountID, testRegion, gw.ID)
	require.NoError(t, err)
	assert.Equal(t, gw.Name, gotGW.Name)

	gotSP, err := bk2.GetServiceProfile(testAccountID, testRegion, sp.ID)
	require.NoError(t, err)
	assert.Equal(t, sp.Name, gotSP.Name)

	gotDest, err := bk2.GetDestination(testAccountID, testRegion, dest.Name)
	require.NoError(t, err)
	assert.Equal(t, dest.Expression, gotDest.Expression)

	// Resource tags are preserved.
	tags, err := bk2.ListTagsForResource(dev.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

// TestInMemoryBackend_PersistenceSnapshotRestore_OperationalState verifies
// that gateway tasks/definitions, positions, queued messages, position
// configurations, and event configurations all survive a snapshot/restore
// round trip. These were previously declared in backendSnapshot but never
// actually populated or restored.
func TestInMemoryBackend_PersistenceSnapshotRestore_OperationalState(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	def, err := bk.CreateWirelessGatewayTaskDefinition("123456789012", "us-east-1", "taskdef-snap", true, nil)
	require.NoError(t, err)

	task, err := bk.CreateWirelessGatewayTask("gw-snap", def.ID)
	require.NoError(t, err)

	require.NoError(
		t,
		bk.UpdatePosition("resource-snap", map[string]any{"Position": []any{1.0, 2.0}}),
	)

	bk.EnqueueMessage(
		"dev-snap",
		iotwireless.QueuedMessage{MessageID: "msg-1", PayloadBase64: "aGk="},
	)

	require.NoError(t, bk.PutPositionConfiguration(
		"resource-snap",
		"WirelessDevice",
		"dest-snap",
		map[string]any{"SemtechGnss": map[string]any{"Status": "Enabled"}},
	))

	bk.UpdateResourceEventConfiguration(
		"resource-snap",
		"WirelessDeviceId",
		"",
		&iotwireless.EventConfigDoc{
			Join: map[string]any{"LoRaWAN": map[string]any{"DevEuiEventTopic": "Enabled"}},
		},
	)

	bk.UpdateEventConfigurationByResourceTypes(&iotwireless.EventConfigDoc{
		Proximity: map[string]any{"WirelessDeviceEventTopic": "Enabled"},
	})

	require.NoError(t, bk.UpdateMetricConfigurationStatus("Disabled"))

	snap := bk.Snapshot(context.Background())
	require.NotNil(t, snap)

	bk2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, bk2.Restore(context.Background(), snap))

	gotTask, err := bk2.GetWirelessGatewayTask(task.WirelessGatewayID)
	require.NoError(t, err)
	assert.Equal(t, def.ID, gotTask.TaskDefID)

	gotDef, err := bk2.GetWirelessGatewayTaskDefinition(def.ID)
	require.NoError(t, err)
	assert.Equal(t, "taskdef-snap", gotDef.Name)

	pos := bk2.GetPosition("resource-snap")
	require.Equal(t, []any{1.0, 2.0}, pos["Position"])

	msgs := bk2.ListQueuedMessages("dev-snap")
	require.Len(t, msgs, 1)
	assert.Equal(t, "msg-1", msgs[0].MessageID)

	cfg, ok := bk2.GetPositionConfiguration("resource-snap")
	require.True(t, ok)
	assert.Equal(t, "dest-snap", cfg.Destination)

	evtCfg, ok := bk2.GetResourceEventConfiguration("resource-snap")
	require.True(t, ok)
	assert.Equal(t, "WirelessDeviceId", evtCfg.IdentifierType)

	defaultCfg := bk2.GetEventConfigurationByResourceTypes()
	assert.NotNil(t, defaultCfg.Proximity)

	assert.Equal(t, "Disabled", bk2.GetMetricConfigurationStatus())
}

func TestInMemoryBackend_Snapshot_IncludesMulticastGroups(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	mg1, err := b.CreateMulticastGroup(
		testAccountID,
		testRegion,
		"mg-snap-1",
		"desc1",
		nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	mg2, err := b.CreateMulticastGroup(testAccountID, testRegion, "mg-snap-2", "desc2", nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	got1, err := b2.GetMulticastGroup(testAccountID, testRegion, mg1.ID)
	require.NoError(t, err)
	assert.Equal(t, mg1.Name, got1.Name)
	assert.Equal(t, "desc1", got1.Description)
	assert.Equal(t, "test", got1.Tags["env"])

	got2, err := b2.GetMulticastGroup(testAccountID, testRegion, mg2.ID)
	require.NoError(t, err)
	assert.Equal(t, mg2.Name, got2.Name)
}

func TestInMemoryBackend_Snapshot_IncludesNetworkAnalyzerConfigs(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	nc, err := b.CreateNetworkAnalyzerConfig(
		testAccountID,
		testRegion,
		"nc-snap-1",
		"my network analyzer",
		[]string{"dev-1", "dev-2"},
		[]string{"gw-1"},
		nil,
		nil,
		map[string]string{"env": "prod"},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	got, err := b2.GetNetworkAnalyzerConfig(testAccountID, testRegion, nc.Name)
	require.NoError(t, err)
	assert.Equal(t, nc.Name, got.Name)
	assert.Equal(t, "my network analyzer", got.Description)
	assert.Equal(t, []string{"dev-1", "dev-2"}, got.WirelessDevices)
	assert.Equal(t, []string{"gw-1"}, got.WirelessGateways)
	assert.Equal(t, "prod", got.Tags["env"])
}

func TestInMemoryBackend_Snapshot_IncludesImportTasks(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	task, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "snap-dest")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	got, err := b2.GetWirelessDeviceImportTask(task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, "snap-dest", got.DestinationName)
	assert.Equal(t, "Initialized", got.Status)
}

func TestInMemoryBackend_Snapshot_IncludesLogLevels(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	require.NoError(t, b.UpdateLogLevelsByResourceTypes(iotwireless.LogLevelsConfig{DefaultLogLevel: "ERROR"}))
	require.NoError(t, b.PutResourceLogLevel("res-001", "DEBUG"))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, "ERROR", b2.GetLogLevelsByResourceTypes().DefaultLogLevel)
	assert.Equal(t, "DEBUG", b2.GetResourceLogLevel("res-001"))
}

func TestInMemoryBackend_SnapshotRestore_FullRoundTrip(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	// Populate a variety of resource types.
	dev, err := b.CreateWirelessDevice(
		testAccountID,
		testRegion,
		"snap-dev",
		"LoRaWAN",
		"dest",
		"desc",
		"",
		nil,
		nil,
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	gw, err := b.CreateWirelessGateway(testAccountID, testRegion, "snap-gw", "a gateway", nil, nil)
	require.NoError(t, err)

	mg, err := b.CreateMulticastGroup(testAccountID, testRegion, "snap-mg", "", nil, nil)
	require.NoError(t, err)

	nc, err := b.CreateNetworkAnalyzerConfig(testAccountID, testRegion, "snap-nc", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	it, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "snap-dest")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotwireless.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify all resources survived.
	gotDev, err := b2.GetWirelessDevice(testAccountID, testRegion, dev.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-dev", gotDev.Name)
	assert.Equal(t, "v", gotDev.Tags["k"])

	gotGW, err := b2.GetWirelessGateway(testAccountID, testRegion, gw.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-gw", gotGW.Name)

	gotMG, err := b2.GetMulticastGroup(testAccountID, testRegion, mg.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-mg", gotMG.Name)

	gotNC, err := b2.GetNetworkAnalyzerConfig(testAccountID, testRegion, nc.Name)
	require.NoError(t, err)
	assert.Equal(t, "snap-nc", gotNC.Name)

	gotIT, err := b2.GetWirelessDeviceImportTask(it.ID)
	require.NoError(t, err)
	assert.Equal(t, "snap-dest", gotIT.DestinationName)
}

// TestInMemoryBackend_PersistenceRoundTrip verifies Snapshot/Restore for all resource types.
func TestInMemoryBackend_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	dp, err := b.CreateDeviceProfile(
		testAccountID,
		testRegion,
		"dp-persist",
		nil,
		nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	ft, err := b.CreateFuotaTask(
		testAccountID,
		testRegion,
		"ft-persist",
		"desc",
		"s3://bucket/fw.bin",
		"arn:role",
		"",
		0,
		0,
		0,
		nil,
		nil,
	)
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

// TestInMemoryBackend_PersistenceRoundTrip_LoRaWANSidewalk verifies the
// typed LoRaWAN/Sidewalk sub-fields on ServiceProfile/DeviceProfile/
// FuotaTask/MulticastGroup survive a Snapshot->Restore cycle, including a
// nested sub-struct field (LoRaWANMulticast.DefaultSessionParameters). Each
// *Record DTO in persistence.go embeds the live struct by pointer rather
// than hand-copying its fields, so this mainly guards against that DTO
// wrapping being replaced with a hand-maintained field list in the future
// (the exact class of bug found in sagemaker's snapshot DTO, which silently
// dropped fields the live struct had).
func TestInMemoryBackend_PersistenceRoundTrip_LoRaWANSidewalk(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()
	drMax := int32(15)
	macVersion := "1.0.3"
	dlDr := int32(5)

	sp, err := b.CreateServiceProfile(
		testAccountID, testRegion, "sp-lorawan",
		&iotwireless.LoRaWANServiceProfile{DrMax: &drMax, AddGwMetadata: true},
		nil,
	)
	require.NoError(t, err)

	dp, err := b.CreateDeviceProfile(
		testAccountID, testRegion, "dp-lorawan",
		&iotwireless.LoRaWANDeviceProfile{MacVersion: &macVersion},
		&iotwireless.SidewalkCreateDeviceProfile{},
		nil,
	)
	require.NoError(t, err)

	ft, err := b.CreateFuotaTask(
		testAccountID, testRegion, "ft-lorawan", "", "s3://img", "role", "",
		0, 0, 0,
		&iotwireless.LoRaWANFuotaTask{RfRegion: "US915"},
		nil,
	)
	require.NoError(t, err)

	startTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, b.StartFuotaTask(testAccountID, testRegion, ft.ID, &startTime))

	mg, err := b.CreateMulticastGroup(
		testAccountID, testRegion, "mg-lorawan", "",
		&iotwireless.LoRaWANMulticast{
			RfRegion:                 "EU868",
			DefaultSessionParameters: &iotwireless.DefaultSessionParametersMulticast{DlDr: &dlDr},
		},
		nil,
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := iotwireless.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	gotSP, err := restored.GetServiceProfile(testAccountID, testRegion, sp.ID)
	require.NoError(t, err)
	require.NotNil(t, gotSP.LoRaWAN)
	assert.Equal(t, int32(15), *gotSP.LoRaWAN.DrMax)
	assert.True(t, gotSP.LoRaWAN.AddGwMetadata)

	gotDP, err := restored.GetDeviceProfile(testAccountID, testRegion, dp.ID)
	require.NoError(t, err)
	require.NotNil(t, gotDP.LoRaWAN)
	assert.Equal(t, "1.0.3", *gotDP.LoRaWAN.MacVersion)
	assert.NotNil(t, gotDP.Sidewalk, "Sidewalk presence must survive restore")

	gotFT, err := restored.GetFuotaTask(testAccountID, testRegion, ft.ID)
	require.NoError(t, err)
	require.NotNil(t, gotFT.LoRaWAN)
	assert.Equal(t, "US915", gotFT.LoRaWAN.RfRegion)
	require.NotNil(t, gotFT.StartTime, "StartFuotaTask's StartTime must survive restore")
	assert.True(t, startTime.Equal(*gotFT.StartTime))

	gotMG, err := restored.GetMulticastGroup(testAccountID, testRegion, mg.ID)
	require.NoError(t, err)
	require.NotNil(t, gotMG.LoRaWAN)
	assert.Equal(t, "EU868", gotMG.LoRaWAN.RfRegion)
	require.NotNil(t, gotMG.LoRaWAN.DefaultSessionParameters)
	assert.Equal(t, int32(5), *gotMG.LoRaWAN.DefaultSessionParameters.DlDr)
}
