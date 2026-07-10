package medialive

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// backend-level map[string]*T resource field on InMemoryBackend is replaced
// with a *store.Table[T], following the pattern established by services/ec2
// (data-driven registration slice), services/ses (DTO-registry for types that
// can't round-trip a direct JSON marshal), and services/sesv2 (clean direct
// registration + composite-key flattening). See pkgs/store's package doc for
// the underlying primitive.
//
// Every value type below already carries its own identity as a real
// (non-json:"-") field -- storedChannel.ID, storedInput.ID,
// storedReservation.ReservationID, etc. were already set consistently at
// every write site before this refactor. So none of the 15 tables need the
// ses-style "dirty" DTO-registry treatment: all 15 are "clean" tables
// registered directly on b.registry, and persistence.go drives every one of
// them through b.registry.SnapshotAll() / RestoreAll().
//
// channelPlacementGroups was previously keyed by the composite
// "<clusterID>/<groupID>" string built by cpgKey. It is registered directly
// here too -- storedChannelPlacementGroup.ClusterID and .ID are both real
// fields, so channelPlacementGroupKeyFn reconstructs the exact same composite
// key from the value itself, with no flattening or secondary index needed.
//
// Nested collections that live inside another table's value type --
// storedCluster.Nodes (map[string]*storedNode) and
// storedMultiplex.Programs (map[string]*storedMultiplexProgram) -- are NOT
// converted: they are fields of a value already held in a Table, not
// backend-level collections, so store.Table (which is keyed by the backend
// field's own map) does not apply to them.
//
// Left as plain maps (not store.Table-backed), each audited against the
// pre-refactor persistence.go / backend.go for its persisted status:
//   - tags (map[string]map[string]string): values are plain strings, not
//     *T, so they do not fit store.Table's keyed-by-identity-value shape.
//     Was persisted before -- still persisted, unchanged.
//   - scheduleActions (map[string][]*storedScheduleAction): slice-valued,
//     not *T. Was persisted before -- still persisted, unchanged.
//   - pendingTransferDeviceIDs (map[string]struct{}): an ephemeral reverse
//     index rebuilt from inputDevices on Restore (rebuildPendingTransferIndex).
//     Was NOT persisted before (absent from the old snapshot struct) --
//     still not persisted, unchanged.
//
// offerings ([]*Offering) is a seeded catalog slice, not a map, and was never
// persisted (re-seeded by NewInMemoryBackend/seedOfferings); untouched here.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func channelKeyFn(v *storedChannel) string { return v.ID }

func inputKeyFn(v *storedInput) string { return v.ID }

func inputSecurityGroupKeyFn(v *storedInputSecurityGroup) string { return v.ID }

func inputDeviceKeyFn(v *storedInputDevice) string { return v.ID }

func multiplexKeyFn(v *storedMultiplex) string { return v.ID }

func clusterKeyFn(v *storedCluster) string { return v.ID }

func signalMapKeyFn(v *storedSignalMap) string { return v.ID }

func cwAlarmTemplateGroupKeyFn(v *storedCloudWatchAlarmTemplateGroup) string { return v.ID }

func cwAlarmTemplateKeyFn(v *storedCloudWatchAlarmTemplate) string { return v.ID }

func ebRuleTemplateGroupKeyFn(v *storedEventBridgeRuleTemplateGroup) string { return v.ID }

func ebRuleTemplateKeyFn(v *storedEventBridgeRuleTemplate) string { return v.ID }

func reservationKeyFn(v *storedReservation) string { return v.ReservationID }

func networkKeyFn(v *storedNetwork) string { return v.ID }

func sdiSourceKeyFn(v *storedSdiSource) string { return v.ID }

// channelPlacementGroupKeyFn reconstructs the composite "<clusterID>/<id>"
// key via cpgKey (backend.go), so Table.Put/Restore derive exactly the same
// key access sites already build by calling cpgKey directly.
func channelPlacementGroupKeyFn(v *storedChannelPlacementGroup) string {
	return cpgKey(v.ClusterID, v.ID)
}

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))
	b.inputs = store.Register(b.registry, "inputs", store.New(inputKeyFn))
	b.inputSecurityGroups = store.Register(
		b.registry, "inputSecurityGroups", store.New(inputSecurityGroupKeyFn),
	)
	b.inputDevices = store.Register(b.registry, "inputDevices", store.New(inputDeviceKeyFn))
	b.multiplexes = store.Register(b.registry, "multiplexes", store.New(multiplexKeyFn))
	b.clusters = store.Register(b.registry, "clusters", store.New(clusterKeyFn))
	b.signalMaps = store.Register(b.registry, "signalMaps", store.New(signalMapKeyFn))
	b.cwAlarmTemplateGroups = store.Register(
		b.registry, "cwAlarmTemplateGroups", store.New(cwAlarmTemplateGroupKeyFn),
	)
	b.cwAlarmTemplates = store.Register(b.registry, "cwAlarmTemplates", store.New(cwAlarmTemplateKeyFn))
	b.ebRuleTemplateGroups = store.Register(
		b.registry, "ebRuleTemplateGroups", store.New(ebRuleTemplateGroupKeyFn),
	)
	b.ebRuleTemplates = store.Register(b.registry, "ebRuleTemplates", store.New(ebRuleTemplateKeyFn))
	b.reservations = store.Register(b.registry, "reservations", store.New(reservationKeyFn))
	b.networks = store.Register(b.registry, "networks", store.New(networkKeyFn))
	b.sdiSources = store.Register(b.registry, "sdiSources", store.New(sdiSourceKeyFn))
	b.channelPlacementGroups = store.Register(
		b.registry, "channelPlacementGroups", store.New(channelPlacementGroupKeyFn),
	)
}
