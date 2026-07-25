package iotwireless

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Snapshottable is an optional interface that a StorageBackend may implement to
// support state serialisation and restoration (e.g. for --persist mode).
// Backends that do not implement it are silently skipped during snapshot/restore.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Resettable is an optional interface that a StorageBackend may implement to
// support clearing all state (e.g. for test teardown).
type Resettable interface {
	Reset()
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}

// iotwirelessSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type or backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (ResetAll
// equivalent, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// iotwirelessSnapshotVersion and is discarded the same way any other
// incompatible snapshot is. Bumped 1 -> 2 when FuotaTaskMulticast/
// FuotaTaskDevices/MulticastGroupDevices changed from single-slot
// map[string]string to set-valued map[string]map[string]bool (see the doc
// comment on InMemoryBackend.fuotaTaskMulticast in store.go); a version-1
// snapshot's values for those fields are JSON strings, not objects, and
// would fail to unmarshal into the new type without this bump forcing a
// clean discard-and-reset instead.
const iotwirelessSnapshotVersion = 2

// deviceRecord serialises a WirelessDevice together with the account/region
// its store.Table keyFn needs but the value type itself excludes from JSON
// (see WirelessDevice.AccountID/Region in models.go).
type deviceRecord struct {
	Device    *WirelessDevice `json:"device"`
	AccountID string          `json:"accountID"`
	Region    string          `json:"region"`
}

func deviceRecordKey(v *deviceRecord) string {
	if v.Device == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.Device.ID)
}

// gatewayRecord serialises a WirelessGateway together with its resource key.
type gatewayRecord struct {
	Gateway   *WirelessGateway `json:"gateway"`
	AccountID string           `json:"accountID"`
	Region    string           `json:"region"`
}

func gatewayRecordKey(v *gatewayRecord) string {
	if v.Gateway == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.Gateway.ID)
}

// serviceProfileRecord serialises a ServiceProfile together with its resource key.
type serviceProfileRecord struct {
	Profile   *ServiceProfile `json:"profile"`
	AccountID string          `json:"accountID"`
	Region    string          `json:"region"`
}

func serviceProfileRecordKey(v *serviceProfileRecord) string {
	if v.Profile == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.Profile.ID)
}

// destinationRecord serialises a Destination together with its resource key.
type destinationRecord struct {
	Destination *Destination `json:"destination"`
	AccountID   string       `json:"accountID"`
	Region      string       `json:"region"`
}

func destinationRecordKey(v *destinationRecord) string {
	if v.Destination == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.Destination.Name)
}

// deviceProfileRecord serialises a DeviceProfile together with its resource key.
type deviceProfileRecord struct {
	DeviceProfile *DeviceProfile `json:"deviceProfile"`
	AccountID     string         `json:"accountID"`
	Region        string         `json:"region"`
}

func deviceProfileRecordKey(v *deviceProfileRecord) string {
	if v.DeviceProfile == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.DeviceProfile.ID)
}

// fuotaTaskRecord serialises a FuotaTask together with its resource key.
type fuotaTaskRecord struct {
	FuotaTask *FuotaTask `json:"fuotaTask"`
	AccountID string     `json:"accountID"`
	Region    string     `json:"region"`
}

func fuotaTaskRecordKey(v *fuotaTaskRecord) string {
	if v.FuotaTask == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.FuotaTask.ID)
}

// multicastGroupRecord serialises a MulticastGroup together with its resource key.
type multicastGroupRecord struct {
	MulticastGroup *MulticastGroup `json:"multicastGroup"`
	AccountID      string          `json:"accountID"`
	Region         string          `json:"region"`
}

func multicastGroupRecordKey(v *multicastGroupRecord) string {
	if v.MulticastGroup == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.MulticastGroup.ID)
}

// networkAnalyzerConfigRecord serialises a NetworkAnalyzerConfig together with its resource key.
type networkAnalyzerConfigRecord struct {
	Config    *NetworkAnalyzerConfig `json:"config"`
	AccountID string                 `json:"accountID"`
	Region    string                 `json:"region"`
}

func networkAnalyzerConfigRecordKey(v *networkAnalyzerConfigRecord) string {
	if v.Config == nil {
		return compositeKey(v.AccountID, v.Region, "")
	}

	return compositeKey(v.AccountID, v.Region, v.Config.Name)
}

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the 8 account/region-scoped "dirty" tables that
// can't be registered on b.registry directly -- see store_setup.go's file
// doc comment. Returns, in order: the ephemeral registry itself, then one
// *store.Table per dirty resource (devices, gateways, serviceProfiles,
// destinations, deviceProfiles, fuotaTasks, multicastGroups,
// networkAnalyzerConfigs).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[deviceRecord],
	*store.Table[gatewayRecord],
	*store.Table[serviceProfileRecord],
	*store.Table[destinationRecord],
	*store.Table[deviceProfileRecord],
	*store.Table[fuotaTaskRecord],
	*store.Table[multicastGroupRecord],
	*store.Table[networkAnalyzerConfigRecord],
) {
	dtoReg := store.NewRegistry()
	deviceDTOs := store.Register(dtoReg, "devices", store.New(deviceRecordKey))
	gatewayDTOs := store.Register(dtoReg, "gateways", store.New(gatewayRecordKey))
	serviceProfileDTOs := store.Register(dtoReg, "serviceProfiles", store.New(serviceProfileRecordKey))
	destinationDTOs := store.Register(dtoReg, "destinations", store.New(destinationRecordKey))
	deviceProfileDTOs := store.Register(dtoReg, "deviceProfiles", store.New(deviceProfileRecordKey))
	fuotaTaskDTOs := store.Register(dtoReg, "fuotaTasks", store.New(fuotaTaskRecordKey))
	multicastGroupDTOs := store.Register(dtoReg, "multicastGroups", store.New(multicastGroupRecordKey))
	networkAnalyzerConfigDTOs := store.Register(
		dtoReg, "networkAnalyzerConfigs", store.New(networkAnalyzerConfigRecordKey),
	)

	return dtoReg, deviceDTOs, gatewayDTOs, serviceProfileDTOs, destinationDTOs,
		deviceProfileDTOs, fuotaTaskDTOs, multicastGroupDTOs, networkAnalyzerConfigDTOs
}

// backendSnapshot is the serialisable form of InMemoryBackend state.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables: gatewayTasks,
// gatewayTaskDefs, importTasks, singleImportTasks, positionConfigs,
// resourceEventConfigs) with the ephemeral DTO registry's SnapshotAll() (the
// "dirty" tables: devices, gateways, serviceProfiles, destinations,
// deviceProfiles, fuotaTasks, multicastGroups, networkAnalyzerConfigs). See
// store_setup.go for why the split exists. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables                     map[string]json.RawMessage   `json:"tables"`
	ResourceTags               map[string]map[string]string `json:"resourceTags,omitempty"`
	PartnerAccounts            map[string]string            `json:"partnerAccounts,omitempty"`
	FuotaTaskMulticast         map[string]map[string]bool   `json:"fuotaTaskMulticast,omitempty"`
	FuotaTaskDevices           map[string]map[string]bool   `json:"fuotaTaskDevices,omitempty"`
	MulticastGroupDevices      map[string]map[string]bool   `json:"multicastGroupDevices,omitempty"`
	MulticastGroupSessions     map[string]bool              `json:"multicastGroupSessions,omitempty"`
	MulticastGroupSessionStart map[string]time.Time         `json:"multicastGroupSessionStart,omitempty"`
	WirelessDeviceThings       map[string]string            `json:"wirelessDeviceThings,omitempty"`
	WirelessGatewayCerts       map[string]string            `json:"wirelessGatewayCerts,omitempty"`
	WirelessGatewayThings      map[string]string            `json:"wirelessGatewayThings,omitempty"`
	LogLevelsConfig            *LogLevelsConfig             `json:"logLevelsConfig,omitempty"`
	ResourceLogLevels          map[string]string            `json:"resourceLogLevels,omitempty"`
	Positions                  map[string]map[string]any    `json:"positions,omitempty"`
	QueuedMessages             map[string][]QueuedMessage   `json:"queuedMessages,omitempty"`
	EventConfigDefault         *EventConfigDoc              `json:"eventConfigDefault,omitempty"`
	MetricConfigStatus         string                       `json:"metricConfigStatus,omitempty"`
	Version                    int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iotwireless: snapshot table marshal failed", "error", err)

		return nil
	}

	dirtyTables, err := b.snapshotDirtyTablesLocked()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iotwireless: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:                    iotwirelessSnapshotVersion,
		Tables:                     tables,
		ResourceTags:               copyTagsMap(b.resourceTags),
		PartnerAccounts:            copyStringMap(b.partnerAccounts),
		FuotaTaskMulticast:         copySetMap(b.fuotaTaskMulticast),
		FuotaTaskDevices:           copySetMap(b.fuotaTaskDevices),
		MulticastGroupDevices:      copySetMap(b.multicastGroupDevices),
		MulticastGroupSessions:     copyBoolMap(b.multicastGroupSessions),
		MulticastGroupSessionStart: copyTimeMap(b.multicastGroupSessionStart),
		WirelessDeviceThings:       copyStringMap(b.wirelessDeviceThings),
		WirelessGatewayCerts:       copyStringMap(b.wirelessGatewayCerts),
		WirelessGatewayThings:      copyStringMap(b.wirelessGatewayThings),
		LogLevelsConfig:            b.logLevelsConfig,
		ResourceLogLevels:          copyStringMap(b.resourceLogLevels),
		Positions:                  copyPositionsMap(b.positions),
		QueuedMessages:             copyQueuedMessagesMap(b.queuedMessages),
		EventConfigDefault:         b.eventConfigDefault,
		MetricConfigStatus:         b.metricConfigStatus,
	}

	data, err := json.Marshal(snap) //nolint:musttag // nested types lack tags
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iotwireless: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// snapshotDirtyTablesLocked builds the ephemeral DTO registry from the 8
// account/region-scoped tables and returns its SnapshotAll() output. Must be
// called with b.mu held for reading.
func (b *InMemoryBackend) snapshotDirtyTablesLocked() (map[string]json.RawMessage, error) {
	dtoReg, deviceDTOs, gatewayDTOs, serviceProfileDTOs, destinationDTOs,
		deviceProfileDTOs, fuotaTaskDTOs, multicastGroupDTOs, networkAnalyzerConfigDTOs := newDirtyDTORegistry()

	for _, v := range b.devices.Snapshot() {
		deviceDTOs.Put(&deviceRecord{Device: v, AccountID: v.AccountID, Region: v.Region})
	}

	for _, v := range b.gateways.Snapshot() {
		gatewayDTOs.Put(&gatewayRecord{Gateway: v, AccountID: v.AccountID, Region: v.Region})
	}

	for _, v := range b.serviceProfiles.Snapshot() {
		serviceProfileDTOs.Put(&serviceProfileRecord{Profile: v, AccountID: v.AccountID, Region: v.Region})
	}

	for _, v := range b.destinations.Snapshot() {
		destinationDTOs.Put(&destinationRecord{Destination: v, AccountID: v.AccountID, Region: v.Region})
	}

	for _, v := range b.deviceProfiles.Snapshot() {
		deviceProfileDTOs.Put(&deviceProfileRecord{DeviceProfile: v, AccountID: v.AccountID, Region: v.Region})
	}

	for _, v := range b.fuotaTasks.Snapshot() {
		fuotaTaskDTOs.Put(&fuotaTaskRecord{FuotaTask: v, AccountID: v.AccountID, Region: v.Region})
	}

	for _, v := range b.multicastGroups.Snapshot() {
		multicastGroupDTOs.Put(&multicastGroupRecord{MulticastGroup: v, AccountID: v.AccountID, Region: v.Region})
	}

	for _, v := range b.networkAnalyzerConfigs.Snapshot() {
		networkAnalyzerConfigDTOs.Put(&networkAnalyzerConfigRecord{Config: v, AccountID: v.AccountID, Region: v.Region})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		return nil, fmt.Errorf("iotwireless: snapshot DTO registry: %w", err)
	}

	return dirtyTables, nil
}

func copyStringMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}

func copyBoolMap(m map[string]bool) map[string]bool {
	cp := make(map[string]bool, len(m))
	maps.Copy(cp, m)

	return cp
}

// copySetMap deep-copies a map of ID sets (fuotaTaskMulticast,
// fuotaTaskDevices, multicastGroupDevices), so Snapshot's output never
// aliases the backend's live sets.
func copySetMap(m map[string]map[string]bool) map[string]map[string]bool {
	cp := make(map[string]map[string]bool, len(m))
	for k, set := range m {
		cp[k] = copyBoolMap(set)
	}

	return cp
}

func copyTimeMap(m map[string]time.Time) map[string]time.Time {
	cp := make(map[string]time.Time, len(m))
	maps.Copy(cp, m)

	return cp
}

func copyTagsMap(m map[string]map[string]string) map[string]map[string]string {
	cp := make(map[string]map[string]string, len(m))
	for arn, tags := range m {
		cp[arn] = copyStringMap(tags)
	}

	return cp
}

func copyPositionsMap(m map[string]map[string]any) map[string]map[string]any {
	cp := make(map[string]map[string]any, len(m))

	for id, pos := range m {
		posCopy := make(map[string]any, len(pos))
		maps.Copy(posCopy, pos)
		cp[id] = posCopy
	}

	return cp
}

func copyQueuedMessagesMap(m map[string][]QueuedMessage) map[string][]QueuedMessage {
	cp := make(map[string][]QueuedMessage, len(m))

	for id, msgs := range m {
		msgsCopy := make([]QueuedMessage, len(msgs))
		copy(msgsCopy, msgs)
		cp[id] = msgsCopy
	}

	return cp
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "iotwireless", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != iotwirelessSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"iotwireless: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", iotwirelessSnapshotVersion)

		b.resetTablesLocked()
		b.resetRawMapsLocked()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("iotwireless: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	b.restoreRawMapsLocked(&snap)

	return nil
}

// restoreDirtyTablesLocked restores the 8 account/region-scoped "dirty"
// tables (devices, gateways, serviceProfiles, destinations, deviceProfiles,
// fuotaTasks, multicastGroups, networkAnalyzerConfigs) from tables via the
// ephemeral DTO registry, converting each DTO back to its live type by
// re-populating the AccountID/Region fields the live type excludes from
// JSON. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, deviceDTOs, gatewayDTOs, serviceProfileDTOs, destinationDTOs,
		deviceProfileDTOs, fuotaTaskDTOs, multicastGroupDTOs, networkAnalyzerConfigDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("iotwireless: restore snapshot DTO tables: %w", err)
	}

	liveDevices := make([]*WirelessDevice, 0, deviceDTOs.Len())

	for _, dto := range deviceDTOs.All() {
		if dto.Device == nil {
			continue
		}

		dto.Device.AccountID = dto.AccountID
		dto.Device.Region = dto.Region
		liveDevices = append(liveDevices, dto.Device)
	}

	b.devices.Restore(liveDevices)

	liveGateways := make([]*WirelessGateway, 0, gatewayDTOs.Len())

	for _, dto := range gatewayDTOs.All() {
		if dto.Gateway == nil {
			continue
		}

		dto.Gateway.AccountID = dto.AccountID
		dto.Gateway.Region = dto.Region
		liveGateways = append(liveGateways, dto.Gateway)
	}

	b.gateways.Restore(liveGateways)

	liveServiceProfiles := make([]*ServiceProfile, 0, serviceProfileDTOs.Len())

	for _, dto := range serviceProfileDTOs.All() {
		if dto.Profile == nil {
			continue
		}

		dto.Profile.AccountID = dto.AccountID
		dto.Profile.Region = dto.Region
		liveServiceProfiles = append(liveServiceProfiles, dto.Profile)
	}

	b.serviceProfiles.Restore(liveServiceProfiles)

	liveDestinations := make([]*Destination, 0, destinationDTOs.Len())

	for _, dto := range destinationDTOs.All() {
		if dto.Destination == nil {
			continue
		}

		dto.Destination.AccountID = dto.AccountID
		dto.Destination.Region = dto.Region
		liveDestinations = append(liveDestinations, dto.Destination)
	}

	b.destinations.Restore(liveDestinations)

	b.restoreRemainingDirtyTablesLocked(deviceProfileDTOs, fuotaTaskDTOs, multicastGroupDTOs, networkAnalyzerConfigDTOs)

	return nil
}

// restoreRemainingDirtyTablesLocked restores deviceProfiles, fuotaTasks,
// multicastGroups, and networkAnalyzerConfigs -- split out from
// restoreDirtyTablesLocked purely to keep that function under the project's
// length/complexity lint budget. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreRemainingDirtyTablesLocked(
	deviceProfileDTOs *store.Table[deviceProfileRecord],
	fuotaTaskDTOs *store.Table[fuotaTaskRecord],
	multicastGroupDTOs *store.Table[multicastGroupRecord],
	networkAnalyzerConfigDTOs *store.Table[networkAnalyzerConfigRecord],
) {
	liveDeviceProfiles := make([]*DeviceProfile, 0, deviceProfileDTOs.Len())

	for _, dto := range deviceProfileDTOs.All() {
		if dto.DeviceProfile == nil {
			continue
		}

		dto.DeviceProfile.AccountID = dto.AccountID
		dto.DeviceProfile.Region = dto.Region
		liveDeviceProfiles = append(liveDeviceProfiles, dto.DeviceProfile)
	}

	b.deviceProfiles.Restore(liveDeviceProfiles)

	liveFuotaTasks := make([]*FuotaTask, 0, fuotaTaskDTOs.Len())

	for _, dto := range fuotaTaskDTOs.All() {
		if dto.FuotaTask == nil {
			continue
		}

		dto.FuotaTask.AccountID = dto.AccountID
		dto.FuotaTask.Region = dto.Region
		liveFuotaTasks = append(liveFuotaTasks, dto.FuotaTask)
	}

	b.fuotaTasks.Restore(liveFuotaTasks)

	liveMulticastGroups := make([]*MulticastGroup, 0, multicastGroupDTOs.Len())

	for _, dto := range multicastGroupDTOs.All() {
		if dto.MulticastGroup == nil {
			continue
		}

		dto.MulticastGroup.AccountID = dto.AccountID
		dto.MulticastGroup.Region = dto.Region
		liveMulticastGroups = append(liveMulticastGroups, dto.MulticastGroup)
	}

	b.multicastGroups.Restore(liveMulticastGroups)

	liveNetworkAnalyzerConfigs := make([]*NetworkAnalyzerConfig, 0, networkAnalyzerConfigDTOs.Len())

	for _, dto := range networkAnalyzerConfigDTOs.All() {
		if dto.Config == nil {
			continue
		}

		dto.Config.AccountID = dto.AccountID
		dto.Config.Region = dto.Region
		liveNetworkAnalyzerConfigs = append(liveNetworkAnalyzerConfigs, dto.Config)
	}

	b.networkAnalyzerConfigs.Restore(liveNetworkAnalyzerConfigs)
}

// resetRawMapsLocked reinitialises every plain (non-store.Table) map field to
// empty. Must be called with b.mu held for writing.
func (b *InMemoryBackend) resetRawMapsLocked() {
	b.resourceTags = make(map[string]map[string]string)
	b.partnerAccounts = make(map[string]string)
	b.fuotaTaskMulticast = make(map[string]map[string]bool)
	b.fuotaTaskDevices = make(map[string]map[string]bool)
	b.multicastGroupDevices = make(map[string]map[string]bool)
	b.multicastGroupSessions = make(map[string]bool)
	b.multicastGroupSessionStart = make(map[string]time.Time)
	b.wirelessDeviceThings = make(map[string]string)
	b.wirelessGatewayCerts = make(map[string]string)
	b.wirelessGatewayThings = make(map[string]string)
	b.logLevelsConfig = nil
	b.resourceLogLevels = make(map[string]string)
	b.positions = make(map[string]map[string]any)
	b.queuedMessages = make(map[string][]QueuedMessage)
	b.eventConfigDefault = nil
	b.metricConfigStatus = ""
}

// restoreRawMapsLocked reinitialises every plain (non-store.Table) map field
// from the snapshot. Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreRawMapsLocked(snap *backendSnapshot) {
	b.resourceTags = make(map[string]map[string]string, len(snap.ResourceTags))
	for arn, tags := range snap.ResourceTags {
		b.resourceTags[arn] = newTagsCopy(tags)
	}

	b.partnerAccounts = make(map[string]string, len(snap.PartnerAccounts))
	maps.Copy(b.partnerAccounts, snap.PartnerAccounts)

	b.fuotaTaskMulticast = copySetMap(snap.FuotaTaskMulticast)
	b.fuotaTaskDevices = copySetMap(snap.FuotaTaskDevices)
	b.multicastGroupDevices = copySetMap(snap.MulticastGroupDevices)

	b.multicastGroupSessions = make(map[string]bool, len(snap.MulticastGroupSessions))
	maps.Copy(b.multicastGroupSessions, snap.MulticastGroupSessions)

	b.multicastGroupSessionStart = make(map[string]time.Time, len(snap.MulticastGroupSessionStart))
	maps.Copy(b.multicastGroupSessionStart, snap.MulticastGroupSessionStart)

	b.wirelessDeviceThings = make(map[string]string, len(snap.WirelessDeviceThings))
	maps.Copy(b.wirelessDeviceThings, snap.WirelessDeviceThings)

	b.wirelessGatewayCerts = make(map[string]string, len(snap.WirelessGatewayCerts))
	maps.Copy(b.wirelessGatewayCerts, snap.WirelessGatewayCerts)

	b.wirelessGatewayThings = make(map[string]string, len(snap.WirelessGatewayThings))
	maps.Copy(b.wirelessGatewayThings, snap.WirelessGatewayThings)

	b.logLevelsConfig = snap.LogLevelsConfig

	b.resourceLogLevels = make(map[string]string, len(snap.ResourceLogLevels))
	maps.Copy(b.resourceLogLevels, snap.ResourceLogLevels)

	b.positions = make(map[string]map[string]any, len(snap.Positions))

	for id, pos := range snap.Positions {
		posCopy := make(map[string]any, len(pos))
		maps.Copy(posCopy, pos)
		b.positions[id] = posCopy
	}

	b.queuedMessages = make(map[string][]QueuedMessage, len(snap.QueuedMessages))

	for id, msgs := range snap.QueuedMessages {
		msgsCopy := make([]QueuedMessage, len(msgs))
		copy(msgsCopy, msgs)
		b.queuedMessages[id] = msgsCopy
	}

	b.eventConfigDefault = snap.EventConfigDefault
	b.metricConfigStatus = snap.MetricConfigStatus
}
