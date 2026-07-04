package iotwireless

import (
	"encoding/json"
	"log/slog"
	"maps"
	"time"
)

// Snapshottable is an optional interface that a StorageBackend may implement to
// support state serialisation and restoration (e.g. for --persist mode).
// Backends that do not implement it are silently skipped during snapshot/restore.
type Snapshottable interface {
	Snapshot() []byte
	Restore(data []byte) error
}

// Resettable is an optional interface that a StorageBackend may implement to
// support clearing all state (e.g. for test teardown).
type Resettable interface {
	Reset()
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot() []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(data)
	}

	return nil
}

// deviceRecord serialises a WirelessDevice together with its resource key.
type deviceRecord struct {
	Device    *WirelessDevice `json:"device"`
	AccountID string          `json:"accountID"`
	Region    string          `json:"region"`
}

// gatewayRecord serialises a WirelessGateway together with its resource key.
type gatewayRecord struct {
	Gateway   *WirelessGateway `json:"gateway"`
	AccountID string           `json:"accountID"`
	Region    string           `json:"region"`
}

// serviceProfileRecord serialises a ServiceProfile together with its resource key.
type serviceProfileRecord struct {
	Profile   *ServiceProfile `json:"profile"`
	AccountID string          `json:"accountID"`
	Region    string          `json:"region"`
}

// destinationRecord serialises a Destination together with its resource key.
type destinationRecord struct {
	Destination *Destination `json:"destination"`
	AccountID   string       `json:"accountID"`
	Region      string       `json:"region"`
}

// deviceProfileRecord serialises a DeviceProfile together with its resource key.
type deviceProfileRecord struct {
	DeviceProfile *DeviceProfile `json:"deviceProfile"`
	AccountID     string         `json:"accountID"`
	Region        string         `json:"region"`
}

// fuotaTaskRecord serialises a FuotaTask together with its resource key.
type fuotaTaskRecord struct {
	FuotaTask *FuotaTask `json:"fuotaTask"`
	AccountID string     `json:"accountID"`
	Region    string     `json:"region"`
}

// multicastGroupRecord serialises a MulticastGroup together with its resource key.
type multicastGroupRecord struct {
	MulticastGroup *MulticastGroup `json:"multicastGroup"`
	AccountID      string          `json:"accountID"`
	Region         string          `json:"region"`
}

// networkAnalyzerConfigRecord serialises a NetworkAnalyzerConfig together with its resource key.
type networkAnalyzerConfigRecord struct {
	Config    *NetworkAnalyzerConfig `json:"config"`
	AccountID string                 `json:"accountID"`
	Region    string                 `json:"region"`
}

// backendSnapshot is the serialisable form of InMemoryBackend state.
type backendSnapshot struct {
	ResourceTags               map[string]map[string]string               `json:"resourceTags,omitempty"`
	PartnerAccounts            map[string]string                          `json:"partnerAccounts,omitempty"`
	FuotaTaskMulticast         map[string]string                          `json:"fuotaTaskMulticast,omitempty"`
	FuotaTaskDevices           map[string]string                          `json:"fuotaTaskDevices,omitempty"`
	MulticastGroupDevices      map[string]string                          `json:"multicastGroupDevices,omitempty"`
	MulticastGroupSessions     map[string]bool                            `json:"multicastGroupSessions,omitempty"`
	MulticastGroupSessionStart map[string]time.Time                       `json:"multicastGroupSessionStart,omitempty"`
	WirelessDeviceThings       map[string]string                          `json:"wirelessDeviceThings,omitempty"`
	WirelessGatewayCerts       map[string]string                          `json:"wirelessGatewayCerts,omitempty"`
	WirelessGatewayThings      map[string]string                          `json:"wirelessGatewayThings,omitempty"`
	LogLevels                  map[string]string                          `json:"logLevels,omitempty"`
	ResourceLogLevels          map[string]string                          `json:"resourceLogLevels,omitempty"`
	ImportTasks                map[string]*WirelessDeviceImportTask       `json:"importTasks,omitempty"`
	SingleImportTasks          map[string]*SingleWirelessDeviceImportTask `json:"singleImportTasks,omitempty"`
	GatewayTasks               map[string]*GatewayTask                    `json:"gatewayTasks,omitempty"`
	GatewayTaskDefs            map[string]*GatewayTaskDefinition          `json:"gatewayTaskDefs,omitempty"`
	Positions                  map[string]map[string]any                  `json:"positions,omitempty"`
	QueuedMessages             map[string][]QueuedMessage                 `json:"queuedMessages,omitempty"`
	Devices                    []deviceRecord                             `json:"devices,omitempty"`
	Gateways                   []gatewayRecord                            `json:"gateways,omitempty"`
	ServiceProfiles            []serviceProfileRecord                     `json:"serviceProfiles,omitempty"`
	Destinations               []destinationRecord                        `json:"destinations,omitempty"`
	DeviceProfiles             []deviceProfileRecord                      `json:"deviceProfiles,omitempty"`
	FuotaTasks                 []fuotaTaskRecord                          `json:"fuotaTasks,omitempty"`
	MulticastGroups            []multicastGroupRecord                     `json:"multicastGroups,omitempty"`
	NetworkAnalyzerConfigs     []networkAnalyzerConfigRecord              `json:"networkAnalyzerConfigs,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := b.buildSnapshotLocked()

	data, err := json.Marshal(snap) //nolint:musttag // nested types lack tags
	if err != nil {
		slog.Default().Warn("iotwireless: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// buildSnapshotLocked populates a backendSnapshot from current in-memory state.
// Must be called with b.mu held for reading.
func (b *InMemoryBackend) buildSnapshotLocked() backendSnapshot {
	snap := backendSnapshot{
		ResourceTags:               make(map[string]map[string]string, len(b.resourceTags)),
		PartnerAccounts:            make(map[string]string, len(b.partnerAccounts)),
		FuotaTaskMulticast:         make(map[string]string, len(b.fuotaTaskMulticast)),
		FuotaTaskDevices:           make(map[string]string, len(b.fuotaTaskDevices)),
		MulticastGroupDevices:      make(map[string]string, len(b.multicastGroupDevices)),
		MulticastGroupSessions:     make(map[string]bool, len(b.multicastGroupSessions)),
		MulticastGroupSessionStart: make(map[string]time.Time, len(b.multicastGroupSessionStart)),
		WirelessDeviceThings:       make(map[string]string, len(b.wirelessDeviceThings)),
		WirelessGatewayCerts:       make(map[string]string, len(b.wirelessGatewayCerts)),
		WirelessGatewayThings:      make(map[string]string, len(b.wirelessGatewayThings)),
		LogLevels:                  make(map[string]string, len(b.logLevels)),
		ResourceLogLevels:          make(map[string]string, len(b.resourceLogLevels)),
		ImportTasks:                make(map[string]*WirelessDeviceImportTask, len(b.importTasks)),
	}

	b.snapshotResourceRecordsLocked(&snap)
	b.snapshotMapsLocked(&snap)

	return snap
}

// snapshotResourceRecordsLocked serialises all keyed resources into the snapshot.
// Must be called with b.mu held for reading.
func (b *InMemoryBackend) snapshotResourceRecordsLocked(snap *backendSnapshot) {
	for k, d := range b.devices {
		snap.Devices = append(snap.Devices, deviceRecord{AccountID: k.AccountID, Region: k.Region, Device: d})
	}

	for k, gw := range b.gateways {
		snap.Gateways = append(snap.Gateways, gatewayRecord{AccountID: k.AccountID, Region: k.Region, Gateway: gw})
	}

	for k, sp := range b.serviceProfiles {
		snap.ServiceProfiles = append(snap.ServiceProfiles, serviceProfileRecord{
			AccountID: k.AccountID, Region: k.Region, Profile: sp,
		})
	}

	for k, dest := range b.destinations {
		snap.Destinations = append(snap.Destinations, destinationRecord{
			AccountID: k.AccountID, Region: k.Region, Destination: dest,
		})
	}

	for k, dp := range b.deviceProfiles {
		snap.DeviceProfiles = append(snap.DeviceProfiles, deviceProfileRecord{
			AccountID: k.AccountID, Region: k.Region, DeviceProfile: dp,
		})
	}

	for k, ft := range b.fuotaTasks {
		snap.FuotaTasks = append(
			snap.FuotaTasks,
			fuotaTaskRecord{AccountID: k.AccountID, Region: k.Region, FuotaTask: ft},
		)
	}

	for k, mg := range b.multicastGroups {
		snap.MulticastGroups = append(snap.MulticastGroups, multicastGroupRecord{
			AccountID: k.AccountID, Region: k.Region, MulticastGroup: mg,
		})
	}

	for k, nc := range b.networkAnalyzerConfigs {
		snap.NetworkAnalyzerConfigs = append(snap.NetworkAnalyzerConfigs, networkAnalyzerConfigRecord{
			AccountID: k.AccountID, Region: k.Region, Config: nc,
		})
	}
}

// snapshotMapsLocked copies all flat maps and tags into the snapshot.
// Must be called with b.mu held for reading.
func (b *InMemoryBackend) snapshotMapsLocked(snap *backendSnapshot) {
	for arn, tags := range b.resourceTags {
		tagsCopy := make(map[string]string, len(tags))
		maps.Copy(tagsCopy, tags)

		snap.ResourceTags[arn] = tagsCopy
	}

	maps.Copy(snap.PartnerAccounts, b.partnerAccounts)
	maps.Copy(snap.FuotaTaskMulticast, b.fuotaTaskMulticast)
	maps.Copy(snap.FuotaTaskDevices, b.fuotaTaskDevices)
	maps.Copy(snap.MulticastGroupDevices, b.multicastGroupDevices)
	maps.Copy(snap.MulticastGroupSessions, b.multicastGroupSessions)
	maps.Copy(snap.MulticastGroupSessionStart, b.multicastGroupSessionStart)
	maps.Copy(snap.WirelessDeviceThings, b.wirelessDeviceThings)
	maps.Copy(snap.WirelessGatewayCerts, b.wirelessGatewayCerts)
	maps.Copy(snap.WirelessGatewayThings, b.wirelessGatewayThings)
	maps.Copy(snap.LogLevels, b.logLevels)
	maps.Copy(snap.ResourceLogLevels, b.resourceLogLevels)

	for id, task := range b.importTasks {
		cp := *task
		snap.ImportTasks[id] = &cp
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	//nolint:musttag // nested types lack tags
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.restoreMapsLocked(&snap)
	b.restoreCoreResourcesLocked(&snap)
	b.restoreNewOpsResourcesLocked(&snap)

	return nil
}

// restoreMapsLocked reinitialises all in-memory maps from the snapshot dimensions.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreMapsLocked(snap *backendSnapshot) {
	b.devices = make(map[resourceKey]*WirelessDevice, len(snap.Devices))
	b.gateways = make(map[resourceKey]*WirelessGateway, len(snap.Gateways))
	b.serviceProfiles = make(map[resourceKey]*ServiceProfile, len(snap.ServiceProfiles))
	b.destinations = make(map[resourceKey]*Destination, len(snap.Destinations))
	b.deviceProfiles = make(map[resourceKey]*DeviceProfile, len(snap.DeviceProfiles))
	b.fuotaTasks = make(map[resourceKey]*FuotaTask, len(snap.FuotaTasks))
	b.multicastGroups = make(map[resourceKey]*MulticastGroup, len(snap.MulticastGroups))
	b.networkAnalyzerConfigs = make(map[resourceKey]*NetworkAnalyzerConfig, len(snap.NetworkAnalyzerConfigs))
	b.resourceTags = make(map[string]map[string]string, len(snap.ResourceTags))
	b.partnerAccounts = make(map[string]string, len(snap.PartnerAccounts))
	b.fuotaTaskMulticast = make(map[string]string, len(snap.FuotaTaskMulticast))
	b.fuotaTaskDevices = make(map[string]string, len(snap.FuotaTaskDevices))
	b.multicastGroupDevices = make(map[string]string, len(snap.MulticastGroupDevices))
	b.multicastGroupSessions = make(map[string]bool, len(snap.MulticastGroupSessions))
	b.multicastGroupSessionStart = make(map[string]time.Time, len(snap.MulticastGroupSessionStart))
	b.wirelessDeviceThings = make(map[string]string, len(snap.WirelessDeviceThings))
	b.wirelessGatewayCerts = make(map[string]string, len(snap.WirelessGatewayCerts))
	b.wirelessGatewayThings = make(map[string]string, len(snap.WirelessGatewayThings))
	b.logLevels = make(map[string]string, len(snap.LogLevels))
	b.resourceLogLevels = make(map[string]string, len(snap.ResourceLogLevels))
	b.importTasks = make(map[string]*WirelessDeviceImportTask, len(snap.ImportTasks))
	b.singleImportTasks = make(map[string]*SingleWirelessDeviceImportTask)
}

// restoreCoreResourcesLocked restores devices, gateways, service profiles, destinations, and tags.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreCoreResourcesLocked(snap *backendSnapshot) {
	for _, rec := range snap.Devices {
		if rec.Device == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Device.ID}
		b.devices[key] = rec.Device
	}

	for _, rec := range snap.Gateways {
		if rec.Gateway == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Gateway.ID}
		b.gateways[key] = rec.Gateway
	}

	for _, rec := range snap.ServiceProfiles {
		if rec.Profile == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Profile.ID}
		b.serviceProfiles[key] = rec.Profile
	}

	for _, rec := range snap.Destinations {
		if rec.Destination == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Destination.Name}
		b.destinations[key] = rec.Destination
	}

	for arn, tags := range snap.ResourceTags {
		b.resourceTags[arn] = newTagsCopy(tags)
	}
}

// restoreNewOpsResourcesLocked restores device profiles, FUOTA tasks, multicast groups,
// network analyzer configs, import tasks, and all association maps.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreNewOpsResourcesLocked(snap *backendSnapshot) {
	for _, rec := range snap.DeviceProfiles {
		if rec.DeviceProfile == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.DeviceProfile.ID}
		b.deviceProfiles[key] = rec.DeviceProfile
	}

	for _, rec := range snap.FuotaTasks {
		if rec.FuotaTask == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.FuotaTask.ID}
		b.fuotaTasks[key] = rec.FuotaTask
	}

	for _, rec := range snap.MulticastGroups {
		if rec.MulticastGroup == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.MulticastGroup.ID}
		b.multicastGroups[key] = rec.MulticastGroup
	}

	for _, rec := range snap.NetworkAnalyzerConfigs {
		if rec.Config == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Config.Name}
		b.networkAnalyzerConfigs[key] = rec.Config
	}

	for id, task := range snap.ImportTasks {
		if task == nil {
			continue
		}

		cp := *task
		b.importTasks[id] = &cp
	}

	maps.Copy(b.partnerAccounts, snap.PartnerAccounts)
	maps.Copy(b.fuotaTaskMulticast, snap.FuotaTaskMulticast)
	maps.Copy(b.fuotaTaskDevices, snap.FuotaTaskDevices)
	maps.Copy(b.multicastGroupDevices, snap.MulticastGroupDevices)
	maps.Copy(b.multicastGroupSessions, snap.MulticastGroupSessions)
	maps.Copy(b.multicastGroupSessionStart, snap.MulticastGroupSessionStart)
	maps.Copy(b.wirelessDeviceThings, snap.WirelessDeviceThings)
	maps.Copy(b.wirelessGatewayCerts, snap.WirelessGatewayCerts)
	maps.Copy(b.wirelessGatewayThings, snap.WirelessGatewayThings)
	maps.Copy(b.logLevels, snap.LogLevels)
	maps.Copy(b.resourceLogLevels, snap.ResourceLogLevels)
}
