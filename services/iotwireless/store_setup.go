package iotwireless

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[resourceKey]*T / map[string]*T resource field on InMemoryBackend is
// replaced with a *store.Table[T], following the pattern established by
// services/ec2 (commit 12e611a4, data-driven registration slice), services/sqs
// (commit 0f09d77c, DTO-registry), and services/ses (commit e9af4cc7,
// identity-less types gain a json:"-" key field + DTO). See pkgs/store's
// package doc for the underlying primitive.
//
// Tables split into two groups:
//
//   - "Clean" tables (gatewayTasks, gatewayTaskDefs, importTasks,
//     singleImportTasks, positionConfigs, resourceEventConfigs) key off a
//     field the value type already carries (WirelessGatewayID / ID / ARN /
//     ResourceIdentifier / Identifier), so they are registered on b.registry
//     here and persistence.go drives them through b.registry.SnapshotAll() /
//     RestoreAll() directly.
//   - "Dirty" tables (devices, gateways, serviceProfiles, destinations,
//     deviceProfiles, fuotaTasks, multicastGroups, networkAnalyzerConfigs) are
//     scoped per account+region, a dimension none of these value types
//     natively carry. Each gained AccountID/Region fields tagged `json:"-"`
//     purely so store.Table's keyFn can derive the composite key this backend
//     previously used resourceKey{AccountID,Region,ID} for -- see the doc
//     comments on those fields in models.go. They are NOT registered on
//     b.registry: persistence.go instead builds a throwaway DTO
//     store.Registry (mirroring services/ses) whose DTO types carry
//     AccountID/Region as real JSON fields, so Snapshot/Restore never depends
//     on a json:"-" field to round-trip correctly.
//
// resourceTags, partnerAccounts, fuotaTaskMulticast, fuotaTaskDevices,
// multicastGroupDevices, multicastGroupSessions, multicastGroupSessionStart,
// wirelessDeviceThings, wirelessGatewayCerts, wirelessGatewayThings,
// logLevels, resourceLogLevels, positions, and queuedMessages are
// deliberately left plain maps: their values are not *T (plain strings,
// bools, times, map[string]any, or []QueuedMessage), so none fits
// store.Table's keyed-by-identity-*T shape. eventConfigDefault and
// metricConfigStatus aren't maps at all.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// compositeKeySep separates the AccountID/Region/ID (or /Name) components of
// a dirty table's composite key. \x00 cannot appear in any AWS
// account ID, region, or resource name/ID, so it can never cause a spurious
// collision the way a printable separator like "/" or "#" could for a
// destination or network analyzer config name.
const compositeKeySep = "\x00"

// compositeKey builds the composite key used by every "dirty" table below,
// replacing the resourceKey{AccountID,Region,ID} struct this backend used to
// map on directly.
func compositeKey(accountID, region, id string) string {
	return accountID + compositeKeySep + region + compositeKeySep + id
}

func deviceKeyFn(v *WirelessDevice) string { return compositeKey(v.AccountID, v.Region, v.ID) }

func gatewayKeyFn(v *WirelessGateway) string { return compositeKey(v.AccountID, v.Region, v.ID) }

func serviceProfileKeyFn(v *ServiceProfile) string {
	return compositeKey(v.AccountID, v.Region, v.ID)
}

func destinationKeyFn(v *Destination) string { return compositeKey(v.AccountID, v.Region, v.Name) }

func deviceProfileKeyFn(v *DeviceProfile) string {
	return compositeKey(v.AccountID, v.Region, v.ID)
}

func fuotaTaskKeyFn(v *FuotaTask) string { return compositeKey(v.AccountID, v.Region, v.ID) }

func multicastGroupKeyFn(v *MulticastGroup) string {
	return compositeKey(v.AccountID, v.Region, v.ID)
}

func networkAnalyzerConfigKeyFn(v *NetworkAnalyzerConfig) string {
	return compositeKey(v.AccountID, v.Region, v.Name)
}

func gatewayTaskKeyFn(v *GatewayTask) string { return v.WirelessGatewayID }

func gatewayTaskDefKeyFn(v *GatewayTaskDefinition) string { return v.ID }

func importTaskKeyFn(v *WirelessDeviceImportTask) string { return v.ID }

func singleImportTaskKeyFn(v *SingleWirelessDeviceImportTask) string { return v.ARN }

func positionConfigKeyFn(v *PositionConfigEntry) string { return v.ResourceIdentifier }

func resourceEventConfigKeyFn(v *ResourceEventConfigEntry) string { return v.Identifier }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. "Clean" tables are registered on
// b.registry; "dirty" tables are constructed alongside them but deliberately
// NOT registered -- see the file doc comment above. It must be called during
// construction only, never on every Reset(): store.Register panics on a
// duplicate name, so runtime resets go through resetTablesLocked
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.gatewayTasks = store.Register(b.registry, "gatewayTasks", store.New(gatewayTaskKeyFn))
	b.gatewayTaskDefs = store.Register(b.registry, "gatewayTaskDefs", store.New(gatewayTaskDefKeyFn))
	b.importTasks = store.Register(b.registry, "importTasks", store.New(importTaskKeyFn))
	b.singleImportTasks = store.Register(b.registry, "singleImportTasks", store.New(singleImportTaskKeyFn))
	b.positionConfigs = store.Register(b.registry, "positionConfigs", store.New(positionConfigKeyFn))
	b.resourceEventConfigs = store.Register(
		b.registry, "resourceEventConfigs", store.New(resourceEventConfigKeyFn),
	)

	b.devices = store.New(deviceKeyFn)
	b.gateways = store.New(gatewayKeyFn)
	b.serviceProfiles = store.New(serviceProfileKeyFn)
	b.destinations = store.New(destinationKeyFn)
	b.deviceProfiles = store.New(deviceProfileKeyFn)
	b.fuotaTasks = store.New(fuotaTaskKeyFn)
	b.multicastGroups = store.New(multicastGroupKeyFn)
	b.networkAnalyzerConfigs = store.New(networkAnalyzerConfigKeyFn)
}

// resetTablesLocked clears every table -- both the "clean" ones registered on
// b.registry and the "dirty" ones held outside it -- back to empty. Must be
// called with b.mu held for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()

	b.devices.Reset()
	b.gateways.Reset()
	b.serviceProfiles.Reset()
	b.destinations.Reset()
	b.deviceProfiles.Reset()
	b.fuotaTasks.Reset()
	b.multicastGroups.Reset()
	b.networkAnalyzerConfigs.Reset()
}
