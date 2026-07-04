package ec2

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"time"
)

// backend_instance_attrs.go implements the instance-attribute misc operation
// cluster: ModifyAvailabilityZoneGroup, ModifyHosts, ModifyInstanceCapacityReservationAttributes,
// ModifyInstanceCpuOptions, ModifyInstanceEventStartTime, ModifyInstanceMaintenanceOptions,
// ModifyInstanceNetworkPerformanceOptions, ModifyInstancePlacement, ModifyPrivateDnsNameOptions,
// ModifyPublicIpDnsNameOptions, AssociateInstanceEventWindow, DisassociateInstanceEventWindow,
// GetInstanceTpmEkPub, and GetInstanceUefiData. Each Modify* mutates real fields on the
// existing Instance/Host/InstanceEventWindow state; Get* returns deterministic,
// instance-keyed generated blobs of the correct (base64) shape.

// CPUOptions holds the per-instance CPU configuration set via ModifyInstanceCpuOptions.
type CPUOptions struct {
	NestedVirtualization string `json:"nestedVirtualization,omitempty"`
	CoreCount            int32  `json:"coreCount,omitempty"`
	ThreadsPerCore       int32  `json:"threadsPerCore,omitempty"`
}

// InstancePlacement holds the per-instance placement configuration set via
// ModifyInstancePlacement.
type InstancePlacement struct {
	Affinity             string `json:"affinity,omitempty"`
	GroupName            string `json:"groupName,omitempty"`
	GroupID              string `json:"groupId,omitempty"`
	HostID               string `json:"hostId,omitempty"`
	Tenancy              string `json:"tenancy,omitempty"`
	HostResourceGroupArn string `json:"hostResourceGroupArn,omitempty"`
	PartitionNumber      int32  `json:"partitionNumber,omitempty"`
}

// PrivateDNSNameOptions holds the per-instance private DNS hostname
// configuration set via ModifyPrivateDnsNameOptions.
type PrivateDNSNameOptions struct {
	HostnameType                    string `json:"hostnameType,omitempty"`
	EnableResourceNameDNSARecord    bool   `json:"enableResourceNameDnsARecord,omitempty"`
	EnableResourceNameDNSAAAARecord bool   `json:"enableResourceNameDnsAAAARecord,omitempty"`
}

// CapacityReservationSpec holds the per-instance Capacity Reservation
// targeting preference set via ModifyInstanceCapacityReservationAttributes.
type CapacityReservationSpec struct {
	CapacityReservationPreference       string `json:"capacityReservationPreference,omitempty"`
	CapacityReservationID               string `json:"capacityReservationId,omitempty"`
	CapacityReservationResourceGroupArn string `json:"capacityReservationResourceGroupArn,omitempty"`
}

// InstanceMaintenanceOptions holds the per-instance auto-recovery/reboot
// migration configuration set via ModifyInstanceMaintenanceOptions.
type InstanceMaintenanceOptions struct {
	AutoRecovery    string `json:"autoRecovery,omitempty"`
	RebootMigration string `json:"rebootMigration,omitempty"`
}

// InstanceNetworkPerformanceOptions holds the per-instance bandwidth
// weighting configuration set via ModifyInstanceNetworkPerformanceOptions.
type InstanceNetworkPerformanceOptions struct {
	BandwidthWeighting string `json:"bandwidthWeighting,omitempty"`
}

// InstanceStatusEventRec represents a scheduled instance status event, as
// returned by ModifyInstanceEventStartTime.
type InstanceStatusEventRec struct {
	NotAfter          time.Time `json:"notAfter"`
	NotBefore         time.Time `json:"notBefore"`
	NotBeforeDeadline time.Time `json:"notBeforeDeadline"`
	Code              string    `json:"code,omitempty"`
	Description       string    `json:"description,omitempty"`
	InstanceEventID   string    `json:"instanceEventId,omitempty"`
}

// HostModifyFailure describes a Dedicated Host that ModifyHosts could not update.
type HostModifyFailure struct {
	HostID  string `json:"hostId,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

const (
	azGroupOptedIn    = "opted-in"
	azGroupNotOptedIn = "not-opted-in"

	instanceEventDefaultCode = "system-reboot"
)

// resetInstanceAttrMapsLocked re-initialises the Availability Zone group
// opt-in state map. Must be called with b.mu held.
func (b *InMemoryBackend) resetInstanceAttrMapsLocked() {
	b.availabilityZoneGroupOptIns = make(map[string]string)
}

// ---- ModifyAvailabilityZoneGroup ----

// ModifyAvailabilityZoneGroup opts an account in (or out) of a Local
// Zone/Wavelength Zone group.
func (b *InMemoryBackend) ModifyAvailabilityZoneGroup(groupName, optInStatus string) (bool, error) {
	if groupName == "" {
		return false, fmt.Errorf("%w: GroupName is required", ErrInvalidParameter)
	}

	if optInStatus != azGroupOptedIn && optInStatus != azGroupNotOptedIn {
		return false, fmt.Errorf("%w: OptInStatus must be opted-in or not-opted-in", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyAvailabilityZoneGroup")
	defer b.mu.Unlock()

	b.availabilityZoneGroupOptIns[groupName] = optInStatus

	return true, nil
}

// ---- ModifyHosts ----

// ModifyHosts updates auto-placement/maintenance/recovery/instance-type-or-family
// settings on one or more Dedicated Hosts.
func (b *InMemoryBackend) ModifyHosts(
	hostIDs []string, autoPlacement, hostMaintenance, hostRecovery, instanceFamily, instanceType string,
) ([]string, []HostModifyFailure, error) {
	if len(hostIDs) == 0 {
		return nil, nil, fmt.Errorf("%w: HostId is required", ErrInvalidParameter)
	}

	if instanceFamily != "" && instanceType != "" {
		return nil, nil, fmt.Errorf(
			"%w: InstanceFamily and InstanceType cannot both be specified", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyHosts")
	defer b.mu.Unlock()

	successful := make([]string, 0, len(hostIDs))
	unsuccessful := make([]HostModifyFailure, 0)

	for _, id := range hostIDs {
		host, ok := b.dedicatedHosts[id]
		if !ok {
			unsuccessful = append(unsuccessful, HostModifyFailure{
				HostID: id, Code: "InvalidHostID.NotFound", Message: "Host " + id + " does not exist",
			})

			continue
		}

		applyHostModification(host, autoPlacement, hostMaintenance, hostRecovery, instanceFamily, instanceType)
		successful = append(successful, id)
	}

	sort.Strings(successful)

	return successful, unsuccessful, nil
}

// applyHostModification mutates host in place with the non-empty fields
// supplied to ModifyHosts. Must be called with b.mu held.
func applyHostModification(
	host *Host, autoPlacement, hostMaintenance, hostRecovery, instanceFamily, instanceType string,
) {
	if autoPlacement != "" {
		host.AutoPlacement = autoPlacement
	}

	if hostMaintenance != "" {
		host.HostMaintenance = hostMaintenance
	}

	if hostRecovery != "" {
		host.HostRecovery = hostRecovery
	}

	if instanceType != "" {
		host.InstanceType = instanceType
		host.InstanceFamily = ""
	} else if instanceFamily != "" {
		host.InstanceFamily = instanceFamily
	}
}

// ---- ModifyInstanceCapacityReservationAttributes ----

// ModifyInstanceCapacityReservationAttributes updates an instance's Capacity
// Reservation targeting preference.
func (b *InMemoryBackend) ModifyInstanceCapacityReservationAttributes(
	instanceID, preference, targetID, targetArn string,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceCapacityReservationAttributes")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if targetID != "" {
		if _, found := b.capacityReservations[targetID]; !found {
			return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, targetID)
		}
	}

	inst.CapacityReservationSpec = CapacityReservationSpec{
		CapacityReservationPreference:       preference,
		CapacityReservationID:               targetID,
		CapacityReservationResourceGroupArn: targetArn,
	}

	cp := *inst

	return &cp, nil
}

// ---- ModifyInstanceCpuOptions ----

// ModifyInstanceCPUOptions updates an instance's CPU core count, threads per
// core, and nested virtualization setting. The instance must be stopped.
func (b *InMemoryBackend) ModifyInstanceCPUOptions(
	instanceID string, coreCount, threadsPerCore *int32, nestedVirtualization string,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceCpuOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if inst.State != StateStopped {
		return nil, fmt.Errorf(
			"%w: instance %s must be in the stopped state to modify CPU options", ErrInvalidInstanceState, instanceID,
		)
	}

	if coreCount != nil {
		inst.CPUOptions.CoreCount = *coreCount
	}

	if threadsPerCore != nil {
		inst.CPUOptions.ThreadsPerCore = *threadsPerCore
	}

	if nestedVirtualization != "" {
		inst.CPUOptions.NestedVirtualization = nestedVirtualization
	}

	cp := *inst

	return &cp, nil
}

// ---- ModifyInstanceEventStartTime ----

// ModifyInstanceEventStartTime overrides the start time of a scheduled
// instance event.
func (b *InMemoryBackend) ModifyInstanceEventStartTime(
	instanceID, instanceEventID string, notBefore time.Time,
) (*InstanceStatusEventRec, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if instanceEventID == "" {
		return nil, fmt.Errorf("%w: InstanceEventId is required", ErrInvalidParameter)
	}

	if notBefore.IsZero() {
		return nil, fmt.Errorf("%w: NotBefore is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceEventStartTime")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if inst.EventStartTimeOverrides == nil {
		inst.EventStartTimeOverrides = make(map[string]time.Time)
	}

	inst.EventStartTimeOverrides[instanceEventID] = notBefore

	return &InstanceStatusEventRec{
		Code:            instanceEventDefaultCode,
		Description:     "The instance is scheduled for a reboot",
		InstanceEventID: instanceEventID,
		NotBefore:       notBefore,
	}, nil
}

// ---- ModifyInstanceMaintenanceOptions ----

// ModifyInstanceMaintenanceOptions updates an instance's automatic recovery
// and reboot migration behavior.
func (b *InMemoryBackend) ModifyInstanceMaintenanceOptions(
	instanceID, autoRecovery, rebootMigration string,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceMaintenanceOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if autoRecovery != "" {
		inst.MaintenanceOptions.AutoRecovery = autoRecovery
	}

	if rebootMigration != "" {
		inst.MaintenanceOptions.RebootMigration = rebootMigration
	}

	cp := *inst

	return &cp, nil
}

// ---- ModifyInstanceNetworkPerformanceOptions ----

// ModifyInstanceNetworkPerformanceOptions updates an instance's bandwidth
// weighting configuration.
func (b *InMemoryBackend) ModifyInstanceNetworkPerformanceOptions(
	instanceID, bandwidthWeighting string,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if bandwidthWeighting == "" {
		return nil, fmt.Errorf("%w: BandwidthWeighting is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceNetworkPerformanceOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	inst.NetworkPerformanceOptions.BandwidthWeighting = bandwidthWeighting

	cp := *inst

	return &cp, nil
}

// ---- ModifyInstancePlacement ----

// ModifyInstancePlacementInput bundles the (mostly optional) fields accepted
// by ModifyInstancePlacement, avoiding a long positional parameter list.
type ModifyInstancePlacementInput struct {
	PartitionNumber      *int32
	InstanceID           string
	Affinity             string
	GroupID              string
	GroupName            string
	HostID               string
	HostResourceGroupArn string
	Tenancy              string
}

// ModifyInstancePlacement updates an instance's placement group, tenancy,
// affinity, or Dedicated Host association. The instance must be stopped.
func (b *InMemoryBackend) ModifyInstancePlacement(in ModifyInstancePlacementInput) (bool, error) {
	if in.InstanceID == "" {
		return false, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstancePlacement")
	defer b.mu.Unlock()

	inst, ok := b.instances[in.InstanceID]
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrInstanceNotFound, in.InstanceID)
	}

	if inst.State != StateStopped {
		return false, fmt.Errorf(
			"%w: instance %s must be in the stopped state to modify placement", ErrInvalidInstanceState, in.InstanceID,
		)
	}

	if in.HostID != "" {
		if _, found := b.dedicatedHosts[in.HostID]; !found {
			return false, fmt.Errorf("%w: %s", ErrHostNotFound, in.HostID)
		}
	}

	applyInstancePlacement(inst, in)

	return true, nil
}

// applyInstancePlacement mutates inst.Placement in place with the non-empty
// fields supplied to ModifyInstancePlacement. Must be called with b.mu held.
func applyInstancePlacement(inst *Instance, in ModifyInstancePlacementInput) {
	if in.Affinity != "" {
		inst.Placement.Affinity = in.Affinity
	}

	if in.GroupID != "" {
		inst.Placement.GroupID = in.GroupID
	}

	if in.GroupName != "" {
		inst.Placement.GroupName = in.GroupName
	}

	if in.HostID != "" {
		inst.Placement.HostID = in.HostID
	}

	if in.HostResourceGroupArn != "" {
		inst.Placement.HostResourceGroupArn = in.HostResourceGroupArn
	}

	if in.Tenancy != "" {
		inst.Placement.Tenancy = in.Tenancy
	}

	if in.PartitionNumber != nil {
		inst.Placement.PartitionNumber = *in.PartitionNumber
	}
}

// ---- ModifyPrivateDnsNameOptions ----

// ModifyPrivateDNSNameOptions updates an instance's private DNS hostname type
// and DNS record settings.
func (b *InMemoryBackend) ModifyPrivateDNSNameOptions(
	instanceID, hostnameType string, enableARecord, enableAAAARecord *bool,
) (bool, error) {
	if instanceID == "" {
		return false, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyPrivateDnsNameOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if hostnameType != "" {
		inst.PrivateDNSNameOptions.HostnameType = hostnameType
	}

	if enableARecord != nil {
		inst.PrivateDNSNameOptions.EnableResourceNameDNSARecord = *enableARecord
	}

	if enableAAAARecord != nil {
		inst.PrivateDNSNameOptions.EnableResourceNameDNSAAAARecord = *enableAAAARecord
	}

	return true, nil
}

// ---- ModifyPublicIpDnsNameOptions ----

// ModifyPublicIPDNSNameOptions updates a network interface's public DNS
// hostname type.
func (b *InMemoryBackend) ModifyPublicIPDNSNameOptions(networkInterfaceID, hostnameType string) (bool, error) {
	if networkInterfaceID == "" {
		return false, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	if hostnameType == "" {
		return false, fmt.Errorf("%w: HostnameType is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyPublicIpDnsNameOptions")
	defer b.mu.Unlock()

	ni, ok := b.networkInterfaces[networkInterfaceID]
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, networkInterfaceID)
	}

	ni.PublicDNSHostnameType = hostnameType

	return true, nil
}

// ---- AssociateInstanceEventWindow / DisassociateInstanceEventWindow ----

// AssociateInstanceEventWindow adds instances and/or Dedicated Hosts as
// targets of an instance event window.
func (b *InMemoryBackend) AssociateInstanceEventWindow(
	windowID string, instanceIDs, dedicatedHostIDs []string,
) (*InstanceEventWindow, error) {
	if windowID == "" {
		return nil, fmt.Errorf("%w: InstanceEventWindowId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateInstanceEventWindow")
	defer b.mu.Unlock()

	ew, ok := b.instanceEventWindows[windowID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceEventWindowNotFound, windowID)
	}

	ew.InstanceIDs = mergeUniqueStrings(ew.InstanceIDs, instanceIDs)
	ew.DedicatedHostIDs = mergeUniqueStrings(ew.DedicatedHostIDs, dedicatedHostIDs)

	cp := *ew

	return &cp, nil
}

// DisassociateInstanceEventWindow removes instances and/or Dedicated Hosts as
// targets of an instance event window.
func (b *InMemoryBackend) DisassociateInstanceEventWindow(
	windowID string, instanceIDs, dedicatedHostIDs []string,
) (*InstanceEventWindow, error) {
	if windowID == "" {
		return nil, fmt.Errorf("%w: InstanceEventWindowId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateInstanceEventWindow")
	defer b.mu.Unlock()

	ew, ok := b.instanceEventWindows[windowID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceEventWindowNotFound, windowID)
	}

	ew.InstanceIDs = removeStrings(ew.InstanceIDs, instanceIDs)
	ew.DedicatedHostIDs = removeStrings(ew.DedicatedHostIDs, dedicatedHostIDs)

	cp := *ew

	return &cp, nil
}

// mergeUniqueStrings returns base with any values from extra appended that are
// not already present, preserving base's original order.
func mergeUniqueStrings(base, extra []string) []string {
	present := make(map[string]bool, len(base))
	for _, v := range base {
		present[v] = true
	}

	out := append([]string{}, base...)

	for _, v := range extra {
		if !present[v] {
			present[v] = true
			out = append(out, v)
		}
	}

	return out
}

// removeStrings returns base with any values in remove filtered out.
func removeStrings(base, remove []string) []string {
	if len(base) == 0 || len(remove) == 0 {
		return base
	}

	drop := make(map[string]bool, len(remove))
	for _, v := range remove {
		drop[v] = true
	}

	out := make([]string, 0, len(base))

	for _, v := range base {
		if !drop[v] {
			out = append(out, v)
		}
	}

	return out
}

// ---- GetInstanceTpmEkPub / GetInstanceUefiData ----

// GetInstanceTpmEkPub returns a deterministic, base64-encoded TPM endorsement
// key blob for the given instance/format/type. The value is stable across
// calls for the same (instanceID, keyFormat, keyType) tuple but is not a real
// cryptographic key.
func (b *InMemoryBackend) GetInstanceTpmEkPub(instanceID, keyFormat, keyType string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	if keyFormat == "" {
		return "", fmt.Errorf("%w: KeyFormat is required", ErrInvalidParameter)
	}

	if keyType == "" {
		return "", fmt.Errorf("%w: KeyType is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetInstanceTpmEkPub")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceID]; !ok {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	sum := sha256.Sum256([]byte("tpm-ek-pub:" + instanceID + ":" + keyFormat + ":" + keyType))

	return base64.StdEncoding.EncodeToString(sum[:]), nil
}

// GetInstanceUefiData returns a deterministic, base64-encoded UEFI
// non-volatile variable store blob for the given instance. The value is
// stable across calls for the same instance but is not real UEFI data.
func (b *InMemoryBackend) GetInstanceUefiData(instanceID string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetInstanceUefiData")
	defer b.mu.RUnlock()

	if _, ok := b.instances[instanceID]; !ok {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	sum := sha256.Sum256([]byte("uefi-data:" + instanceID))

	return base64.StdEncoding.EncodeToString(sum[:]), nil
}
