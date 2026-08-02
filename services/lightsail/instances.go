package lightsail

// This file backs family B (9 ops: CreateInstances, CreateInstancesFromSnapshot,
// DeleteInstance, GetInstance, GetInstances, GetInstanceState, RebootInstance,
// StartInstance, StopInstance) -- the core VPS lifecycle, and the resource
// every other family (snapshots, disks, LB attachment, GUI sessions)
// references by name (PARITY.md's suggested implementation ordering).

import (
	"sort"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	OperationTypeCreateInstance = "CreateInstance"
	opTypeDeleteInstance        = "DeleteInstance"
	opTypeStartInstance         = "StartInstance"
	opTypeStopInstance          = "StopInstance"
	opTypeRebootInstance        = "RebootInstance"
)

// findBlueprint looks up a Blueprint by ID in the seed catalog.
func findBlueprint(id string) (*Blueprint, bool) {
	for _, bp := range seedBlueprints {
		if bp.BlueprintID == id {
			return &bp, true
		}
	}

	return nil, false
}

// findBundle looks up a Bundle by ID in the seed catalog.
func findBundle(id string) (*Bundle, bool) {
	for _, bd := range seedBundles {
		if bd.BundleID == id {
			return &bd, true
		}
	}

	return nil, false
}

// CreateInstancesRequest bundles CreateInstances' input fields.
type CreateInstancesRequest struct {
	Tags             map[string]string
	AvailabilityZone string
	BlueprintID      string
	BundleID         string
	KeyPairName      string
	IPAddressType    string
	Names            []string
	AddOns           []AddOnRequest
}

// CreateInstances creates one Instance per req.Names, each starting
// pending and transitioning to running after asyncTransitionDelay --
// returns one Operation per instance (PARITY.md family B: CreateInstances
// is a batch op sharing the singular OperationType "CreateInstance").
func (b *InMemoryBackend) CreateInstances(req CreateInstancesRequest) ([]Operation, error) {
	if len(req.Names) == 0 {
		return nil, validationError("InstanceNames is required")
	}

	bp, ok := findBlueprint(req.BlueprintID)
	if !ok {
		return nil, validationError("unknown BlueprintId: " + req.BlueprintID)
	}

	bd, ok := findBundle(req.BundleID)
	if !ok {
		return nil, validationError("unknown BundleId: " + req.BundleID)
	}

	b.mu.Lock("CreateInstances")
	defer b.mu.Unlock()

	az := req.AvailabilityZone
	if az == "" {
		az = availabilityZoneA(b.region)
	}

	for _, name := range req.Names {
		if err := b.registerNameLocked(ResourceTypeInstance, name); err != nil {
			return nil, err
		}
	}

	ipType := req.IPAddressType
	if ipType == "" {
		ipType = ipAddressTypeDualStack
	}

	for _, name := range req.Names {
		inst := &Instance{
			Name:             name,
			Arn:              b.regionalARN(ResourceTypeInstance, newUUID()),
			SupportCode:      newSupportCode(),
			BlueprintID:      bp.BlueprintID,
			BlueprintName:    bp.Name,
			BundleID:         bd.BundleID,
			CPUCount:         bd.CPUCount,
			RAMSizeInGb:      bd.RAMSizeInGb,
			DiskSizeInGb:     bd.DiskSizeInGb,
			MonthlyTransfer:  bd.TransferPerMonthInGb,
			PrivateIPAddress: privateIPForName(name),
			PublicIPAddress:  publicIPForName(name),
			IPAddressType:    ipType,
			SSHKeyName:       req.KeyPairName,
			Username:         defaultUsernameForBlueprint(bp),
			StateCode:        InstanceStateCodePending,
			StateName:        InstanceStateNamePending,
			CreatedAt:        nowUTC(),
			Location:         ResourceLocation{RegionName: b.region, AvailabilityZone: az},
			Tags:             tags.New("lightsail.instance." + name + ".tags"),
		}
		inst.Tags.Merge(req.Tags)

		for _, a := range req.AddOns {
			inst.AddOns = applyAddOnRequestLocked(inst.AddOns, a)
		}

		b.instances.Put(inst)
		b.scheduleInstanceRunningLocked(name)
	}

	return b.newOperationsLocked(OperationTypeCreateInstance, ResourceTypeInstance, req.Names), nil
}

// CreateInstancesFromSnapshotRequest bundles CreateInstancesFromSnapshot's
// input fields.
type CreateInstancesFromSnapshotRequest struct {
	Tags                 map[string]string
	AvailabilityZone     string
	BundleID             string
	InstanceSnapshotName string
	KeyPairName          string
	IPAddressType        string
	InstanceNames        []string
	AddOns               []AddOnRequest
}

// CreateInstancesFromSnapshot restores req.InstanceNames from an existing
// InstanceSnapshot, carrying its BlueprintId forward.
func (b *InMemoryBackend) CreateInstancesFromSnapshot(req CreateInstancesFromSnapshotRequest) ([]Operation, error) {
	if len(req.InstanceNames) == 0 {
		return nil, validationError("InstanceNames is required")
	}

	bd, ok := findBundle(req.BundleID)
	if !ok {
		return nil, validationError("unknown BundleId: " + req.BundleID)
	}

	b.mu.Lock("CreateInstancesFromSnapshot")
	defer b.mu.Unlock()

	snap, found := b.instanceSnapshots.Get(req.InstanceSnapshotName)
	if !found {
		return nil, notFoundError("InstanceSnapshot", req.InstanceSnapshotName)
	}

	for _, name := range req.InstanceNames {
		if err := b.registerNameLocked(ResourceTypeInstance, name); err != nil {
			return nil, err
		}
	}

	az := req.AvailabilityZone
	if az == "" {
		az = availabilityZoneA(b.region)
	}

	for _, name := range req.InstanceNames {
		inst := &Instance{
			Name:             name,
			Arn:              b.regionalARN(ResourceTypeInstance, newUUID()),
			SupportCode:      newSupportCode(),
			BlueprintID:      snap.FromBlueprintID,
			BundleID:         bd.BundleID,
			CPUCount:         bd.CPUCount,
			RAMSizeInGb:      bd.RAMSizeInGb,
			DiskSizeInGb:     bd.DiskSizeInGb,
			MonthlyTransfer:  bd.TransferPerMonthInGb,
			PrivateIPAddress: privateIPForName(name),
			PublicIPAddress:  publicIPForName(name),
			IPAddressType:    req.IPAddressType,
			SSHKeyName:       req.KeyPairName,
			StateCode:        InstanceStateCodePending,
			StateName:        InstanceStateNamePending,
			CreatedAt:        nowUTC(),
			Location:         ResourceLocation{RegionName: b.region, AvailabilityZone: az},
			Tags:             tags.New("lightsail.instance." + name + ".tags"),
		}
		inst.Tags.Merge(req.Tags)

		for _, a := range req.AddOns {
			inst.AddOns = applyAddOnRequestLocked(inst.AddOns, a)
		}

		b.instances.Put(inst)
		b.scheduleInstanceRunningLocked(name)
	}

	return b.newOperationsLocked(OperationTypeCreateInstance, ResourceTypeInstance, req.InstanceNames), nil
}

// scheduleInstanceRunningLocked schedules name's pending -> running
// transition. Callers must hold b.mu.
func (b *InMemoryBackend) scheduleInstanceRunningLocked(name string) {
	b.work.After("InstanceRunning", asyncTransitionDelay, func() {
		b.mu.Lock("Instance-async-running")
		defer b.mu.Unlock()

		if i, ok := b.instances.Get(name); ok && i.StateCode == InstanceStateCodePending {
			i.StateCode = InstanceStateCodeRunning
			i.StateName = InstanceStateNameRunning
		}
	})
}

// DeleteInstance deletes the named instance.
func (b *InMemoryBackend) DeleteInstance(name string) ([]Operation, error) {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	if i.Tags != nil {
		i.Tags.Close()
	}

	b.instances.Delete(name)
	b.unregisterNameLocked(name)

	return b.newOperationsLocked(opTypeDeleteInstance, ResourceTypeInstance, []string{name}), nil
}

// GetInstance returns the named instance.
func (b *InMemoryBackend) GetInstance(name string) (*Instance, error) {
	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	return i.clone(), nil
}

// GetInstances returns every instance, paginated.
func (b *InMemoryBackend) GetInstances(token string) (page.Page[*Instance], error) {
	b.mu.RLock("GetInstances")
	defer b.mu.RUnlock()

	all := b.instances.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*Instance, len(all))
	for i, v := range all {
		out[i] = v.clone()
	}

	return paginateGeneric(out, token)
}

// GetInstanceState returns the named instance's free-form
// InstanceState{Code, Name} -- see models.go's Instance doc comment.
func (b *InMemoryBackend) GetInstanceState(name string) (int32, string, error) {
	b.mu.RLock("GetInstanceState")
	defer b.mu.RUnlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return 0, "", notFoundError("Instance", name)
	}

	return i.StateCode, i.StateName, nil
}

// RebootInstance reboots the named instance (a no-op state-wise -- it stays
// running -- but records a real Operation).
func (b *InMemoryBackend) RebootInstance(name string) ([]Operation, error) {
	b.mu.Lock("RebootInstance")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(name); !ok {
		return nil, notFoundError("Instance", name)
	}

	return b.newOperationsLocked(opTypeRebootInstance, ResourceTypeInstance, []string{name}), nil
}

// StartInstance transitions the named instance from stopped to running.
func (b *InMemoryBackend) StartInstance(name string) ([]Operation, error) {
	b.mu.Lock("StartInstance")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	i.StateCode = InstanceStateCodeRunning
	i.StateName = InstanceStateNameRunning

	return b.newOperationsLocked(opTypeStartInstance, ResourceTypeInstance, []string{name}), nil
}

// StopInstance transitions the named instance from running to stopped.
func (b *InMemoryBackend) StopInstance(name string) ([]Operation, error) {
	b.mu.Lock("StopInstance")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	i.StateCode = InstanceStateCodeStopped
	i.StateName = InstanceStateNameStopped

	return b.newOperationsLocked(opTypeStopInstance, ResourceTypeInstance, []string{name}), nil
}

// octetSalt1/octetSalt2/octetSalt3 give privateIPForName/publicIPForName's
// three derived address octets distinct hash inputs so they don't collapse
// to the same value for a given name.
const (
	octetSalt1 = 1
	octetSalt2 = 2
	octetSalt3 = 3

	// hashMultiplier is an arbitrary small-prime multiplier for the
	// per-character rolling hash below -- not itself a meaningful quantity,
	// just a common odd-prime choice for spreading hash output.
	hashMultiplier = 31

	// octetModulo bounds a derived octet to 0-253 (avoiding the .0/.254+
	// edge values a real host address wouldn't use) -- purely a cosmetic
	// choice for this emulator's synthetic addresses, not a real IPv4 rule.
	octetModulo = 254
)

// privateIPForName/publicIPForName derive stable, deterministic-looking
// synthetic IPs from a resource name (using an RFC 5737/1918-style
// documentation range) -- never claimed as real routable addresses, purely
// so repeated Get calls return a stable value for the same instance.
func privateIPForName(name string) string {
	return "172.26." + hashOctet(name, octetSalt1) + "." + hashOctet(name, octetSalt2)
}

func publicIPForName(name string) string { return "203.0.113." + hashOctet(name, octetSalt3) }

func hashOctet(name string, salt int) string {
	h := 0
	for _, c := range name {
		h = h*hashMultiplier + int(c) + salt
	}

	if h < 0 {
		h = -h
	}

	return strconv.Itoa(h % octetModulo)
}

// defaultUsernameForBlueprint returns the conventional default SSH/console
// username for bp's platform -- "ec2-user" for the amazon_linux family,
// "ubuntu" for ubuntu, "admin" otherwise for Linux, "Administrator" for
// Windows. These follow each distribution's own well-known, real default
// login convention (not Lightsail-specific, and not fabricated) rather than
// a single hardcoded value across every blueprint.
func defaultUsernameForBlueprint(bp *Blueprint) string {
	switch {
	case bp.Platform == InstancePlatformWindows:
		return "Administrator"
	case bp.Group == blueprintGroupUbuntu:
		return blueprintGroupUbuntu
	case bp.Group == "amazon_linux":
		return "ec2-user"
	default:
		return "admin"
	}
}
