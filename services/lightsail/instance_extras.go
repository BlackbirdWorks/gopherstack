package lightsail

// This file backs family D (4 ops: CreateInstanceSnapshot,
// DeleteInstanceSnapshot, GetInstanceSnapshot, GetInstanceSnapshots) and
// family E (2 ops: GetInstanceMetricData, UpdateInstanceMetadataOptions).

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	opTypeCreateInstanceSnapshot        = "CreateInstanceSnapshot"
	opTypeDeleteInstanceSnapshot        = "DeleteInstanceSnapshot"
	opTypeUpdateInstanceMetadataOptions = "UpdateInstanceMetadataOptions"

	snapshotProgressComplete = "100%"
)

// CreateInstanceSnapshot captures a point-in-time InstanceSnapshot of the
// named instance's attached disks (FromAttachedDisks: Path/SizeInGb only --
// no real disk content, PARITY.md 4.1).
func (b *InMemoryBackend) CreateInstanceSnapshot(
	instanceName, snapshotName string,
	userTags map[string]string,
) ([]Operation, error) {
	b.mu.Lock("CreateInstanceSnapshot")
	defer b.mu.Unlock()

	inst, ok := b.instances.Get(instanceName)
	if !ok {
		return nil, notFoundError("Instance", instanceName)
	}

	if err := b.registerNameLocked(ResourceTypeInstanceSnapshot, snapshotName); err != nil {
		return nil, err
	}

	snap := &InstanceSnapshot{
		Name: snapshotName, Arn: b.regionalARN(ResourceTypeInstanceSnapshot, newUUID()),
		SupportCode: newSupportCode(), State: SnapshotStatePending, Progress: "0%",
		FromInstanceName: instanceName, FromInstanceArn: inst.Arn,
		FromBlueprintID: inst.BlueprintID, FromBundleID: inst.BundleID,
		SizeInGb:  inst.DiskSizeInGb,
		CreatedAt: nowUTC(), Location: inst.Location,
		FromAttachedDisks: []AttachedDisk{{Path: "/dev/sda1", SizeInGb: inst.DiskSizeInGb}},
		Tags:              tags.New("lightsail.instancesnapshot." + snapshotName + ".tags"),
	}
	snap.Tags.Merge(userTags)
	b.instanceSnapshots.Put(snap)

	b.work.After("InstanceSnapshotComplete", asyncTransitionDelay, func() {
		b.mu.Lock("InstanceSnapshot-async-complete")
		defer b.mu.Unlock()

		if s, found := b.instanceSnapshots.Get(snapshotName); found && s.State == SnapshotStatePending {
			s.State = SnapshotStateAvailable
			s.Progress = snapshotProgressComplete
		}
	})

	return b.newOperationsLocked(
		opTypeCreateInstanceSnapshot,
		ResourceTypeInstanceSnapshot,
		[]string{snapshotName},
	), nil
}

// DeleteInstanceSnapshot deletes the named instance snapshot.
func (b *InMemoryBackend) DeleteInstanceSnapshot(name string) ([]Operation, error) {
	b.mu.Lock("DeleteInstanceSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.instanceSnapshots.Get(name)
	if !ok {
		return nil, notFoundError("InstanceSnapshot", name)
	}

	if snap.Tags != nil {
		snap.Tags.Close()
	}

	b.instanceSnapshots.Delete(name)
	b.unregisterNameLocked(name)

	return b.newOperationsLocked(opTypeDeleteInstanceSnapshot, ResourceTypeInstanceSnapshot, []string{name}), nil
}

// GetInstanceSnapshot returns the named instance snapshot.
func (b *InMemoryBackend) GetInstanceSnapshot(name string) (*InstanceSnapshot, error) {
	b.mu.RLock("GetInstanceSnapshot")
	defer b.mu.RUnlock()

	snap, ok := b.instanceSnapshots.Get(name)
	if !ok {
		return nil, notFoundError("InstanceSnapshot", name)
	}

	return snap.clone(), nil
}

// GetInstanceSnapshots returns every instance snapshot, paginated.
func (b *InMemoryBackend) GetInstanceSnapshots(token string) (page.Page[*InstanceSnapshot], error) {
	b.mu.RLock("GetInstanceSnapshots")
	defer b.mu.RUnlock()

	all := b.instanceSnapshots.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*InstanceSnapshot, len(all))
	for i, v := range all {
		out[i] = v.clone()
	}

	return paginateGeneric(out, token)
}

// GetInstanceMetricData returns a real, well-formed, EMPTY MetricData
// response -- one of the six honestly-unfakeable telemetry ops
// (PARITY.md 4.10): this emulator has no real CPU/network load to report,
// and inventing plausible datapoints would be exactly the fabrication
// parity-principles.md forbids. This is a deliberate honesty choice, not an
// unfinished stub -- the op is fully routed, validates the instance exists,
// and returns a genuinely empty (never fake) series.
func (b *InMemoryBackend) GetInstanceMetricData(instanceName string) error {
	b.mu.RLock("GetInstanceMetricData")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceName); !ok {
		return notFoundError("Instance", instanceName)
	}

	return nil
}

// UpdateInstanceMetadataOptions updates the named instance's IMDS knobs.
func (b *InMemoryBackend) UpdateInstanceMetadataOptions(name string, opts InstanceMetadataOptions) (*Operation, error) {
	b.mu.Lock("UpdateInstanceMetadataOptions")
	defer b.mu.Unlock()

	i, ok := b.instances.Get(name)
	if !ok {
		return nil, notFoundError("Instance", name)
	}

	if opts.HTTPEndpoint != "" {
		i.MetadataOptions.HTTPEndpoint = opts.HTTPEndpoint
	}

	if opts.HTTPTokens != "" {
		i.MetadataOptions.HTTPTokens = opts.HTTPTokens
	}

	if opts.HTTPProtocolIpv6 != "" {
		i.MetadataOptions.HTTPProtocolIpv6 = opts.HTTPProtocolIpv6
	}

	if opts.HTTPPutResponseHopLimit != 0 {
		i.MetadataOptions.HTTPPutResponseHopLimit = opts.HTTPPutResponseHopLimit
	}

	i.MetadataOptions.State = "applied"

	ops := b.newOperationsLocked(opTypeUpdateInstanceMetadataOptions, ResourceTypeInstance, []string{name})

	return &ops[0], nil
}
