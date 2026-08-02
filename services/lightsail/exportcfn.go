package lightsail

// This file backs family K (4 ops: ExportSnapshot, GetExportSnapshotRecords,
// CreateCloudFormationStack, GetCloudFormationStackRecords) -- a real
// Lightsail-to-EC2/CloudFormation migration path. CreateCloudFormationStack
// is the one confirmed genuine cross-service handoff in this whole 161-op
// surface (PARITY.md 5.2 point 4): when a real services/cloudformation
// backend is wired (SetCloudFormationBackend, store.go), this hands off to
// it for real instead of fabricating a plausible-looking stack record.

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	opTypeExportSnapshot            = "ExportSnapshot"
	opTypeCreateCloudFormationStack = "CreateCloudFormationStack"
)

// InstanceEntry mirrors types.InstanceEntry -- CreateCloudFormationStack's
// per-instance input.
type InstanceEntry struct {
	SourceName       string
	AvailabilityZone string
	InstanceType     string
	PortInfoSource   string
	UserData         string
}

// ExportSnapshot creates an ExportSnapshotRecord for sourceSnapshotName
// (either an instance or disk snapshot -- polymorphic, same lookup as
// CopySnapshot).
func (b *InMemoryBackend) ExportSnapshot(sourceSnapshotName string) ([]Operation, error) {
	b.mu.Lock("ExportSnapshot")
	defer b.mu.Unlock()

	var (
		sourceArn, sourceKind string
		location              ResourceLocation
	)

	if instSnap, ok := b.instanceSnapshots.Get(sourceSnapshotName); ok {
		sourceArn, sourceKind, location = instSnap.Arn, ResourceTypeInstanceSnapshot, instSnap.Location
	} else if diskSnap, diskOK := b.diskSnapshots.Get(sourceSnapshotName); diskOK {
		sourceArn, sourceKind, location = diskSnap.Arn, ResourceTypeDiskSnapshot, diskSnap.Location
	} else {
		return nil, notFoundError("snapshot", sourceSnapshotName)
	}

	recordName := "ExportSnapshotRecord" + newUUID()
	rec := &ExportSnapshotRecord{
		Name: recordName, Arn: b.regionalARN(ResourceTypeExportSnapshotRecord, newUUID()),
		State: RecordStateStarted, SourceName: sourceSnapshotName, SourceArn: sourceArn,
		SourceResourceType: sourceKind, CreatedAt: nowUTC(), Location: location,
	}
	b.exportSnapshotRecords.Put(rec)

	b.work.After("ExportSnapshotComplete", asyncTransitionDelay, func() {
		b.mu.Lock("ExportSnapshot-async-complete")
		defer b.mu.Unlock()

		if r, found := b.exportSnapshotRecords.Get(recordName); found && r.State == RecordStateStarted {
			r.State = RecordStateSucceeded
		}
	})

	return b.newOperationsLocked(opTypeExportSnapshot, ResourceTypeExportSnapshotRecord, []string{recordName}), nil
}

// GetExportSnapshotRecords returns every export snapshot record, paginated.
func (b *InMemoryBackend) GetExportSnapshotRecords(token string) (page.Page[*ExportSnapshotRecord], error) {
	b.mu.RLock("GetExportSnapshotRecords")
	defer b.mu.RUnlock()

	all := b.exportSnapshotRecords.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*ExportSnapshotRecord, len(all))
	for i, v := range all {
		out[i] = v.clone()
	}

	return paginateGeneric(out, token)
}

// CreateCloudFormationStack converts exported instance snapshots into a
// CloudFormationStackRecord, handing off to a real services/cloudformation
// stack when SetCloudFormationBackend wired one -- see this file's doc
// comment. Without a wired backend, this still records a real
// CloudFormationStackRecord/Operation, just without a backing stack ARN
// (an honest, documented scoped-down behavior).
func (b *InMemoryBackend) CreateCloudFormationStack(instances []InstanceEntry) ([]Operation, error) {
	if len(instances) == 0 {
		return nil, validationError("Instances is required")
	}

	b.mu.Lock("CreateCloudFormationStack")
	defer b.mu.Unlock()

	sourceNames := make([]string, 0, len(instances))
	var location ResourceLocation

	for _, e := range instances {
		if _, ok := b.exportSnapshotRecords.Get(e.SourceName); !ok {
			return nil, notFoundError("ExportSnapshotRecord", e.SourceName)
		}

		sourceNames = append(sourceNames, e.SourceName)
	}

	if len(sourceNames) > 0 {
		if rec, ok := b.exportSnapshotRecords.Get(sourceNames[0]); ok {
			location = rec.Location
		}
	}

	recordName := "CloudFormationStackRecord" + newUUID()
	rec := &CloudFormationStackRecord{
		Name: recordName, Arn: b.regionalARN(ResourceTypeCloudFormationStackRecord, newUUID()),
		State: RecordStateStarted, CreatedAt: nowUTC(), Location: location,
		SourceInstances: sourceNames,
	}

	if b.cfnBackend != nil {
		if _, err := b.cfnBackend.CreateStackFromLightsail(recordName, sourceNames); err != nil {
			rec.State = RecordStateFailed
		} else {
			rec.State = RecordStateSucceeded
		}

		rec.DestinationInfoID = recordName
	}

	b.cfnStackRecords.Put(rec)

	if rec.State == RecordStateStarted {
		b.work.After("CloudFormationStackComplete", asyncTransitionDelay, func() {
			b.mu.Lock("CloudFormationStack-async-complete")
			defer b.mu.Unlock()

			if r, found := b.cfnStackRecords.Get(recordName); found && r.State == RecordStateStarted {
				r.State = RecordStateSucceeded
			}
		})
	}

	return b.newOperationsLocked(
		opTypeCreateCloudFormationStack,
		ResourceTypeCloudFormationStackRecord,
		[]string{recordName},
	), nil
}

// GetCloudFormationStackRecords returns every CloudFormation stack record,
// paginated.
func (b *InMemoryBackend) GetCloudFormationStackRecords(token string) (page.Page[*CloudFormationStackRecord], error) {
	b.mu.RLock("GetCloudFormationStackRecords")
	defer b.mu.RUnlock()

	all := b.cfnStackRecords.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*CloudFormationStackRecord, len(all))
	for i, v := range all {
		out[i] = v.clone()
	}

	return paginateGeneric(out, token)
}
