package gcpcompute

import (
	"sort"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	physBlockBytes = 4096
	minDiskGb      = 10
	maxDiskGb      = 65536
	defaultDiskTyp = "pd-standard"
)

func diskTarget(project, zone string, d *computeapi.Disk) target {
	return target{project: project, scope: scopeZone, loc: zone, collection: "disks", name: d.Name, id: d.Id}
}

func (b *InMemoryBackend) diskUsersLocked(project, zone, name string) []string {
	var users []string

	self := zoneLink(project, zone, "disks", name)

	for _, r := range b.instances {
		if lastSegment(r.Inst.Zone) != zone || !strings.Contains(r.Inst.SelfLink, "/projects/"+project+"/") {
			continue
		}

		for _, ad := range r.Inst.Disks {
			if ad.Source == self {
				users = append(users, r.Inst.SelfLink)
			}
		}
	}

	sort.Strings(users)

	return users
}

func (b *InMemoryBackend) diskViewLocked(project string, d *computeapi.Disk) *computeapi.Disk {
	out := clone(d)
	out.Users = b.diskUsersLocked(project, lastSegment(d.Zone), d.Name)

	return out
}

func (b *InMemoryBackend) resolveDiskTypeLocked(project, zone, raw string) (diskTypeSpec, *APIError) {
	if raw == "" {
		raw = defaultDiskTyp
	}

	r, err := refLocated(raw, "diskTypes", scopeZone, project, zone)
	if err != nil {
		return diskTypeSpec{}, errField("diskType", raw, "")
	}

	spec, ok := findDiskType(r.name)
	if !ok || r.loc != zone {
		return diskTypeSpec{}, errNotFound(relPath(r.project, scopeZone, r.loc, "diskTypes", r.name))
	}

	return spec, nil
}

type diskSource struct {
	image    *computeapi.Image
	snapshot *computeapi.Snapshot
	sizeGb   int64
}

func (b *InMemoryBackend) resolveDiskSourceLocked(project string, in *computeapi.Disk) (diskSource, *APIError) {
	var src diskSource

	if in.SourceImage != "" && in.SourceSnapshot != "" {
		return src, errInvalid("Only one of sourceImage and sourceSnapshot may be specified.")
	}

	switch {
	case in.SourceImage != "":
		img, err := b.resolveImageLocked(project, in.SourceImage)
		if err != nil {
			return src, err
		}

		src.image, src.sizeGb = img, img.DiskSizeGb
	case in.SourceSnapshot != "":
		r, err := refGlobal(in.SourceSnapshot, "snapshots", project)
		if err != nil {
			return src, errField("sourceSnapshot", in.SourceSnapshot, "")
		}

		snap, ok := b.snapshots[key(r.project, r.name)]
		if !ok {
			return src, errNotFound(relPath(r.project, scopeGlobal, "", "snapshots", r.name))
		}

		src.snapshot, src.sizeGb = snap, snap.DiskSizeGb
	}

	return src, nil
}

func (b *InMemoryBackend) createDiskLocked(project, zone string, in *computeapi.Disk) (*computeapi.Disk, *APIError) {
	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if !zoneExists(zone) {
		return nil, errNotFound(scopePath(project, scopeZone, zone))
	}

	if _, dup := b.disks[key(project, zone, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeZone, zone, "disks", in.Name))
	}

	dt, err := b.resolveDiskTypeLocked(project, zone, in.Type)
	if err != nil {
		return nil, err
	}

	if apiErr := validLabels(in.Labels); apiErr != nil {
		return nil, apiErr
	}

	policies, apiErr := b.resolveResPoliciesLocked(
		project, regionOfZone(zone), in.ResourcePolicies, "resource.resourcePolicies", diskPolicyKinds)
	if apiErr != nil {
		return nil, apiErr
	}

	src, err := b.resolveDiskSourceLocked(project, in)
	if err != nil {
		return nil, err
	}

	size := in.SizeGb

	switch {
	case size == 0 && src.sizeGb > 0:
		size = src.sizeGb
	case size == 0:
		size = dt.defaultGb
	case size < src.sizeGb:
		return nil, errField("resource.sizeGb", sizeStr(size),
			"Requested disk size cannot be smaller than the source size ("+sizeStr(src.sizeGb)+" GB).")
	}

	if size < minDiskGb || size > maxDiskGb {
		return nil, errField("resource.sizeGb", sizeStr(size), "Disk size must be between 10 and 65536 GB.")
	}

	d := &computeapi.Disk{
		Kind:                   "compute#disk",
		Id:                     newID(),
		CreationTimestamp:      b.stamp(),
		Name:                   in.Name,
		Description:            in.Description,
		SizeGb:                 size,
		Zone:                   scopeURL(project, scopeZone, zone),
		Status:                 statusReady,
		SelfLink:               zoneLink(project, zone, "disks", in.Name),
		Type:                   zoneLink(project, zone, "diskTypes", dt.name),
		Labels:                 in.Labels,
		LabelFingerprint:       labelFP(in.Labels),
		PhysicalBlockSizeBytes: physBlockBytes,
		Architecture:           archX86,
		ResourcePolicies:       policies,
	}

	if src.image != nil {
		d.SourceImage, d.SourceImageId = src.image.SelfLink, strconv.FormatUint(src.image.Id, 10)
		d.GuestOsFeatures, d.Licenses = src.image.GuestOsFeatures, src.image.Licenses
	}

	if src.snapshot != nil {
		d.SourceSnapshot, d.SourceSnapshotId = src.snapshot.SelfLink, strconv.FormatUint(src.snapshot.Id, 10)
	}

	b.disks[key(project, zone, in.Name)] = d

	return d, nil
}

func sizeStr(n int64) string { return strconv.FormatInt(n, 10) }

func (b *InMemoryBackend) insertDisk(project, zone string, in *computeapi.Disk) (*computeapi.Operation, error) {
	b.mu.Lock("InsertDisk")
	defer b.mu.Unlock()

	d, err := b.createDiskLocked(project, zone, in)
	if err != nil {
		return nil, err
	}

	return b.newOpLocked("insert", diskTarget(project, zone, d), b.timing.Done), nil
}

func (b *InMemoryBackend) getDisk(project, zone, name string) (*computeapi.Disk, error) {
	b.mu.RLock("GetDisk")
	defer b.mu.RUnlock()

	d, ok := b.disks[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "disks", name))
	}

	return b.diskViewLocked(project, d), nil
}

func (b *InMemoryBackend) listDisks(project, scope, loc string) []entry {
	b.mu.RLock("ListDisks")
	defer b.mu.RUnlock()

	var out []entry

	for k, d := range b.disks {
		zone := lastSegment(d.Zone)
		if !strings.HasPrefix(k, project+"/") || (scope == scopeZone && zone != loc) {
			continue
		}

		out = append(out, entry{scope: scopeZone + "/" + zone, item: b.diskViewLocked(project, d)})
	}

	return out
}

func (b *InMemoryBackend) deleteDisk(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteDisk")
	defer b.mu.Unlock()

	d, ok := b.disks[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "disks", name))
	}

	if users := b.diskUsersLocked(project, zone, name); len(users) > 0 {
		return nil, errInUse("disk", relPath(project, scopeZone, zone, "disks", name), trimURL(users[0]))
	}

	delete(b.disks, key(project, zone, name))

	return b.newOpLocked("delete", diskTarget(project, zone, d), b.timing.Done), nil
}

func (b *InMemoryBackend) resizeDisk(project, zone, name string, sizeGb int64) (*computeapi.Operation, error) {
	b.mu.Lock("ResizeDisk")
	defer b.mu.Unlock()

	d, ok := b.disks[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "disks", name))
	}

	if sizeGb <= d.SizeGb || sizeGb > maxDiskGb {
		return nil, errField("sizeGb", sizeStr(sizeGb), "New disk size must be larger than the current size ("+
			sizeStr(d.SizeGb)+" GB) and at most 65536 GB.")
	}

	d.SizeGb = sizeGb

	return b.newOpLocked("resize", diskTarget(project, zone, d), b.timing.Done), nil
}

func (b *InMemoryBackend) setDiskLabels(project, zone, name string, req labelsRequest) (*computeapi.Operation, error) {
	b.mu.Lock("SetDiskLabels")
	defer b.mu.Unlock()

	d, ok := b.disks[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "disks", name))
	}

	labels, fp, err := applyLabels(d.Labels, d.LabelFingerprint, req)
	if err != nil {
		return nil, err
	}

	d.Labels, d.LabelFingerprint = labels, fp

	return b.newOpLocked("setLabels", diskTarget(project, zone, d), b.timing.Done), nil
}

func storageLocationFor(zone string) string {
	switch {
	case strings.HasPrefix(zone, "us-"),
		strings.HasPrefix(zone, "northamerica-"),
		strings.HasPrefix(zone, "southamerica-"):
		return "us"
	case strings.HasPrefix(zone, "europe-"):
		return "eu"
	}

	return "asia"
}

func (b *InMemoryBackend) createSnapshot(
	project, zone, name string,
	in *computeapi.Snapshot,
) (*computeapi.Operation, error) {
	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	d, ok := b.disks[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "disks", name))
	}

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if err := validLabels(in.Labels); err != nil {
		return nil, err
	}

	if _, dup := b.snapshots[key(project, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeGlobal, "", "snapshots", in.Name))
	}

	locs := in.StorageLocations
	if len(locs) == 0 {
		locs = []string{storageLocationFor(zone)}
	}

	s := &computeapi.Snapshot{
		Kind: "compute#snapshot", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name, Description: in.Description,
		Status: statusReady, DiskSizeGb: d.SizeGb, SourceDisk: d.SelfLink, SourceDiskId: strconv.FormatUint(d.Id, 10),
		SelfLink: globalLink(project, "snapshots", in.Name), Labels: in.Labels, LabelFingerprint: labelFP(in.Labels),
		StorageLocations: locs, SnapshotType: tierStandard, Architecture: d.Architecture,
		Licenses: d.Licenses, GuestOsFeatures: d.GuestOsFeatures,
	}
	b.snapshots[key(project, in.Name)] = s

	return b.newOpLocked("createSnapshot", diskTarget(project, zone, d), b.timing.Done), nil
}

func (b *InMemoryBackend) getSnapshot(project, name string) (*computeapi.Snapshot, error) {
	b.mu.RLock("GetSnapshot")
	defer b.mu.RUnlock()

	s, ok := b.snapshots[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "snapshots", name))
	}

	return clone(s), nil
}

func (b *InMemoryBackend) listSnapshots(project string) []entry {
	b.mu.RLock("ListSnapshots")
	defer b.mu.RUnlock()

	var out []entry

	for k, s := range b.snapshots {
		if strings.HasPrefix(k, project+"/") {
			out = append(out, entry{scope: scopeGlobal, item: clone(s)})
		}
	}

	return out
}

func (b *InMemoryBackend) deleteSnapshot(project, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	s, ok := b.snapshots[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "snapshots", name))
	}

	delete(b.snapshots, key(project, name))

	return b.newOpLocked(
		"delete",
		target{project: project, scope: scopeGlobal, collection: "snapshots", name: name, id: s.Id},
		b.timing.Done,
	), nil
}

func (b *InMemoryBackend) setSnapshotLabels(project, name string, req labelsRequest) (*computeapi.Operation, error) {
	b.mu.Lock("SetSnapshotLabels")
	defer b.mu.Unlock()

	s, ok := b.snapshots[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "snapshots", name))
	}

	labels, fp, err := applyLabels(s.Labels, s.LabelFingerprint, req)
	if err != nil {
		return nil, err
	}

	s.Labels, s.LabelFingerprint = labels, fp

	return b.newOpLocked(
		"setLabels",
		target{project: project, scope: scopeGlobal, collection: "snapshots", name: name, id: s.Id},
		b.timing.Done,
	), nil
}
