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

	regionalReplicas      = 2
	regionalStandardMinGb = 200
)

func diskTarget(project, loc string, d *computeapi.Disk) target {
	scope := scopeZone
	if d.Region != "" {
		scope, loc = scopeRegion, lastSegment(d.Region)
	}

	return target{project: project, scope: scope, loc: loc, collection: "disks", name: d.Name, id: d.Id}
}

func diskLoc(d *computeapi.Disk) string {
	if d.Region != "" {
		return lastSegment(d.Region)
	}

	return lastSegment(d.Zone)
}

func diskScope(d *computeapi.Disk) string {
	if d.Region != "" {
		return scopeRegion
	}

	return scopeZone
}

// diskKeyFor keys the disk named by an attached-disk source (zonal or regional) for an instance in zone.
func diskKeyFor(project, zone, source string) string {
	if r, ok := parseRef(source, "disks", project, zone); ok && r.scope == scopeRegion {
		return key(r.project, r.loc, r.name)
	}

	return key(project, zone, lastSegment(source))
}

func (b *InMemoryBackend) diskUsersLocked(project string, d *computeapi.Disk) []string {
	var users []string

	self := d.SelfLink

	for _, r := range b.instances {
		if !strings.Contains(r.Inst.SelfLink, "/projects/"+project+"/") {
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
	out.Users = b.diskUsersLocked(project, d)

	return out
}

func (b *InMemoryBackend) resolveDiskTypeLocked(project, scope, loc, raw string) (diskTypeSpec, *APIError) {
	if raw == "" {
		raw = defaultDiskTyp
	}

	r, err := refLocated(raw, "diskTypes", scope, project, loc)
	if err != nil {
		return diskTypeSpec{}, errField("diskType", raw, "")
	}

	spec, ok := findDiskType(r.name)
	if !ok || r.loc != loc {
		return diskTypeSpec{}, errNotFound(relPath(r.project, scope, r.loc, "diskTypes", r.name))
	}

	return spec, nil
}

type diskSource struct {
	image    *computeapi.Image
	snapshot *computeapi.Snapshot
	instant  *computeapi.InstantSnapshot
	sizeGb   int64
}

func (b *InMemoryBackend) resolveDiskSourceLocked(
	project, scope, loc string, in *computeapi.Disk,
) (diskSource, *APIError) {
	var src diskSource

	n := 0

	for _, v := range []string{in.SourceImage, in.SourceSnapshot, in.SourceInstantSnapshot} {
		if v != "" {
			n++
		}
	}

	if n > 1 {
		return src, errInvalid(
			"Only one of sourceImage, sourceSnapshot and sourceInstantSnapshot may be specified.")
	}

	switch {
	case in.SourceImage != "":
		img, err := b.resolveImageLocked(project, in.SourceImage)
		if err != nil {
			return src, err
		}

		src.image, src.sizeGb = img, img.DiskSizeGb
	case in.SourceInstantSnapshot != "":
		is, err := b.resolveInstantSnapshotLocked(project, scope, loc, in.SourceInstantSnapshot)
		if err != nil {
			return src, err
		}

		src.instant, src.sizeGb = is, is.DiskSizeGb
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
	return b.createDiskAtLocked(project, scopeZone, zone, in)
}

func (b *InMemoryBackend) validReplicaZonesLocked(region string, in *computeapi.Disk) ([]string, *APIError) {
	if len(in.ReplicaZones) != regionalReplicas {
		return nil, errField("resource.replicaZones", "", "A regional disk needs exactly two replica zones.")
	}

	out := make([]string, 0, regionalReplicas)
	seen := map[string]bool{}

	for _, z := range in.ReplicaZones {
		zn := lastSegment(z)
		if !zoneExists(zn) || regionOfZone(zn) != region || seen[zn] {
			return nil, errField("resource.replicaZones", z, "Not a distinct zone of the disk's region.")
		}

		seen[zn] = true
		out = append(out, zn)
	}

	return out, nil
}

func (b *InMemoryBackend) checkDiskSlotLocked(project, scope, loc, name string) *APIError {
	if err := validName("name", name); err != nil {
		return err
	}

	if scope == scopeZone && !zoneExists(loc) || scope == scopeRegion && regionMissing(loc) {
		return errNotFound(scopePath(project, scope, loc))
	}

	if _, dup := b.disks[key(project, loc, name)]; dup {
		return errExists(relPath(project, scope, loc, "disks", name))
	}

	return nil
}

func (b *InMemoryBackend) replicaURLsLocked(project, scope, region string, in *computeapi.Disk) ([]string, *APIError) {
	if scope == scopeZone {
		if len(in.ReplicaZones) > 0 {
			return nil, errInvalid("Invalid value for field 'resource.replicaZones': only valid for regional disks.")
		}

		return nil, nil
	}

	replicas, err := b.validReplicaZonesLocked(region, in)
	if err != nil {
		return nil, err
	}

	for i, z := range replicas {
		replicas[i] = scopeURL(project, scopeZone, z)
	}

	return replicas, nil
}

func (b *InMemoryBackend) createDiskAtLocked(
	project, scope, loc string, in *computeapi.Disk,
) (*computeapi.Disk, *APIError) {
	if err := b.checkDiskSlotLocked(project, scope, loc, in.Name); err != nil {
		return nil, err
	}

	region := loc
	if scope == scopeZone {
		region = regionOfZone(loc)
	}

	dt, err := b.resolveDiskTypeLocked(project, scope, loc, in.Type)
	if err != nil {
		return nil, err
	}

	replicas, err := b.replicaURLsLocked(project, scope, region, in)
	if err != nil {
		return nil, err
	}

	policies, apiErr := b.diskLabelsAndPoliciesLocked(project, region, in)
	if apiErr != nil {
		return nil, apiErr
	}

	src, err := b.resolveDiskSourceLocked(project, scope, loc, in)
	if err != nil {
		return nil, err
	}

	size, err := diskSize(in.SizeGb, src.sizeGb, dt, scope)
	if err != nil {
		return nil, err
	}

	d := &computeapi.Disk{
		Kind:                   "compute#disk",
		Id:                     newID(),
		CreationTimestamp:      b.stamp(),
		Name:                   in.Name,
		Description:            in.Description,
		SizeGb:                 size,
		Status:                 statusReady,
		SelfLink:               link(project, scope, loc, "disks", in.Name),
		Type:                   link(project, scope, loc, "diskTypes", dt.name),
		Labels:                 in.Labels,
		LabelFingerprint:       labelFP(in.Labels),
		PhysicalBlockSizeBytes: physBlockBytes,
		Architecture:           archX86,
		ResourcePolicies:       policies,
		ReplicaZones:           replicas,
	}

	if scope == scopeRegion {
		d.Region = scopeURL(project, scopeRegion, loc)
	} else {
		d.Zone = scopeURL(project, scopeZone, loc)
	}

	src.apply(d)
	b.disks[key(project, loc, in.Name)] = d

	return d, nil
}

func diskSize(req, srcGb int64, dt diskTypeSpec, scope string) (int64, *APIError) {
	size := req

	switch {
	case size == 0 && srcGb > 0:
		size = srcGb
	case size == 0:
		size = dt.defaultGb
	case size < srcGb:
		return 0, errField("resource.sizeGb", sizeStr(size),
			"Requested disk size cannot be smaller than the source size ("+sizeStr(srcGb)+" GB).")
	}

	lo := int64(minDiskGb)
	if scope == scopeRegion && dt.name == defaultDiskTyp {
		lo = regionalStandardMinGb
	}

	if size < lo || size > maxDiskGb {
		return 0, errField("resource.sizeGb", sizeStr(size),
			"Disk size must be between "+sizeStr(lo)+" and 65536 GB.")
	}

	return size, nil
}

func (s diskSource) apply(d *computeapi.Disk) {
	if s.image != nil {
		d.SourceImage, d.SourceImageId = s.image.SelfLink, strconv.FormatUint(s.image.Id, 10)
		d.GuestOsFeatures, d.Licenses = s.image.GuestOsFeatures, s.image.Licenses
	}

	if s.snapshot != nil {
		d.SourceSnapshot, d.SourceSnapshotId = s.snapshot.SelfLink, strconv.FormatUint(s.snapshot.Id, 10)
	}

	if s.instant != nil {
		d.SourceInstantSnapshot = s.instant.SelfLink
		d.SourceInstantSnapshotId = strconv.FormatUint(s.instant.Id, 10)
	}
}

func sizeStr(n int64) string { return strconv.FormatInt(n, 10) }

func (b *InMemoryBackend) insertDisk(project, scope, loc string, in *computeapi.Disk) (*computeapi.Operation, error) {
	b.mu.Lock("InsertDisk")
	defer b.mu.Unlock()

	d, err := b.createDiskAtLocked(project, scope, loc, in)
	if err != nil {
		return nil, err
	}

	return b.newOpLocked("insert", diskTarget(project, loc, d), b.timing.Done), nil
}

func (b *InMemoryBackend) getDiskLocked(project, scope, loc, name string) (*computeapi.Disk, *APIError) {
	d, ok := b.disks[key(project, loc, name)]
	if !ok || diskScope(d) != scope {
		return nil, errNotFound(relPath(project, scope, loc, "disks", name))
	}

	return d, nil
}

func (b *InMemoryBackend) getDisk(project, scope, loc, name string) (*computeapi.Disk, error) {
	b.mu.RLock("GetDisk")
	defer b.mu.RUnlock()

	d, err := b.getDiskLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	return b.diskViewLocked(project, d), nil
}

func (b *InMemoryBackend) listDisks(project, scope, loc string) []entry {
	b.mu.RLock("ListDisks")
	defer b.mu.RUnlock()

	var out []entry

	for k, d := range b.disks {
		if !strings.HasPrefix(k, project+"/") || (scope != "" && (diskScope(d) != scope || diskLoc(d) != loc)) {
			continue
		}

		out = append(out, entry{scope: diskScope(d) + "/" + diskLoc(d), item: b.diskViewLocked(project, d)})
	}

	return out
}

func (b *InMemoryBackend) deleteDisk(project, scope, loc, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteDisk")
	defer b.mu.Unlock()

	d, err := b.getDiskLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	if users := b.diskUsersLocked(project, d); len(users) > 0 {
		return nil, errInUse("disk", relPath(project, scope, loc, "disks", name), trimURL(users[0]))
	}

	delete(b.disks, key(project, loc, name))

	return b.newOpLocked("delete", diskTarget(project, loc, d), b.timing.Done), nil
}

func (b *InMemoryBackend) resizeDisk(project, scope, loc, name string, sizeGb int64) (*computeapi.Operation, error) {
	b.mu.Lock("ResizeDisk")
	defer b.mu.Unlock()

	d, err := b.getDiskLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	if sizeGb <= d.SizeGb || sizeGb > maxDiskGb {
		return nil, errField("sizeGb", sizeStr(sizeGb), "New disk size must be larger than the current size ("+
			sizeStr(d.SizeGb)+" GB) and at most 65536 GB.")
	}

	d.SizeGb = sizeGb

	return b.newOpLocked("resize", diskTarget(project, loc, d), b.timing.Done), nil
}

func (b *InMemoryBackend) setDiskLabels(
	project, scope, loc, name string, req labelsRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetDiskLabels")
	defer b.mu.Unlock()

	d, err := b.getDiskLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	labels, fp, lerr := applyLabels(d.Labels, d.LabelFingerprint, req)
	if lerr != nil {
		return nil, lerr
	}

	d.Labels, d.LabelFingerprint = labels, fp

	return b.newOpLocked("setLabels", diskTarget(project, loc, d), b.timing.Done), nil
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
	project, scope, zone, name string,
	in *computeapi.Snapshot,
) (*computeapi.Operation, error) {
	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	d, derr := b.getDiskLocked(project, scope, zone, name)
	if derr != nil {
		return nil, derr
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

func (b *InMemoryBackend) diskLabelsAndPoliciesLocked(
	project, region string,
	in *computeapi.Disk,
) ([]string, *APIError) {
	if err := validLabels(in.Labels); err != nil {
		return nil, err
	}

	return b.resolveResPoliciesLocked(
		project, region, in.ResourcePolicies, "resource.resourcePolicies", diskPolicyKinds())
}
