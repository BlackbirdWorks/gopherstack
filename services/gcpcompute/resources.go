package gcpcompute

import (
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

func imgTarget(project string, img *computeapi.Image) target {
	return target{project: project, scope: scopeGlobal, collection: "images", name: img.Name, id: img.Id}
}

func (b *InMemoryBackend) insertImage(project string, in *computeapi.Image) (*computeapi.Operation, error) {
	b.mu.Lock("InsertImage")
	defer b.mu.Unlock()

	if publicImageProject(project) {
		return nil, errForbidden("Required 'compute.images.create' permission for 'projects/%s'", project)
	}

	if err := firstErr(validName("name", in.Name), validLabels(in.Labels)); err != nil {
		return nil, err
	}

	if in.Family != "" {
		if err := validName("resource.family", in.Family); err != nil {
			return nil, err
		}
	}

	if in.RawDisk != nil {
		return nil, errField(
			"resource.rawDisk",
			"",
			"Importing raw disks from Cloud Storage is not supported by this emulator.",
		)
	}

	if _, dup := b.images[key(project, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeGlobal, "", "images", in.Name))
	}

	img := &computeapi.Image{
		Kind:              "compute#image",
		Id:                newID(),
		CreationTimestamp: b.stamp(),
		Name:              in.Name,
		Description:       in.Description,
		Family:            in.Family,
		Status:            statusReady,
		SourceType:        "RAW",
		Labels:            in.Labels,
		LabelFingerprint:  labelFP(in.Labels),
		SelfLink:          globalLink(project, "images", in.Name),
		Architecture:      archX86,
		StorageLocations:  in.StorageLocations,
	}

	if err := b.imageSourceLocked(project, in, img); err != nil {
		return nil, err
	}

	if len(img.StorageLocations) == 0 {
		img.StorageLocations = []string{"us"}
	}

	b.images[key(project, img.Name)] = img

	return b.newOpLocked("insert", imgTarget(project, img), b.timing.Done), nil
}

func (b *InMemoryBackend) imageSourceLocked(project string, in, img *computeapi.Image) *APIError {
	n := 0

	for _, s := range []string{in.SourceDisk, in.SourceSnapshot, in.SourceImage} {
		if s != "" {
			n++
		}
	}

	if n != 1 {
		return errInvalid(
			"Invalid value for field 'resource': ''. Exactly one of sourceDisk, sourceSnapshot " +
				"or sourceImage must be specified.",
		)
	}

	switch {
	case in.SourceDisk != "":
		r, err := refLocated(in.SourceDisk, "disks", scopeZone, project, "")
		if err != nil {
			return errField("resource.sourceDisk", in.SourceDisk, "The URL is malformed.")
		}

		d, ok := b.disks[key(r.project, r.loc, r.name)]
		if !ok {
			return errNotFound(relPath(r.project, scopeZone, r.loc, "disks", r.name))
		}

		img.SourceDisk, img.SourceDiskId, img.DiskSizeGb = d.SelfLink, strconv.FormatUint(d.Id, 10), d.SizeGb
		img.Licenses, img.GuestOsFeatures = d.Licenses, d.GuestOsFeatures
	case in.SourceSnapshot != "":
		r, err := refGlobal(in.SourceSnapshot, "snapshots", project)
		if err != nil {
			return errField("resource.sourceSnapshot", in.SourceSnapshot, "The URL is malformed.")
		}

		s, ok := b.snapshots[key(r.project, r.name)]
		if !ok {
			return errNotFound(relPath(r.project, scopeGlobal, "", "snapshots", r.name))
		}

		img.SourceSnapshot, img.SourceSnapshotId, img.DiskSizeGb = s.SelfLink, strconv.FormatUint(
			s.Id,
			10,
		), s.DiskSizeGb
		img.Licenses, img.GuestOsFeatures = s.Licenses, s.GuestOsFeatures
	default:
		src, err := b.resolveImageLocked(project, in.SourceImage)
		if err != nil {
			return err
		}

		img.SourceImage, img.SourceImageId, img.DiskSizeGb = src.SelfLink, strconv.FormatUint(
			src.Id,
			10,
		), src.DiskSizeGb
		img.Licenses, img.GuestOsFeatures = src.Licenses, src.GuestOsFeatures
	}

	if len(in.GuestOsFeatures) > 0 {
		img.GuestOsFeatures = in.GuestOsFeatures
	}

	return nil
}

func (b *InMemoryBackend) deleteImage(project, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteImage")
	defer b.mu.Unlock()

	if publicImageProject(project) {
		return nil, errForbidden(
			"Required 'compute.images.delete' permission for 'projects/%s/global/images/%s'",
			project,
			name,
		)
	}

	img, ok := b.images[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "images", name))
	}

	delete(b.images, key(project, name))

	return b.newOpLocked("delete", imgTarget(project, img), b.timing.Done), nil
}

func (b *InMemoryBackend) setImageLabels(project, name string, req labelsRequest) (*computeapi.Operation, error) {
	b.mu.Lock("SetImageLabels")
	defer b.mu.Unlock()

	if publicImageProject(project) {
		return nil, errForbidden(
			"Required 'compute.images.setLabels' permission for 'projects/%s/global/images/%s'",
			project,
			name,
		)
	}

	img, ok := b.images[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "images", name))
	}

	labels, fp, err := applyLabels(img.Labels, img.LabelFingerprint, req)
	if err != nil {
		return nil, err
	}

	img.Labels, img.LabelFingerprint = labels, fp

	return b.newOpLocked("setLabels", imgTarget(project, img), b.timing.Done), nil
}

// projectMetaLocked returns the project's common instance metadata, creating the empty default on first use.
func (b *InMemoryBackend) projectMetaLocked(project string) *computeapi.Metadata {
	m, ok := b.projects[project]
	if !ok {
		m = normalizeMetadata(nil)
		b.projects[project] = m
	}

	return m
}

func (b *InMemoryBackend) getProject(project string) *computeapi.Project {
	b.mu.Lock("GetProject")
	defer b.mu.Unlock()

	return &computeapi.Project{
		Kind:                   "compute#project",
		Id:                     stableID("project", project),
		Name:                   project,
		CreationTimestamp:      catalogCreated,
		CommonInstanceMetadata: clone(b.projectMetaLocked(project)),
		SelfLink:               apiBase + "projects/" + project,
		DefaultNetworkTier:     tierPremium,
		XpnProjectStatus:       "UNSPECIFIED_XPN_PROJECT_STATUS",
		DefaultServiceAccount: strconv.FormatUint(
			stableID("project", project)%1_000_000_000_000,
			10,
		) + "-compute@developer.gserviceaccount.com",
	}
}

func (b *InMemoryBackend) setCommonInstanceMetadata(
	project string,
	in *computeapi.Metadata,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetCommonInstanceMetadata")
	defer b.mu.Unlock()

	cur := b.projectMetaLocked(project)
	if in.Fingerprint != cur.Fingerprint {
		return nil, errPrecondition("Supplied fingerprint does not match current metadata fingerprint.")
	}

	if err := validMetadata(in); err != nil {
		return nil, err
	}

	b.projects[project] = normalizeMetadata(in)

	return b.newOpLocked("setCommonInstanceMetadata", target{
		project: project, scope: scopeGlobal, collection: collProjects, name: "",
	}, b.timing.Done), nil
}

func (b *InMemoryBackend) listZones(project string) []entry {
	zones := allZones()
	out := make([]entry, 0, len(zones))

	for _, z := range zones {
		out = append(out, entry{scope: scopeGlobal, item: zoneResource(project, z)})
	}

	return out
}

func (b *InMemoryBackend) listRegions(project string) []entry {
	out := make([]entry, 0, len(regionCatalog))

	for _, r := range regionCatalog {
		out = append(out, entry{scope: scopeGlobal, item: regionResource(project, r)})
	}

	return out
}

func (b *InMemoryBackend) listMachineTypes(project, scope, loc string) []entry {
	var out []entry

	for _, z := range allZones() {
		if scope == scopeZone && z != loc {
			continue
		}

		for _, m := range machineSpecs {
			out = append(out, entry{scope: scopeZone + "/" + z, item: machineResource(project, z, m)})
		}
	}

	return out
}

func (b *InMemoryBackend) listDiskTypes(project, scope, loc string) []entry {
	var out []entry

	for _, z := range allZones() {
		if scope == scopeZone && z != loc {
			continue
		}

		for _, d := range diskTypeCatalog {
			out = append(out, entry{scope: scopeZone + "/" + z, item: diskTypeResource(project, z, d)})
		}
	}

	return out
}

func getMachineType(project, zone, name string) (*computeapi.MachineType, error) {
	if !zoneExists(zone) {
		return nil, errNotFound(scopePath(project, scopeZone, zone))
	}

	if m, ok := findMachine(name); ok {
		return machineResource(project, zone, m), nil
	}

	if cpus, mem, ok := customMachine(name); ok {
		m := machineSpec{
			name: name, cpus: cpus, memMb: mem,
			desc: strconv.FormatInt(cpus, 10) + " vCPUs " + trimFloat(float64(mem)/gbToMb) + " GB RAM",
		}

		return machineResource(project, zone, m), nil
	}

	return nil, errNotFound(relPath(project, scopeZone, zone, "machineTypes", name))
}

func getDiskType(project, zone, name string) (*computeapi.DiskType, error) {
	if !zoneExists(zone) {
		return nil, errNotFound(scopePath(project, scopeZone, zone))
	}

	d, ok := findDiskType(name)
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "diskTypes", name))
	}

	return diskTypeResource(project, zone, d), nil
}

func getZone(project, name string) (*computeapi.Zone, error) {
	if !zoneExists(name) {
		return nil, errNotFound(scopePath(project, scopeZone, name))
	}

	return zoneResource(project, name), nil
}

func getRegion(project, name string) (*computeapi.Region, error) {
	r, ok := findRegion(name)
	if !ok {
		return nil, errNotFound(scopePath(project, scopeRegion, name))
	}

	return regionResource(project, r), nil
}
