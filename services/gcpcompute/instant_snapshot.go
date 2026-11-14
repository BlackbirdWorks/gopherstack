package gcpcompute

import (
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const collInstantSnaps = "instantSnapshots"

func instantTarget(project, scope, loc string, s *computeapi.InstantSnapshot) target {
	return target{project: project, scope: scope, loc: loc, collection: collInstantSnaps, name: s.Name, id: s.Id}
}

func (b *InMemoryBackend) getInstantLocked(project, scope, loc, name string) (*computeapi.InstantSnapshot, *APIError) {
	s, ok := b.instantSnaps[key(project, loc, name)]
	if !ok || (scope == scopeRegion) != (s.Region != "") {
		return nil, errNotFound(relPath(project, scope, loc, collInstantSnaps, name))
	}

	return s, nil
}

func (b *InMemoryBackend) resolveInstantSnapshotLocked(
	project, scope, loc, raw string,
) (*computeapi.InstantSnapshot, *APIError) {
	r, ok := parseRef(raw, collInstantSnaps, project, loc)
	if !ok || (r.scope != "" && r.scope != scope) || r.loc != loc {
		return nil, errField("sourceInstantSnapshot", raw, "The URL is malformed or names a different location.")
	}

	return b.getInstantLocked(r.project, scope, loc, r.name)
}

func (b *InMemoryBackend) insertInstantSnapshot(
	project, scope, loc string, in *computeapi.InstantSnapshot,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertInstantSnapshot")
	defer b.mu.Unlock()

	if err := firstErr(validName("name", in.Name), validLabels(in.Labels)); err != nil {
		return nil, err
	}

	if scope == scopeZone && !zoneExists(loc) || scope == scopeRegion && regionMissing(loc) {
		return nil, errNotFound(scopePath(project, scope, loc))
	}

	if _, dup := b.instantSnaps[key(project, loc, in.Name)]; dup {
		return nil, errExists(relPath(project, scope, loc, collInstantSnaps, in.Name))
	}

	if in.SourceDisk == "" {
		return nil, errRequired("sourceDisk")
	}

	r, ok := parseRef(in.SourceDisk, "disks", project, loc)
	if !ok || r.loc != loc || (r.scope != "" && r.scope != scope) {
		return nil, errField("sourceDisk", in.SourceDisk, "The URL is malformed or names a different location.")
	}

	d, err := b.getDiskLocked(r.project, scope, loc, r.name)
	if err != nil {
		return nil, err
	}

	s := &computeapi.InstantSnapshot{
		Kind: "compute#instantSnapshot", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name,
		Description: in.Description, Status: statusReady, DiskSizeGb: d.SizeGb, Architecture: d.Architecture,
		SourceDisk: d.SelfLink, SourceDiskId: strconv.FormatUint(d.Id, 10), Labels: in.Labels,
		LabelFingerprint: labelFP(in.Labels), SelfLink: link(project, scope, loc, collInstantSnaps, in.Name),
		ResourceStatus: &computeapi.InstantSnapshotResourceStatus{},
	}

	if scope == scopeRegion {
		s.Region = scopeURL(project, scopeRegion, loc)
	} else {
		s.Zone = scopeURL(project, scopeZone, loc)
	}

	b.instantSnaps[key(project, loc, in.Name)] = s

	return b.newOpLocked("insert", instantTarget(project, scope, loc, s), b.timing.Done), nil
}

func (b *InMemoryBackend) getInstantSnapshot(project, scope, loc, name string) (*computeapi.InstantSnapshot, error) {
	b.mu.RLock("GetInstantSnapshot")
	defer b.mu.RUnlock()

	s, err := b.getInstantLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	return clone(s), nil
}

func (b *InMemoryBackend) listInstantSnapshots(project, scope, loc string) []entry {
	b.mu.RLock("ListInstantSnapshots")
	defer b.mu.RUnlock()

	var out []entry

	for k, s := range b.instantSnaps {
		if !strings.HasPrefix(k, project+"/") {
			continue
		}

		sc, l := scopeZone, lastSegment(s.Zone)
		if s.Region != "" {
			sc, l = scopeRegion, lastSegment(s.Region)
		}

		if scope != "" && (sc != scope || l != loc) {
			continue
		}

		out = append(out, entry{scope: sc + "/" + l, item: clone(s)})
	}

	return out
}

func (b *InMemoryBackend) deleteInstantSnapshot(project, scope, loc, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteInstantSnapshot")
	defer b.mu.Unlock()

	s, err := b.getInstantLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	delete(b.instantSnaps, key(project, loc, name))

	return b.newOpLocked("delete", instantTarget(project, scope, loc, s), b.timing.Done), nil
}

func (b *InMemoryBackend) setInstantSnapshotLabels(
	project, scope, loc, name string, req labelsRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetInstantSnapshotLabels")
	defer b.mu.Unlock()

	s, err := b.getInstantLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	labels, fp, lerr := applyLabels(s.Labels, s.LabelFingerprint, req)
	if lerr != nil {
		return nil, lerr
	}

	s.Labels, s.LabelFingerprint = labels, fp

	return b.newOpLocked("setLabels", instantTarget(project, scope, loc, s), b.timing.Done), nil
}
