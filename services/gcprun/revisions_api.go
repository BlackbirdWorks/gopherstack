package gcprun

import (
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
)

// GetRevision returns the named revision.
func (b *InMemoryBackend) GetRevision(name string) (*runpb.Revision, error) {
	n, err := parseName(name, "services", "revisions")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetRevision")
	defer b.mu.RUnlock()

	r, ok := b.revisions[name]
	if !ok {
		return nil, errNotFound("REVISION", n.ids[1], n.location, n.project)
	}

	return clone(r.pb), nil
}

// ListRevisions lists a service's revisions ordered by name.
func (b *InMemoryBackend) ListRevisions(in *runpb.ListRevisionsRequest) ([]*runpb.Revision, string, error) {
	n, err := parseName(in.GetParent(), "services")
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListRevisions")

	if _, ok := b.services[in.GetParent()]; !ok {
		b.mu.RUnlock()

		return nil, "", errNotFound("SERVICE", n.ids[0], n.location, n.project)
	}

	all := collect(b.revisions, in.GetParent()+"/revisions/", func(r *revRec) *runpb.Revision { return clone(r.pb) })
	b.mu.RUnlock()

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

// DeleteRevision removes a revision that is not serving traffic and stops its container.
func (b *InMemoryBackend) DeleteRevision(in *runpb.DeleteRevisionRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "services", "revisions")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteRevision")

	r, ok := b.revisions[in.GetName()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("REVISION", n.ids[1], n.location, n.project)
	}

	if in.GetEtag() != "" && in.GetEtag() != r.pb.GetEtag() {
		b.mu.Unlock()

		return nil, errPrecondition("The etag %q does not match the revision's current etag.", in.GetEtag())
	}

	if b.servingLocked(r) {
		b.mu.Unlock()

		return nil, errPrecondition("Revision '%s' is serving traffic and cannot be deleted.", n.ids[1])
	}

	view := clone(r.pb)
	view.DeleteTime = b.ts()

	if in.GetValidateOnly() {
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	delete(b.revisions, in.GetName())
	b.mu.Unlock()

	b.teardown(r)

	return b.doneOp(opName(n.parent()), view, view)
}

func (b *InMemoryBackend) servingLocked(r *revRec) bool {
	svc, ok := b.services[r.pb.GetService()]
	if !ok {
		return false
	}

	short := lastSegment(r.pb.GetName())

	for _, st := range svc.pb.GetTrafficStatuses() {
		if st.GetRevision() == short && (st.GetPercent() > 0 || st.GetTag() != "") {
			return true
		}
	}

	return false
}
