package ociidentity

import (
	"net/http"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	opChangeTagNSComp  = "ChangeTagNamespaceCompartment"
	opCascadeDeleteNS  = "CascadeDeleteTagNamespace"
	opGetTaggingWR     = "GetTaggingWorkRequest"
	opListTaggingWR    = "ListTaggingWorkRequests"
	opListTaggingWRErr = "ListTaggingWorkRequestErrors"
	opListTaggingWRLog = "ListTaggingWorkRequestLogs"

	wrDeleteNonEmptyNS = "DELETE_NON_EMPTY_TAG_NAMESPACE"
	wrDeleteTagDef     = "DELETE_TAG_DEFINITION"
)

type changeTagNSCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

// ChangeTagNamespaceCompartment moves a tag namespace and its tag definitions to another compartment.
func (b *InMemoryBackend) ChangeTagNamespaceCompartment(id, compartment, token string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeTagNamespaceCompartment")
	defer b.mu.Unlock()

	scope := "ChangeTagNamespaceCompartment/" + id
	if _, ok := b.retry.Lookup(scope, token, b.nowUTC()); ok {
		return nil
	}

	n, aerr := b.tagNSByID(id)
	if aerr != nil {
		return aerr
	}

	if n.LifecycleState != stateActive {
		return errState("Tag namespace " + id + " is in state " + n.LifecycleState + "; the operation requires ACTIVE")
	}

	if aerr = b.activeParent(compartment); aerr != nil {
		return aerr
	}

	n.CompartmentID, n.Internal.Etag = compartment, newEtag()

	for _, d := range b.tagDefs {
		if d.NamespaceID == id {
			d.CompartmentID = compartment
		}
	}

	b.retry.Remember(scope, token, id, b.nowUTC())

	return nil
}

// CascadeDeleteTagNamespace deletes a retired namespace together with every tag definition in it.
func (b *InMemoryBackend) CascadeDeleteTagNamespace(id, ifMatch, token string) (string, error) {
	b.mu.Lock("CascadeDeleteTagNamespace")
	defer b.mu.Unlock()

	scope := "CascadeDeleteTagNamespace/" + id
	if v, ok := b.retry.Lookup(scope, token, b.nowUTC()); ok {
		return v, nil
	}

	n, aerr := b.tagNSByID(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(n.Internal.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if n.LifecycleState != stateActive {
		return "", errState("Tag namespace " + id + " is already being deleted")
	}

	if !n.IsRetired {
		return "", errConflict("Conflict", "Tag namespace "+id+" must be retired before it is deleted")
	}

	finish := b.nowUTC().Add(transitionWindow)

	for tid, td := range b.tagDefaults {
		if d, ok := b.tagDefs[td.TagID]; ok && d.NamespaceID == id {
			delete(b.tagDefaults, tid)
		}
	}

	for _, d := range b.tagDefs {
		if d.NamespaceID == id && d.LifecycleState != stateDeleted {
			d.LifecycleState, d.Internal.TransitionAt, d.IsCostTrack = stateDeleting, &finish, false
		}
	}

	n.LifecycleState, n.Internal.TransitionAt, n.Internal.Etag = stateDeleting, &finish, newEtag()

	wr := b.newWR(wrDeleteNonEmptyNS, n.CompartmentID, "tagnamespace", id, "DELETED", finish)
	b.retry.Remember(scope, token, wr, b.nowUTC())

	return wr, nil
}

func isTaggingOp(op string) bool {
	return slices.Contains([]string{wrDeleteTagDef, wrDeleteNonEmptyNS}, op)
}

// GetTaggingWorkRequest returns one tagging work request.
func (b *InMemoryBackend) GetTaggingWorkRequest(id string) (WorkRequestWire, error) {
	b.mu.RLock("GetTaggingWorkRequest")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok || !isTaggingOp(wr.Operation) {
		return WorkRequestWire{}, errNotFound(id)
	}

	return b.wrView(wr), nil
}

// ListTaggingWorkRequests lists the tagging work requests of a compartment, optionally for one resource.
func (b *InMemoryBackend) ListTaggingWorkRequests(
	compartment, resourceID string,
	o ListOpts,
) (page.Page[WorkRequestWire], error) {
	if compartment == "" {
		return page.Page[WorkRequestWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListTaggingWorkRequests")
	defer b.mu.RUnlock()

	var all []*WorkRequest

	for _, wr := range b.workReqs {
		if isTaggingOp(wr.Operation) && (wr.CompartmentID == compartment || compartment == TenancyID) &&
			(resourceID == "" || wr.EntityID == resourceID) {
			all = append(all, wr)
		}
	}

	pg := paginate(all, o, func(w *WorkRequest) string { return w.ID },
		func(w *WorkRequest) (time.Time, int64) { return w.Accepted, w.Seq })

	out := make([]WorkRequestWire, 0, len(pg.Data))
	for _, wr := range pg.Data {
		out = append(out, b.wrView(wr))
	}

	return page.Page[WorkRequestWire]{Data: out, Next: pg.Next}, nil
}

// TaggingWRLogWire is the tagging work request log wire shape.
type TaggingWRLogWire struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

// TaggingWorkRequestLogs lists the log lines of a tagging work request.
func (b *InMemoryBackend) TaggingWorkRequestLogs(id string, o ListOpts) (page.Page[TaggingWRLogWire], error) {
	b.mu.RLock("TaggingWorkRequestLogs")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok || !isTaggingOp(wr.Operation) {
		return page.Page[TaggingWRLogWire]{}, errNotFound(id)
	}

	logs := []TaggingWRLogWire{{Timestamp: wr.Accepted, Message: wr.Operation + " work request accepted"}}
	if !wr.FinishAt.After(b.nowUTC()) {
		logs = append(logs, TaggingWRLogWire{Timestamp: wr.FinishAt, Message: wr.Operation + " work request succeeded"})
	}

	return page.New(logs, o.Page, o.Limit, defaultListLimit), nil
}

func (h *Handler) changeTagNamespaceCompartment(q *req) error {
	var in changeTagNSCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	err := h.Backend.ChangeTagNamespaceCompartment(q.param("tagNamespaceId"), in.CompartmentID,
		q.r.Header.Get(headerRetry))
	if err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}

func (h *Handler) cascadeDeleteTagNamespace(q *req) error {
	wr, err := h.Backend.CascadeDeleteTagNamespace(q.param("tagNamespaceId"), q.ifMatch(), q.r.Header.Get(headerRetry))

	return accepted(q, wr, err)
}

func (h *Handler) getTaggingWorkRequest(q *req) error {
	v, err := h.Backend.GetTaggingWorkRequest(q.param("workRequestId"))

	return reply(q, v, "", err)
}

func (h *Handler) listTaggingWorkRequests(q *req) error {
	pg, err := h.Backend.ListTaggingWorkRequests(q.query("compartmentId"), q.query("resourceIdentifier"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) listTaggingWorkRequestErrors(q *req) error {
	if _, err := h.Backend.GetTaggingWorkRequest(q.param("workRequestId")); err != nil {
		return err
	}

	return q.json(http.StatusOK, []struct{}{}, "")
}

func (h *Handler) listTaggingWorkRequestLogs(q *req) error {
	pg, err := h.Backend.TaggingWorkRequestLogs(q.param("workRequestId"), q.listOpts())

	return listReply(q, pg, err)
}
