package ociidentity

import (
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const halfComplete = 50

func itoa(n int) string { return strconv.Itoa(n) }

// newWR records a work request that completes at finish; callers hold the write lock.
func (b *InMemoryBackend) newWR(op, compartment, entityType, id, action string, finish time.Time) string {
	wr := &WorkRequest{
		ID: newOCID("workrequest"), Operation: op, CompartmentID: compartment, EntityType: entityType,
		EntityID: id, Action: action, Accepted: b.nowUTC(), FinishAt: finish, Seq: b.nextSeq(),
	}
	b.workReqs[wr.ID] = wr

	return wr.ID
}

func (b *InMemoryBackend) wrView(wr *WorkRequest) workRequestWire {
	out := workRequestWire{
		OperationType: wr.Operation, ID: wr.ID, CompartmentID: wr.CompartmentID, TimeAccepted: wr.Accepted,
		Status: "IN_PROGRESS", PercentComplete: halfComplete, TimeStarted: &wr.Accepted, Errors: []string{},
		Resources: []wrResourceWire{{EntityType: wr.EntityType, ActionType: "IN_PROGRESS", Identifier: wr.EntityID}},
	}

	if !wr.FinishAt.After(b.nowUTC()) {
		out.Status, out.PercentComplete, out.TimeFinished = "SUCCEEDED", 100, &wr.FinishAt
		out.Resources[0].ActionType = wr.Action
	}

	return out
}

// GetWorkRequest returns one work request.
func (b *InMemoryBackend) GetWorkRequest(id string) (workRequestWire, error) {
	b.mu.RLock("GetWorkRequest")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return workRequestWire{}, errNotFound(id)
	}

	return b.wrView(wr), nil
}

// ListWorkRequests lists the work requests of a compartment, optionally for one resource.
func (b *InMemoryBackend) ListWorkRequests(compartment, resourceID string, o ListOpts) (page.Page[workRequestWire], error) {
	if compartment == "" {
		return page.Page[workRequestWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	var all []*WorkRequest

	for _, wr := range b.workReqs {
		if (wr.CompartmentID == compartment || compartment == TenancyID) && (resourceID == "" || wr.EntityID == resourceID) {
			all = append(all, wr)
		}
	}

	pg := paginate(all, o, func(w *WorkRequest) string { return w.ID },
		func(w *WorkRequest) (time.Time, int64) { return w.Accepted, w.Seq })

	out := make([]workRequestWire, 0, len(pg.Data))
	for _, wr := range pg.Data {
		out = append(out, b.wrView(wr))
	}

	return page.Page[workRequestWire]{Data: out, Next: pg.Next}, nil
}

func (h *Handler) getWorkRequest(q *req) error {
	v, err := h.Backend.GetWorkRequest(q.param("workRequestId"))

	return reply(q, v, "", err)
}

func (h *Handler) listWorkRequests(q *req) error {
	pg, err := h.Backend.ListWorkRequests(q.query("compartmentId"), q.query("resourceIdentifier"), q.listOpts())

	return listReply(q, pg, err)
}
