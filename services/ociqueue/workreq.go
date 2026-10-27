package ociqueue

import (
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const halfComplete = 50

type wrResourceWire struct {
	EntityType string `json:"entityType"`
	ActionType string `json:"actionType"`
	Identifier string `json:"identifier"`
	EntityURI  string `json:"entityUri,omitempty"`
}

type WorkRequestWire struct {
	TimeAccepted    time.Time        `json:"timeAccepted"`
	TimeStarted     *time.Time       `json:"timeStarted,omitempty"`
	TimeFinished    *time.Time       `json:"timeFinished,omitempty"`
	OperationType   string           `json:"operationType"`
	Status          string           `json:"status"`
	ID              string           `json:"id"`
	CompartmentID   string           `json:"compartmentId"`
	Resources       []wrResourceWire `json:"resources"`
	PercentComplete float32          `json:"percentComplete"`
}

type wrLogWire struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

// newWR records a work request that completes with the resource transition.
func (b *InMemoryBackend) newWR(op, compartment, id, action, uri string, finish time.Time) string {
	wr := &WorkRequest{
		ID: newOCID("queueworkrequest"), Operation: op, CompartmentID: compartment, EntityType: "queue",
		EntityID: id, EntityURI: uri, Action: action, Accepted: b.nowUTC(), FinishAt: finish, Seq: b.nextSeq(),
	}
	b.workReqs[wr.ID] = wr

	return wr.ID
}

func (b *InMemoryBackend) wrView(wr *WorkRequest) WorkRequestWire {
	now := b.nowUTC()
	out := WorkRequestWire{
		OperationType: wr.Operation, ID: wr.ID, CompartmentID: wr.CompartmentID, TimeAccepted: wr.Accepted,
		Status: "IN_PROGRESS", PercentComplete: halfComplete, TimeStarted: &wr.Accepted,
		Resources: []wrResourceWire{{
			EntityType: wr.EntityType, ActionType: "IN_PROGRESS", Identifier: wr.EntityID, EntityURI: wr.EntityURI,
		}},
	}

	if !wr.FinishAt.After(now) {
		out.Status, out.PercentComplete, out.TimeFinished = "SUCCEEDED", 100, &wr.FinishAt
		out.Resources[0].ActionType = wr.Action
	}

	return out
}

// GetWorkRequest returns one work request.
func (b *InMemoryBackend) GetWorkRequest(id string) (WorkRequestWire, error) {
	b.mu.RLock("GetWorkRequest")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return WorkRequestWire{}, errNotFound(id)
	}

	return b.wrView(wr), nil
}

// ListWorkRequests lists the work requests of a compartment, optionally for one resource.
func (b *InMemoryBackend) ListWorkRequests(
	compartment, resourceID, id string, o ListOpts,
) (page.Page[WorkRequestWire], error) {
	if compartment == "" && id == "" {
		return page.Page[WorkRequestWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	var all []*WorkRequest

	for _, wr := range b.workReqs {
		if (id != "" && wr.ID != id) || (compartment != "" && wr.CompartmentID != compartment) ||
			(resourceID != "" && wr.EntityID != resourceID) {
			continue
		}

		all = append(all, wr)
	}

	pg := paginate(all, o, func(w *WorkRequest) string { return w.ID },
		func(w *WorkRequest) (time.Time, int64) { return w.Accepted, w.Seq })

	out := make([]WorkRequestWire, 0, len(pg.Data))
	for _, wr := range pg.Data {
		out = append(out, b.wrView(wr))
	}

	return page.Page[WorkRequestWire]{Data: out, Next: pg.Next}, nil
}

// WorkRequestLogs returns the log entries of a work request.
func (b *InMemoryBackend) WorkRequestLogs(id string, o ListOpts) (page.Page[wrLogWire], error) {
	b.mu.RLock("WorkRequestLogs")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return page.Page[wrLogWire]{}, errNotFound(id)
	}

	logs := []wrLogWire{{Timestamp: wr.Accepted, Message: wr.Operation + " accepted"}}
	if !wr.FinishAt.After(b.nowUTC()) {
		logs = append(logs, wrLogWire{Timestamp: wr.FinishAt, Message: wr.Operation + " succeeded"})
	}

	return page.New(logs, o.Page, o.Limit, defaultListLimit), nil
}

// WorkRequestExists reports whether a work request exists (its error list is always empty).
func (b *InMemoryBackend) WorkRequestExists(id string) error {
	b.mu.RLock("WorkRequestExists")
	defer b.mu.RUnlock()

	if _, ok := b.workReqs[id]; !ok {
		return errNotFound(id)
	}

	return nil
}

func (h *Handler) getWorkRequest(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.param("workRequestId"))

	return reply(q, w, "", err)
}

func (h *Handler) listWorkRequests(q *req) error {
	pg, err := h.Backend.ListWorkRequests(q.query("compartmentId"), q.query("resourceId"),
		q.query("workRequestId"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) listWorkRequestLogs(q *req) error {
	pg, err := h.Backend.WorkRequestLogs(q.param("workRequestId"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) listWorkRequestErrors(q *req) error {
	if err := h.Backend.WorkRequestExists(q.param("workRequestId")); err != nil {
		return err
	}

	return q.json(http.StatusOK, map[string]any{itemsKey: []struct{}{}}, "")
}
