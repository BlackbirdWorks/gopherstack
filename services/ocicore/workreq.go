package ocicore

import (
	"cmp"
	"net/http"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	kindWorkRequest = "coreservicesworkrequest"

	wrAccepted   = "ACCEPTED"
	wrInProgress = "IN_PROGRESS"
	wrSucceeded  = "SUCCEEDED"

	wrActionUpdated = "UPDATED"
	wrActionCreated = "CREATED"
	wrActionRelated = "RELATED"

	percentDone = 100
	percentHalf = 50
)

// WorkResource is an entity a work request touches.
type WorkResource struct {
	ActionType string `json:"actionType"`
	EntityType string `json:"entityType"`
	Identifier string `json:"identifier"`
	EntityURI  string `json:"entityUri,omitempty"`
}

// WorkRequest is an asynchronous Core Services operation; it runs on the injectable clock.
type WorkRequest struct {
	TimeAccepted  time.Time      `json:"timeAccepted"`
	FinishAt      time.Time      `json:"finishAt"`
	ID            string         `json:"id"`
	OperationType string         `json:"operationType"`
	CompartmentID string         `json:"compartmentId"`
	Resources     []WorkResource `json:"resources"`
	Seq           int64          `json:"seq"`
}

type workRequestWire struct {
	TimeAccepted    time.Time      `json:"timeAccepted"`
	TimeStarted     *time.Time     `json:"timeStarted,omitempty"`
	TimeFinished    *time.Time     `json:"timeFinished,omitempty"`
	OperationType   string         `json:"operationType"`
	Status          string         `json:"status"`
	ID              string         `json:"id"`
	CompartmentID   string         `json:"compartmentId"`
	Resources       []WorkResource `json:"resources"`
	PercentComplete float64        `json:"percentComplete"`
}

type workRequestSummaryWire struct {
	TimeAccepted    time.Time  `json:"timeAccepted"`
	TimeStarted     *time.Time `json:"timeStarted,omitempty"`
	TimeFinished    *time.Time `json:"timeFinished,omitempty"`
	OperationType   string     `json:"operationType"`
	Status          string     `json:"status"`
	ID              string     `json:"id"`
	CompartmentID   string     `json:"compartmentId"`
	PercentComplete float64    `json:"percentComplete"`
}

// WorkRequestEntry is one log line of a work request.
type WorkRequestEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

type workRequestError struct {
	Timestamp time.Time `json:"timestamp"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
}

// newWorkRequest records an asynchronous operation; callers hold the write lock.
func (b *InMemoryBackend) newWorkRequest(op, compartment string, res []WorkResource) string {
	now := b.nowUTC()
	w := &WorkRequest{
		ID: newOCID(kindWorkRequest), OperationType: op, CompartmentID: compartment, Resources: res,
		TimeAccepted: now, FinishAt: now.Add(transitionWindow), Seq: b.nextSeq(),
	}
	b.workReqs[w.ID] = w

	return w.ID
}

func (b *InMemoryBackend) wrState(w *WorkRequest) (string, float64) {
	now := b.nowUTC()

	switch {
	case !now.Before(w.FinishAt):
		return wrSucceeded, percentDone
	case now.After(w.TimeAccepted):
		return wrInProgress, percentHalf
	}

	return wrAccepted, 0
}

func (b *InMemoryBackend) wrTimes(w *WorkRequest, status string) (*time.Time, *time.Time) {
	if status == wrAccepted {
		return nil, nil
	}

	started := w.TimeAccepted
	if status != wrSucceeded {
		return &started, nil
	}

	fin := w.FinishAt

	return &started, &fin
}

func (b *InMemoryBackend) wrWire(w *WorkRequest) workRequestWire {
	status, pct := b.wrState(w)
	started, finished := b.wrTimes(w, status)

	res := slices.Clone(w.Resources)
	if status != wrSucceeded {
		for i := range res {
			if res[i].ActionType == wrActionUpdated || res[i].ActionType == wrActionCreated {
				res[i].ActionType = wrInProgress
			}
		}
	}

	return workRequestWire{
		TimeAccepted: w.TimeAccepted, TimeStarted: started, TimeFinished: finished, OperationType: w.OperationType,
		Status: status, ID: w.ID, CompartmentID: w.CompartmentID, Resources: res, PercentComplete: pct,
	}
}

func (b *InMemoryBackend) findWorkRequest(id string) (*WorkRequest, *APIError) {
	w, ok := b.workReqs[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return w, nil
}

// GetWorkRequest returns one work request.
func (b *InMemoryBackend) GetWorkRequest(id string) (workRequestWire, *APIError) {
	b.mu.RLock("GetWorkRequest")
	defer b.mu.RUnlock()

	w, aerr := b.findWorkRequest(id)
	if aerr != nil {
		return workRequestWire{}, aerr
	}

	return b.wrWire(w), nil
}

// ListWorkRequests lists the work requests of a compartment, newest first, optionally for one resource.
func (b *InMemoryBackend) ListWorkRequests(
	compartment, resourceID, token string, limit int,
) (page.Page[workRequestSummaryWire], *APIError) {
	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return page.Page[workRequestSummaryWire]{}, aerr
	}

	var all []*WorkRequest

	for _, w := range b.workReqs {
		if w.CompartmentID != compartment {
			continue
		}

		if resourceID == "" ||
			slices.ContainsFunc(w.Resources, func(r WorkResource) bool { return r.Identifier == resourceID }) {
			all = append(all, w)
		}
	}

	slices.SortFunc(all, func(a, c *WorkRequest) int {
		return cmp.Or(c.TimeAccepted.Compare(a.TimeAccepted), cmp.Compare(c.Seq, a.Seq))
	})

	pg := page.New(all, token, limit, defaultListLimit)
	out := make([]workRequestSummaryWire, 0, len(pg.Data))

	for _, w := range pg.Data {
		v := b.wrWire(w)
		out = append(out, workRequestSummaryWire{
			TimeAccepted: v.TimeAccepted, TimeStarted: v.TimeStarted, TimeFinished: v.TimeFinished,
			OperationType: v.OperationType, Status: v.Status, ID: v.ID, CompartmentID: v.CompartmentID,
			PercentComplete: v.PercentComplete,
		})
	}

	return page.Page[workRequestSummaryWire]{Data: out, Next: pg.Next}, nil
}

// ListWorkRequestLogs returns the log lines of a work request.
func (b *InMemoryBackend) ListWorkRequestLogs(
	id, order, token string, limit int,
) (page.Page[WorkRequestEntry], *APIError) {
	b.mu.RLock("ListWorkRequestLogs")
	defer b.mu.RUnlock()

	w, aerr := b.findWorkRequest(id)
	if aerr != nil {
		return page.Page[WorkRequestEntry]{}, aerr
	}

	entries := []WorkRequestEntry{{Timestamp: w.TimeAccepted, Message: w.OperationType + " accepted"}}
	if status, _ := b.wrState(w); status == wrSucceeded {
		entries = append(entries, WorkRequestEntry{Timestamp: w.FinishAt, Message: w.OperationType + " succeeded"})
	}

	if order == "DESC" {
		slices.Reverse(entries)
	}

	return page.New(entries, token, limit, defaultListLimit), nil
}

// ListWorkRequestErrors returns the errors of a work request; these operations never fail.
func (b *InMemoryBackend) ListWorkRequestErrors(id string) ([]workRequestError, *APIError) {
	b.mu.RLock("ListWorkRequestErrors")
	defer b.mu.RUnlock()

	if _, aerr := b.findWorkRequest(id); aerr != nil {
		return nil, aerr
	}

	return []workRequestError{}, nil
}

func (b *InMemoryBackend) sweepWorkRequests(now time.Time) {
	for id, w := range b.workReqs {
		if now.Sub(w.FinishAt) > retention {
			delete(b.workReqs, id)
		}
	}
}

func (q *req) acceptWorkRequest(id string) error {
	q.header("opc-work-request-id", id)

	return q.json(http.StatusOK, struct{}{}, "")
}

func workRequestEntries() []entry {
	wr := idPath("/workRequests", "workRequestId")

	return []entry{
		{(*Handler).getWorkRequest, http.MethodGet, wr, "GetWorkRequest"},
		{(*Handler).listWorkRequests, http.MethodGet, "/workRequests", "ListWorkRequests"},
		{(*Handler).listWorkRequestErrors, http.MethodGet, wr + "/errors", "ListWorkRequestErrors"},
		{(*Handler).listWorkRequestLogs, http.MethodGet, wr + "/logs", "ListWorkRequestLogs"},
	}
}

func (h *Handler) getWorkRequest(q *req) error {
	w, aerr := h.Backend.GetWorkRequest(q.param("workRequestId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, w, "")
}

func (h *Handler) listWorkRequests(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListWorkRequests(o.Compartment, q.query("resourceId"), o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) listWorkRequestErrors(q *req) error {
	errs, aerr := h.Backend.ListWorkRequestErrors(q.param("workRequestId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, errs, "")
}

func (h *Handler) listWorkRequestLogs(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListWorkRequestLogs(q.param("workRequestId"), q.query("sortOrder"), o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}
