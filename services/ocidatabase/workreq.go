package ocidatabase

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	percentMid    = 50
	percentDone   = 100
	percentMax    = 99
	percentFactor = 100
)

type wrResourceWire struct {
	EntityType string `json:"entityType"`
	Action     string `json:"actionType"`
	Identifier string `json:"identifier"`
	EntityURI  string `json:"entityUri"`
}

// WorkRequestWire is the wire form of a work request.
type WorkRequestWire struct {
	TimeAccepted time.Time        `json:"timeAccepted"`
	TimeStarted  time.Time        `json:"timeStarted"`
	TimeFinished *time.Time       `json:"timeFinished,omitempty"`
	Type         string           `json:"operationType"`
	Status       string           `json:"status"`
	ID           string           `json:"id"`
	Compartment  string           `json:"compartmentId"`
	Resources    []wrResourceWire `json:"resources"`
	Percent      float32          `json:"percentComplete"`
}

// WrErrorWire is the work request error wire shape.
type WrErrorWire struct {
	Timestamp time.Time `json:"timestamp"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
}

// WrLogWire is the work request log wire shape.
type WrLogWire struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

// newWR records a work request. Callers hold the write lock.
func (b *InMemoryBackend) newWR(compartment, resourceID, resourceType, typ, action string) *WorkRequest {
	wr := &WorkRequest{
		ID: newOCID("workrequest"), Type: typ, Compartment: compartment, ResourceID: resourceID,
		ResourceType: resourceType, Action: action, Accepted: b.nowUTC(), Status: wrInProgress, Seq: b.nextSeq(),
	}
	b.workReqs[wr.ID] = wr

	return wr
}

// finishNow marks wr succeeded at the current time.
func (b *InMemoryBackend) finishNow(wr *WorkRequest) {
	now := b.nowUTC()
	wr.Status, wr.FinishedAt = wrSucceeded, &now
}

func (b *InMemoryBackend) failWR(wr *WorkRequest, code, msg string) {
	now := b.nowUTC()
	wr.Status, wr.FinishedAt, wr.ErrCode, wr.ErrMsg = wrFailed, &now, code, msg
}

func (b *InMemoryBackend) wrView(wr *WorkRequest) WorkRequestWire {
	uri := apiBase + "/autonomousDatabases/" + wr.ResourceID
	if wr.ResourceType == "autonomousdatabasebackup" {
		uri = apiBase + "/autonomousDatabaseBackups/" + wr.ResourceID
	}

	pct := float32(percentMid)

	switch {
	case wr.FinishedAt != nil:
		pct = percentDone
	case wr.FinishAt != nil:
		if total := wr.FinishAt.Sub(wr.Accepted); total > 0 {
			pct = min(percentMax, float32(b.nowUTC().Sub(wr.Accepted))*percentFactor/float32(total))
		}
	}

	return WorkRequestWire{
		ID: wr.ID, Type: wr.Type, Status: wr.Status, Compartment: wr.Compartment, Percent: max(pct, 0),
		TimeAccepted: wr.Accepted, TimeStarted: wr.Accepted, TimeFinished: wr.FinishedAt,
		Resources: []wrResourceWire{{
			EntityType: wr.ResourceType, Action: wr.Action, Identifier: wr.ResourceID, EntityURI: uri,
		}},
	}
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

// WorkRequestFilter selects work requests in a list.
type WorkRequestFilter struct {
	Compartment string
	ResourceID  string
}

// ListWorkRequests lists the work requests of a compartment, newest first.
func (b *InMemoryBackend) ListWorkRequests(f WorkRequestFilter, o ListOpts) (page.Page[WorkRequestWire], error) {
	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(f.Compartment); aerr != nil {
		return page.Page[WorkRequestWire]{}, aerr
	}

	var all []*WorkRequest

	for _, wr := range b.workReqs {
		if wr.Compartment == f.Compartment && (f.ResourceID == "" || wr.ResourceID == f.ResourceID) {
			all = append(all, wr)
		}
	}

	pg := paginate(
		all,
		ListOpts{Page: o.Page, Limit: o.Limit, Order: o.Order},
		func(w *WorkRequest) string { return w.ID },
		func(w *WorkRequest) (time.Time, int64) { return w.Accepted, w.Seq },
	)

	out := make([]WorkRequestWire, 0, len(pg.Data))
	for _, wr := range pg.Data {
		out = append(out, b.wrView(wr))
	}

	return page.Page[WorkRequestWire]{Data: out, Next: pg.Next}, nil
}

// WorkRequestErrors lists the errors of a failed work request.
func (b *InMemoryBackend) WorkRequestErrors(id string, o ListOpts) (page.Page[WrErrorWire], error) {
	b.mu.RLock("WorkRequestErrors")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return page.Page[WrErrorWire]{}, errNotFound(id)
	}

	all := []WrErrorWire{}
	if wr.Status == wrFailed && wr.FinishedAt != nil {
		all = append(all, WrErrorWire{Timestamp: *wr.FinishedAt, Code: wr.ErrCode, Message: wr.ErrMsg})
	}

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}

// WorkRequestLogs lists the progress log of a work request, oldest first.
func (b *InMemoryBackend) WorkRequestLogs(id string, o ListOpts) (page.Page[WrLogWire], error) {
	b.mu.RLock("WorkRequestLogs")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return page.Page[WrLogWire]{}, errNotFound(id)
	}

	all := []WrLogWire{
		{Timestamp: wr.Accepted, Message: wr.Type + " accepted"},
		{Timestamp: wr.Accepted, Message: wr.Type + " in progress"},
	}

	if wr.FinishedAt != nil {
		msg := wr.Type + " succeeded"
		if wr.Status == wrFailed {
			msg = wr.Type + " failed: " + wr.ErrMsg
		}

		all = append(all, WrLogWire{Timestamp: *wr.FinishedAt, Message: msg})
	}

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}
