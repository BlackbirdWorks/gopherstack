package ocimysql

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	entityDBSystem  = "dbsystem"
	entityBackup    = "backup"
	entityChannel   = "channel"
	wrStatusPending = "IN_PROGRESS"
	actionCreated   = "CREATED"
	actionUpdated   = "UPDATED"
	actionDeleted   = "DELETED"
	wrHalfway       = 50
)

// Work request operation types (SDK WorkRequestOperationTypeEnum).
const (
	wrCreateDBSystem  = "CREATE_DBSYSTEM"
	wrUpdateDBSystem  = "UPDATE_DBSYSTEM"
	wrDeleteDBSystem  = "DELETE_DBSYSTEM"
	wrStartDBSystem   = "START_DBSYSTEM"
	wrStopDBSystem    = "STOP_DBSYSTEM"
	wrRestartDBSystem = "RESTART_DBSYSTEM"
	wrCreateBackup    = "CREATE_BACKUP"
	wrDeleteBackup    = "DELETE_BACKUP"
	wrUpdateBackup    = "UPDATE_BACKUP"
	wrCreateChannel   = "CREATE_CHANNEL"
	wrUpdateChannel   = "UPDATE_CHANNEL"
	wrResumeChannel   = "RESUME_CHANNEL"
	wrResetChannel    = "RESET_CHANNEL"
	wrDeleteChannel   = "DELETE_CHANNEL"
)

type wrResourceWire struct {
	EntityType string `json:"entityType"`
	ActionType string `json:"actionType"`
	Identifier string `json:"identifier"`
	EntityURI  string `json:"entityUri,omitempty"`
}

// WorkRequestWire is the WorkRequest and WorkRequestSummary body.
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

type wrErrorWire struct {
	Timestamp time.Time `json:"timestamp"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
}

// newWR records a work request; finish is nil while completion waits on the engine.
func (b *InMemoryBackend) newWR(op, compartment, entityType, id, action string, finish *time.Time) string {
	wr := &WorkRequest{
		ID: newOCID("mysqlworkrequest"), Operation: op, CompartmentID: compartment, EntityType: entityType,
		EntityID: id, EntityURI: "/" + entityPath(entityType) + "/" + id, Action: action, Accepted: b.nowUTC(),
		FinishAt: finish, Seq: b.nextSeq(),
	}
	b.workReqs[wr.ID] = wr

	return wr.ID
}

func entityPath(entityType string) string {
	switch entityType {
	case entityBackup:
		return "backups"
	case entityChannel:
		return "channels"
	default:
		return "dbSystems"
	}
}

func (wr *WorkRequest) finished(now time.Time) bool {
	return wr.Failure != "" || (wr.FinishAt != nil && !wr.FinishAt.After(now))
}

// finishWR completes a work request at now; an empty failure means success.
func (b *InMemoryBackend) finishWR(id, failure string) {
	wr, ok := b.workReqs[id]
	if !ok {
		return
	}

	now := b.nowUTC()
	wr.FinishAt, wr.Failure = &now, failure
}

func (b *InMemoryBackend) wrView(wr *WorkRequest) WorkRequestWire {
	started := wr.Accepted
	out := WorkRequestWire{
		OperationType: wr.Operation, ID: wr.ID, CompartmentID: wr.CompartmentID, TimeAccepted: wr.Accepted,
		Status: wrStatusPending, TimeStarted: &started, PercentComplete: wrHalfway,
		Resources: []wrResourceWire{{
			EntityType: wr.EntityType, ActionType: wrStatusPending, Identifier: wr.EntityID, EntityURI: wr.EntityURI,
		}},
	}

	if !wr.finished(b.nowUTC()) {
		return out
	}

	out.TimeFinished, out.PercentComplete = wr.FinishAt, 100

	if wr.Failure != "" {
		out.Status, out.Resources[0].ActionType = "FAILED", "FAILED"

		return out
	}

	out.Status, out.Resources[0].ActionType = "SUCCEEDED", wr.Action

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

// ListWorkRequests lists the work requests of a compartment.
func (b *InMemoryBackend) ListWorkRequests(compartment string, o ListOpts) (page.Page[WorkRequestWire], error) {
	if aerr := firstErr(
		b.checkCompartmentLocked(compartment),
		validListOpts(o, "ID", "OPERATION_TYPE", "STATUS", "TIME_ACCEPTED", "TIME_STARTED", "TIME_FINISHED"),
	); aerr != nil {
		return page.Page[WorkRequestWire]{}, aerr
	}

	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	all := make([]WorkRequestWire, 0, len(b.workReqs))
	seqs := map[string]int64{}

	for _, wr := range b.workReqs {
		if wr.CompartmentID == compartment {
			all = append(all, b.wrView(wr))
			seqs[wr.ID] = wr.Seq
		}
	}

	desc := !strings.EqualFold(o.Order, "ASC")
	by := strings.ToUpper(o.SortBy)

	slices.SortStableFunc(all, func(a, c WorkRequestWire) int {
		var r int

		switch by {
		case "ID":
			r = cmp.Compare(a.ID, c.ID)
		case "OPERATION_TYPE":
			r = cmp.Compare(a.OperationType, c.OperationType)
		case "STATUS":
			r = cmp.Compare(a.Status, c.Status)
		default:
			r = a.TimeAccepted.Compare(c.TimeAccepted)
		}

		r = cmp.Or(r, cmp.Compare(seqs[a.ID], seqs[c.ID]))
		if desc {
			return -r
		}

		return r
	})

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
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

	switch {
	case wr.Failure != "":
		logs = append(logs, wrLogWire{Timestamp: *wr.FinishAt, Message: wr.Operation + " failed: " + wr.Failure})
	case wr.finished(b.nowUTC()):
		logs = append(logs, wrLogWire{Timestamp: *wr.FinishAt, Message: wr.Operation + " succeeded"})
	}

	return page.New(logs, o.Page, o.Limit, defaultListLimit), nil
}

// WorkRequestErrors returns the errors of a work request.
func (b *InMemoryBackend) WorkRequestErrors(id string, o ListOpts) (page.Page[wrErrorWire], error) {
	b.mu.RLock("WorkRequestErrors")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return page.Page[wrErrorWire]{}, errNotFound(id)
	}

	out := []wrErrorWire{}
	if wr.Failure != "" {
		out = append(out, wrErrorWire{Timestamp: *wr.FinishAt, Code: "InternalError", Message: wr.Failure})
	}

	return page.New(out, o.Page, o.Limit, defaultListLimit), nil
}

func (h *Handler) getWorkRequest(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.param("workRequestId"))

	return reply(q, w, "", err)
}

func (h *Handler) listWorkRequests(q *req) error {
	pg, err := h.Backend.ListWorkRequests(q.query(keyCompartment), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) listWorkRequestLogs(q *req) error {
	pg, err := h.Backend.WorkRequestLogs(q.param("workRequestId"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) listWorkRequestErrors(q *req) error {
	pg, err := h.Backend.WorkRequestErrors(q.param("workRequestId"), q.listOpts())

	return collectionReply(q, pg, err)
}
