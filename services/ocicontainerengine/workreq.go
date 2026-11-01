package ocicontainerengine

import (
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	entityCluster   = "cluster"
	entityNodePool  = "nodepool"
	wrStatusPending = "IN_PROGRESS"
	actionCreated   = "CREATED"
	actionUpdated   = "UPDATED"
	actionDeleted   = "DELETED"
)

// Work request operation types.
const (
	wrClusterCreate  = "CLUSTER_CREATE"
	wrClusterUpdate  = "CLUSTER_UPDATE"
	wrClusterDelete  = "CLUSTER_DELETE"
	wrNodePoolCreate = "NODEPOOL_CREATE"
	wrNodePoolUpdate = "NODEPOOL_UPDATE"
	wrNodePoolDelete = "NODEPOOL_DELETE"
)

type wrResourceWire struct {
	EntityType string `json:"entityType"`
	ActionType string `json:"actionType"`
	Identifier string `json:"identifier"`
	EntityURI  string `json:"entityUri,omitempty"`
}

// WorkRequestWire is the WorkRequest and WorkRequestSummary body.
type WorkRequestWire struct {
	TimeAccepted  time.Time        `json:"timeAccepted"`
	TimeStarted   *time.Time       `json:"timeStarted,omitempty"`
	TimeFinished  *time.Time       `json:"timeFinished,omitempty"`
	OperationType string           `json:"operationType"`
	Status        string           `json:"status"`
	ID            string           `json:"id"`
	CompartmentID string           `json:"compartmentId"`
	Resources     []wrResourceWire `json:"resources"`
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
		ID: newOCID("clustersworkrequest"), Operation: op, CompartmentID: compartment, EntityType: entityType,
		EntityID: id, EntityURI: "/" + entityPath(entityType) + "/" + id, Action: action, Accepted: b.nowUTC(),
		FinishAt: finish, Seq: b.nextSeq(),
	}
	b.workReqs[wr.ID] = wr

	return wr.ID
}

func entityPath(entityType string) string {
	if entityType == entityNodePool {
		return "nodePools"
	}

	return "clusters"
}

func (wr *WorkRequest) finished(now time.Time) bool {
	return wr.Failure != "" || (wr.FinishAt != nil && !wr.FinishAt.After(now))
}

func (b *InMemoryBackend) wrView(wr *WorkRequest) WorkRequestWire {
	started := wr.Accepted
	out := WorkRequestWire{
		OperationType: wr.Operation, ID: wr.ID, CompartmentID: wr.CompartmentID, TimeAccepted: wr.Accepted,
		Status: wrStatusPending, TimeStarted: &started,
		Resources: []wrResourceWire{{
			EntityType: wr.EntityType, ActionType: wrStatusPending, Identifier: wr.EntityID, EntityURI: wr.EntityURI,
		}},
	}

	if !wr.finished(b.nowUTC()) {
		return out
	}

	out.TimeFinished = wr.FinishAt

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

// WRListIn are the ListWorkRequests filters.
type WRListIn struct {
	Compartment  string
	ClusterID    string
	ResourceID   string
	ResourceType string
	Statuses     []string
	ListOpts
}

// ListWorkRequests lists the work requests of a compartment.
func (b *InMemoryBackend) ListWorkRequests(in WRListIn) (page.Page[WorkRequestWire], error) {
	if aerr := firstErr(
		b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, "ID", "OPERATION_TYPE", "STATUS", "TIME_ACCEPTED", "TIME_STARTED", "TIME_FINISHED"),
	); aerr != nil {
		return page.Page[WorkRequestWire]{}, aerr
	}

	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	var all []*WorkRequest

	for _, wr := range b.workReqs {
		if wr.CompartmentID != in.Compartment || (in.ResourceID != "" && wr.EntityID != in.ResourceID) ||
			(in.ResourceType != "" && !equalFoldStr(wr.EntityType, in.ResourceType)) ||
			(in.ClusterID != "" && !b.wrInCluster(wr, in.ClusterID)) {
			continue
		}

		if len(in.Statuses) > 0 && !containsFold(in.Statuses, b.wrView(wr).Status) {
			continue
		}

		all = append(all, wr)
	}

	pg := paginate(all, in.ListOpts, func(w *WorkRequest) string { return w.ID },
		func(w *WorkRequest) string { return w.Operation },
		func(w *WorkRequest) (time.Time, int64) { return w.Accepted, w.Seq })

	out := make([]WorkRequestWire, 0, len(pg.Data))
	for _, wr := range pg.Data {
		out = append(out, b.wrView(wr))
	}

	return page.Page[WorkRequestWire]{Data: out, Next: pg.Next}, nil
}

func (b *InMemoryBackend) wrInCluster(wr *WorkRequest, clusterID string) bool {
	if wr.EntityID == clusterID {
		return true
	}

	np, ok := b.pools[wr.EntityID]

	return ok && np.ClusterID == clusterID
}

func (b *InMemoryBackend) checkCompartmentLocked(id string) *APIError {
	b.mu.RLock("checkCompartment")
	defer b.mu.RUnlock()

	return b.checkCompartment(id)
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
func (b *InMemoryBackend) WorkRequestErrors(id string) ([]wrErrorWire, error) {
	b.mu.RLock("WorkRequestErrors")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok {
		return nil, errNotFound(id)
	}

	out := []wrErrorWire{}
	if wr.Failure != "" {
		out = append(out, wrErrorWire{Timestamp: *wr.FinishAt, Code: "ClusterStartFailed", Message: wr.Failure})
	}

	return out, nil
}

func (h *Handler) getWorkRequest(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.param("workRequestId"))

	return reply(q, w, "", err)
}

func (h *Handler) listWorkRequests(q *req) error {
	pg, err := h.Backend.ListWorkRequests(WRListIn{
		Compartment: q.query("compartmentId"), ClusterID: q.query("clusterId"), ResourceID: q.query("resourceId"),
		ResourceType: q.query("resourceType"), Statuses: q.queryAll("status"), ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}

func (h *Handler) listWorkRequestLogs(q *req) error {
	pg, err := h.Backend.WorkRequestLogs(q.param("workRequestId"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) listWorkRequestErrors(q *req) error {
	errs, err := h.Backend.WorkRequestErrors(q.param("workRequestId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, errs, "")
}
