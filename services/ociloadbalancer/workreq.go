package ociloadbalancer

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Work request lifecycle states.
const (
	wrInProgress = "IN_PROGRESS"
	wrSucceeded  = "SUCCEEDED"
)

// WorkRequestWire is the wire form of a work request.
type WorkRequestWire struct {
	TimeAccepted   time.Time  `json:"timeAccepted"`
	TimeFinished   *time.Time `json:"timeFinished,omitempty"`
	ID             string     `json:"id"`
	LoadBalancerID string     `json:"loadBalancerId"`
	CompartmentID  string     `json:"compartmentId,omitempty"`
	Type           string     `json:"type"`
	LifecycleState string     `json:"lifecycleState"`
	Message        string     `json:"message"`
	ErrorDetails   []struct{} `json:"errorDetails"`
}

// newWR records a work request that completes at finish. Callers hold the write lock.
func (b *InMemoryBackend) newWR(lb *LoadBalancer, typ string, finish time.Time) string {
	wr := &WorkRequest{
		ID: newOCID("loadbalancerworkrequest"), LoadBalancerID: lb.ID, CompartmentID: lb.CompartmentID, Type: typ,
		Accepted: b.nowUTC(), FinishAt: finish, Seq: b.nextSeq(),
	}
	b.workReqs[wr.ID] = wr

	return wr.ID
}

func (b *InMemoryBackend) wrView(wr *WorkRequest) WorkRequestWire {
	out := WorkRequestWire{
		ID: wr.ID, LoadBalancerID: wr.LoadBalancerID, CompartmentID: wr.CompartmentID, Type: wr.Type,
		TimeAccepted: wr.Accepted, LifecycleState: wrInProgress, ErrorDetails: []struct{}{},
	}

	if !wr.FinishAt.After(b.nowUTC()) {
		out.LifecycleState, out.TimeFinished, out.Message = wrSucceeded, &wr.FinishAt, wr.Type+" succeeded"
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

// ListWorkRequests lists the work requests of one load balancer, newest first.
func (b *InMemoryBackend) ListWorkRequests(lbID string, o ListOpts) (page.Page[WorkRequestWire], error) {
	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	if _, ok := b.lbs[lbID]; !ok {
		return page.Page[WorkRequestWire]{}, errNotFound(lbID)
	}

	var all []*WorkRequest

	for _, wr := range b.workReqs {
		if wr.LoadBalancerID == lbID {
			all = append(all, wr)
		}
	}

	pg := paginate(all, ListOpts{Page: o.Page, Limit: o.Limit}, func(w *WorkRequest) string { return w.ID },
		func(w *WorkRequest) (time.Time, int64) { return w.Accepted, w.Seq })

	out := make([]WorkRequestWire, 0, len(pg.Data))
	for _, wr := range pg.Data {
		out = append(out, b.wrView(wr))
	}

	return page.Page[WorkRequestWire]{Data: out, Next: pg.Next}, nil
}
