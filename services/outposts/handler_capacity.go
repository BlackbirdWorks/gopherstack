package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleStartCapacityTask(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req startCapacityTaskRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	t, err := h.Backend.StartCapacityTask(segs[1], &req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toCapacityTaskResponse(t))
}

func (h *Handler) handleGetCapacityTask(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	t, err := h.Backend.GetCapacityTask(segs[1], segs[3])
	if err != nil {
		return nil, err
	}

	return marshalResponse(toCapacityTaskResponse(t))
}

func (h *Handler) handleCancelCapacityTask(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	if err := h.Backend.CancelCapacityTask(segs[1], segs[3]); err != nil {
		return nil, err
	}

	return nil, nil // void response, matches services/grafana precedent
}

func (h *Handler) handleListCapacityTasks(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	f := capacityTaskFilter{
		statuses:                q["CapacityTaskStatusFilter"],
		outpostIdentifierFilter: q.Get("OutpostIdentifierFilter"),
	}

	p := h.Backend.ListCapacityTasks(f, q.Get("NextToken"), queryMaxResults(q))

	resp := listCapacityTasksResponse{NextToken: p.Next, CapacityTasks: make([]capacityTaskSummaryWire, 0, len(p.Data))}
	for _, t := range p.Data {
		resp.CapacityTasks = append(resp.CapacityTasks, toCapacityTaskSummaryWire(t))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleListBlockingInstancesForCapacityTask(
	_ context.Context, r *http.Request, _ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)

	if err := h.Backend.ListBlockingInstancesForCapacityTask(segs[1], segs[3]); err != nil {
		return nil, err
	}

	return marshalResponse(listBlockingInstancesResponse{BlockingInstances: []blockingInstanceWire{}})
}
