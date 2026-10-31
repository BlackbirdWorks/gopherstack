package ocicontainerinstances

import (
	"net/http"
	"strings"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreate: (*Handler).createInstance, opGet: (*Handler).getInstance, opList: (*Handler).listInstances,
		opUpdate: (*Handler).updateInstance, opDelete: act(func(b *InMemoryBackend, id, m string) (string, error) {
			return b.DeleteInstance(id, m)
		}),
		opStart:      act((*InMemoryBackend).Start),
		opStop:       act((*InMemoryBackend).Stop),
		opRestart:    act((*InMemoryBackend).Restart),
		opChangeComp: (*Handler).changeCompartment, opListShapes: (*Handler).listShapes,
		opGetContainer: (*Handler).getContainer, opListContainer: (*Handler).listContainers,
		opUpdContainer: (*Handler).updateContainer, opRetrieveLogs: (*Handler).retrieveLogs,
		opGetWR: (*Handler).getWorkRequest, opListWR: (*Handler).listWorkRequests,
		opListWRErrors: (*Handler).listWRErrors, opListWRLogs: (*Handler).listWRLogs,
	}
}

func act(fn func(b *InMemoryBackend, id, ifMatch string) (string, error)) opFunc {
	return func(h *Handler, q *req) error {
		wr, err := fn(h.Backend, q.param("containerInstanceId"), q.ifMatch())
		if err != nil {
			return err
		}

		return q.accepted(wr)
	}
}

func (h *Handler) createInstance(q *req) error {
	var in createIn
	if err := q.decode(&in); err != nil {
		return err
	}

	view, wr, etag, err := h.Backend.CreateInstance(in, q.r.Header.Get("Opc-Retry-Token"))
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusAccepted, view, etag)
}

func (h *Handler) getInstance(q *req) error {
	w, etag, err := h.Backend.GetInstance(q.param("containerInstanceId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func (h *Handler) listInstances(q *req) error {
	pg, err := h.Backend.ListInstances(InstanceFilter{
		Compartment: q.query("compartmentId"), State: q.query("lifecycleState"),
		DisplayName: q.query("displayName"), AD: q.query("availabilityDomain"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) updateInstance(q *req) error {
	var in updateIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateInstance(q.param("containerInstanceId"), q.ifMatch(), in)
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func (h *Handler) changeCompartment(q *req) error {
	var in compartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeCompartment(q.param("containerInstanceId"), q.ifMatch(), in.Compartment)
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func (h *Handler) listShapes(q *req) error {
	pg, err := h.Backend.ListShapes(q.query("compartmentId"), q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) getContainer(q *req) error {
	w, err := h.Backend.GetContainer(q.param("containerId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, "")
}

func (h *Handler) listContainers(q *req) error {
	pg, err := h.Backend.ListContainers(ContainerFilter{
		Compartment: q.query("compartmentId"), State: q.query("lifecycleState"), DisplayName: q.query("displayName"),
		InstanceID: q.query("containerInstanceId"), AD: q.query("availabilityDomain"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) updateContainer(q *req) error {
	var in updateIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateContainer(q.param("containerId"), q.ifMatch(), in)
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func (h *Handler) retrieveLogs(q *req) error {
	out, err := h.Backend.RetrieveLogs(q.param("containerId"), strings.EqualFold(q.query("isPrevious"), "true"))
	if err != nil {
		return err
	}

	q.header("Content-Type", "application/octet-stream")
	q.w.WriteHeader(http.StatusOK)
	_, _ = q.w.Write([]byte(out))

	return nil
}

func (h *Handler) getWorkRequest(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.param("workRequestId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, "")
}

func (h *Handler) listWorkRequests(q *req) error {
	pg, err := h.Backend.ListWorkRequests(WorkRequestFilter{
		Compartment: q.query("compartmentId"), ID: q.query("workRequestId"), Status: q.query("status"),
		ResourceID: q.query("resourceId"), AD: q.query("availabilityDomain"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) listWRErrors(q *req) error {
	pg, err := h.Backend.WorkRequestErrors(q.param("workRequestId"), q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) listWRLogs(q *req) error {
	pg, err := h.Backend.WorkRequestLogs(q.param("workRequestId"), q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}
