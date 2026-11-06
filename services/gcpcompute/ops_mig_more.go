package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

func (h *Handler) registerMIGStateOps(scope string) {
	for _, verb := range []string{"updatePerInstanceConfigs", "patchPerInstanceConfigs"} {
		h.reg(collMIG, scope, verb, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[perInstanceConfigsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.upsertPerInstanceConfigs(rq.rt.project, scope, rq.rt.loc, rq.rt.name, verb, in))
		})
	}

	h.reg(collMIG, scope, "deletePerInstanceConfigs", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[namesRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.deletePerInstanceConfigs(rq.rt.project, scope, rq.rt.loc, rq.rt.name, in))
	})
	h.reg(collMIG, scope, "listPerInstanceConfigs", func(h *Handler, rq *request) (any, error) {
		entries, err := h.Backend.listPerInstanceConfigs(rq.rt.project, scope, rq.rt.loc, rq.rt.name)
		if err != nil {
			return nil, err
		}

		return listBody(rq, "compute#instanceGroupManagersListPerInstanceConfigsResp", entries)
	})
	h.reg(collMIG, scope, "createInstances", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[createInstancesRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.createMIGInstances(rq.rt.project, scope, rq.rt.loc, rq.rt.name, in))
	})

	for verb := range powerSpecs() {
		h.reg(collMIG, scope, verb, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[migInstancesRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.powerMIG(rq.rt.project, scope, rq.rt.loc, rq.rt.name, verb, *in))
		})
	}

	h.registerResizeOps(scope)
}

func (h *Handler) registerResizeOps(scope string) {
	id := func(rq *request) string {
		return relPath(rq.rt.project, scope, rq.rt.loc, collMIG, rq.rt.parent) + "/resizeRequests"
	}

	h.reg(collMIGResize, scope, verbList, func(h *Handler, rq *request) (any, error) {
		entries, err := h.Backend.listResizeRequests(rq.rt.project, scope, rq.rt.loc, rq.rt.parent)
		if err != nil {
			return nil, err
		}

		return listBodyAt(rq, "compute#instanceGroupManagerResizeRequestsListResponse", id(rq), entries)
	})
	h.reg(collMIGResize, scope, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getResizeRequest(rq.rt.project, scope, rq.rt.loc, rq.rt.parent, rq.rt.name)
	})
	h.reg(collMIGResize, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.InstanceGroupManagerResizeRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertResizeRequest(rq.rt.project, scope, rq.rt.loc, rq.rt.parent, in))
	})
	h.reg(collMIGResize, scope, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteResizeRequest(rq.rt.project, scope, rq.rt.loc, rq.rt.parent, rq.rt.name))
	})
	h.reg(collMIGResize, scope, "cancel", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.cancelResizeRequest(rq.rt.project, scope, rq.rt.loc, rq.rt.parent, rq.rt.name))
	})
}
