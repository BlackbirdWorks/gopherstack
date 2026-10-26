package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

func (h *Handler) registerAddressOps() {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listAddresses(p, s, l) }

	h.scopedList("addresses", scopeGlobal, "compute#addressList", "", "", nil, list)
	h.scopedList("addresses", scopeRegion, "compute#addressList", "compute#addressAggregatedList", "addresses",
		append(regionScopes(), scopeGlobal), list)

	for _, scope := range []string{scopeGlobal, scopeRegion} {
		h.reg("addresses", scope, verbGet, func(h *Handler, rq *request) (any, error) {
			return h.Backend.getAddress(rq.rt.project, rq.rt.scope, rq.rt.loc, rq.rt.name)
		})
		h.reg("addresses", scope, verbInsert, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.Address](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.insertAddress(rq.rt.project, rq.rt.loc, in))
		})
		h.reg("addresses", scope, verbDelete, func(h *Handler, rq *request) (any, error) {
			return opResult(h.Backend.deleteAddress(rq.rt.project, rq.rt.scope, rq.rt.loc, rq.rt.name))
		})
		h.reg("addresses", scope, "setLabels", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[labelsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.setAddressLabels(rq.rt.project, rq.rt.scope, rq.rt.loc, rq.rt.name, *in))
		})
	}

	h.registerRouterOps()
}

func (h *Handler) registerRouterOps() {
	r := scopeRegion
	h.scopedList("routers", r, "compute#routerList", "compute#routerAggregatedList", "routers", regionScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listRouters(p, s, l) })
	h.reg("routers", r, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getRouter(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
	h.reg("routers", r, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Router](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertRouter(rq.rt.project, rq.rt.loc, in))
	})
	h.reg("routers", r, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteRouter(rq.rt.project, rq.rt.loc, rq.rt.name))
	})
	h.reg("routers", r, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.Router](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateRouter(rq.rt.project, rq.rt.loc, rq.rt.name, "patch",
			func(cur *computeapi.Router) (*computeapi.Router, *APIError) { return mergeInto(cur, raw) }))
	})
	h.reg("routers", r, verbUpdate, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Router](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateRouter(rq.rt.project, rq.rt.loc, rq.rt.name, "update",
			func(*computeapi.Router) (*computeapi.Router, *APIError) { return in, nil }))
	})
	h.reg("routers", r, "getRouterStatus", func(h *Handler, rq *request) (any, error) {
		return h.Backend.routerStatus(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
}
