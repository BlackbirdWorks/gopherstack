package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

func (h *Handler) registerCatalogOps() {
	h.reg("projects", "", verbGet, func(_ *Handler, rq *request) (any, error) {
		return h.Backend.getProject(rq.rt.name), nil
	})
	h.reg("projects", "", "setCommonInstanceMetadata", func(_ *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Metadata](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setCommonInstanceMetadata(rq.rt.name, in))
	})

	h.scopedList("zones", "", "compute#zoneList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listZones(p) })
	h.reg(
		"zones",
		"",
		verbGet,
		func(_ *Handler, rq *request) (any, error) { return getZone(rq.rt.project, rq.rt.name) },
	)
	h.scopedList("regions", "", "compute#regionList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listRegions(p) })
	h.reg(
		"regions",
		"",
		verbGet,
		func(_ *Handler, rq *request) (any, error) { return getRegion(rq.rt.project, rq.rt.name) },
	)

	h.scopedList(
		"machineTypes",
		scopeZone,
		"compute#machineTypeList",
		"compute#machineTypeAggregatedList",
		"machineTypes",
		zoneScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listMachineTypes(p, s, l) },
	)
	h.reg("machineTypes", scopeZone, verbGet, func(_ *Handler, rq *request) (any, error) {
		return getMachineType(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
	h.scopedList(
		"diskTypes",
		scopeZone,
		"compute#diskTypeList",
		"compute#diskTypeAggregatedList",
		"diskTypes",
		zoneScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listDiskTypes(p, s, l) },
	)
	h.reg("diskTypes", scopeZone, verbGet, func(_ *Handler, rq *request) (any, error) {
		return getDiskType(rq.rt.project, rq.rt.loc, rq.rt.name)
	})

	h.registerOperationOps()
}

func (h *Handler) registerOperationOps() {
	all := append(append(zoneScopes(), regionScopes()...), scopeGlobal)
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listOperations(p, s, l) }

	for _, scope := range []string{scopeGlobal, scopeRegion, scopeZone} {
		if scope == scopeGlobal {
			h.scopedList(
				"operations",
				scope,
				"compute#operationList",
				"compute#operationAggregatedList",
				"operations",
				all,
				list,
			)
		} else {
			h.scopedList("operations", scope, "compute#operationList", "", "", nil, list)
		}

		h.reg("operations", scope, verbGet, func(_ *Handler, rq *request) (any, error) {
			return h.Backend.getOperation(rq.rt.project, rq.rt.scope, rq.rt.loc, rq.rt.name)
		})
		h.reg("operations", scope, verbDelete, func(_ *Handler, rq *request) (any, error) {
			if err := h.Backend.deleteOperation(rq.rt.project, rq.rt.scope, rq.rt.loc, rq.rt.name); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		})
		h.reg("operations", scope, "wait", func(_ *Handler, rq *request) (any, error) {
			return h.Backend.waitOperation(rq.ctx, rq.rt.project, rq.rt.scope, rq.rt.loc, rq.rt.name)
		})
	}
}
