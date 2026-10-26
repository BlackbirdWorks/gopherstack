package gcpcompute

import (
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

func (h *Handler) registerGroupOps() {
	h.registerTemplateOps()
	h.registerHealthCheckOps()
	h.registerMIGOps()
	h.registerInstanceGroupOps()
	h.registerAutoscalerOps()
}

func (h *Handler) registerTemplateOps() {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listTemplates(p, s, l) }

	h.scopedList(collTemplates, scopeGlobal, "compute#instanceTemplateList", "compute#instanceTemplateAggregatedList",
		collTemplates, append(regionScopes(), scopeGlobal), list)
	h.scopedList(collTemplates, scopeRegion, "compute#instanceTemplateList", "", "", nil, list)

	for _, scope := range []string{scopeGlobal, scopeRegion} {
		region := func(rq *request) string {
			if rq.rt.scope == scopeRegion {
				return rq.rt.loc
			}

			return ""
		}

		h.reg(collTemplates, scope, verbGet, func(h *Handler, rq *request) (any, error) {
			return h.Backend.getTemplate(rq.rt.project, region(rq), rq.rt.name)
		})
		h.reg(collTemplates, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.InstanceTemplate](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.insertTemplate(rq.rt.project, region(rq), in))
		})
		h.reg(collTemplates, scope, verbDelete, func(h *Handler, rq *request) (any, error) {
			return opResult(h.Backend.deleteTemplate(rq.rt.project, region(rq), rq.rt.name))
		})
	}
}

func (h *Handler) registerHealthCheckOps() {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listHealthChecks(p, s, l) }

	h.scopedList(collHealth, scopeGlobal, "compute#healthCheckList", "compute#healthChecksAggregatedList",
		collHealth, append(regionScopes(), scopeGlobal), list)
	h.scopedList(collHealth, scopeRegion, "compute#healthCheckList", "", "", nil, list)

	for _, scope := range []string{scopeGlobal, scopeRegion} {
		region := func(rq *request) string {
			if rq.rt.scope == scopeRegion {
				return rq.rt.loc
			}

			return ""
		}

		h.reg(collHealth, scope, verbGet, func(h *Handler, rq *request) (any, error) {
			return h.Backend.getHealthCheck(rq.rt.project, region(rq), rq.rt.name)
		})
		h.reg(collHealth, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.HealthCheck](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.insertHealthCheck(rq.rt.project, region(rq), in))
		})
		h.reg(collHealth, scope, verbDelete, func(h *Handler, rq *request) (any, error) {
			return opResult(h.Backend.deleteHealthCheck(rq.rt.project, region(rq), rq.rt.name))
		})
		h.reg(collHealth, scope, verbPatch, func(h *Handler, rq *request) (any, error) {
			_, raw, err := decode[computeapi.HealthCheck](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.updateHealthCheck(rq.rt.project, region(rq), rq.rt.name, "patch", raw, nil))
		})
		h.reg(collHealth, scope, verbUpdate, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.HealthCheck](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.updateHealthCheck(rq.rt.project, region(rq), rq.rt.name, "update", nil, in))
		})
	}
}

func (h *Handler) registerMIGOps() {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listMIGs(p, s, l) }
	aggScopes := append(zoneScopes(), regionScopes()...)

	h.scopedList(collMIG, scopeZone, "compute#instanceGroupManagerList", "compute#instanceGroupManagerAggregatedList",
		collMIG, aggScopes, list)
	h.scopedList(collMIG, scopeRegion, "compute#instanceGroupManagerList", "", "", nil, list)

	for _, scope := range []string{scopeZone, scopeRegion} {
		h.registerMIGCrud(scope)
		h.registerMIGActions(scope)
	}
}

func (h *Handler) registerMIGCrud(scope string) {
	h.reg(collMIG, scope, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getMIG(rq.rt.project, scope, rq.rt.loc, rq.rt.name)
	})
	h.reg(collMIG, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.InstanceGroupManager](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertMIG(rq.rt.project, scope, rq.rt.loc, in))
	})
	h.reg(collMIG, scope, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteMIG(rq.rt.project, scope, rq.rt.loc, rq.rt.name))
	})
	h.reg(collMIG, scope, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.InstanceGroupManager](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateMIG(rq.rt.project, scope, rq.rt.loc, rq.rt.name, "patch", raw, nil))
	})
	h.reg(collMIG, scope, verbUpdate, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.InstanceGroupManager](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateMIG(rq.rt.project, scope, rq.rt.loc, rq.rt.name, "update", nil, in))
	})
}

func (h *Handler) registerMIGActions(scope string) {
	h.reg(collMIG, scope, "resize", func(h *Handler, rq *request) (any, error) {
		raw := rq.q.Get("size")
		if raw == "" {
			return nil, errRequired("size")
		}

		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, errField("size", raw, "")
		}

		return opResult(h.Backend.resizeMIG(rq.rt.project, scope, rq.rt.loc, rq.rt.name, n))
	})
	h.reg(collMIG, scope, "setInstanceTemplate", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[templateRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setMIGTemplate(rq.rt.project, scope, rq.rt.loc, rq.rt.name, in.InstanceTemplate))
	})
	h.reg(collMIG, scope, "setTargetPools", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[targetPoolsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setMIGTargetPools(rq.rt.project, scope, rq.rt.loc, rq.rt.name, *in))
	})

	h.registerMIGMemberOps(scope)
}

func (h *Handler) registerMIGMemberOps(scope string) {
	for verb, fn := range map[string]migMemberOp{
		"recreateInstances": (*InMemoryBackend).recreateMIGInstances,
		"deleteInstances":   (*InMemoryBackend).deleteMIGInstances,
		"abandonInstances":  (*InMemoryBackend).abandonMIGInstances,
	} {
		h.reg(collMIG, scope, verb, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[migInstancesRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(fn(h.Backend, rq.rt.project, scope, rq.rt.loc, rq.rt.name, verb, *in))
		})
	}

	h.reg(collMIG, scope, "listManagedInstances", func(h *Handler, rq *request) (any, error) {
		entries, err := h.Backend.listManagedInstances(rq.rt.project, scope, rq.rt.loc, rq.rt.name)
		if err != nil {
			return nil, err
		}

		lq, perr := parseListQuery(rq.q)
		if perr != nil {
			return nil, perr
		}

		pg := applyList(entries, lq)
		out := &computeapi.InstanceGroupManagersListManagedInstancesResponse{NextPageToken: pg.Next}

		for _, e := range pg.Data {
			if mi, ok := e.item.(*computeapi.ManagedInstance); ok {
				out.ManagedInstances = append(out.ManagedInstances, mi)
			}
		}

		return out, nil
	})
}

func (h *Handler) registerInstanceGroupOps() {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listGroups(p, s, l) }

	h.scopedList(collGroups, scopeZone, "compute#instanceGroupList", "compute#instanceGroupAggregatedList",
		collGroups, append(zoneScopes(), regionScopes()...), list)
	h.scopedList(collGroups, scopeRegion, "compute#instanceGroupList", "", "", nil, list)

	for _, scope := range []string{scopeZone, scopeRegion} {
		h.reg(collGroups, scope, verbGet, func(h *Handler, rq *request) (any, error) {
			return h.Backend.getGroup(rq.rt.project, scope, rq.rt.loc, rq.rt.name)
		})
		h.reg(collGroups, scope, "setNamedPorts", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[namedPortsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.setGroupNamedPorts(rq.rt.project, scope, rq.rt.loc, rq.rt.name, *in))
		})
		h.reg(collGroups, scope, "listInstances", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[listGroupInstancesRequest](rq)
			if err != nil {
				return nil, err
			}

			entries, lerr := h.Backend.listGroupInstances(rq.rt.project, scope, rq.rt.loc, rq.rt.name, in.InstanceState)
			if lerr != nil {
				return nil, lerr
			}

			body, berr := listBody(rq, "compute#instanceGroupsListInstances", entries)
			if berr != nil {
				return nil, berr
			}

			return body, nil
		})
	}

	z := scopeZone
	h.reg(collGroups, z, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.InstanceGroup](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertGroup(rq.rt.project, rq.rt.loc, in))
	})
	h.reg(collGroups, z, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteGroup(rq.rt.project, rq.rt.loc, rq.rt.name))
	})
	h.reg(collGroups, z, "addInstances", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[groupInstancesRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.addGroupInstances(rq.rt.project, rq.rt.loc, rq.rt.name, *in))
	})
	h.reg(collGroups, z, "removeInstances", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[groupInstancesRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.removeGroupInstances(rq.rt.project, rq.rt.loc, rq.rt.name, *in))
	})
}

func (h *Handler) registerAutoscalerOps() {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return b.listScalers(p, s, l) }

	h.scopedList(collScalers, scopeZone, "compute#autoscalerList", "compute#autoscalerAggregatedList",
		collScalers, append(zoneScopes(), regionScopes()...), list)
	h.scopedList(collScalers, scopeRegion, "compute#autoscalerList", "", "", nil, list)

	name := func(rq *request) string {
		if n := rq.q.Get("autoscaler"); n != "" {
			return n
		}

		return rq.rt.name
	}

	for _, scope := range []string{scopeZone, scopeRegion} {
		h.reg(collScalers, scope, verbGet, func(h *Handler, rq *request) (any, error) {
			return h.Backend.getScaler(rq.rt.project, scope, rq.rt.loc, rq.rt.name)
		})
		h.reg(collScalers, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.Autoscaler](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.insertScaler(rq.rt.project, scope, rq.rt.loc, in))
		})
		h.reg(collScalers, scope, verbDelete, func(h *Handler, rq *request) (any, error) {
			return opResult(h.Backend.deleteScaler(rq.rt.project, scope, rq.rt.loc, rq.rt.name))
		})
		h.reg(collScalers, scope, verbPatch, func(h *Handler, rq *request) (any, error) {
			_, raw, err := decode[computeapi.Autoscaler](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.updateScaler(rq.rt.project, scope, rq.rt.loc, name(rq), "patch", raw, nil))
		})
		h.reg(collScalers, scope, verbUpdate, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.Autoscaler](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.updateScaler(rq.rt.project, scope, rq.rt.loc, name(rq), "update", nil, in))
		})
	}
}
