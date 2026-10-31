package gcpcompute

import (
	"net/url"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

type machineTypeRequest struct {
	MachineType string `json:"machineType"`
}

func (h *Handler) registerInstanceOps() {
	z := scopeZone
	h.scopedList("instances", z, "compute#instanceList", "compute#instanceAggregatedList", "instances", zoneScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listInstances(p, s, l) })
	h.reg("instances", z, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getInstance(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
	h.reg("instances", z, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.Instance](rq)
		if err != nil {
			return nil, err
		}

		if tmpl := rq.q.Get("sourceInstanceTemplate"); tmpl != "" {
			merged, merr := h.Backend.instanceFromTemplate(rq.rt.project, tmpl, raw)
			if merr != nil {
				return nil, merr
			}

			in = merged
		}

		return opResult(h.Backend.insertInstance(rq.rt.project, rq.rt.loc, in))
	})

	simple := map[string]func(b *InMemoryBackend, p, z, n string) (*computeapi.Operation, error){
		verbDelete: func(b *InMemoryBackend, p, z, n string) (*computeapi.Operation, error) {
			return b.deleteInstance(p, z, n)
		},
		"start": func(b *InMemoryBackend, p, z, n string) (*computeapi.Operation, error) {
			return b.startInstance(p, z, n)
		},
		"stop": func(b *InMemoryBackend, p, z, n string) (*computeapi.Operation, error) {
			return b.stopInstance(p, z, n)
		},
		"reset": func(b *InMemoryBackend, p, z, n string) (*computeapi.Operation, error) {
			return b.resetInstance(p, z, n)
		},
	}

	for verb, fn := range simple {
		h.reg("instances", z, verb, func(h *Handler, rq *request) (any, error) {
			return opResult(fn(h.Backend, rq.rt.project, rq.rt.loc, rq.rt.name))
		})
	}

	h.registerInstanceMutations()
	h.registerInstanceStateOps()
}

func (h *Handler) registerInstanceMutations() {
	z := scopeZone
	h.reg("instances", z, "setMetadata", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Metadata](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceMetadata(rq.rt.project, rq.rt.loc, rq.rt.name, in))
	})
	h.reg("instances", z, "setLabels", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[labelsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceLabels(rq.rt.project, rq.rt.loc, rq.rt.name, *in))
	})
	h.reg("instances", z, "setTags", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Tags](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceTags(rq.rt.project, rq.rt.loc, rq.rt.name, in))
	})
	h.reg("instances", z, "setMachineType", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[machineTypeRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceMachineType(rq.rt.project, rq.rt.loc, rq.rt.name, in.MachineType))
	})
	h.reg("instances", z, "setDeletionProtection", func(h *Handler, rq *request) (any, error) {
		return opResult(
			h.Backend.setDeletionProtection(
				rq.rt.project,
				rq.rt.loc,
				rq.rt.name,
				rq.q.Get("deletionProtection") != "false",
			),
		)
	})
	h.reg("instances", z, "attachDisk", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.AttachedDisk](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.attachDisk(rq.rt.project, rq.rt.loc, rq.rt.name, in))
	})
	h.reg("instances", z, "detachDisk", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.detachDisk(rq.rt.project, rq.rt.loc, rq.rt.name, rq.q.Get("deviceName")))
	})
	h.reg("instances", z, "addAccessConfig", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.AccessConfig](rq)
		if err != nil {
			return nil, err
		}

		return opResult(
			h.Backend.addAccessConfig(rq.rt.project, rq.rt.loc, rq.rt.name, rq.q.Get("networkInterface"), in),
		)
	})
	h.reg("instances", z, "deleteAccessConfig", func(h *Handler, rq *request) (any, error) {
		return opResult(
			h.Backend.deleteAccessConfig(
				rq.rt.project,
				rq.rt.loc,
				rq.rt.name,
				rq.q.Get("networkInterface"),
				rq.q.Get("accessConfig"),
			),
		)
	})
}

func (h *Handler) registerInstanceStateOps() {
	z := scopeZone
	h.reg("instances", z, verbUpdate, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.Instance](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateInstance(rq.rt.project, rq.rt.loc, rq.rt.name, in, raw, updateOpts{
			minimal: rq.q.Get("minimalAction"), mostDisruptive: rq.q.Get("mostDisruptiveAllowedAction"),
		}))
	})
	h.reg("instances", z, "setScheduling", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Scheduling](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceScheduling(rq.rt.project, rq.rt.loc, rq.rt.name, in))
	})
	h.reg("instances", z, "setServiceAccount", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[serviceAccountRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceServiceAccount(rq.rt.project, rq.rt.loc, rq.rt.name, in))
	})
	h.reg("instances", z, "setMinCpuPlatform", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[minCPURequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceMinCPUPlatform(rq.rt.project, rq.rt.loc, rq.rt.name, in.MinCPUPlatform))
	})
	h.reg("instances", z, "updateShieldedInstanceConfig", func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.ShieldedInstanceConfig](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateInstanceShielded(rq.rt.project, rq.rt.loc, rq.rt.name, in, raw))
	})
	h.reg("instances", z, "setShieldedInstanceIntegrityPolicy", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.ShieldedInstanceIntegrityPolicy](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInstanceIntegrityPolicy(rq.rt.project, rq.rt.loc, rq.rt.name, in))
	})
	h.reg("instances", z, "suspend", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.suspendInstance(rq.rt.project, rq.rt.loc, rq.rt.name))
	})
	h.reg("instances", z, "resume", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.resumeInstance(rq.rt.project, rq.rt.loc, rq.rt.name))
	})
	h.reg("instances", z, "serialPort", func(h *Handler, rq *request) (any, error) {
		port, start, err := serialArgs(rq.q)
		if err != nil {
			return nil, err
		}

		return h.Backend.serialPortOutput(rq.rt.project, rq.rt.loc, rq.rt.name, port, start)
	})
	h.reg("instances", z, "getGuestAttributes", func(h *Handler, rq *request) (any, error) {
		return h.Backend.guestAttributes(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
}

func serialArgs(q url.Values) (int64, int64, *APIError) {
	port, start := int64(1), int64(0)

	for name, dst := range map[string]*int64{"port": &port, "start": &start} {
		v := q.Get(name)
		if v == "" {
			continue
		}

		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, 0, errField(name, v, "")
		}

		*dst = n
	}

	return port, start, nil
}
