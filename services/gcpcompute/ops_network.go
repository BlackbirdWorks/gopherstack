package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

type cidrRequest struct {
	IPCIDRRange string `json:"ipCidrRange"`
}

type googleAccessRequest struct {
	PrivateIPGoogleAccess bool `json:"privateIpGoogleAccess"`
}

func (h *Handler) registerNetworkOps() {
	h.registerVPCOps()
	h.registerSubnetOps()
	h.registerFirewallOps()
	h.registerRouteOps()
	h.registerPeeringOps()
}

func (h *Handler) registerVPCOps() {
	g := scopeGlobal
	h.scopedList("networks", g, "compute#networkList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listNetworks(p) })
	h.reg("networks", g, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getNetwork(rq.rt.project, rq.rt.name)
	})
	h.reg("networks", g, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.Network](rq)
		if err != nil {
			return nil, err
		}

		_, given := raw["autoCreateSubnetworks"]

		return opResult(h.Backend.insertNetwork(rq.rt.project, in, given))
	})
	h.reg("networks", g, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteNetwork(rq.rt.project, rq.rt.name))
	})
	h.reg("networks", g, verbPatch, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.Network](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.patchNetwork(rq.rt.project, rq.rt.name, in, raw))
	})
	h.reg("networks", g, "switchToCustomMode", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.switchToCustomMode(rq.rt.project, rq.rt.name))
	})
}

func (h *Handler) registerSubnetOps() {
	r := scopeRegion
	h.scopedList(
		"subnetworks",
		r,
		"compute#subnetworkList",
		"compute#subnetworkAggregatedList",
		"subnetworks",
		regionScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listSubnets(p, s, l) },
	)
	h.reg("subnetworks", r, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getSubnet(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
	h.reg("subnetworks", r, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Subnetwork](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertSubnet(rq.rt.project, rq.rt.loc, in))
	})
	h.reg("subnetworks", r, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteSubnet(rq.rt.project, rq.rt.loc, rq.rt.name))
	})
	h.reg("subnetworks", r, verbPatch, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.Subnetwork](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.patchSubnet(rq.rt.project, rq.rt.loc, rq.rt.name, in, raw))
	})
	h.reg("subnetworks", r, "expandIpCidrRange", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[cidrRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.expandSubnet(rq.rt.project, rq.rt.loc, rq.rt.name, in.IPCIDRRange))
	})
	h.reg("subnetworks", r, "setPrivateIpGoogleAccess", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[googleAccessRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(
			h.Backend.setSubnetPrivateGoogleAccess(rq.rt.project, rq.rt.loc, rq.rt.name, in.PrivateIPGoogleAccess),
		)
	})
}

func (h *Handler) registerFirewallOps() {
	g := scopeGlobal
	h.scopedList("firewalls", g, "compute#firewallList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listFirewalls(p) })
	h.reg("firewalls", g, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getFirewall(rq.rt.project, rq.rt.name)
	})
	h.reg("firewalls", g, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.Firewall](rq)
		if err != nil {
			return nil, err
		}

		forcePriority(in, raw)

		return opResult(h.Backend.insertFirewall(rq.rt.project, in))
	})
	h.reg("firewalls", g, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteFirewall(rq.rt.project, rq.rt.name))
	})
	h.reg("firewalls", g, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.Firewall](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updateFirewall(rq.rt.project, rq.rt.name, "patch",
			func(cur *computeapi.Firewall) (*computeapi.Firewall, *APIError) {
				next, merr := mergeInto(cur, raw)
				if merr == nil {
					forcePriority(next, mergedPriority(cur, raw))
				}

				return next, merr
			}))
	})
	h.reg("firewalls", g, verbUpdate, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.Firewall](rq)
		if err != nil {
			return nil, err
		}

		forcePriority(in, raw)

		return opResult(h.Backend.updateFirewall(rq.rt.project, rq.rt.name, "update",
			func(*computeapi.Firewall) (*computeapi.Firewall, *APIError) { return in, nil }))
	})
}

func forcePriority(f *computeapi.Firewall, raw map[string]any) {
	if _, ok := raw["priority"]; ok {
		f.ForceSendFields = append(f.ForceSendFields, "Priority")
	}
}

// mergedPriority reports a priority key when either the stored rule or the patch carries one.
func mergedPriority(cur *computeapi.Firewall, raw map[string]any) map[string]any {
	if _, ok := raw["priority"]; ok || cur.Priority != 0 {
		return map[string]any{"priority": true}
	}

	return nil
}

func (h *Handler) registerRouteOps() {
	g := scopeGlobal
	h.scopedList("routes", g, "compute#routeList", "", "", nil,
		func(b *InMemoryBackend, p, _, _ string) []entry { return b.listRoutes(p) })
	h.reg("routes", g, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getRoute(rq.rt.project, rq.rt.name)
	})
	h.reg("routes", g, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.Route](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.insertRoute(rq.rt.project, in))
	})
	h.reg("routes", g, verbDelete, func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteRoute(rq.rt.project, rq.rt.name))
	})
}
