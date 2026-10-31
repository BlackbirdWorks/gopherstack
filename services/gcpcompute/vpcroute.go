package gcpcompute

import (
	"net/netip"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const defaultRoutePrio = 1000

func routeTarget(project string, r *computeapi.Route) target {
	return target{project: project, scope: scopeGlobal, collection: "routes", name: r.Name, id: r.Id}
}

func (b *InMemoryBackend) resolveNextHopLocked(project string, in, out *computeapi.Route) *APIError {
	if in.NextHopVpnTunnel != "" || in.NextHopIlb != "" || in.NextHopHub != "" || in.NextHopPeering != "" ||
		in.NextHopNetwork != "" {
		return errInvalid(
			"Invalid value for field 'resource': next hop type is not supported by this emulator " +
				"(use nextHopGateway, nextHopIp or nextHopInstance).",
		)
	}

	set := 0

	for _, given := range []bool{in.NextHopGateway != "", in.NextHopIp != "", in.NextHopInstance != ""} {
		if given {
			set++
		}
	}

	if set != 1 {
		return errInvalid(
			"Invalid value for field 'resource': ''. Exactly one next hop " +
				"(nextHopGateway, nextHopIp, nextHopInstance) must be specified.",
		)
	}

	switch {
	case in.NextHopGateway != "":
		return resolveGateway(project, in, out)
	case in.NextHopIp != "":
		return resolveNextHopIP(in, out)
	}

	return b.resolveNextHopInstanceLocked(project, in, out)
}

func resolveGateway(project string, in, out *computeapi.Route) *APIError {
	r, err := refGlobal(in.NextHopGateway, "gateways", project)
	if err != nil || r.name != defaultGateway {
		return errField(
			"resource.nextHopGateway",
			in.NextHopGateway,
			"Only the default internet gateway is supported.",
		)
	}

	out.NextHopGateway = globalLink(r.project, "gateways", defaultGateway)

	return nil
}

func resolveNextHopIP(in, out *computeapi.Route) *APIError {
	if a, err := netip.ParseAddr(in.NextHopIp); err != nil || !a.Is4() {
		return errField("resource.nextHopIp", in.NextHopIp, "Must be a valid IPv4 address.")
	}

	out.NextHopIp = in.NextHopIp

	return nil
}

func (b *InMemoryBackend) resolveNextHopInstanceLocked(project string, in, out *computeapi.Route) *APIError {
	r, err := refLocated(in.NextHopInstance, "instances", scopeZone, project, "")
	if err != nil {
		return errField("resource.nextHopInstance", in.NextHopInstance, "The URL is malformed.")
	}

	rec, ok := b.instances[key(r.project, r.loc, r.name)]
	if !ok {
		return errNotFound(relPath(r.project, scopeZone, r.loc, "instances", r.name))
	}

	out.NextHopInstance = rec.Inst.SelfLink

	return nil
}

func (b *InMemoryBackend) insertRoute(project string, in *computeapi.Route) (*computeapi.Operation, error) {
	b.mu.Lock("InsertRoute")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if _, dup := b.routes[key(project, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeGlobal, "", "routes", in.Name))
	}

	netRef := in.Network
	if netRef == "" {
		netRef = "global/networks/default"
	}

	n, err := b.resolveNetworkLocked(project, netRef)
	if err != nil {
		return nil, err
	}

	if in.DestRange == "" {
		return nil, errRequired("destRange")
	}

	if _, err = parseCIDR("resource.destRange", in.DestRange); err != nil {
		return nil, err
	}

	if in.Priority < 0 || in.Priority > maxPriority {
		return nil, errField("resource.priority", int64Str(in.Priority), "Priority must be between 0 and 65535.")
	}

	if err = validTags("resource.tags", in.Tags); err != nil {
		return nil, err
	}

	r := &computeapi.Route{
		Kind: kindRoute, Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name, Description: in.Description,
		Network: n.SelfLink, DestRange: in.DestRange, Priority: in.Priority, Tags: in.Tags,
		SelfLink: globalLink(project, "routes", in.Name), RouteType: "STATIC",
	}

	if r.Priority == 0 {
		r.Priority = defaultRoutePrio
	}

	if err = b.resolveNextHopLocked(project, in, r); err != nil {
		return nil, err
	}

	b.routes[key(project, r.Name)] = r

	return b.newOpLocked("insert", routeTarget(project, r), b.timing.Done), nil
}

func (b *InMemoryBackend) getRoute(project, name string) (*computeapi.Route, error) {
	b.mu.RLock("GetRoute")
	defer b.mu.RUnlock()

	r, ok := b.routes[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "routes", name))
	}

	return viewRoute(r), nil
}

func (b *InMemoryBackend) listRoutes(project string) []entry {
	b.mu.RLock("ListRoutes")
	defer b.mu.RUnlock()

	var out []entry

	for k, r := range b.routes {
		if strings.HasPrefix(k, project+"/") {
			out = append(out, entry{scope: scopeGlobal, item: viewRoute(r)})
		}
	}

	return out
}

func (b *InMemoryBackend) deleteRoute(project, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteRoute")
	defer b.mu.Unlock()

	r, ok := b.routes[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "routes", name))
	}

	if isSubnetRoute(r) {
		return nil, errField("route", name,
			"System-generated subnet routes cannot be deleted; delete or change the subnetwork instead.")
	}

	delete(b.routes, key(project, name))

	return b.newOpLocked("delete", routeTarget(project, r), b.timing.Done), nil
}
