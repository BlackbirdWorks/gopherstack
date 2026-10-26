package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

const (
	healthHealthy      = "HEALTHY"
	healthUnhealthy    = "UNHEALTHY"
	healthUnknown      = "UNKNOWN"
	defaultBackendPort = 80
)

// healthStateFor derives a health state without probing: no health check is UNKNOWN, otherwise a running
// backend is HEALTHY and a stopped one UNHEALTHY.
func healthStateFor(hasHC, running bool) string {
	switch {
	case !hasHC:
		return healthUnknown
	case running:
		return healthHealthy
	}

	return healthUnhealthy
}

func namedPortValue(ports []*computeapi.NamedPort, name string) (int64, bool) {
	for _, p := range ports {
		if p.Name == name {
			return p.Port, true
		}
	}

	return 0, false
}

func backendPort(bs *computeapi.BackendService, named []*computeapi.NamedPort) int64 {
	if bs.PortName != "" {
		if p, ok := namedPortValue(named, bs.PortName); ok {
			return p
		}
	}

	if bs.Port != 0 {
		return bs.Port
	}

	return defaultBackendPort
}

func (b *InMemoryBackend) backendServiceHealth(
	c lbCtx, name, group string,
) (*computeapi.BackendServiceGroupHealth, error) {
	b.mu.RLock("GetBackendServiceHealth")
	defer b.mu.RUnlock()

	bs, ok := b.backendSvcs[key(c.project, c.locKey(), name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, c.scope, c.loc, collBackendSvc, name))
	}

	g, err := b.resolveBackendGroupLocked(c, group)
	if err != nil {
		return nil, err
	}

	member := false

	for _, be := range bs.Backends {
		member = member || be.Group == g.link
	}

	if !member {
		return nil, errInvalid("Invalid value for field 'group': '%s'. The group is not a backend of '%s'.", group, name)
	}

	out := &computeapi.BackendServiceGroupHealth{Kind: "compute#backendServiceGroupHealth"}
	hasHC := len(bs.HealthChecks) > 0

	if g.neg != nil {
		for _, e := range b.negEndpoints[g.negKey] {
			out.HealthStatus = append(out.HealthStatus, &computeapi.HealthStatus{
				Instance: e.Instance, IpAddress: e.IpAddress, Port: e.Port, HealthState: healthStateFor(hasHC, true),
			})
		}

		return out, nil
	}

	port := backendPort(bs, g.named)

	for _, m := range g.members {
		hs := &computeapi.HealthStatus{Instance: m, Port: port, HealthState: healthStateFor(hasHC, false)}

		if r, found := b.instanceByLinkLocked(m); found {
			hs.HealthState = healthStateFor(hasHC, instRunning(r, b.now().UTC(), b.timing))

			if len(r.Inst.NetworkInterfaces) > 0 {
				hs.IpAddress = r.Inst.NetworkInterfaces[0].NetworkIP
			}
		}

		out.HealthStatus = append(out.HealthStatus, hs)
	}

	return out, nil
}

func (b *InMemoryBackend) instanceByLinkLocked(link string) (*instRec, bool) {
	r, ok := parseRef(link, "instances", "", "")
	if !ok || r.loc == "" {
		return nil, false
	}

	rec, found := b.instances[key(r.project, r.loc, r.name)]

	return rec, found
}
