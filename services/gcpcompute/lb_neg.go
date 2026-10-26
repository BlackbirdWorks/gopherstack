package gcpcompute

import (
	"fmt"
	"net/netip"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	negGCEVMIPPort    = "GCE_VM_IP_PORT"
	negGCEVMIP        = "GCE_VM_IP"
	negNonGCP         = "NON_GCP_PRIVATE_IP_PORT"
	negInternetIPPort = "INTERNET_IP_PORT"
	negInternetFQDN   = "INTERNET_FQDN_PORT"
	negServerless     = "SERVERLESS"
	maxNEGBatch       = 500
	maxFQDN           = 253
	defaultNetwork    = "default"
)

func negSpec() lbSpec[computeapi.NetworkEndpointGroup] {
	return lbSpec[computeapi.NetworkEndpointGroup]{
		coll: collNEG, kind: "compute#networkEndpointGroup", noun: "network_endpoint_group",
		m: func(b *InMemoryBackend) map[string]*computeapi.NetworkEndpointGroup { return b.negs },
		meta: func(v *computeapi.NetworkEndpointGroup) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, zone: &v.Zone, id: &v.Id,
			}
		},
		prep: prepNEG,
	}
}

func prepNEG(b *InMemoryBackend, c lbCtx, in *computeapi.NetworkEndpointGroup, _ map[string]any) *APIError {
	if in.DefaultPort < 0 || in.DefaultPort > hcMaxPort {
		return errField("resource.defaultPort", "", "Port must be in the range 1-65535.")
	}

	switch c.scope {
	case scopeZone:
		return b.prepZonalNEG(c, in)
	case scopeRegion:
		return prepServerlessNEG(in)
	}

	if in.NetworkEndpointType != negInternetIPPort && in.NetworkEndpointType != negInternetFQDN {
		return errField("resource.networkEndpointType", in.NetworkEndpointType,
			"A global NEG must be INTERNET_IP_PORT or INTERNET_FQDN_PORT.")
	}

	if in.Network != "" || in.Subnetwork != "" {
		return errInvalid("Invalid value for field 'resource.network': an internet NEG takes no network.")
	}

	return nil
}

func (b *InMemoryBackend) prepZonalNEG(c lbCtx, in *computeapi.NetworkEndpointGroup) *APIError {
	if in.NetworkEndpointType == "" {
		in.NetworkEndpointType = negGCEVMIPPort
	}

	switch in.NetworkEndpointType {
	case negGCEVMIPPort, negGCEVMIP, negNonGCP:
	default:
		return errField("resource.networkEndpointType", in.NetworkEndpointType,
			"A zonal NEG must be GCE_VM_IP_PORT, GCE_VM_IP or NON_GCP_PRIVATE_IP_PORT.")
	}

	if in.NetworkEndpointType == negGCEVMIP && in.DefaultPort != 0 {
		return errField("resource.defaultPort", "", "GCE_VM_IP endpoints have no port.")
	}

	if in.Network == "" {
		in.Network = defaultNetwork
	}

	n, err := b.resolveNetworkLocked(c.project, in.Network)
	if err != nil {
		return err
	}

	in.Network = n.SelfLink

	if in.NetworkEndpointType == negNonGCP {
		return nil
	}

	region := regionOfZone(c.loc)
	if in.Subnetwork == "" && n.AutoCreateSubnetworks {
		if s, ok := b.subnets[key(c.project, region, n.Name)]; ok {
			in.Subnetwork = s.SelfLink
		}
	}

	if in.Subnetwork == "" {
		return errRequired("resource.subnetwork")
	}

	s, serr := b.resolveSubnetLocked(c.project, region, in.Subnetwork)
	if serr != nil {
		return serr
	}

	if s.Network != n.SelfLink {
		return errField("resource.subnetwork", in.Subnetwork, "The subnetwork is not in the network.")
	}

	in.Subnetwork = s.SelfLink

	return nil
}

func prepServerlessNEG(in *computeapi.NetworkEndpointGroup) *APIError {
	if in.NetworkEndpointType != negServerless {
		return errField("resource.networkEndpointType", in.NetworkEndpointType,
			"A regional NEG must be SERVERLESS; other types are not supported by this emulator.")
	}

	if in.Network != "" || in.Subnetwork != "" || in.DefaultPort != 0 {
		return errInvalid("Invalid value for field 'resource': a serverless NEG takes no network or defaultPort.")
	}

	n := 0
	if in.CloudRun != nil {
		n++
	}

	if in.AppEngine != nil {
		n++
	}

	if in.CloudFunction != nil {
		n++
	}

	if n != 1 {
		return errInvalid("Invalid value for field 'resource': exactly one of cloudRun, appEngine or " +
			"cloudFunction must be specified for a SERVERLESS network endpoint group.")
	}

	if cr := in.CloudRun; cr != nil {
		if cr.Service == "" && cr.UrlMask == "" {
			return errRequired("resource.cloudRun.service")
		}

		if cr.Service != "" && cr.UrlMask != "" {
			return errInvalid("Invalid value for field 'resource.cloudRun': service and urlMask are mutually exclusive.")
		}

		if cr.Service != "" {
			if err := validName("resource.cloudRun.service", cr.Service); err != nil {
				return err
			}
		}
	}

	if cf := in.CloudFunction; cf != nil && cf.Function == "" && cf.UrlMask == "" {
		return errRequired("resource.cloudFunction.function")
	}

	return nil
}

func negCtx(rq *request, scope string) lbCtx {
	return lbCtx{project: rq.rt.project, scope: scope, loc: rq.rt.loc}
}

func endpointKey(e *computeapi.NetworkEndpoint) string {
	return fmt.Sprintf("%s|%s|%d|%s", e.Instance, e.IpAddress, e.Port, e.Fqdn)
}

func (b *InMemoryBackend) normalizeEndpointLocked(
	c lbCtx, neg *computeapi.NetworkEndpointGroup, e *computeapi.NetworkEndpoint,
) *APIError {
	if e.Port == 0 {
		e.Port = neg.DefaultPort
	}

	if e.Port < 0 || e.Port > hcMaxPort {
		return errField("networkEndpoints.port", strconv.FormatInt(e.Port, 10), "Port must be in the range 1-65535.")
	}

	switch neg.NetworkEndpointType {
	case negGCEVMIPPort, negGCEVMIP:
		return b.normalizeVMEndpointLocked(c, neg, e)
	case negNonGCP:
		if err := endpointIP(e); err != nil {
			return err
		}

		if e.Port == 0 {
			return errRequired("networkEndpoints.port")
		}
	case negInternetIPPort:
		return endpointIP(e)
	case negInternetFQDN:
		if e.Fqdn == "" || len(e.Fqdn) > maxFQDN || e.IpAddress != "" {
			return errField("networkEndpoints.fqdn", e.Fqdn, "An INTERNET_FQDN_PORT endpoint needs an FQDN and no IP address.")
		}
	}

	return nil
}

func endpointIP(e *computeapi.NetworkEndpoint) *APIError {
	if ip, err := netip.ParseAddr(e.IpAddress); err != nil || !ip.Is4() {
		return errField("networkEndpoints.ipAddress", e.IpAddress, "Must be a valid IPv4 address.")
	}

	return nil
}

func (b *InMemoryBackend) normalizeVMEndpointLocked(
	c lbCtx, neg *computeapi.NetworkEndpointGroup, e *computeapi.NetworkEndpoint,
) *APIError {
	if e.Instance == "" {
		return errRequired("networkEndpoints.instance")
	}

	r, err := refLocated(e.Instance, "instances", scopeZone, c.project, c.loc)
	if err != nil || r.loc != c.loc {
		return errField("networkEndpoints.instance", e.Instance, "The instance must be in the NEG's zone.")
	}

	inst, ok := b.instances[key(r.project, c.loc, r.name)]
	if !ok {
		return errNotFound(relPath(r.project, scopeZone, c.loc, "instances", r.name))
	}

	var nic *computeapi.NetworkInterface

	for _, n := range inst.Inst.NetworkInterfaces {
		if n.Network == neg.Network {
			nic = n

			break
		}
	}

	if nic == nil {
		return errField("networkEndpoints.instance", e.Instance, "The instance has no interface in the NEG's network.")
	}

	if e.IpAddress == "" {
		e.IpAddress = nic.NetworkIP
	}

	if e.IpAddress != nic.NetworkIP {
		return errField("networkEndpoints.ipAddress", e.IpAddress, "Not an address of the instance's interface.")
	}

	e.Instance = r.name

	if neg.NetworkEndpointType == negGCEVMIP {
		e.Port = 0
	} else if e.Port == 0 {
		return errRequired("networkEndpoints.port")
	}

	return nil
}

func (b *InMemoryBackend) attachEndpoints(c lbCtx, name string, eps []*computeapi.NetworkEndpoint) (*computeapi.Operation, error) {
	b.mu.Lock("AttachNetworkEndpoints")
	defer b.mu.Unlock()

	neg, k, err := b.negForEndpointsLocked(c, name, eps)
	if err != nil {
		return nil, err
	}

	cur := b.negEndpoints[k]
	have := map[string]bool{}

	for _, e := range cur {
		have[endpointKey(e)] = true
	}

	next := append([]*computeapi.NetworkEndpoint{}, cur...)

	for _, e := range eps {
		if err = b.normalizeEndpointLocked(c, neg, e); err != nil {
			return nil, err
		}

		if have[endpointKey(e)] {
			return nil, errInvalid("Invalid value for field 'networkEndpoints': endpoint '%s' is already attached.", endpointKey(e))
		}

		have[endpointKey(e)] = true

		next = append(next, e)
	}

	b.negEndpoints[k], neg.Size = next, int64(len(next))

	return b.newOpLocked(opPrefix+collNEG+".attachNetworkEndpoints", lbTargetOf(c, collNEG, name, neg.Id), b.timing.Done), nil
}

func (b *InMemoryBackend) detachEndpoints(c lbCtx, name string, eps []*computeapi.NetworkEndpoint) (*computeapi.Operation, error) {
	b.mu.Lock("DetachNetworkEndpoints")
	defer b.mu.Unlock()

	neg, k, err := b.negForEndpointsLocked(c, name, eps)
	if err != nil {
		return nil, err
	}

	drop := map[string]bool{}

	for _, e := range eps {
		if err = b.normalizeEndpointLocked(c, neg, e); err != nil {
			return nil, err
		}

		drop[endpointKey(e)] = true
	}

	var next []*computeapi.NetworkEndpoint

	for _, e := range b.negEndpoints[k] {
		if drop[endpointKey(e)] {
			delete(drop, endpointKey(e))

			continue
		}

		next = append(next, e)
	}

	if len(drop) > 0 {
		return nil, errInvalid("Invalid value for field 'networkEndpoints': endpoint is not attached to the group.")
	}

	b.negEndpoints[k], neg.Size = next, int64(len(next))

	return b.newOpLocked(opPrefix+collNEG+".detachNetworkEndpoints", lbTargetOf(c, collNEG, name, neg.Id), b.timing.Done), nil
}

func (b *InMemoryBackend) negForEndpointsLocked(
	c lbCtx, name string, eps []*computeapi.NetworkEndpoint,
) (*computeapi.NetworkEndpointGroup, string, *APIError) {
	k := key(c.project, c.locKey(), name)

	neg, ok := b.negs[k]
	if !ok {
		return nil, "", errNotFound(relPath(c.project, c.scope, c.loc, collNEG, name))
	}

	if len(eps) == 0 {
		return nil, "", errRequired("networkEndpoints")
	}

	if len(eps) > maxNEGBatch {
		return nil, "", errInvalid("At most %d endpoints can be attached or detached in one request.", maxNEGBatch)
	}

	return neg, k, nil
}

func (b *InMemoryBackend) listEndpoints(c lbCtx, name string, showHealth bool) ([]entry, error) {
	b.mu.RLock("ListNetworkEndpoints")
	defer b.mu.RUnlock()

	k := key(c.project, c.locKey(), name)

	neg, ok := b.negs[k]
	if !ok {
		return nil, errNotFound(relPath(c.project, c.scope, c.loc, collNEG, name))
	}

	var out []entry

	for _, e := range b.negEndpoints[k] {
		item := &computeapi.NetworkEndpointWithHealthStatus{NetworkEndpoint: clone(e)}
		if showHealth {
			item.Healths = b.endpointHealthsLocked(neg.SelfLink)
		}

		out = append(out, entry{scope: scopeGlobal, item: item})
	}

	return out, nil
}

func (b *InMemoryBackend) endpointHealthsLocked(negLink string) []*computeapi.HealthStatusForNetworkEndpoint {
	var out []*computeapi.HealthStatusForNetworkEndpoint

	for _, bs := range b.backendSvcs {
		for _, be := range bs.Backends {
			if be.Group != negLink {
				continue
			}

			h := &computeapi.HealthStatusForNetworkEndpoint{
				BackendService: &computeapi.BackendServiceReference{BackendService: bs.SelfLink},
				HealthState:    healthStateFor(len(bs.HealthChecks) > 0, true),
			}
			if len(bs.HealthChecks) > 0 {
				h.HealthCheck = &computeapi.HealthCheckReference{HealthCheck: bs.HealthChecks[0]}
			}

			out = append(out, h)
		}
	}

	return out
}

func (b *InMemoryBackend) deleteNEG(c lbCtx, name string) (*computeapi.Operation, error) {
	return lbDelete(b, negSpec(), c, name, func(b *InMemoryBackend, v *computeapi.NetworkEndpointGroup) *APIError {
		delete(b.negEndpoints, key(c.project, c.locKey(), v.Name))

		return nil
	})
}
