package gcpcompute

import (
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	maxASN               = 4294967294
	defaultKeepalive     = 20
	defaultMinPorts      = 64
	defaultDynMinPorts   = 32
	defaultUDPIdle       = 30
	defaultICMPIdle      = 30
	defaultTCPEstablish  = 1200
	defaultTCPTransitory = 30
	defaultTCPTimeWait   = 120
	natAuto              = "AUTO_ONLY"
	natManual            = "MANUAL_ONLY"
	natList              = "LIST_OF_SUBNETWORKS"
)

var (
	natSourceModes = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		"ALL_SUBNETWORKS_ALL_IP_RANGES": true, "ALL_SUBNETWORKS_ALL_PRIMARY_IP_RANGES": true, natList: true,
	}
	natRangeKinds = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		"ALL_IP_RANGES":               true,
		"PRIMARY_IP_RANGE":            true,
		"LIST_OF_SECONDARY_IP_RANGES": true,
	}
)

func routerTarget(project, region string, r *computeapi.Router) target {
	return target{project: project, scope: scopeRegion, loc: region, collection: "routers", name: r.Name, id: r.Id}
}

func (b *InMemoryBackend) normalizeNatLocked(
	project, region string,
	router *computeapi.Router,
	nat *computeapi.RouterNat,
	self string,
) *APIError {
	if err := validName("resource.nats.name", nat.Name); err != nil {
		return err
	}

	if !natSourceModes[nat.SourceSubnetworkIpRangesToNat] {
		return errField("resource.nats.sourceSubnetworkIpRangesToNat", nat.SourceSubnetworkIpRangesToNat, "")
	}

	switch nat.NatIpAllocateOption {
	case natAuto:
		if len(nat.NatIps) > 0 {
			return errInvalid("Invalid value for field 'resource.nats.natIps': natIps cannot be set with AUTO_ONLY.")
		}
	case natManual:
		ips, err := b.resolveNatIPsLocked(project, region, nat.NatIps, self)
		if err != nil {
			return err
		}

		nat.NatIps = ips
	default:
		return errField(
			"resource.nats.natIpAllocateOption",
			nat.NatIpAllocateOption,
			"Must be AUTO_ONLY or MANUAL_ONLY.",
		)
	}

	if err := b.resolveNatSubnetsLocked(project, region, router, nat); err != nil {
		return err
	}

	setNatDefaults(nat)

	return nil
}

func (b *InMemoryBackend) resolveNatIPsLocked(
	project, region string,
	refs []string,
	self string,
) ([]string, *APIError) {
	if len(refs) == 0 {
		return nil, errRequired("resource.nats.natIps")
	}

	out := make([]string, 0, len(refs))

	for _, raw := range refs {
		r, err := refLocated(raw, "addresses", scopeRegion, project, region)
		if err != nil {
			return nil, errField("resource.nats.natIps", raw, "The URL is malformed.")
		}

		a, aerr := b.getAddressLocked(r.project, scopeRegion, r.loc, r.name)
		if aerr != nil {
			return nil, aerr
		}

		if a.AddressType == addrInternal || r.loc != region {
			return nil, errField("resource.nats.natIps", raw, "Must be an external address in the router's region.")
		}

		for _, u := range b.addressUsersLocked(a) {
			if u != self {
				return nil, errInUse("address", trimURL(a.SelfLink), trimURL(u))
			}
		}

		out = append(out, a.SelfLink)
	}

	return out, nil
}

func (b *InMemoryBackend) resolveNatSubnetsLocked(
	project, region string,
	router *computeapi.Router,
	nat *computeapi.RouterNat,
) *APIError {
	if nat.SourceSubnetworkIpRangesToNat != natList {
		if len(nat.Subnetworks) > 0 {
			return errInvalid(
				"Invalid value for field 'resource.nats.subnetworks': only valid with LIST_OF_SUBNETWORKS.",
			)
		}

		return nil
	}

	if len(nat.Subnetworks) == 0 {
		return errRequired("resource.nats.subnetworks")
	}

	for _, sn := range nat.Subnetworks {
		s, err := b.resolveSubnetLocked(project, region, sn.Name)
		if err != nil {
			return err
		}

		if s.Network != router.Network {
			return errField("resource.nats.subnetworks.name", sn.Name, "Subnetwork is not in the router's network.")
		}

		if len(sn.SourceIpRangesToNat) == 0 {
			return errRequired("resource.nats.subnetworks.sourceIpRangesToNat")
		}

		for _, k := range sn.SourceIpRangesToNat {
			if !natRangeKinds[k] {
				return errField("resource.nats.subnetworks.sourceIpRangesToNat", k, "")
			}
		}

		sn.Name = s.SelfLink
	}

	return nil
}

func setNatDefaults(nat *computeapi.RouterNat) {
	def := func(v *int64, d int64) {
		if *v == 0 {
			*v = d
		}
	}

	def(&nat.UdpIdleTimeoutSec, defaultUDPIdle)
	def(&nat.IcmpIdleTimeoutSec, defaultICMPIdle)
	def(&nat.TcpEstablishedIdleTimeoutSec, defaultTCPEstablish)
	def(&nat.TcpTransitoryIdleTimeoutSec, defaultTCPTransitory)
	def(&nat.TcpTimeWaitTimeoutSec, defaultTCPTimeWait)

	if nat.EnableDynamicPortAllocation {
		def(&nat.MinPortsPerVm, defaultDynMinPorts)
	} else {
		def(&nat.MinPortsPerVm, defaultMinPorts)
	}

	if nat.LogConfig == nil {
		nat.LogConfig = &computeapi.RouterNatLogConfig{Filter: "ALL"}
	}

	if len(nat.EndpointTypes) == 0 {
		nat.EndpointTypes = []string{"ENDPOINT_TYPE_VM"}
	}

	if nat.Type == "" {
		nat.Type = "PUBLIC"
	}
}

func validateBgp(bgp *computeapi.RouterBgp) *APIError {
	if bgp == nil {
		return nil
	}

	if bgp.Asn < 1 || bgp.Asn > maxASN {
		return errField("resource.bgp.asn", int64Str(bgp.Asn), "ASN must be between 1 and 4294967294.")
	}

	switch bgp.AdvertiseMode {
	case "":
		bgp.AdvertiseMode = "DEFAULT"
	case "DEFAULT", "CUSTOM":
	default:
		return errField("resource.bgp.advertiseMode", bgp.AdvertiseMode, "")
	}

	if bgp.KeepaliveInterval == 0 {
		bgp.KeepaliveInterval = defaultKeepalive
	}

	return nil
}

func (b *InMemoryBackend) finishRouterLocked(
	project, region string,
	r *computeapi.Router,
	old *routerRec,
) (*routerRec, *APIError) {
	if err := validateBgp(r.Bgp); err != nil {
		return nil, err
	}

	rec := &routerRec{Router: r, AutoIPs: map[string][]string{}}
	seen := map[string]bool{}

	var taken []string

	for _, nat := range r.Nats {
		if seen[nat.Name] {
			return nil, errField("resource.nats.name", nat.Name, "Duplicate NAT name.")
		}

		seen[nat.Name] = true

		if err := b.normalizeNatLocked(project, region, r, nat, r.SelfLink); err != nil {
			return nil, err
		}
	}

	for _, nat := range r.Nats {
		if nat.NatIpAllocateOption != natAuto {
			continue
		}

		if old != nil && len(old.AutoIPs[nat.Name]) > 0 {
			rec.AutoIPs[nat.Name] = old.AutoIPs[nat.Name]

			continue
		}

		ip, err := b.allocExternalLocked(region, taken...)
		if err != nil {
			return nil, err
		}

		taken = append(taken, ip)
		rec.AutoIPs[nat.Name] = []string{ip}
	}

	return rec, nil
}

func (b *InMemoryBackend) insertRouter(project, region string, in *computeapi.Router) (*computeapi.Operation, error) {
	b.mu.Lock("InsertRouter")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if _, ok := findRegion(region); !ok {
		return nil, errNotFound(scopePath(project, scopeRegion, region))
	}

	if _, dup := b.routers[key(project, region, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeRegion, region, "routers", in.Name))
	}

	if in.Network == "" {
		return nil, errRequired("network")
	}

	n, err := b.resolveNetworkLocked(project, in.Network)
	if err != nil {
		return nil, err
	}

	r := clone(in)
	r.Kind, r.Id, r.CreationTimestamp = "compute#router", newID(), b.stamp()
	r.Region, r.Network = scopeURL(project, scopeRegion, region), n.SelfLink
	r.SelfLink = regionLink(project, region, "routers", r.Name)

	rec, apiErr := b.finishRouterLocked(project, region, r, nil)
	if apiErr != nil {
		return nil, apiErr
	}

	b.routers[key(project, region, r.Name)] = rec

	return b.newOpLocked("insert", routerTarget(project, region, r), b.timing.Done), nil
}

func (b *InMemoryBackend) getRouterLocked(project, region, name string) (*routerRec, *APIError) {
	r, ok := b.routers[key(project, region, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeRegion, region, "routers", name))
	}

	return r, nil
}

func (b *InMemoryBackend) getRouter(project, region, name string) (*computeapi.Router, error) {
	b.mu.RLock("GetRouter")
	defer b.mu.RUnlock()

	r, err := b.getRouterLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	return clone(r.Router), nil
}

func (b *InMemoryBackend) listRouters(project, scope, loc string) []entry {
	b.mu.RLock("ListRouters")
	defer b.mu.RUnlock()

	var out []entry

	for k, r := range b.routers {
		region := lastSegment(r.Router.Region)
		if !strings.HasPrefix(k, project+"/") || (scope == scopeRegion && region != loc) {
			continue
		}

		out = append(out, entry{scope: scopeRegion + "/" + region, item: clone(r.Router)})
	}

	return out
}

func (b *InMemoryBackend) deleteRouter(project, region, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteRouter")
	defer b.mu.Unlock()

	r, err := b.getRouterLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	delete(b.routers, key(project, region, name))

	return b.newOpLocked("delete", routerTarget(project, region, r.Router), b.timing.Done), nil
}

func (b *InMemoryBackend) updateRouter(
	project, region, name, opType string,
	build func(cur *computeapi.Router) (*computeapi.Router, *APIError),
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateRouter")
	defer b.mu.Unlock()

	cur, err := b.getRouterLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	next, apiErr := build(clone(cur.Router))
	if apiErr != nil {
		return nil, apiErr
	}

	if next.Name != "" && next.Name != cur.Router.Name {
		return nil, errField("resource.name", next.Name, "Field is not updatable.")
	}

	if next.Network != "" && next.Network != cur.Router.Network {
		return nil, errField("resource.network", next.Network, "Field is not updatable.")
	}

	next.Name, next.Network, next.Id, next.Kind = cur.Router.Name, cur.Router.Network, cur.Router.Id, cur.Router.Kind
	next.CreationTimestamp, next.SelfLink = cur.Router.CreationTimestamp, cur.Router.SelfLink
	next.Region = cur.Router.Region

	rec, apiErr := b.finishRouterLocked(project, region, next, cur)
	if apiErr != nil {
		return nil, apiErr
	}

	b.routers[key(project, region, name)] = rec

	return b.newOpLocked(opType, routerTarget(project, region, next), b.timing.Done), nil
}

func (b *InMemoryBackend) natCoversLocked(nat *computeapi.RouterNat, subnet string) bool {
	if nat.SourceSubnetworkIpRangesToNat != natList {
		return true
	}

	for _, sn := range nat.Subnetworks {
		if sn.Name == subnet {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) routerStatus(project, region, name string) (*computeapi.RouterStatusResponse, error) {
	b.mu.RLock("GetRouterStatus")
	defer b.mu.RUnlock()

	rec, err := b.getRouterLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	res := &computeapi.RouterStatus{Network: rec.Router.Network}
	now := b.now().UTC()

	for _, nat := range rec.Router.Nats {
		st := &computeapi.RouterStatusNatStatus{
			Name: nat.Name, AutoAllocatedNatIps: rec.AutoIPs[nat.Name], UserAllocatedNatIpResources: nat.NatIps,
		}

		for _, link := range nat.NatIps {
			if a, ok := b.addresses[key(project, scopeRegion, region, lastSegment(link))]; ok {
				st.UserAllocatedNatIps = append(st.UserAllocatedNatIps, a.Address)
			}
		}

		for _, inst := range b.instances {
			if instRunning(inst, now, b.timing) && natServes(b, nat, rec.Router, inst.Inst, region) {
				st.NumVmEndpointsWithNatMappings++
			}
		}

		res.NatStatus = append(res.NatStatus, st)
	}

	return &computeapi.RouterStatusResponse{Kind: "compute#routerStatusResponse", Result: res}, nil
}

func natServes(
	b *InMemoryBackend,
	nat *computeapi.RouterNat,
	router *computeapi.Router,
	inst *computeapi.Instance,
	region string,
) bool {
	if regionOfZone(lastSegment(inst.Zone)) != region {
		return false
	}

	for _, nic := range inst.NetworkInterfaces {
		if nic.Network != router.Network || !b.natCoversLocked(nat, nic.Subnetwork) {
			continue
		}

		external := false

		for _, ac := range nic.AccessConfigs {
			external = external || ac.NatIP != ""
		}

		if !external {
			return true
		}
	}

	return false
}
