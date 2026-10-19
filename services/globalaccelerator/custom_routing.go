package globalaccelerator

import (
	"encoding/binary"
	"iter"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	// reservedSubnetAddrs: network, three AWS-reserved addresses and broadcast are never destinations.
	reservedSubnetAddrs = 5
	firstHostOffset     = 4
	ipv4Bits            = 32
	subnetIDPrefix      = "subnet-"
	maxPortMappings     = 100
)

// CustomGroupInput holds CreateCustomRoutingEndpointGroup parameters.
type CustomGroupInput struct {
	Region       string
	Destinations []Destination
}

// TrafficChange is an Allow/DenyCustomRoutingTraffic request.
type TrafficChange struct {
	EndpointGroupARN string
	EndpointID       string
	State            string
	Addresses        []string
	Ports            []int32
	All              bool
}

// PortMapping is one accelerator-port to destination-socket mapping.
type PortMapping struct {
	GroupARN   string
	EndpointID string
	IP         string
	State      string
	Protocols  []string
	AccPort    int32
	DstPort    int32
}

// DestinationMapping is a PortMapping located by destination, with accelerator context.
type DestinationMapping struct {
	AcceleratorARN string
	GroupRegion    string
	AcceleratorIPs []string
	PortMapping
}

type mapping struct {
	group    *EndpointGroup
	endpoint *Endpoint
	ip       string
	dest     Destination
	accPort  int32
	dstPort  int32
}

func subnetHostCount(cidr string) int64 {
	p, err := netip.ParsePrefix(cidr)
	if err != nil || !p.Addr().Is4() {
		return 0
	}

	size := int64(1) << (ipv4Bits - p.Bits())

	return max(size-reservedSubnetAddrs, 0)
}

func subnetHosts(cidr string) iter.Seq[string] {
	return func(yield func(string) bool) {
		p, err := netip.ParsePrefix(cidr)
		if err != nil || !p.Addr().Is4() {
			return
		}

		base := binary.BigEndian.Uint32(p.Addr().AsSlice())
		count := subnetHostCount(cidr)

		for i := range count {
			var raw [4]byte

			binary.BigEndian.PutUint32(raw[:], base+uint32(firstHostOffset+i))

			if !yield(netip.AddrFrom4(raw).String()) {
				return
			}
		}
	}
}

// mappingsLocked yields the accelerator's port mappings in a stable order: per listener, groups by creation,
// endpoints in add order, hosts ascending, destination ports ascending; accelerator ports are consumed in
// the listener's port-range order.
func (b *InMemoryBackend) mappingsLocked(acceleratorARN string) iter.Seq[mapping] {
	return func(yield func(mapping) bool) {
		for _, l := range b.listenersOfLocked(acceleratorARN) {
			if !l.Custom {
				continue
			}

			next := portCursor(l.PortRanges)

			for _, g := range b.groupsOfLocked(l.ARN) {
				if !yieldGroupMappings(g, next, yield) {
					return
				}
			}
		}
	}
}

func yieldGroupMappings(g *EndpointGroup, next func() (int32, bool), yield func(mapping) bool) bool {
	for i := range g.Endpoints {
		ep := &g.Endpoints[i]

		for ip := range subnetHosts(ep.SubnetCIDR) {
			for _, d := range g.Destinations {
				for p := d.FromPort; p <= d.ToPort; p++ {
					acc, ok := next()
					if !ok {
						return true
					}

					if !yield(mapping{group: g, endpoint: ep, dest: d, ip: ip, accPort: acc, dstPort: p}) {
						return false
					}
				}
			}
		}
	}

	return true
}

func portCursor(ranges []PortRange) func() (int32, bool) {
	idx, cur := 0, int32(0)
	if len(ranges) > 0 {
		cur = ranges[0].FromPort
	}

	return func() (int32, bool) {
		if idx >= len(ranges) {
			return 0, false
		}

		port := cur
		if cur >= ranges[idx].ToPort {
			idx++
			if idx < len(ranges) {
				cur = ranges[idx].FromPort
			}
		} else {
			cur++
		}

		return port, true
	}
}

func (e *Endpoint) trafficState(ip string, port int32) string {
	state := e.DefaultState
	if state == "" {
		state = trafficDeny
	}

	for _, r := range e.Rules {
		if (len(r.Addresses) == 0 || slices.Contains(r.Addresses, ip)) &&
			(len(r.Ports) == 0 || slices.Contains(r.Ports, port)) {
			state = r.State
		}
	}

	return state
}

func (m mapping) toPortMapping() PortMapping {
	return PortMapping{
		GroupARN: m.group.ARN, EndpointID: m.endpoint.ID, IP: m.ip, DstPort: m.dstPort, AccPort: m.accPort,
		Protocols: slices.Clone(m.dest.Protocols), State: m.endpoint.trafficState(m.ip, m.dstPort),
	}
}

// checkCapacityLocked fails when the listener's ports cannot map every destination socket.
func (b *InMemoryBackend) checkCapacityLocked(listenerARN string, ranges []PortRange) error {
	var need int64

	for _, g := range b.groupsOfLocked(listenerARN) {
		var ports int64

		for _, d := range g.Destinations {
			ports += int64(d.ToPort-d.FromPort) + 1
		}

		for _, ep := range g.Endpoints {
			need += subnetHostCount(ep.SubnetCIDR) * ports
		}
	}

	if need > countPorts(ranges) {
		return fail(ErrLimitExceeded,
			"the listener's port ranges cannot map %d destination sockets", need)
	}

	return nil
}

func validateDestinations(dests []Destination) error {
	if len(dests) == 0 {
		return fail(ErrInvalidArgument, "destinationConfigurations must not be empty")
	}

	ranges := make([]PortRange, 0, len(dests))

	for _, d := range dests {
		if len(d.Protocols) == 0 {
			return fail(ErrInvalidArgument, "each destination needs at least one protocol")
		}

		for _, p := range d.Protocols {
			if err := validateProtocol(p); err != nil {
				return err
			}
		}

		ranges = append(ranges, PortRange{FromPort: d.FromPort, ToPort: d.ToPort})
	}

	return validateDestinationRanges(ranges)
}

func validateDestinationRanges(ranges []PortRange) error {
	for i, r := range ranges {
		if r.FromPort < 1 || r.ToPort > maxPort || r.FromPort > r.ToPort {
			return fail(ErrInvalidPortRange, "port range %d-%d is invalid", r.FromPort, r.ToPort)
		}

		for _, o := range ranges[i+1:] {
			if rangesOverlap(r, o) {
				return fail(ErrInvalidPortRange, "destination port ranges %d-%d and %d-%d overlap",
					r.FromPort, r.ToPort, o.FromPort, o.ToPort)
			}
		}
	}

	return nil
}

// CreateCustomRoutingEndpointGroup creates a custom routing endpoint group.
func (b *InMemoryBackend) CreateCustomRoutingEndpointGroup(
	listenerARN string,
	in CustomGroupInput,
) (*EndpointGroup, error) {
	if err := validateDestinations(in.Destinations); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCustomRoutingEndpointGroup")
	defer b.mu.Unlock()

	l, err := b.listenerLocked(listenerARN, true)
	if err != nil {
		return nil, err
	}

	g, err := b.newGroupLocked(l, in.Region, true)
	if err != nil {
		return nil, err
	}

	for _, d := range in.Destinations {
		g.Destinations = append(g.Destinations, Destination{
			FromPort: d.FromPort, ToPort: d.ToPort, Protocols: slices.Clone(d.Protocols),
		})
	}

	b.endpointGroups.Put(g)
	b.touchAcceleratorLocked(l.AcceleratorARN)

	return b.viewGroupLocked(g), nil
}

// AddCustomRoutingEndpoints adds VPC subnets to a custom routing endpoint group.
func (b *InMemoryBackend) AddCustomRoutingEndpoints(
	arn string,
	cfgs []EndpointConfig,
) (*EndpointGroup, []Endpoint, error) {
	if len(cfgs) == 0 {
		return nil, nil, fail(ErrInvalidArgument, "endpointConfigurations must not be empty")
	}

	b.mu.Lock("AddCustomRoutingEndpoints")
	defer b.mu.Unlock()

	g, err := b.groupLocked(arn, true)
	if err != nil {
		return nil, nil, err
	}

	next, err := b.buildSubnetsLocked(g, cfgs)
	if err != nil {
		return nil, nil, err
	}

	prev := g.Endpoints
	g.Endpoints = next

	l, _ := b.listeners.Get(g.ListenerARN)
	if err = b.checkCapacityLocked(l.ARN, l.PortRanges); err != nil {
		g.Endpoints = prev

		return nil, nil, err
	}

	b.touchAcceleratorLocked(l.AcceleratorARN)

	view := b.viewGroupLocked(g)

	return view, view.Endpoints[len(prev):], nil
}

func (b *InMemoryBackend) buildSubnetsLocked(g *EndpointGroup, cfgs []EndpointConfig) ([]Endpoint, error) {
	out := slices.Clone(g.Endpoints)
	now := b.now()

	for _, c := range cfgs {
		if !strings.HasPrefix(c.EndpointID, subnetIDPrefix) {
			return nil, fail(ErrInvalidArgument, "endpointId %q must be a VPC subnet ID", c.EndpointID)
		}

		if slices.ContainsFunc(out, func(e Endpoint) bool { return e.ID == c.EndpointID }) {
			return nil, fail(ErrEndpointAlreadyExists, "endpoint %s already exists in the endpoint group", c.EndpointID)
		}

		if c.AttachmentARN != "" && !b.attachments.Has(c.AttachmentARN) {
			return nil, fail(ErrAttachmentNotFound, "attachment %s not found", c.AttachmentARN)
		}

		cidr := ""

		if b.resolver != nil {
			var ok bool
			if cidr, ok = b.resolver.SubnetCIDR(g.Region, c.EndpointID); !ok {
				return nil, fail(ErrInvalidArgument, "subnet %s was not found in %s", c.EndpointID, g.Region)
			}
		}

		out = append(out, Endpoint{
			ID: c.EndpointID, AttachmentARN: c.AttachmentARN, SubnetCIDR: cidr, AddedAt: now, DefaultState: trafficDeny,
		})
	}

	if len(out) > maxEndpoints {
		return nil, fail(ErrLimitExceeded, "an endpoint group can have at most %d endpoints", maxEndpoints)
	}

	return out, nil
}

// RemoveCustomRoutingEndpoints removes subnets by ID; every ID must exist.
func (b *InMemoryBackend) RemoveCustomRoutingEndpoints(arn string, ids []string) error {
	b.mu.Lock("RemoveCustomRoutingEndpoints")
	defer b.mu.Unlock()

	g, err := b.groupLocked(arn, true)
	if err != nil {
		return err
	}

	return b.removeEndpointsLocked(g, ids)
}

// ChangeCustomRoutingTraffic applies an Allow/DenyCustomRoutingTraffic request.
func (b *InMemoryBackend) ChangeCustomRoutingTraffic(c TrafficChange) error {
	b.mu.Lock("ChangeCustomRoutingTraffic")
	defer b.mu.Unlock()

	g, err := b.groupLocked(c.EndpointGroupARN, true)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(g.Endpoints, func(e Endpoint) bool { return e.ID == c.EndpointID })
	if i < 0 {
		return fail(ErrEndpointNotFound, "endpoint %s not found in the endpoint group", c.EndpointID)
	}

	ep := &g.Endpoints[i]

	if c.All {
		ep.DefaultState, ep.Rules = c.State, nil

		return nil
	}

	if len(c.Addresses) == 0 && len(c.Ports) == 0 {
		return fail(
			ErrInvalidArgument,
			"specify destination addresses and ports, or apply the change to the whole endpoint",
		)
	}

	if err = checkDestinations(g, ep, c); err != nil {
		return err
	}

	if len(ep.Rules) >= maxTrafficRules {
		return fail(ErrLimitExceeded, "an endpoint can hold at most %d traffic rules", maxTrafficRules)
	}

	ep.Rules = append(
		ep.Rules,
		TrafficRule{State: c.State, Addresses: slices.Clone(c.Addresses), Ports: slices.Clone(c.Ports)},
	)

	return nil
}

func checkDestinations(g *EndpointGroup, ep *Endpoint, c TrafficChange) error {
	for _, ip := range c.Addresses {
		a, err := netip.ParseAddr(ip)
		if err != nil {
			return fail(ErrInvalidArgument, "%q is not an IP address", ip)
		}

		if p, perr := netip.ParsePrefix(ep.SubnetCIDR); perr == nil && !p.Contains(a) {
			return fail(ErrInvalidArgument, "address %s is outside subnet %s", ip, ep.ID)
		}
	}

	for _, port := range c.Ports {
		if !slices.ContainsFunc(
			g.Destinations,
			func(d Destination) bool { return port >= d.FromPort && port <= d.ToPort },
		) {
			return fail(ErrInvalidPortRange, "port %d is outside the endpoint group's destination port ranges", port)
		}
	}

	return nil
}

func checkOffsetToken(token string) (int, error) {
	if err := page.ValidateToken(token); err != nil {
		return 0, fail(ErrInvalidNextToken, "the nextToken is invalid")
	}

	return page.DecodeToken(token), nil
}

func resolveLimit(maxResults *int32) (int, error) {
	if maxResults == nil {
		return defaultPageSize, nil
	}

	if *maxResults < 1 || *maxResults > maxPortMappings {
		return 0, fail(ErrInvalidArgument, "maxResults must be between 1 and %d", maxPortMappings)
	}

	return int(*maxResults), nil
}

// ListCustomRoutingPortMappings pages the accelerator's port mappings, optionally for one endpoint group.
func (b *InMemoryBackend) ListCustomRoutingPortMappings(
	acceleratorARN, groupARN, token string, maxResults *int32,
) ([]PortMapping, string, error) {
	limit, err := resolveLimit(maxResults)
	if err != nil {
		return nil, "", err
	}

	offset, err := checkOffsetToken(token)
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListCustomRoutingPortMappings")
	defer b.mu.RUnlock()

	if _, err = b.acceleratorLocked(acceleratorARN, true); err != nil {
		return nil, "", err
	}

	if groupARN != "" {
		g, ok := b.endpointGroups.Get(groupARN)
		if !ok || !g.Custom {
			return nil, "", fail(ErrEndpointGroupNotFound, "endpoint group %s not found", groupARN)
		}

		if l, lok := b.listeners.Get(g.ListenerARN); !lok || l.AcceleratorARN != acceleratorARN {
			return nil, "", fail(ErrEndpointGroupNotFound, "endpoint group %s not found", groupARN)
		}
	}

	var out []PortMapping

	seen := 0

	for m := range b.mappingsLocked(acceleratorARN) {
		if groupARN != "" && m.group.ARN != groupARN {
			continue
		}

		seen++
		if seen <= offset {
			continue
		}

		if len(out) == limit {
			return out, page.EncodeToken(offset + limit), nil
		}

		out = append(out, m.toPortMapping())
	}

	return out, "", nil
}

// ListCustomRoutingPortMappingsByDestination returns every mapping to one destination address in a subnet.
func (b *InMemoryBackend) ListCustomRoutingPortMappingsByDestination(
	endpointID, address, token string, maxResults *int32,
) (page.Page[DestinationMapping], error) {
	b.mu.RLock("ListCustomRoutingPortMappingsByDestination")
	defer b.mu.RUnlock()

	var all []DestinationMapping

	for _, a := range b.sortedAcceleratorsLocked(true) {
		for m := range b.mappingsLocked(a.ARN) {
			if m.endpoint.ID != endpointID || m.ip != address {
				continue
			}

			all = append(all, DestinationMapping{
				PortMapping: m.toPortMapping(), AcceleratorARN: a.ARN, GroupRegion: m.group.Region,
				AcceleratorIPs: slices.Clone(a.IPv4),
			})
		}
	}

	if len(all) == 0 {
		return page.Page[DestinationMapping]{}, fail(ErrEndpointNotFound,
			"no custom routing destination %s was found in %s", address, endpointID)
	}

	return paginate(all, token, maxResults)
}
