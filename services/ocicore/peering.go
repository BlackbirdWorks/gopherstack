package ocicore

import (
	"net/netip"
	"slices"
)

const (
	peerNew       = "NEW"
	peerPeered    = "PEERED"
	maxLpgPerVcn  = 10
	maxReservedIP = 50

	lifetimeReserved  = "RESERVED"
	lifetimeEphemeral = "EPHEMERAL"
	scopeRegion       = "REGION"
	scopeAD           = "AVAILABILITY_DOMAIN"
)

// CreateLpgIn is the CreateLocalPeeringGateway request body.
type CreateLpgIn struct {
	TagsIn
	CompartmentID string `json:"compartmentId"`
	VcnID         string `json:"vcnId"`
	DisplayName   string `json:"displayName"`
	RouteTableID  string `json:"routeTableId"`
}

// CreateLocalPeeringGateway creates a local peering gateway.
func (b *InMemoryBackend) CreateLocalPeeringGateway(in CreateLpgIn) (resource, string, *APIError) {
	b.mu.Lock("CreateLocalPeeringGateway")
	defer b.mu.Unlock()

	v, aerr := b.gatewayVcn(in.CompartmentID, in.VcnID, in.DisplayName, in.RouteTableID)
	if aerr == nil {
		aerr = gatewayLimit[*LocalPeeringGateway](b, v.ID, "local peering gateways", maxLpgPerVcn)
	}

	if aerr != nil {
		return nil, "", aerr
	}

	g := &LocalPeeringGateway{
		Base: b.newBase(kindLPG, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID,
		RouteTableID: in.RouteTableID, PeeringStatus: peerNew,
	}
	b.add(g)

	return g.view(), g.Internal.Etag, nil
}

// UpdateLpgIn is the UpdateLocalPeeringGateway request body.
type UpdateLpgIn struct {
	RouteTableID *string `json:"routeTableId"`
	UpdateIn
}

// UpdateLocalPeeringGateway changes a local peering gateway.
func (b *InMemoryBackend) UpdateLocalPeeringGateway(id, ifMatch string, in UpdateLpgIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(g *LocalPeeringGateway) *APIError {
		if in.RouteTableID != nil {
			if aerr := b.optRouteTable(*in.RouteTableID, g.VcnID); aerr != nil {
				return aerr
			}

			g.RouteTableID = *in.RouteTableID
		}

		return nil
	})
}

// ConnectLocalPeeringGateways peers a gateway with another one in a different VCN.
func (b *InMemoryBackend) ConnectLocalPeeringGateways(id, peerID, ifMatch string) *APIError {
	b.mu.Lock("ConnectLocalPeeringGateways")
	defer b.mu.Unlock()

	if peerID == "" {
		return errMissing("peerId is required")
	}

	g, aerr := getLive[*LocalPeeringGateway](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(g, ifMatch); aerr != nil {
		return aerr
	}

	p, aerr := getLive[*LocalPeeringGateway](b, peerID)
	if aerr != nil {
		return aerr
	}

	if p.VcnID == g.VcnID {
		return errParam("Local peering gateways in the same VCN cannot be connected")
	}

	if g.PeeringStatus == peerPeered || p.PeeringStatus == peerPeered {
		return errConflict("A local peering gateway is already peered")
	}

	gv, _ := get[*Vcn](b, g.VcnID)
	pv, _ := get[*Vcn](b, p.VcnID)

	if cidrsOverlap(gv.CidrBlocks, pv.CidrBlocks) {
		return errParam("The CIDR blocks of the peered VCNs overlap")
	}

	pair := [][2]*LocalPeeringGateway{{g, p}, {p, g}}
	vcns := map[*LocalPeeringGateway]*Vcn{g: pv, p: gv}

	for _, pr := range pair {
		self, other := pr[0], pr[1]
		self.PeerID, self.PeeringStatus, self.PeeringStatusDetails = other.ID, peerPeered, "Connected to peer"
		self.PeerAdvertisedCidr, self.PeerAdvertisedDetails = vcns[self].CidrBlock, slices.Clone(vcns[self].CidrBlocks)
		b.bump(self)
	}

	return nil
}

func cidrsOverlap(a, c []string) bool {
	for _, x := range a {
		for _, y := range c {
			if netip.MustParsePrefix(x).Overlaps(netip.MustParsePrefix(y)) {
				return true
			}
		}
	}

	return false
}

// CreatePublicIPIn is the CreatePublicIp request body.
type CreatePublicIPIn struct {
	TagsIn
	CompartmentID  string `json:"compartmentId"`
	DisplayName    string `json:"displayName"`
	Lifetime       string `json:"lifetime"`
	PrivateIPID    string `json:"privateIpId"`
	PublicIPPoolID string `json:"publicIpPoolId"`
}

// CreatePublicIP allocates a reserved public IP. Ephemeral IPs belong to VNICs, which this emulator lacks.
func (b *InMemoryBackend) CreatePublicIP(in CreatePublicIPIn) (resource, string, *APIError) {
	b.mu.Lock("CreatePublicIP")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	switch in.Lifetime {
	case lifetimeReserved:
	case lifetimeEphemeral:
		if in.PrivateIPID == "" {
			return nil, "", errParam("privateIpId is required to create an ephemeral public IP")
		}
	default:
		return nil, "", errParam("lifetime must be EPHEMERAL or RESERVED")
	}

	if in.PrivateIPID != "" {
		return nil, "", errNotFound(in.PrivateIPID)
	}

	if in.PublicIPPoolID != "" {
		return nil, "", errNotFound(in.PublicIPPoolID)
	}

	reserved := liveOf(b, func(p *PublicIP) bool { return p.Lifetime == lifetimeReserved })
	if len(reserved) >= maxReservedIP {
		return nil, "", errLimit("The region reserved public IP limit (50) has been reached")
	}

	p := &PublicIP{
		Base: b.newBase(kindPublicIP, in.CompartmentID, in.DisplayName, in.TagsIn), IPAddress: b.allocIP(),
		Lifetime: lifetimeReserved, Scope: scopeRegion,
	}
	b.add(p)

	return p.view(), p.Internal.Etag, nil
}

// UpdatePublicIPIn is the UpdatePublicIp request body.
type UpdatePublicIPIn struct {
	PrivateIPID *string `json:"privateIpId"`
	UpdateIn
}

// UpdatePublicIP changes a public IP's name and tags; assigning to a private IP needs VNICs, which are not emulated.
func (b *InMemoryBackend) UpdatePublicIP(id, ifMatch string, in UpdatePublicIPIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(p *PublicIP) *APIError {
		if in.PrivateIPID == nil {
			return nil
		}

		if *in.PrivateIPID != "" {
			return errNotFound(*in.PrivateIPID)
		}

		if p.AssignedEntityType == entityNAT {
			return errConflict("The public IP " + p.ID + " is assigned to a NAT gateway")
		}

		return nil
	})
}

// PublicIPByAddress finds a public IP by its address.
func (b *InMemoryBackend) PublicIPByAddress(addr string) (resource, string, *APIError) {
	b.mu.RLock("PublicIPByAddress")
	defer b.mu.RUnlock()

	for _, r := range b.res {
		if p, ok := r.(*PublicIP); ok && live(p) && p.IPAddress == addr {
			return p.view(), p.Internal.Etag, nil
		}
	}

	return nil, "", errNotFound(addr)
}
