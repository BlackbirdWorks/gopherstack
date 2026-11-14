package ociapigateway

import (
	"errors"
	"net/http"
	"slices"
)

const (
	ipModeV4   = "IPV4"
	ipModeV6   = "IPV6"
	ipModeDual = "DUAL_STACK"
)

// SubnetInfo is what the gateway needs to know about a subnet.
type SubnetInfo struct {
	// Regional is false for an availability-domain-specific subnet.
	Regional bool
	// Public is false for a subnet that prohibits public IPs.
	Public bool
}

// AddressRequest asks the network for the VNIC that backs a gateway.
type AddressRequest struct {
	CompartmentID   string
	SubnetID        string
	DisplayName     string
	ReservedIPIDs   []string
	IPv6Addresses   []string
	IPv6SubnetCidrs []string
	Public          bool
	AssignIPv6      bool
}

// Allocation is the VNIC created for a gateway and the addresses it carries.
type Allocation struct {
	VnicID        string
	PrivateIP     string
	PublicIPs     []string
	IPv6Addresses []string
}

// Network backs gateways with subnets and VNICs of OCI Core.
type Network struct {
	Subnet   func(id string) (SubnetInfo, bool)
	Allocate func(AddressRequest) (Allocation, error)
	Release  func(vnicID string)
}

type ipv4Config struct {
	ReservedIPIDs []string `json:"reservedIpIds,omitempty"`
}

type ipv6Config struct {
	Addresses   []string `json:"addresses,omitempty"`
	SubnetCidrs []string `json:"subnetCidrs,omitempty"`
}

// SetNetwork wires gateway subnets and addresses to OCI Core.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	b.network = &n
	b.mu.Unlock()
}

func wantsV4(mode string) bool { return mode != ipModeV6 }

func wantsV6(mode string) bool { return mode == ipModeV6 || mode == ipModeDual }

func (b *InMemoryBackend) checkAddressConfig(in *createGatewayIn) *APIError {
	if in.IPv4Config != nil && !wantsV4(in.IPMode) {
		return errParam("ipv4AddressConfiguration cannot be set when ipMode is IPV6")
	}

	if in.IPv6Config != nil && !wantsV6(in.IPMode) {
		return errParam("ipv6AddressConfiguration requires ipMode IPV6 or DUAL_STACK")
	}

	if in.IPv4Config != nil && len(in.IPv4Config.ReservedIPIDs) > 0 && in.EndpointType != endpointPublic {
		return errParam("reservedIpIds are only allowed on a PUBLIC gateway")
	}

	return nil
}

func (b *InMemoryBackend) checkSubnet(in *createGatewayIn) *APIError {
	switch {
	case b.network != nil && b.network.Subnet != nil:
		info, ok := b.network.Subnet(in.SubnetID)
		if !ok {
			return errNotFound(in.SubnetID)
		}

		if !info.Regional {
			return errParam("subnetId must be a regional subnet")
		}

		if in.EndpointType == endpointPublic && !info.Public {
			return errParam("a PUBLIC gateway must be in a public regional subnet")
		}
	case b.subnet != nil && !b.subnet(in.SubnetID):
		return errNotFound(in.SubnetID)
	}

	return nil
}

func (b *InMemoryBackend) allocateAddresses(in *createGatewayIn, name string) (Allocation, []string, *APIError) {
	if b.network == nil || b.network.Allocate == nil {
		return Allocation{}, nil, nil
	}

	req := AddressRequest{
		CompartmentID: in.CompartmentID, SubnetID: in.SubnetID, DisplayName: name,
		Public: in.EndpointType == endpointPublic && wantsV4(in.IPMode),
	}

	if in.IPv4Config != nil {
		req.ReservedIPIDs = in.IPv4Config.ReservedIPIDs
	}

	if in.IPv6Config != nil {
		req.IPv6Addresses, req.IPv6SubnetCidrs = in.IPv6Config.Addresses, in.IPv6Config.SubnetCidrs
	}

	req.AssignIPv6 = wantsV6(in.IPMode) && len(req.IPv6Addresses)+len(req.IPv6SubnetCidrs) == 0

	if len(req.ReservedIPIDs) > 0 {
		req.Public = false
	}

	alloc, err := b.network.Allocate(req)
	if err != nil {
		if ae, ok := errors.AsType[*APIError](err); ok {
			return Allocation{}, nil, ae
		}

		return Allocation{}, nil, newErr(http.StatusInternalServerError, "InternalServerError", err.Error())
	}

	var ips []string

	if wantsV4(in.IPMode) {
		if in.EndpointType == endpointPublic && len(alloc.PublicIPs) > 0 {
			ips = append(ips, alloc.PublicIPs...)
		} else {
			ips = append(ips, alloc.PrivateIP)
		}
	}

	if wantsV6(in.IPMode) {
		ips = append(ips, alloc.IPv6Addresses...)
	}

	return alloc, slices.Clip(ips), nil
}

func (b *InMemoryBackend) releaseAddresses(g *Gateway) {
	if g.VnicID != "" && b.network != nil && b.network.Release != nil {
		b.network.Release(g.VnicID)
	}

	g.VnicID = ""
}
