package ec2

import (
	"fmt"
	"net"
	"strings"
)

func (s RouteSpec) destination() string {
	switch {
	case s.DestinationCIDR != "":
		return s.DestinationCIDR
	case s.DestinationIPv6CIDR != "":
		return s.DestinationIPv6CIDR
	default:
		return s.DestinationPrefixListID
	}
}

func routeDestination(r Route) string {
	return RouteSpec{
		DestinationCIDR:         r.DestinationCIDR,
		DestinationIPv6CIDR:     r.DestinationIPv6CIDR,
		DestinationPrefixListID: r.DestinationPrefixListID,
	}.destination()
}

func (s RouteSpec) validateDestination() error {
	set := 0

	for _, v := range []string{s.DestinationCIDR, s.DestinationIPv6CIDR, s.DestinationPrefixListID} {
		if v != "" {
			set++
		}
	}

	if set != 1 {
		return fmt.Errorf(
			"%w: exactly one of DestinationCidrBlock, DestinationIpv6CidrBlock or DestinationPrefixListId is required",
			ErrInvalidParameter,
		)
	}

	if err := validateRouteCIDR(s.DestinationCIDR, false); err != nil {
		return err
	}

	return validateRouteCIDR(s.DestinationIPv6CIDR, true)
}

func (s RouteSpec) route(origin string) Route {
	r := Route{
		DestinationCIDR:             s.DestinationCIDR,
		DestinationIPv6CIDR:         s.DestinationIPv6CIDR,
		DestinationPrefixListID:     s.DestinationPrefixListID,
		GatewayID:                   s.GatewayID,
		NatGatewayID:                s.NatGatewayID,
		InstanceID:                  s.InstanceID,
		NetworkInterfaceID:          s.NetworkInterfaceID,
		VpcPeeringConnectionID:      s.VpcPeeringConnectionID,
		TransitGatewayID:            s.TransitGatewayID,
		EgressOnlyInternetGatewayID: s.EgressOnlyInternetGatewayID,
		LocalGatewayID:              s.LocalGatewayID,
		CarrierGatewayID:            s.CarrierGatewayID,
		CoreNetworkArn:              s.CoreNetworkArn,
		State:                       stateActive,
		Origin:                      origin,
	}

	if s.GatewayID == "" && s.VpcEndpointID != "" {
		r.GatewayID = s.VpcEndpointID
	}

	if s.LocalTarget {
		r.GatewayID, r.NatGatewayID = routeGatewayLocal, ""
	}

	return r
}

// validateRouteTargetLocked checks the gateway-style targets that can be resolved locally.
func (b *InMemoryBackend) validateRouteTargetLocked(rt *RouteTable, s RouteSpec) error {
	switch gw := s.GatewayID; {
	case strings.HasPrefix(gw, "igw-"):
		igw, ok := b.internetGateways.Get(gw)
		if !ok {
			return fmt.Errorf("%w: The gateway ID '%s' does not exist", ErrGatewayIDNotFound, gw)
		}

		for _, att := range igw.Attachments {
			if att.VPCID == rt.VPCID {
				return nil
			}
		}

		return fmt.Errorf("%w: route table %s and network gateway %s belong to different networks",
			ErrInvalidParameter, rt.ID, gw)
	case strings.HasPrefix(gw, "vgw-"):
		if _, ok := b.vpnGateways.Get(gw); !ok {
			return fmt.Errorf("%w: The gateway ID '%s' does not exist", ErrGatewayIDNotFound, gw)
		}
	}

	if s.NatGatewayID != "" {
		if _, ok := b.natGateways.Get(s.NatGatewayID); !ok {
			return fmt.Errorf("%w: %s", ErrNatGatewayNotFound, s.NatGatewayID)
		}
	}

	return nil
}

// CreateRouteSpec adds a route to a route table.
func (b *InMemoryBackend) CreateRouteSpec(rtID string, spec RouteSpec) error {
	b.mu.Lock("CreateRoute")
	defer b.mu.Unlock()

	rt, ok := b.routeTables.Get(rtID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRouteTableNotFound, rtID)
	}

	if err := spec.validateDestination(); err != nil {
		return err
	}

	dest := spec.destination()
	for _, r := range rt.Routes {
		if routeDestination(r) == dest {
			return fmt.Errorf("%w: route identified by %s already exists", ErrRouteAlreadyExists, dest)
		}
	}

	if err := b.validateRouteTargetLocked(rt, spec); err != nil {
		return err
	}

	rt.Routes = append(rt.Routes, spec.route(routeOriginCreateRoute))

	return nil
}

// DeleteRouteSpec removes the route identified by the destination in spec.
func (b *InMemoryBackend) DeleteRouteSpec(rtID string, spec RouteSpec) error {
	b.mu.Lock("DeleteRoute")
	defer b.mu.Unlock()

	rt, ok := b.routeTables.Get(rtID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRouteTableNotFound, rtID)
	}

	if err := spec.validateDestination(); err != nil {
		return err
	}

	dest := spec.destination()

	for i, r := range rt.Routes {
		if routeDestination(r) != dest {
			continue
		}

		if r.GatewayID == routeGatewayLocal {
			return fmt.Errorf("%w: cannot remove local route %s in route table %s", ErrInvalidParameter, dest, rtID)
		}

		rt.Routes = append(rt.Routes[:i], rt.Routes[i+1:]...)

		return nil
	}

	return fmt.Errorf("%w: no route with destination %s in %s", ErrRouteNotFound, dest, rtID)
}

// ReplaceRouteSpec replaces the target of the existing route identified by spec's destination.
func (b *InMemoryBackend) ReplaceRouteSpec(rtID string, spec RouteSpec) error {
	if rtID == "" {
		return fmt.Errorf("%w: RouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReplaceRoute")
	defer b.mu.Unlock()

	rt, ok := b.routeTables.Get(rtID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRouteTableNotFound, rtID)
	}

	if err := spec.validateDestination(); err != nil {
		return err
	}

	dest := spec.destination()

	for i, r := range rt.Routes {
		if routeDestination(r) != dest {
			continue
		}

		if !spec.LocalTarget {
			if err := b.validateRouteTargetLocked(rt, spec); err != nil {
				return err
			}
		}

		origin := r.Origin
		if origin == "" {
			origin = routeOriginCreateRoute
		}

		rt.Routes[i] = spec.route(origin)

		return nil
	}

	return fmt.Errorf("%w: no route with destination %s in %s", ErrRouteNotFound, dest, rtID)
}

func validateRouteCIDR(cidr string, wantIPv6 bool) error {
	if cidr == "" {
		return nil
	}

	ip, _, err := net.ParseCIDR(cidr)
	if err != nil || (ip.To4() == nil) != wantIPv6 {
		return fmt.Errorf("%w: %q is not a valid CIDR block", ErrInvalidParameter, cidr)
	}

	return nil
}
