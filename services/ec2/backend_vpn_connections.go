package ec2

import "fmt"

// ModifyVpnConnection moves a VPN connection onto a different VPN Gateway. An empty
// vpnGatewayID leaves the connection's gateway attachment unchanged.
func (b *InMemoryBackend) ModifyVpnConnection(vpnConnectionID, vpnGatewayID string) error {
	if vpnConnectionID == "" {
		return fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpnConnection")
	defer b.mu.Unlock()

	conn, ok := b.vpnConnections.Get(vpnConnectionID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if vpnGatewayID != "" {
		if _, exists := b.vpnGateways.Get(vpnGatewayID); !exists {
			return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vpnGatewayID)
		}

		conn.VpnGatewayID = vpnGatewayID
		conn.TransitGatewayID = ""
	}

	return nil
}

// VpnConnectionRoute represents a static route in a VPN connection.
type VpnConnectionRoute struct {
	VpnConnectionID string `json:"vpnConnectionId,omitempty"`
	DestinationCIDR string `json:"destinationCidrBlock,omitempty"`
	State           string `json:"state,omitempty"`
}

// CreateVpnConnectionRoute adds a static route to a VPN connection.
func (b *InMemoryBackend) CreateVpnConnectionRoute(
	vpnConnectionID, destinationCIDR string,
) (*VpnConnectionRoute, error) {
	if vpnConnectionID == "" || destinationCIDR == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateVpnConnectionRoute")
	defer b.mu.Unlock()

	if _, ok := b.vpnConnections.Get(vpnConnectionID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, vpnConnectionID)
	}

	route := &VpnConnectionRoute{
		VpnConnectionID: vpnConnectionID,
		DestinationCIDR: destinationCIDR,
		State:           stateActive,
	}
	b.vpnConnectionRoutes.Put(route)

	return route, nil
}

// DeleteVpnConnectionRoute removes a static route from a VPN connection.
func (b *InMemoryBackend) DeleteVpnConnectionRoute(vpnConnectionID, destinationCIDR string) error {
	if vpnConnectionID == "" || destinationCIDR == "" {
		return fmt.Errorf(
			"%w: VpnConnectionId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteVpnConnectionRoute")
	defer b.mu.Unlock()

	key := vpnConnectionID + ":" + destinationCIDR
	if _, ok := b.vpnConnectionRoutes.Get(key); !ok {
		return fmt.Errorf("%w: route %s not found", ErrInvalidParameter, destinationCIDR)
	}
	b.vpnConnectionRoutes.Delete(key)

	return nil
}

// ---- ModifyTransitGateway ----
