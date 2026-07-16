package ec2

import "fmt"

// ReplaceRoute replaces an existing route in a route table.
func (b *InMemoryBackend) ReplaceRoute(rtID, destCIDR, gatewayID, natGatewayID string) error {
	if rtID == "" || destCIDR == "" {
		return fmt.Errorf(
			"%w: RouteTableId and DestinationCidrBlock are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("ReplaceRoute")
	defer b.mu.Unlock()

	rt, ok := b.routeTables.Get(rtID)
	if !ok {
		return fmt.Errorf("%w: route table %s not found", ErrInvalidParameter, rtID)
	}

	for i, route := range rt.Routes {
		if route.DestinationCIDR == destCIDR {
			rt.Routes[i] = Route{
				DestinationCIDR: destCIDR,
				GatewayID:       gatewayID,
				NatGatewayID:    natGatewayID,
				State:           stateActive,
			}

			return nil
		}
	}

	return fmt.Errorf("%w: route %s not found in %s", ErrInvalidParameter, destCIDR, rtID)
}

// ---- RegisterInstanceEventNotificationAttributes ----
