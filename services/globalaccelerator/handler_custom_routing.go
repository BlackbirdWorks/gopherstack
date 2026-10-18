package globalaccelerator

import "context"

func registerCustomRoutingOps(ops map[string]opFunc) {
	ops["CreateCustomRoutingEndpointGroup"] = bind((*Handler).createCustomEndpointGroup)
	ops["DescribeCustomRoutingEndpointGroup"] = bind((*Handler).describeCustomEndpointGroup)
	ops["ListCustomRoutingEndpointGroups"] = bind((*Handler).listCustomEndpointGroups)
	ops["DeleteCustomRoutingEndpointGroup"] = bind((*Handler).deleteCustomEndpointGroup)
	ops["AddCustomRoutingEndpoints"] = bind((*Handler).addCustomEndpoints)
	ops["RemoveCustomRoutingEndpoints"] = bind((*Handler).removeCustomEndpoints)
	ops["AllowCustomRoutingTraffic"] = bind((*Handler).allowTraffic)
	ops["DenyCustomRoutingTraffic"] = bind((*Handler).denyTraffic)
	ops["ListCustomRoutingPortMappings"] = bind((*Handler).listPortMappings)
	ops["ListCustomRoutingPortMappingsByDestination"] = bind((*Handler).listPortMappingsByDestination)
}

func (h *Handler) createCustomEndpointGroup(
	_ context.Context, in *createCustomEndpointGroupReq,
) (*endpointGroupResp, error) {
	dests := make([]Destination, len(in.DestinationConfigurations))
	for i, d := range in.DestinationConfigurations {
		dests[i] = Destination(d)
	}

	g, err := createOnce(h, "CreateCustomRoutingEndpointGroup", in.IdempotencyToken, in,
		func(g *EndpointGroup) string { return g.ARN },
		func(arn string) (*EndpointGroup, error) { return h.Backend.GetEndpointGroup(arn, true) },
		func() (*EndpointGroup, error) {
			return h.Backend.CreateCustomRoutingEndpointGroup(in.ListenerArn,
				CustomGroupInput{Region: in.EndpointGroupRegion, Destinations: dests})
		})
	if err != nil {
		return nil, err
	}

	return &endpointGroupResp{EndpointGroup: groupToWire(g)}, nil
}

func (h *Handler) describeCustomEndpointGroup(_ context.Context, in *endpointGroupArnReq) (*endpointGroupResp, error) {
	return h.describeEndpointGroupOf(in, true)
}

func (h *Handler) listCustomEndpointGroups(_ context.Context, in *listByListenerReq) (*listEndpointGroupsResp, error) {
	return h.listEndpointGroupsOf(in, true)
}

func (h *Handler) deleteCustomEndpointGroup(_ context.Context, in *endpointGroupArnReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteEndpointGroup(in.EndpointGroupArn, true)
}

func (h *Handler) addCustomEndpoints(_ context.Context, in *addEndpointsReq) (*addEndpointsResp, error) {
	g, added, err := h.Backend.AddCustomRoutingEndpoints(
		in.EndpointGroupArn,
		endpointConfigs(in.EndpointConfigurations),
	)
	if err != nil {
		return nil, err
	}

	return &addEndpointsResp{EndpointGroupArn: g.ARN, EndpointDescriptions: endpointDescriptions(added, true)}, nil
}

func (h *Handler) removeCustomEndpoints(_ context.Context, in *removeCustomEndpointsReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.RemoveCustomRoutingEndpoints(in.EndpointGroupArn, in.EndpointIDs)
}

func (h *Handler) changeTraffic(in *trafficReq, state string, all *bool) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.ChangeCustomRoutingTraffic(TrafficChange{
		EndpointGroupARN: in.EndpointGroupArn, EndpointID: in.EndpointID, State: state,
		Addresses: in.Addresses, Ports: in.Ports, All: all != nil && *all,
	})
}

func (h *Handler) allowTraffic(_ context.Context, in *trafficReq) (*emptyResp, error) {
	return h.changeTraffic(in, trafficAllow, in.AllowAll)
}

func (h *Handler) denyTraffic(_ context.Context, in *trafficReq) (*emptyResp, error) {
	return h.changeTraffic(in, trafficDeny, in.DenyAll)
}

func (h *Handler) listPortMappings(_ context.Context, in *portMappingsReq) (*portMappingsResp, error) {
	items, next, err := h.Backend.ListCustomRoutingPortMappings(
		in.AcceleratorArn,
		in.EndpointGroupArn,
		in.NextToken,
		in.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	out := make([]portMappingWire, len(items))
	for i, m := range items {
		out[i] = portMappingWire{
			AcceleratorPort:          m.AccPort,
			DestinationSocketAddress: socketWire{IPAddress: m.IP, Port: m.DstPort},
			DestinationTrafficState:  m.State,
			EndpointGroupArn:         m.GroupARN,
			EndpointID:               m.EndpointID,
			Protocols:                m.Protocols,
		}
	}

	return &portMappingsResp{NextToken: next, PortMappings: out}, nil
}

func (h *Handler) listPortMappingsByDestination(_ context.Context, in *byDestinationReq) (*byDestinationResp, error) {
	pg, err := h.Backend.ListCustomRoutingPortMappingsByDestination(
		in.EndpointID,
		in.DestinationAddress,
		in.NextToken,
		in.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &byDestinationResp{NextToken: pg.Next, DestinationPortMappings: mapPage(pg, destinationMappingToWire)}, nil
}

func destinationMappingToWire(m DestinationMapping) destinationMappingWire {
	socks := make([]socketWire, len(m.AcceleratorIPs))
	for i, ip := range m.AcceleratorIPs {
		socks[i] = socketWire{IPAddress: ip, Port: m.AccPort}
	}

	return destinationMappingWire{
		AcceleratorArn:             m.AcceleratorARN,
		AcceleratorSocketAddresses: socks,
		DestinationSocketAddress:   socketWire{IPAddress: m.IP, Port: m.DstPort},
		DestinationTrafficState:    m.State,
		EndpointGroupArn:           m.GroupARN,
		EndpointGroupRegion:        m.GroupRegion,
		EndpointID:                 m.EndpointID,
		IPAddressType:              ipTypeV4,
	}
}
