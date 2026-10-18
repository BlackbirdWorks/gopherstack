package globalaccelerator

import "context"

func registerEndpointGroupOps(ops map[string]opFunc) {
	ops["CreateEndpointGroup"] = bind((*Handler).createEndpointGroup)
	ops["DescribeEndpointGroup"] = bind((*Handler).describeEndpointGroup)
	ops["ListEndpointGroups"] = bind((*Handler).listEndpointGroups)
	ops["UpdateEndpointGroup"] = bind((*Handler).updateEndpointGroup)
	ops["DeleteEndpointGroup"] = bind((*Handler).deleteEndpointGroup)
	ops["AddEndpoints"] = bind((*Handler).addEndpoints)
	ops["RemoveEndpoints"] = bind((*Handler).removeEndpoints)
}

func endpointConfigs(in []endpointConfigWire) []EndpointConfig {
	if in == nil {
		return nil
	}

	out := make([]EndpointConfig, len(in))
	for i, c := range in {
		out[i] = EndpointConfig{
			EndpointID: c.EndpointID, Weight: c.Weight, ClientIPPreservation: c.ClientIPPreservationEnabled,
			AttachmentARN: c.AttachmentArn,
		}
	}

	return out
}

func (s endpointGroupSettingsReq) toInput(region string) EndpointGroupInput {
	return EndpointGroupInput{
		Region: region, HealthCheckInterval: s.HealthCheckIntervalSeconds, HealthCheckPath: s.HealthCheckPath,
		HealthCheckPort: s.HealthCheckPort, HealthCheckProtocol: s.HealthCheckProtocol,
		ThresholdCount: s.ThresholdCount, TrafficDial: s.TrafficDialPercentage,
		Endpoints: endpointConfigs(s.EndpointConfigurations), PortOverrides: s.PortOverrides,
	}
}

func (h *Handler) createEndpointGroup(_ context.Context, in *createEndpointGroupReq) (*endpointGroupResp, error) {
	g, err := createOnce(h, "CreateEndpointGroup", in.IdempotencyToken, in,
		func(g *EndpointGroup) string { return g.ARN },
		func(arn string) (*EndpointGroup, error) { return h.Backend.GetEndpointGroup(arn, false) },
		func() (*EndpointGroup, error) {
			return h.Backend.CreateEndpointGroup(in.ListenerArn, in.toInput(in.EndpointGroupRegion))
		})
	if err != nil {
		return nil, err
	}

	return &endpointGroupResp{EndpointGroup: groupToWire(g)}, nil
}

func (h *Handler) describeEndpointGroupOf(in *endpointGroupArnReq, custom bool) (*endpointGroupResp, error) {
	g, err := h.Backend.GetEndpointGroup(in.EndpointGroupArn, custom)
	if err != nil {
		return nil, err
	}

	return &endpointGroupResp{EndpointGroup: groupToWire(g)}, nil
}

func (h *Handler) describeEndpointGroup(_ context.Context, in *endpointGroupArnReq) (*endpointGroupResp, error) {
	return h.describeEndpointGroupOf(in, false)
}

func (h *Handler) listEndpointGroupsOf(in *listByListenerReq, custom bool) (*listEndpointGroupsResp, error) {
	pg, err := h.Backend.ListEndpointGroups(in.ListenerArn, custom, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listEndpointGroupsResp{NextToken: pg.Next, EndpointGroups: mapPage(pg, groupToWire)}, nil
}

func (h *Handler) listEndpointGroups(_ context.Context, in *listByListenerReq) (*listEndpointGroupsResp, error) {
	return h.listEndpointGroupsOf(in, false)
}

func (h *Handler) updateEndpointGroup(_ context.Context, in *updateEndpointGroupReq) (*endpointGroupResp, error) {
	g, err := h.Backend.UpdateEndpointGroup(in.EndpointGroupArn, in.toInput(""))
	if err != nil {
		return nil, err
	}

	return &endpointGroupResp{EndpointGroup: groupToWire(g)}, nil
}

func (h *Handler) deleteEndpointGroup(_ context.Context, in *endpointGroupArnReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteEndpointGroup(in.EndpointGroupArn, false)
}

func (h *Handler) addEndpoints(_ context.Context, in *addEndpointsReq) (*addEndpointsResp, error) {
	g, added, err := h.Backend.AddEndpoints(in.EndpointGroupArn, endpointConfigs(in.EndpointConfigurations))
	if err != nil {
		return nil, err
	}

	return &addEndpointsResp{EndpointGroupArn: g.ARN, EndpointDescriptions: endpointDescriptions(added, false)}, nil
}

func (h *Handler) removeEndpoints(_ context.Context, in *removeEndpointsReq) (*emptyResp, error) {
	ids := make([]string, len(in.EndpointIdentifiers))
	for i, e := range in.EndpointIdentifiers {
		ids[i] = e.EndpointID
	}

	return &emptyResp{}, h.Backend.RemoveEndpoints(in.EndpointGroupArn, ids)
}
