package outposts

import (
	"context"
	"net/http"
)

func toPrivateConnectivityConfigWire(c *PrivateConnectivityConfig) *privateConnectivityConfigWire {
	vpcs := make([]vpcInformationWire, len(c.VPCs))
	for i, v := range c.VPCs {
		vpcs[i] = vpcInformationWire{VpcId: v.VpcID, VpcEndpointId: v.VpcEndpointID, SubnetIds: v.SubnetIDs}
	}

	return &privateConnectivityConfigWire{
		RoleArn:                   c.RoleARN,
		PrivateConnectivityStatus: c.Status,
		VpcInformationList:        vpcs,
	}
}

func (h *Handler) handleCreatePrivateConnectivityConfig(
	_ context.Context, r *http.Request, body []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)

	var req createPrivateConnectivityConfigRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	vpcs := make([]VpcInformation, len(req.VpcInformationList))
	for i, v := range req.VpcInformationList {
		vpcs[i] = VpcInformation{VpcID: v.VpcId, VpcEndpointID: v.VpcEndpointId, SubnetIDs: v.SubnetIds}
	}

	outpostID, cfg, err := h.Backend.CreatePrivateConnectivityConfig(segs[1], vpcs)
	if err != nil {
		return nil, err
	}

	return marshalResponse(createPrivateConnectivityConfigResponse{
		OutpostID:                 outpostID,
		PrivateConnectivityConfig: toPrivateConnectivityConfigWire(cfg),
	})
}

func (h *Handler) handleGetPrivateConnectivityConfig(
	_ context.Context, r *http.Request, _ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)

	cfg, err := h.Backend.GetPrivateConnectivityConfig(segs[1])
	if err != nil {
		return nil, err
	}

	return marshalResponse(getPrivateConnectivityConfigResponse{
		PrivateConnectivityConfig: toPrivateConnectivityConfigWire(cfg),
	})
}
