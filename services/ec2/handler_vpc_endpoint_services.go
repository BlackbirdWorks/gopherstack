package ec2

import (
	"net/url"
)

// ---- VPC Endpoint Service Configuration handlers ----

func (h *Handler) handleCreateVpcEndpointServiceConfiguration(
	vals url.Values,
	reqID string,
) (any, error) {
	acceptanceRequiredStr := vals.Get("AcceptanceRequired")
	acceptanceRequired := acceptanceRequiredStr == ec2BooleanTrue

	nlbARNs := parseMemberList(vals, "NetworkLoadBalancerArn")

	cfg, err := h.Backend.CreateVpcEndpointServiceConfiguration(acceptanceRequired, nlbARNs)
	if err != nil {
		return nil, err
	}

	return &createVpcEndpointServiceConfigurationResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		ServiceConfig: toVpcEndpointServiceConfigItem(cfg),
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointServiceConfigurations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ServiceId")
	cfgs := h.Backend.DescribeVpcEndpointServiceConfigurations(ids)

	resp := &describeVpcEndpointServiceConfigurationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, cfg := range cfgs {
		resp.ServiceConfigSet.Items = append(resp.ServiceConfigSet.Items, toVpcEndpointServiceConfigItem(cfg))
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpcEndpointServiceConfigurations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ServiceId")

	if err := h.Backend.DeleteVpcEndpointServiceConfigurations(ids); err != nil {
		return nil, err
	}

	return &deleteVpcEndpointServiceConfigurationsResponse{RequestID: reqID}, nil
}

func (h *Handler) handleModifyVpcEndpointServiceConfiguration(
	vals url.Values,
	reqID string,
) (any, error) {
	svcID := vals.Get("ServiceId")
	acceptanceRequired := vals.Get("AcceptanceRequired") == ec2BooleanTrue

	if err := h.Backend.ModifyVpcEndpointServiceConfiguration(svcID, acceptanceRequired); err != nil {
		return nil, err
	}

	return &modifyVpcEndpointServiceConfigurationResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleStartVpcEndpointServicePrivateDNSVerification(
	vals url.Values,
	reqID string,
) (any, error) {
	svcID := vals.Get("ServiceId")

	if err := h.Backend.StartVpcEndpointServicePrivateDNSVerification(svcID); err != nil {
		return nil, err
	}

	return &startVpcEndpointServicePrivateDNSVerificationResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}
