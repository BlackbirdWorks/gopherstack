package elb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleApplySecurityGroupsToLoadBalancer(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	sgs := parseMembers(vals, "SecurityGroups.member")

	result, err := h.Backend.ApplySecurityGroupsToLoadBalancer(ctx, name, sgs)
	if err != nil {
		return nil, err
	}

	sgMembers := make([]xmlStringValue, 0, len(result))
	for _, sg := range result {
		sgMembers = append(sgMembers, xmlStringValue{Value: sg})
	}

	return &applySecurityGroupsResponse{
		Xmlns: elbXMLNS,
		Result: applySecurityGroupsResult{
			SecurityGroups: xmlStringValueList{Members: sgMembers},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-applysg-" + name},
	}, nil
}

type applySecurityGroupsResult struct {
	SecurityGroups xmlStringValueList `xml:"SecurityGroups"`
}

type applySecurityGroupsResponse struct {
	XMLName          xml.Name                  `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           applySecurityGroupsResult `xml:"ApplySecurityGroupsToLoadBalancerResult"`
}
