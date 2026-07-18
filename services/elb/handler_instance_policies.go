package elb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleSetLoadBalancerPoliciesForBackendServer(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instancePort, err := parseInt32(vals.Get("InstancePort"))
	if err != nil || instancePort == 0 {
		return nil, fmt.Errorf("%w: InstancePort is required", ErrInvalidParameter)
	}

	policyNames := parseMembers(vals, "PolicyNames.member")

	setErr := h.Backend.SetLoadBalancerPoliciesForBackendServer(ctx, name, instancePort, policyNames)
	if setErr != nil {
		return nil, setErr
	}

	return &setLoadBalancerPoliciesForBackendServerResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-backendpolicies-" + name},
	}, nil
}

type setLoadBalancerPoliciesForBackendServerResult struct{}

type setLoadBalancerPoliciesForBackendServerResponse struct {
	XMLName          xml.Name                                      `xml:"SetLoadBalancerPoliciesForBackendServerResponse"`
	Xmlns            string                                        `xml:"xmlns,attr"`
	Result           setLoadBalancerPoliciesForBackendServerResult `xml:"SetLoadBalancerPoliciesForBackendServerResult"`
	ResponseMetadata xmlResponseMetadata                           `xml:"ResponseMetadata"`
}
