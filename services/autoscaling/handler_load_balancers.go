package autoscaling

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleAttachLoadBalancerTargetGroups(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	targetGroupARNs := parseMembers(vals, "TargetGroupARNs.member")

	if err := h.Backend.AttachLoadBalancerTargetGroups(groupName, targetGroupARNs); err != nil {
		return nil, err
	}

	return &attachLoadBalancerTargetGroupsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-tgs"},
	}, nil
}

func (h *Handler) handleAttachLoadBalancers(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	lbNames := parseMembers(vals, "LoadBalancerNames.member")

	if err := h.Backend.AttachLoadBalancers(groupName, lbNames); err != nil {
		return nil, err
	}

	return &attachLoadBalancersResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-lbs"},
	}, nil
}

type attachLoadBalancerTargetGroupsResponse struct {
	XMLName          xml.Name            `xml:"AttachLoadBalancerTargetGroupsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type attachLoadBalancersResponse struct {
	XMLName          xml.Name            `xml:"AttachLoadBalancersResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDescribeLoadBalancers(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	lbs, err := h.Backend.DescribeLoadBalancers(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLoadBalancerState, 0, len(lbs))
	for _, lb := range lbs {
		members = append(members, xmlLoadBalancerState(lb))
	}

	return &describeLoadBalancersResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLoadBalancersResult{
			LoadBalancers: xmlLoadBalancerStateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-load-balancers"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerTargetGroups(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	tgs, err := h.Backend.DescribeLoadBalancerTargetGroups(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLoadBalancerTargetGroupState, 0, len(tgs))
	for _, tg := range tgs {
		members = append(members, xmlLoadBalancerTargetGroupState(tg))
	}

	return &describeLoadBalancerTargetGroupsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLoadBalancerTargetGroupsResult{
			LoadBalancerTargetGroups: xmlLoadBalancerTargetGroupStateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lb-target-groups"},
	}, nil
}

func (h *Handler) handleDetachLoadBalancerTargetGroups(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	targetGroupARNs := parseMembers(vals, "TargetGroupARNs.member")

	if err := h.Backend.DetachLoadBalancerTargetGroups(groupName, targetGroupARNs); err != nil {
		return nil, err
	}

	return &detachLoadBalancerTargetGroupsResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-lb-target-groups"},
	}, nil
}

func (h *Handler) handleDetachLoadBalancers(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	lbNames := parseMembers(vals, "LoadBalancerNames.member")

	if err := h.Backend.DetachLoadBalancers(groupName, lbNames); err != nil {
		return nil, err
	}

	return &detachLoadBalancersResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-load-balancers"},
	}, nil
}

type xmlLoadBalancerState struct {
	LoadBalancerName string `xml:"LoadBalancerName"`
	State            string `xml:"State"`
}

type xmlLoadBalancerStateList struct {
	Members []xmlLoadBalancerState `xml:"member"`
}

type describeLoadBalancersResult struct {
	LoadBalancers xmlLoadBalancerStateList `xml:"LoadBalancers"`
}

type describeLoadBalancersResponse struct {
	XMLName          xml.Name                    `xml:"DescribeLoadBalancersResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeLoadBalancersResult `xml:"DescribeLoadBalancersResult"`
}

type xmlLoadBalancerTargetGroupState struct {
	LoadBalancerTargetGroupARN string `xml:"LoadBalancerTargetGroupARN"`
	State                      string `xml:"State"`
}

type xmlLoadBalancerTargetGroupStateList struct {
	Members []xmlLoadBalancerTargetGroupState `xml:"member"`
}

type describeLoadBalancerTargetGroupsResult struct {
	LoadBalancerTargetGroups xmlLoadBalancerTargetGroupStateList `xml:"LoadBalancerTargetGroups"`
}

type describeLoadBalancerTargetGroupsResponse struct {
	XMLName          xml.Name                               `xml:"DescribeLoadBalancerTargetGroupsResponse"`
	Xmlns            string                                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                    `xml:"ResponseMetadata"`
	Result           describeLoadBalancerTargetGroupsResult `xml:"DescribeLoadBalancerTargetGroupsResult"`
}

type detachLoadBalancerTargetGroupsResponse struct {
	XMLName          xml.Name            `xml:"DetachLoadBalancerTargetGroupsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type detachLoadBalancersResponse struct {
	XMLName          xml.Name            `xml:"DetachLoadBalancersResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
