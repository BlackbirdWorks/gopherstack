package autoscaling

import (
	"encoding/xml"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultLBMaxRecords is DescribeLoadBalancers's and DescribeLoadBalancerTargetGroups's
// documented default/max page size (api_op_DescribeLoadBalancers.go /
// api_op_DescribeLoadBalancerTargetGroups.go: "The default value is 100 and the maximum value
// is 100" -- default equals max for both operations).
const defaultLBMaxRecords = 100

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
	Result           emptyResultXML      `xml:"AttachLoadBalancerTargetGroupsResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type attachLoadBalancersResponse struct {
	XMLName          xml.Name            `xml:"AttachLoadBalancersResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	Result           emptyResultXML      `xml:"AttachLoadBalancersResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

//nolint:dupl // DescribeLoadBalancers and DescribeLoadBalancerTargetGroups share list-pagination structure
func (h *Handler) handleDescribeLoadBalancers(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	lbs, err := h.Backend.DescribeLoadBalancers(groupName)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultLBMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), defaultLBMaxRecords)
		}
	}

	p := page.New(lbs, vals.Get("NextToken"), maxRecords, defaultLBMaxRecords)

	members := make([]xmlLoadBalancerState, 0, len(p.Data))
	for _, lb := range p.Data {
		members = append(members, xmlLoadBalancerState(lb))
	}

	return &describeLoadBalancersResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLoadBalancersResult{
			NextToken:     p.Next,
			LoadBalancers: xmlLoadBalancerStateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-load-balancers"},
	}, nil
}

//nolint:dupl // DescribeLoadBalancers and DescribeLoadBalancerTargetGroups share list-pagination structure
func (h *Handler) handleDescribeLoadBalancerTargetGroups(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	tgs, err := h.Backend.DescribeLoadBalancerTargetGroups(groupName)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultLBMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), defaultLBMaxRecords)
		}
	}

	p := page.New(tgs, vals.Get("NextToken"), maxRecords, defaultLBMaxRecords)

	members := make([]xmlLoadBalancerTargetGroupState, 0, len(p.Data))
	for _, tg := range p.Data {
		members = append(members, xmlLoadBalancerTargetGroupState(tg))
	}

	return &describeLoadBalancerTargetGroupsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLoadBalancerTargetGroupsResult{
			NextToken:                p.Next,
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
	NextToken     string                   `xml:"NextToken,omitempty"`
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
	NextToken                string                              `xml:"NextToken,omitempty"`
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
	Result           emptyResultXML      `xml:"DetachLoadBalancerTargetGroupsResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type detachLoadBalancersResponse struct {
	XMLName          xml.Name            `xml:"DetachLoadBalancersResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	Result           emptyResultXML      `xml:"DetachLoadBalancersResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
