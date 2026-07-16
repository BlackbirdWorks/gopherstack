package ec2

import (
	"encoding/xml"
	"net/url"
)

type createTrafficMirrorFilterResponse struct {
	XMLName             xml.Name                `xml:"CreateTrafficMirrorFilterResponse"`
	RequestID           string                  `xml:"requestId"`
	TrafficMirrorFilter trafficMirrorFilterItem `xml:"trafficMirrorFilter"`
}

type describeTrafficMirrorFiltersResponse struct {
	XMLName              xml.Name `xml:"DescribeTrafficMirrorFiltersResponse"`
	RequestID            string   `xml:"requestId"`
	TrafficMirrorFilters struct {
		Items []trafficMirrorFilterItem `xml:"item"`
	} `xml:"trafficMirrorFilterSet"`
}

type trafficMirrorFilterRuleItem struct {
	DestinationPortRange      *trafficMirrorPortRangeItem `xml:"destinationPortRange"`
	SourcePortRange           *trafficMirrorPortRangeItem `xml:"sourcePortRange"`
	TrafficMirrorFilterRuleID string                      `xml:"trafficMirrorFilterRuleId"`
	TrafficMirrorFilterID     string                      `xml:"trafficMirrorFilterId"`
	RuleAction                string                      `xml:"ruleAction,omitempty"`
	TrafficDirection          string                      `xml:"trafficDirection,omitempty"`
	DestinationCidrBlock      string                      `xml:"destinationCidrBlock,omitempty"`
	SourceCidrBlock           string                      `xml:"sourceCidrBlock,omitempty"`
	Description               string                      `xml:"description,omitempty"`
	RuleNumber                int                         `xml:"ruleNumber"`
	Protocol                  int                         `xml:"protocol,omitempty"`
}

type createTrafficMirrorFilterRuleResponse struct {
	XMLName                 xml.Name                    `xml:"CreateTrafficMirrorFilterRuleResponse"`
	RequestID               string                      `xml:"requestId"`
	TrafficMirrorFilterRule trafficMirrorFilterRuleItem `xml:"trafficMirrorFilterRule"`
}

type describeTrafficMirrorFilterRulesResponse struct {
	XMLName                  xml.Name `xml:"DescribeTrafficMirrorFilterRulesResponse"`
	RequestID                string   `xml:"requestId"`
	TrafficMirrorFilterRules struct {
		Items []trafficMirrorFilterRuleItem `xml:"item"`
	} `xml:"trafficMirrorFilterRuleSet"`
}

type trafficMirrorSessionItem struct {
	TrafficMirrorSessionID string `xml:"trafficMirrorSessionId"`
	NetworkInterfaceID     string `xml:"networkInterfaceId,omitempty"`
	OwnerID                string `xml:"ownerId,omitempty"`
	TrafficMirrorTargetID  string `xml:"trafficMirrorTargetId,omitempty"`
	TrafficMirrorFilterID  string `xml:"trafficMirrorFilterId,omitempty"`
	Description            string `xml:"description,omitempty"`
	PacketLength           int    `xml:"packetLength,omitempty"`
	SessionNumber          int    `xml:"sessionNumber,omitempty"`
	VirtualNetworkID       int    `xml:"virtualNetworkId,omitempty"`
}

type createTrafficMirrorSessionResponse struct {
	XMLName              xml.Name                 `xml:"CreateTrafficMirrorSessionResponse"`
	RequestID            string                   `xml:"requestId"`
	TrafficMirrorSession trafficMirrorSessionItem `xml:"trafficMirrorSession"`
}

type describeTrafficMirrorSessionsResponse struct {
	XMLName               xml.Name `xml:"DescribeTrafficMirrorSessionsResponse"`
	RequestID             string   `xml:"requestId"`
	TrafficMirrorSessions struct {
		Items []trafficMirrorSessionItem `xml:"item"`
	} `xml:"trafficMirrorSessionSet"`
}

type trafficMirrorTargetItem struct {
	TrafficMirrorTargetID         string `xml:"trafficMirrorTargetId"`
	NetworkInterfaceID            string `xml:"networkInterfaceId,omitempty"`
	NetworkLoadBalancerArn        string `xml:"networkLoadBalancerArn,omitempty"`
	GatewayLoadBalancerEndpointID string `xml:"gatewayLoadBalancerEndpointId,omitempty"`
	OwnerID                       string `xml:"ownerId,omitempty"`
	Type                          string `xml:"type,omitempty"`
	Description                   string `xml:"description,omitempty"`
}

type createTrafficMirrorTargetResponse struct {
	XMLName             xml.Name                `xml:"CreateTrafficMirrorTargetResponse"`
	RequestID           string                  `xml:"requestId"`
	TrafficMirrorTarget trafficMirrorTargetItem `xml:"trafficMirrorTarget"`
}

type describeTrafficMirrorTargetsResponse struct {
	XMLName              xml.Name `xml:"DescribeTrafficMirrorTargetsResponse"`
	RequestID            string   `xml:"requestId"`
	TrafficMirrorTargets struct {
		Items []trafficMirrorTargetItem `xml:"item"`
	} `xml:"trafficMirrorTargetSet"`
}

type fleetItem struct {
	FleetID                         string `xml:"fleetId"`
	FleetState                      string `xml:"fleetState"`
	FleetType                       string `xml:"fleetType,omitempty"`
	ExcessCapacityTerminationPolicy string `xml:"excessCapacityTerminationPolicy,omitempty"`
	TotalTargetCapacity             int    `xml:"targetCapacitySpecification>totalTargetCapacity"`
}

type fleetErrorItem struct {
	ErrorCode    string `xml:"errorCode,omitempty"`
	ErrorMessage string `xml:"errorMessage,omitempty"`
}

type fleetErrorSet struct {
	Items []fleetErrorItem `xml:"item"`
}

type fleetInstanceIDSet struct {
	Items []string `xml:"item"`
}

type fleetInstanceItem struct {
	InstanceType string             `xml:"instanceType,omitempty"`
	Platform     string             `xml:"platform,omitempty"`
	InstanceIDs  fleetInstanceIDSet `xml:"instanceIds"`
}

type fleetInstanceItemSet struct {
	Items []fleetInstanceItem `xml:"item"`
}

// createFleetResponse matches the AWS CreateFleet response shape:
// fleetId, errors (per-launch-spec failures), and instances (launched set).

func toTrafficMirrorFilterItem(f *TrafficMirrorFilter) trafficMirrorFilterItem {
	item := trafficMirrorFilterItem{
		TrafficMirrorFilterID: f.TrafficMirrorFilterID,
		Description:           f.Description,
		NetworkServices:       f.NetworkServices,
	}

	for _, r := range f.IngressFilterRules {
		item.IngressFilterRules = append(item.IngressFilterRules, toTrafficMirrorFilterRuleItem(r))
	}

	for _, r := range f.EgressFilterRules {
		item.EgressFilterRules = append(item.EgressFilterRules, toTrafficMirrorFilterRuleItem(r))
	}

	return item
}

func (h *Handler) handleCreateTrafficMirrorFilter(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")

	f, err := h.Backend.CreateTrafficMirrorFilter(description)
	if err != nil {
		return nil, err
	}

	return &createTrafficMirrorFilterResponse{
		RequestID:           reqID,
		TrafficMirrorFilter: toTrafficMirrorFilterItem(f),
	}, nil
}

func (h *Handler) handleDeleteTrafficMirrorFilter(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TrafficMirrorFilterId")
	if err := h.Backend.DeleteTrafficMirrorFilter(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorFilterResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeTrafficMirrorFilters(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "TrafficMirrorFilterId")
	filters := h.Backend.DescribeTrafficMirrorFilters(ids)

	resp := &describeTrafficMirrorFiltersResponse{RequestID: reqID}
	for _, f := range filters {
		resp.TrafficMirrorFilters.Items = append(
			resp.TrafficMirrorFilters.Items,
			toTrafficMirrorFilterItem(f),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyTrafficMirrorFilterNetworkServices(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("TrafficMirrorFilterId")
	add := parseMemberList(vals, "AddNetworkService")
	remove := parseMemberList(vals, "RemoveNetworkService")

	if err := h.Backend.ModifyTrafficMirrorFilterNetworkServices(id, add, remove); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTrafficMirrorFilterNetworkServicesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- Traffic Mirror Filter Rule handlers ----

func toTrafficMirrorPortRangeItem(r *TrafficMirrorPortRange) *trafficMirrorPortRangeItem {
	if r == nil {
		return nil
	}

	return &trafficMirrorPortRangeItem{FromPort: r.FromPort, ToPort: r.ToPort}
}

func toTrafficMirrorFilterRuleItem(r *TrafficMirrorFilterRule) trafficMirrorFilterRuleItem {
	return trafficMirrorFilterRuleItem{
		TrafficMirrorFilterRuleID: r.TrafficMirrorFilterRuleID,
		TrafficMirrorFilterID:     r.TrafficMirrorFilterID,
		RuleNumber:                r.RuleNumber,
		RuleAction:                r.RuleAction,
		TrafficDirection:          r.TrafficDirection,
		Protocol:                  r.Protocol,
		DestinationCidrBlock:      r.DestinationCidrBlock,
		SourceCidrBlock:           r.SourceCidrBlock,
		Description:               r.Description,
		DestinationPortRange:      toTrafficMirrorPortRangeItem(r.DestinationPortRange),
		SourcePortRange:           toTrafficMirrorPortRangeItem(r.SourcePortRange),
	}
}

func (h *Handler) handleCreateTrafficMirrorFilterRule(vals url.Values, reqID string) (any, error) {
	filterID := vals.Get("TrafficMirrorFilterId")
	direction := vals.Get("TrafficDirection")
	action := vals.Get("RuleAction")
	srcCIDR := vals.Get("SourceCidrBlock")
	dstCIDR := vals.Get("DestinationCidrBlock")
	description := vals.Get("Description")

	ruleNumber := 0
	parseIntValue(vals.Get("RuleNumber"), &ruleNumber)

	protocol := 0
	parseIntValue(vals.Get("Protocol"), &protocol)

	rule, err := h.Backend.CreateTrafficMirrorFilterRule(
		filterID, direction, action, srcCIDR, dstCIDR, description, ruleNumber, protocol,
		parseTrafficMirrorPortRangePair(vals),
	)
	if err != nil {
		return nil, err
	}

	return &createTrafficMirrorFilterRuleResponse{
		RequestID:               reqID,
		TrafficMirrorFilterRule: toTrafficMirrorFilterRuleItem(rule),
	}, nil
}

// parseTrafficMirrorPortRangePair parses the optional SourcePortRange and
// DestinationPortRange request parameters for a Traffic Mirror filter rule.

// parseTrafficMirrorPortRangePair parses the optional SourcePortRange and
// DestinationPortRange request parameters for a Traffic Mirror filter rule.
func parseTrafficMirrorPortRangePair(vals url.Values) TrafficMirrorPortRangePair {
	var pair TrafficMirrorPortRangePair

	if vals.Get("SourcePortRange.FromPort") != "" || vals.Get("SourcePortRange.ToPort") != "" {
		var from, to int
		parseIntValue(vals.Get("SourcePortRange.FromPort"), &from)
		parseIntValue(vals.Get("SourcePortRange.ToPort"), &to)
		pair.Source = &TrafficMirrorPortRange{FromPort: from, ToPort: to}
	}

	if vals.Get("DestinationPortRange.FromPort") != "" || vals.Get("DestinationPortRange.ToPort") != "" {
		var from, to int
		parseIntValue(vals.Get("DestinationPortRange.FromPort"), &from)
		parseIntValue(vals.Get("DestinationPortRange.ToPort"), &to)
		pair.Destination = &TrafficMirrorPortRange{FromPort: from, ToPort: to}
	}

	return pair
}

func (h *Handler) handleDeleteTrafficMirrorFilterRule(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TrafficMirrorFilterRuleId")
	if err := h.Backend.DeleteTrafficMirrorFilterRule(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorFilterRuleResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeTrafficMirrorFilterRules(
	vals url.Values,
	reqID string,
) (any, error) {
	filterID := vals.Get("TrafficMirrorFilterId")

	rules, err := h.Backend.DescribeTrafficMirrorFilterRules(filterID)
	if err != nil {
		return nil, err
	}

	resp := &describeTrafficMirrorFilterRulesResponse{RequestID: reqID}
	for _, r := range rules {
		resp.TrafficMirrorFilterRules.Items = append(
			resp.TrafficMirrorFilterRules.Items,
			toTrafficMirrorFilterRuleItem(r),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyTrafficMirrorFilterRule(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TrafficMirrorFilterRuleId")
	action := vals.Get("RuleAction")
	description := vals.Get("Description")

	if err := h.Backend.ModifyTrafficMirrorFilterRule(id, action, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTrafficMirrorFilterRuleResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- Traffic Mirror Session handlers ----

func toTrafficMirrorSessionItem(s *TrafficMirrorSession) trafficMirrorSessionItem {
	return trafficMirrorSessionItem{
		TrafficMirrorSessionID: s.TrafficMirrorSessionID,
		NetworkInterfaceID:     s.NetworkInterfaceID,
		OwnerID:                s.OwnerID,
		TrafficMirrorTargetID:  s.TrafficMirrorTargetID,
		TrafficMirrorFilterID:  s.TrafficMirrorFilterID,
		SessionNumber:          s.SessionNumber,
		Description:            s.Description,
		PacketLength:           s.PacketLength,
		VirtualNetworkID:       s.VirtualNetworkID,
	}
}

func (h *Handler) handleCreateTrafficMirrorSession(vals url.Values, reqID string) (any, error) {
	networkInterfaceID := vals.Get("NetworkInterfaceId")
	targetID := vals.Get("TrafficMirrorTargetId")
	filterID := vals.Get("TrafficMirrorFilterId")
	description := vals.Get("Description")

	sessionNumber := 0
	parseIntValue(vals.Get("SessionNumber"), &sessionNumber)

	packetLength := 0
	parseIntValue(vals.Get("PacketLength"), &packetLength)

	s, err := h.Backend.CreateTrafficMirrorSession(
		networkInterfaceID, targetID, filterID, description, sessionNumber, packetLength,
	)
	if err != nil {
		return nil, err
	}

	return &createTrafficMirrorSessionResponse{
		RequestID:            reqID,
		TrafficMirrorSession: toTrafficMirrorSessionItem(s),
	}, nil
}

func (h *Handler) handleDeleteTrafficMirrorSession(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TrafficMirrorSessionId")
	if err := h.Backend.DeleteTrafficMirrorSession(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorSessionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeTrafficMirrorSessions(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "TrafficMirrorSessionId")
	sessions := h.Backend.DescribeTrafficMirrorSessions(ids)

	resp := &describeTrafficMirrorSessionsResponse{RequestID: reqID}
	for _, s := range sessions {
		resp.TrafficMirrorSessions.Items = append(
			resp.TrafficMirrorSessions.Items,
			toTrafficMirrorSessionItem(s),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyTrafficMirrorSession(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TrafficMirrorSessionId")
	targetID := vals.Get("TrafficMirrorTargetId")
	filterID := vals.Get("TrafficMirrorFilterId")
	description := vals.Get("Description")

	if err := h.Backend.ModifyTrafficMirrorSession(id, targetID, filterID, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTrafficMirrorSessionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- Traffic Mirror Target handlers ----

func toTrafficMirrorTargetItem(t *TrafficMirrorTarget) trafficMirrorTargetItem {
	return trafficMirrorTargetItem{
		TrafficMirrorTargetID:         t.TrafficMirrorTargetID,
		NetworkInterfaceID:            t.NetworkInterfaceID,
		NetworkLoadBalancerArn:        t.NetworkLoadBalancerArn,
		GatewayLoadBalancerEndpointID: t.GatewayLoadBalancerEndpointID,
		OwnerID:                       t.OwnerID,
		Type:                          t.Type,
		Description:                   t.Description,
	}
}

func (h *Handler) handleCreateTrafficMirrorTarget(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	nlbArn := vals.Get("NetworkLoadBalancerArn")
	glbEndpointID := vals.Get("GatewayLoadBalancerEndpointId")
	description := vals.Get("Description")

	t, err := h.Backend.CreateTrafficMirrorTarget(niID, nlbArn, description, glbEndpointID)
	if err != nil {
		return nil, err
	}

	return &createTrafficMirrorTargetResponse{
		RequestID:           reqID,
		TrafficMirrorTarget: toTrafficMirrorTargetItem(t),
	}, nil
}

func (h *Handler) handleDeleteTrafficMirrorTarget(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TrafficMirrorTargetId")
	if err := h.Backend.DeleteTrafficMirrorTarget(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorTargetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeTrafficMirrorTargets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "TrafficMirrorTargetId")
	targets := h.Backend.DescribeTrafficMirrorTargets(ids)

	resp := &describeTrafficMirrorTargetsResponse{RequestID: reqID}
	for _, t := range targets {
		resp.TrafficMirrorTargets.Items = append(
			resp.TrafficMirrorTargets.Items,
			toTrafficMirrorTargetItem(t),
		)
	}

	return resp, nil
}

// ---- EC2 Fleet handlers ----

type trafficMirrorPortRangeItem struct {
	FromPort int `xml:"fromPort,omitempty"`
	ToPort   int `xml:"toPort,omitempty"`
}

type trafficMirrorFilterItem struct {
	TrafficMirrorFilterID string                        `xml:"trafficMirrorFilterId"`
	Description           string                        `xml:"description,omitempty"`
	IngressFilterRules    []trafficMirrorFilterRuleItem `xml:"ingressFilterRuleSet>item"`
	EgressFilterRules     []trafficMirrorFilterRuleItem `xml:"egressFilterRuleSet>item"`
	NetworkServices       []string                      `xml:"networkServiceSet>item"`
}

// registerTrafficMirrorOps registers the TrafficMirror operation handlers.
func registerTrafficMirrorOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateTrafficMirrorFilter"] = h.handleCreateTrafficMirrorFilter
	ops["DeleteTrafficMirrorFilter"] = h.handleDeleteTrafficMirrorFilter
	ops["DescribeTrafficMirrorFilters"] = h.handleDescribeTrafficMirrorFilters
	ops["ModifyTrafficMirrorFilterNetworkServices"] = h.handleModifyTrafficMirrorFilterNetworkServices
	ops["CreateTrafficMirrorFilterRule"] = h.handleCreateTrafficMirrorFilterRule
	ops["DeleteTrafficMirrorFilterRule"] = h.handleDeleteTrafficMirrorFilterRule
	ops["DescribeTrafficMirrorFilterRules"] = h.handleDescribeTrafficMirrorFilterRules
	ops["ModifyTrafficMirrorFilterRule"] = h.handleModifyTrafficMirrorFilterRule
	ops["CreateTrafficMirrorSession"] = h.handleCreateTrafficMirrorSession
	ops["DeleteTrafficMirrorSession"] = h.handleDeleteTrafficMirrorSession
	ops["DescribeTrafficMirrorSessions"] = h.handleDescribeTrafficMirrorSessions
	ops["ModifyTrafficMirrorSession"] = h.handleModifyTrafficMirrorSession
	ops["CreateTrafficMirrorTarget"] = h.handleCreateTrafficMirrorTarget
	ops["DeleteTrafficMirrorTarget"] = h.handleDeleteTrafficMirrorTarget
	ops["DescribeTrafficMirrorTargets"] = h.handleDescribeTrafficMirrorTargets
}
