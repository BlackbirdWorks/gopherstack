package ec2

import (
	"encoding/xml"
	"maps"
	"net/url"
)

// ---- Registration ----

func registerBatch5Ops(h *Handler, ops map[string]ec2ActionFn) {
	maps.Copy(ops, map[string]ec2ActionFn{
		"CreateTrafficMirrorFilter":                     h.handleCreateTrafficMirrorFilter,
		"DeleteTrafficMirrorFilter":                     h.handleDeleteTrafficMirrorFilter,
		"DescribeTrafficMirrorFilters":                  h.handleDescribeTrafficMirrorFilters,
		"ModifyTrafficMirrorFilterNetworkServices":      h.handleModifyTrafficMirrorFilterNetworkServices,
		"CreateTrafficMirrorFilterRule":                 h.handleCreateTrafficMirrorFilterRule,
		"DeleteTrafficMirrorFilterRule":                 h.handleDeleteTrafficMirrorFilterRule,
		"DescribeTrafficMirrorFilterRules":              h.handleDescribeTrafficMirrorFilterRules,
		"ModifyTrafficMirrorFilterRule":                 h.handleModifyTrafficMirrorFilterRule,
		"CreateTrafficMirrorSession":                    h.handleCreateTrafficMirrorSession,
		"DeleteTrafficMirrorSession":                    h.handleDeleteTrafficMirrorSession,
		"DescribeTrafficMirrorSessions":                 h.handleDescribeTrafficMirrorSessions,
		"ModifyTrafficMirrorSession":                    h.handleModifyTrafficMirrorSession,
		"CreateTrafficMirrorTarget":                     h.handleCreateTrafficMirrorTarget,
		"DeleteTrafficMirrorTarget":                     h.handleDeleteTrafficMirrorTarget,
		"DescribeTrafficMirrorTargets":                  h.handleDescribeTrafficMirrorTargets,
		"CreateFleet":                                   h.handleCreateFleet,
		"DeleteFleets":                                  h.handleDeleteFleets,
		"DescribeFleets":                                h.handleDescribeFleets,
		"ModifyFleet":                                   h.handleModifyFleet,
		"DescribeFleetHistory":                          h.handleDescribeFleetHistory,
		"DescribeFleetInstances":                        h.handleDescribeFleetInstances,
		"CreateNetworkInsightsPath":                     h.handleCreateNetworkInsightsPath,
		"DeleteNetworkInsightsPath":                     h.handleDeleteNetworkInsightsPath,
		"DescribeNetworkInsightsPaths":                  h.handleDescribeNetworkInsightsPaths,
		"StartNetworkInsightsAnalysis":                  h.handleStartNetworkInsightsAnalysis,
		"DeleteNetworkInsightsAnalysis":                 h.handleDeleteNetworkInsightsAnalysis,
		"DescribeNetworkInsightsAnalyses":               h.handleDescribeNetworkInsightsAnalyses,
		"CreateNetworkInsightsAccessScope":              h.handleCreateNetworkInsightsAccessScope,
		"DeleteNetworkInsightsAccessScope":              h.handleDeleteNetworkInsightsAccessScope,
		"DescribeNetworkInsightsAccessScopes":           h.handleDescribeNetworkInsightsAccessScopes,
		"GetNetworkInsightsAccessScopeContent":          h.handleGetNetworkInsightsAccessScopeContent,
		"StartNetworkInsightsAccessScopeAnalysis":       h.handleStartNetworkInsightsAccessScopeAnalysis,
		"DeleteNetworkInsightsAccessScopeAnalysis":      h.handleDeleteNetworkInsightsAccessScopeAnalysis,
		"DescribeNetworkInsightsAccessScopeAnalyses":    h.handleDescribeNetworkInsightsAccessScopeAnalyses,
		"GetNetworkInsightsAccessScopeAnalysisFindings": h.handleGetNetworkInsightsAccessScopeAnalysisFindings,
		"ProvisionByoipCidr":                            h.handleProvisionByoipCidr,
		"DeprovisionByoipCidr":                          h.handleDeprovisionByoipCidr,
		"WithdrawByoipCidr":                             h.handleWithdrawByoipCidr,
		"CreateCarrierGateway":                          h.handleCreateCarrierGateway,
		"DeleteCarrierGateway":                          h.handleDeleteCarrierGateway,
		"DescribeCarrierGateways":                       h.handleDescribeCarrierGateways,
		"DescribeReservedInstances":                     h.handleDescribeReservedInstances,
		"DescribeReservedInstancesOfferings":            h.handleDescribeReservedInstancesOfferings,
		"PurchaseReservedInstancesOffering":             h.handlePurchaseReservedInstancesOffering,
		"CreateReservedInstancesListing":                h.handleCreateReservedInstancesListing,
		"CancelReservedInstancesListing":                h.handleCancelReservedInstancesListing,
		"DescribeReservedInstancesListings":             h.handleDescribeReservedInstancesListings,
		"DescribeReservedInstancesModifications":        h.handleDescribeReservedInstancesModifications,
		"ModifyReservedInstances":                       h.handleModifyReservedInstances,
		"DeleteQueuedReservedInstances":                 h.handleDeleteQueuedReservedInstances,
		"GetReservedInstancesExchangeQuote":             h.handleGetReservedInstancesExchangeQuote,
	})
}

// ---- XML response types ----

type trafficMirrorFilterItem struct {
	TrafficMirrorFilterID string `xml:"trafficMirrorFilterId"`
	Description           string `xml:"description,omitempty"`
}

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
	TrafficMirrorFilterRuleID string `xml:"trafficMirrorFilterRuleId"`
	TrafficMirrorFilterID     string `xml:"trafficMirrorFilterId"`
	RuleAction                string `xml:"ruleAction,omitempty"`
	TrafficDirection          string `xml:"trafficDirection,omitempty"`
	DestinationCidrBlock      string `xml:"destinationCidrBlock,omitempty"`
	SourceCidrBlock           string `xml:"sourceCidrBlock,omitempty"`
	Description               string `xml:"description,omitempty"`
	RuleNumber                int    `xml:"ruleNumber"`
	Protocol                  int    `xml:"protocol,omitempty"`
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
	TrafficMirrorTargetID  string `xml:"trafficMirrorTargetId,omitempty"`
	TrafficMirrorFilterID  string `xml:"trafficMirrorFilterId,omitempty"`
	Description            string `xml:"description,omitempty"`
	SessionNumber          int    `xml:"sessionNumber,omitempty"`
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
	TrafficMirrorTargetID  string `xml:"trafficMirrorTargetId"`
	NetworkInterfaceID     string `xml:"networkInterfaceId,omitempty"`
	NetworkLoadBalancerArn string `xml:"networkLoadBalancerArn,omitempty"`
	Description            string `xml:"description,omitempty"`
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
type createFleetResponse struct {
	XMLName   xml.Name             `xml:"CreateFleetResponse"`
	RequestID string               `xml:"requestId"`
	FleetID   string               `xml:"fleetId"`
	FleetType string               `xml:"type,omitempty"`
	Errors    fleetErrorSet        `xml:"errors"`
	Instances fleetInstanceItemSet `xml:"fleetInstanceSet"`
}

type deleteFleetsResponse struct {
	XMLName                  xml.Name `xml:"DeleteFleetsResponse"`
	RequestID                string   `xml:"requestId"`
	SuccessfulFleetDeletions struct {
		Items []fleetItem `xml:"item"`
	} `xml:"successfulFleetDeletionSet"`
	UnsuccessfulFleetDeletions struct {
		Items []struct{} `xml:"item"`
	} `xml:"unsuccessfulFleetDeletionSet"`
}

type describeFleetsResponse struct {
	XMLName   xml.Name `xml:"DescribeFleetsResponse"`
	RequestID string   `xml:"requestId"`
	FleetSet  struct {
		Items []fleetItem `xml:"item"`
	} `xml:"fleetSet"`
}

type networkInsightsPathItem struct {
	NetworkInsightsPathID  string `xml:"networkInsightsPathId"`
	NetworkInsightsPathArn string `xml:"networkInsightsPathArn,omitempty"`
	SourceID               string `xml:"sourceId,omitempty"`
	DestinationID          string `xml:"destinationId,omitempty"`
	Protocol               string `xml:"protocol,omitempty"`
	DestinationPort        int    `xml:"destinationPort,omitempty"`
}

type createNetworkInsightsPathResponse struct {
	XMLName             xml.Name                `xml:"CreateNetworkInsightsPathResponse"`
	RequestID           string                  `xml:"requestId"`
	NetworkInsightsPath networkInsightsPathItem `xml:"networkInsightsPath"`
}

type describeNetworkInsightsPathsResponse struct {
	XMLName              xml.Name `xml:"DescribeNetworkInsightsPathsResponse"`
	RequestID            string   `xml:"requestId"`
	NetworkInsightsPaths struct {
		Items []networkInsightsPathItem `xml:"item"`
	} `xml:"networkInsightsPathSet"`
}

type networkInsightsAnalysisItem struct {
	NetworkInsightsAnalysisID string `xml:"networkInsightsAnalysisId"`
	NetworkInsightsPathID     string `xml:"networkInsightsPathId,omitempty"`
	Status                    string `xml:"status,omitempty"`
	NetworkPathFound          bool   `xml:"networkPathFound,omitempty"`
}

type startNetworkInsightsAnalysisResponse struct {
	XMLName                 xml.Name                    `xml:"StartNetworkInsightsAnalysisResponse"`
	RequestID               string                      `xml:"requestId"`
	NetworkInsightsAnalysis networkInsightsAnalysisItem `xml:"networkInsightsAnalysis"`
}

type describeNetworkInsightsAnalysesResponse struct {
	XMLName                 xml.Name `xml:"DescribeNetworkInsightsAnalysesResponse"`
	RequestID               string   `xml:"requestId"`
	NetworkInsightsAnalyses struct {
		Items []networkInsightsAnalysisItem `xml:"item"`
	} `xml:"networkInsightsAnalysisSet"`
}

type networkInsightsAccessScopeItem struct {
	NetworkInsightsAccessScopeID  string `xml:"networkInsightsAccessScopeId"`
	NetworkInsightsAccessScopeArn string `xml:"networkInsightsAccessScopeArn,omitempty"`
}

type createNetworkInsightsAccessScopeResponse struct {
	XMLName                    xml.Name                       `xml:"CreateNetworkInsightsAccessScopeResponse"`
	RequestID                  string                         `xml:"requestId"`
	NetworkInsightsAccessScope networkInsightsAccessScopeItem `xml:"networkInsightsAccessScope"`
}

type describeNetworkInsightsAccessScopesResponse struct {
	XMLName                     xml.Name `xml:"DescribeNetworkInsightsAccessScopesResponse"`
	RequestID                   string   `xml:"requestId"`
	NetworkInsightsAccessScopes struct {
		Items []networkInsightsAccessScopeItem `xml:"item"`
	} `xml:"networkInsightsAccessScopeSet"`
}

type getNetworkInsightsAccessScopeContentResponse struct {
	XMLName                    xml.Name                       `xml:"GetNetworkInsightsAccessScopeContentResponse"`
	RequestID                  string                         `xml:"requestId"`
	NetworkInsightsAccessScope networkInsightsAccessScopeItem `xml:"networkInsightsAccessScope"`
}

type networkInsightsAccessScopeAnalysisItem struct {
	NetworkInsightsAccessScopeAnalysisID string `xml:"networkInsightsAccessScopeAnalysisId"`
	NetworkInsightsAccessScopeID         string `xml:"networkInsightsAccessScopeId,omitempty"`
	Status                               string `xml:"status,omitempty"`
	AnalyzedEniCount                     int    `xml:"analyzedEniCount,omitempty"`
}

type startNetworkInsightsAccessScopeAnalysisResponse struct {
	XMLName   xml.Name                               `xml:"StartNetworkInsightsAccessScopeAnalysisResponse"`
	RequestID string                                 `xml:"requestId"`
	Analysis  networkInsightsAccessScopeAnalysisItem `xml:"networkInsightsAccessScopeAnalysis"`
}

type describeNetworkInsightsAccessScopeAnalysesResponse struct {
	XMLName                            xml.Name `xml:"DescribeNetworkInsightsAccessScopeAnalysesResponse"`
	RequestID                          string   `xml:"requestId"`
	NetworkInsightsAccessScopeAnalyses struct {
		Items []networkInsightsAccessScopeAnalysisItem `xml:"item"`
	} `xml:"networkInsightsAccessScopeAnalysisSet"`
}

type getNetworkInsightsAccessScopeAnalysisFindingsResponse struct {
	XMLName        xml.Name `xml:"GetNetworkInsightsAccessScopeAnalysisFindingsResponse"`
	RequestID      string   `xml:"requestId"`
	AnalysisID     string   `xml:"analysisId,omitempty"`
	AnalysisStatus string   `xml:"analysisStatus,omitempty"`
	Findings       struct {
		Items []struct{} `xml:"item"`
	} `xml:"accessScopeAnalysisFindingSet"`
}

type provisionByoipCidrResponse struct {
	XMLName   xml.Name      `xml:"ProvisionByoipCidrResponse"`
	RequestID string        `xml:"requestId"`
	ByoipCidr byoipCidrItem `xml:"byoipCidr"`
}

type deprovisionByoipCidrResponse struct {
	XMLName   xml.Name      `xml:"DeprovisionByoipCidrResponse"`
	RequestID string        `xml:"requestId"`
	ByoipCidr byoipCidrItem `xml:"byoipCidr"`
}

type withdrawByoipCidrResponse struct {
	XMLName   xml.Name      `xml:"WithdrawByoipCidrResponse"`
	RequestID string        `xml:"requestId"`
	ByoipCidr byoipCidrItem `xml:"byoipCidr"`
}

type carrierGatewayItem struct {
	CarrierGatewayID string `xml:"carrierGatewayId"`
	VpcID            string `xml:"vpcId,omitempty"`
	State            string `xml:"state,omitempty"`
	OwnerID          string `xml:"ownerId,omitempty"`
}

type createCarrierGatewayResponse struct {
	XMLName        xml.Name           `xml:"CreateCarrierGatewayResponse"`
	RequestID      string             `xml:"requestId"`
	CarrierGateway carrierGatewayItem `xml:"carrierGateway"`
}

type describeCarrierGatewaysResponse struct {
	XMLName         xml.Name `xml:"DescribeCarrierGatewaysResponse"`
	RequestID       string   `xml:"requestId"`
	CarrierGateways struct {
		Items []carrierGatewayItem `xml:"item"`
	} `xml:"carrierGatewaySet"`
}

type reservedInstanceItem struct {
	ReservedInstancesID string  `xml:"reservedInstancesId"`
	InstanceType        string  `xml:"instanceType,omitempty"`
	AvailabilityZone    string  `xml:"availabilityZone,omitempty"`
	ProductDescription  string  `xml:"productDescription,omitempty"`
	State               string  `xml:"state,omitempty"`
	OfferingType        string  `xml:"offeringType,omitempty"`
	InstanceCount       int     `xml:"instanceCount,omitempty"`
	Duration            int64   `xml:"duration"`
	FixedPrice          float64 `xml:"fixedPrice"`
	UsagePrice          float64 `xml:"usagePrice"`
}

type describeReservedInstancesResponse struct {
	XMLName              xml.Name `xml:"DescribeReservedInstancesResponse"`
	RequestID            string   `xml:"requestId"`
	ReservedInstancesSet struct {
		Items []reservedInstanceItem `xml:"item"`
	} `xml:"reservedInstancesSet"`
}

type reservedInstancesOfferingItem struct {
	ReservedInstancesOfferingID string  `xml:"reservedInstancesOfferingId"`
	InstanceType                string  `xml:"instanceType,omitempty"`
	AvailabilityZone            string  `xml:"availabilityZone,omitempty"`
	ProductDescription          string  `xml:"productDescription,omitempty"`
	OfferingType                string  `xml:"offeringType,omitempty"`
	Duration                    int64   `xml:"duration"`
	FixedPrice                  float64 `xml:"fixedPrice"`
	UsagePrice                  float64 `xml:"usagePrice"`
}

type describeReservedInstancesOfferingsResponse struct {
	XMLName                       xml.Name `xml:"DescribeReservedInstancesOfferingsResponse"`
	RequestID                     string   `xml:"requestId"`
	ReservedInstancesOfferingsSet struct {
		Items []reservedInstancesOfferingItem `xml:"item"`
	} `xml:"reservedInstancesOfferingsSet"`
}

type purchaseReservedInstancesOfferingResponse struct {
	XMLName             xml.Name `xml:"PurchaseReservedInstancesOfferingResponse"`
	RequestID           string   `xml:"requestId"`
	ReservedInstancesID string   `xml:"reservedInstancesId"`
}

type reservedInstancesListingItem struct {
	ReservedInstancesListingID string `xml:"reservedInstancesListingId"`
	ReservedInstancesID        string `xml:"reservedInstancesId,omitempty"`
	Status                     string `xml:"status,omitempty"`
	StatusMessage              string `xml:"statusMessage,omitempty"`
}

type createReservedInstancesListingResponse struct {
	XMLName                      xml.Name `xml:"CreateReservedInstancesListingResponse"`
	RequestID                    string   `xml:"requestId"`
	ReservedInstancesListingsSet struct {
		Items []reservedInstancesListingItem `xml:"item"`
	} `xml:"reservedInstancesListingsSet"`
}

type describeReservedInstancesListingsResponse struct {
	XMLName                      xml.Name `xml:"DescribeReservedInstancesListingsResponse"`
	RequestID                    string   `xml:"requestId"`
	ReservedInstancesListingsSet struct {
		Items []reservedInstancesListingItem `xml:"item"`
	} `xml:"reservedInstancesListingsSet"`
}

type reservedInstancesModificationItem struct {
	ReservedInstancesModificationID string `xml:"reservedInstancesModificationId"`
	Status                          string `xml:"status,omitempty"`
	StatusMessage                   string `xml:"statusMessage,omitempty"`
}

type describeReservedInstancesModificationsResponse struct {
	XMLName                           xml.Name `xml:"DescribeReservedInstancesModificationsResponse"`
	RequestID                         string   `xml:"requestId"`
	ReservedInstancesModificationsSet struct {
		Items []reservedInstancesModificationItem `xml:"item"`
	} `xml:"reservedInstancesModificationsSet"`
}

type modifyReservedInstancesResponse struct {
	XMLName                         xml.Name `xml:"ModifyReservedInstancesResponse"`
	RequestID                       string   `xml:"requestId"`
	ReservedInstancesModificationID string   `xml:"reservedInstancesModificationId"`
}

type getReservedInstancesExchangeQuoteResponse struct {
	XMLName         xml.Name `xml:"GetReservedInstancesExchangeQuoteResponse"`
	RequestID       string   `xml:"requestId"`
	IsValidExchange bool     `xml:"isValidExchange"`
}

// ---- Traffic Mirror Filter handlers ----

func toTrafficMirrorFilterItem(f *TrafficMirrorFilter) trafficMirrorFilterItem {
	return trafficMirrorFilterItem{
		TrafficMirrorFilterID: f.TrafficMirrorFilterID,
		Description:           f.Description,
	}
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
	)
	if err != nil {
		return nil, err
	}

	return &createTrafficMirrorFilterRuleResponse{
		RequestID:               reqID,
		TrafficMirrorFilterRule: toTrafficMirrorFilterRuleItem(rule),
	}, nil
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
		TrafficMirrorTargetID:  s.TrafficMirrorTargetID,
		TrafficMirrorFilterID:  s.TrafficMirrorFilterID,
		SessionNumber:          s.SessionNumber,
		Description:            s.Description,
	}
}

func (h *Handler) handleCreateTrafficMirrorSession(vals url.Values, reqID string) (any, error) {
	networkInterfaceID := vals.Get("NetworkInterfaceId")
	targetID := vals.Get("TrafficMirrorTargetId")
	filterID := vals.Get("TrafficMirrorFilterId")
	description := vals.Get("Description")

	sessionNumber := 0
	parseIntValue(vals.Get("SessionNumber"), &sessionNumber)

	s, err := h.Backend.CreateTrafficMirrorSession(
		networkInterfaceID, targetID, filterID, description, sessionNumber,
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
		TrafficMirrorTargetID:  t.TrafficMirrorTargetID,
		NetworkInterfaceID:     t.NetworkInterfaceID,
		NetworkLoadBalancerArn: t.NetworkLoadBalancerArn,
		Description:            t.Description,
	}
}

func (h *Handler) handleCreateTrafficMirrorTarget(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	nlbArn := vals.Get("NetworkLoadBalancerArn")
	description := vals.Get("Description")

	t, err := h.Backend.CreateTrafficMirrorTarget(niID, nlbArn, description)
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

func toFleetItem(f *Fleet) fleetItem {
	return fleetItem{
		FleetID:                         f.FleetID,
		FleetState:                      f.FleetState,
		FleetType:                       f.FleetType,
		TotalTargetCapacity:             f.TotalTargetCapacity,
		ExcessCapacityTerminationPolicy: f.ExcessCapacityTerminationPolicy,
	}
}

func (h *Handler) handleCreateFleet(vals url.Values, reqID string) (any, error) {
	fleetType := vals.Get("Type")
	if fleetType == "" {
		fleetType = fleetTypeDefault
	}

	totalTarget := 0
	parseIntValue(vals.Get("TargetCapacitySpecification.TotalTargetCapacity"), &totalTarget)

	f, err := h.Backend.CreateFleet(fleetType, totalTarget)
	if err != nil {
		return nil, err
	}

	return &createFleetResponse{
		RequestID: reqID,
		FleetID:   f.FleetID,
		FleetType: fleetType,
		Errors:    fleetErrorSet{Items: []fleetErrorItem{}},
		Instances: fleetInstanceItemSet{Items: []fleetInstanceItem{}},
	}, nil
}

func (h *Handler) handleDeleteFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FleetId")
	deleted := h.Backend.DeleteFleets(ids)

	resp := &deleteFleetsResponse{RequestID: reqID}
	for _, id := range deleted {
		resp.SuccessfulFleetDeletions.Items = append(resp.SuccessfulFleetDeletions.Items, fleetItem{
			FleetID:    id,
			FleetState: "deleted",
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FleetId")
	fleets := h.Backend.DescribeFleets(ids)

	resp := &describeFleetsResponse{RequestID: reqID}
	for _, f := range fleets {
		resp.FleetSet.Items = append(resp.FleetSet.Items, toFleetItem(f))
	}

	return resp, nil
}

func (h *Handler) handleModifyFleet(vals url.Values, reqID string) (any, error) {
	id := vals.Get("FleetId")
	excessPolicy := vals.Get("ExcessCapacityTerminationPolicy")

	totalTarget := 0
	parseIntValue(vals.Get("TargetCapacitySpecification.TotalTargetCapacity"), &totalTarget)

	if err := h.Backend.ModifyFleet(id, totalTarget, excessPolicy); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeFleetHistory(_ url.Values, reqID string) (any, error) {
	type describeFleetHistoryResponse struct {
		XMLName        xml.Name `xml:"DescribeFleetHistoryResponse"`
		RequestID      string   `xml:"requestId"`
		HistoryRecords struct {
			Items []struct{} `xml:"item"`
		} `xml:"historyRecords"`
	}

	return &describeFleetHistoryResponse{RequestID: reqID}, nil
}

func (h *Handler) handleDescribeFleetInstances(_ url.Values, reqID string) (any, error) {
	type describeFleetInstancesResponse struct {
		XMLName         xml.Name `xml:"DescribeFleetInstancesResponse"`
		RequestID       string   `xml:"requestId"`
		ActiveInstances struct {
			Items []struct{} `xml:"item"`
		} `xml:"activeInstanceSet"`
	}

	return &describeFleetInstancesResponse{RequestID: reqID}, nil
}

// ---- Network Insights Path handlers ----

func toNetworkInsightsPathItem(p *NetworkInsightsPath) networkInsightsPathItem {
	return networkInsightsPathItem{
		NetworkInsightsPathID:  p.NetworkInsightsPathID,
		NetworkInsightsPathArn: p.NetworkInsightsPathArn,
		SourceID:               p.SourceID,
		DestinationID:          p.DestinationID,
		Protocol:               p.Protocol,
		DestinationPort:        p.DestinationPort,
	}
}

func (h *Handler) handleCreateNetworkInsightsPath(vals url.Values, reqID string) (any, error) {
	sourceID := vals.Get("SourceId")
	destinationID := vals.Get("DestinationId")
	protocol := vals.Get("Protocol")

	destPort := 0
	parseIntValue(vals.Get("DestinationPort"), &destPort)

	p, err := h.Backend.CreateNetworkInsightsPath(sourceID, destinationID, protocol, destPort)
	if err != nil {
		return nil, err
	}

	return &createNetworkInsightsPathResponse{
		RequestID:           reqID,
		NetworkInsightsPath: toNetworkInsightsPathItem(p),
	}, nil
}

func (h *Handler) handleDeleteNetworkInsightsPath(vals url.Values, reqID string) (any, error) {
	id := vals.Get("NetworkInsightsPathId")
	if err := h.Backend.DeleteNetworkInsightsPath(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsPathResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeNetworkInsightsPaths(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "NetworkInsightsPathId")
	paths := h.Backend.DescribeNetworkInsightsPaths(ids)

	resp := &describeNetworkInsightsPathsResponse{RequestID: reqID}
	for _, p := range paths {
		resp.NetworkInsightsPaths.Items = append(
			resp.NetworkInsightsPaths.Items,
			toNetworkInsightsPathItem(p),
		)
	}

	return resp, nil
}

// ---- Network Insights Analysis handlers ----

func toNetworkInsightsAnalysisItem(a *NetworkInsightsAnalysis) networkInsightsAnalysisItem {
	return networkInsightsAnalysisItem{
		NetworkInsightsAnalysisID: a.NetworkInsightsAnalysisID,
		NetworkInsightsPathID:     a.NetworkInsightsPathID,
		Status:                    a.Status,
		NetworkPathFound:          a.NetworkPathFound,
	}
}

func (h *Handler) handleStartNetworkInsightsAnalysis(vals url.Values, reqID string) (any, error) {
	pathID := vals.Get("NetworkInsightsPathId")

	a, err := h.Backend.StartNetworkInsightsAnalysis(pathID)
	if err != nil {
		return nil, err
	}

	return &startNetworkInsightsAnalysisResponse{
		RequestID:               reqID,
		NetworkInsightsAnalysis: toNetworkInsightsAnalysisItem(a),
	}, nil
}

func (h *Handler) handleDeleteNetworkInsightsAnalysis(vals url.Values, reqID string) (any, error) {
	id := vals.Get("NetworkInsightsAnalysisId")
	if err := h.Backend.DeleteNetworkInsightsAnalysis(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsAnalysisResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeNetworkInsightsAnalyses(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "NetworkInsightsAnalysisId")
	analyses := h.Backend.DescribeNetworkInsightsAnalyses(ids)

	resp := &describeNetworkInsightsAnalysesResponse{RequestID: reqID}
	for _, a := range analyses {
		resp.NetworkInsightsAnalyses.Items = append(
			resp.NetworkInsightsAnalyses.Items,
			toNetworkInsightsAnalysisItem(a),
		)
	}

	return resp, nil
}

// ---- Network Insights Access Scope handlers ----

func toNetworkInsightsAccessScopeItem(
	s *NetworkInsightsAccessScope,
) networkInsightsAccessScopeItem {
	return networkInsightsAccessScopeItem{
		NetworkInsightsAccessScopeID:  s.NetworkInsightsAccessScopeID,
		NetworkInsightsAccessScopeArn: s.NetworkInsightsAccessScopeArn,
	}
}

func (h *Handler) handleCreateNetworkInsightsAccessScope(_ url.Values, reqID string) (any, error) {
	s, err := h.Backend.CreateNetworkInsightsAccessScope()
	if err != nil {
		return nil, err
	}

	return &createNetworkInsightsAccessScopeResponse{
		RequestID:                  reqID,
		NetworkInsightsAccessScope: toNetworkInsightsAccessScopeItem(s),
	}, nil
}

func (h *Handler) handleDeleteNetworkInsightsAccessScope(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("NetworkInsightsAccessScopeId")
	if err := h.Backend.DeleteNetworkInsightsAccessScope(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsAccessScopeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeNetworkInsightsAccessScopes(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "NetworkInsightsAccessScopeId")
	scopes := h.Backend.DescribeNetworkInsightsAccessScopes(ids)

	resp := &describeNetworkInsightsAccessScopesResponse{RequestID: reqID}
	for _, s := range scopes {
		resp.NetworkInsightsAccessScopes.Items = append(
			resp.NetworkInsightsAccessScopes.Items,
			toNetworkInsightsAccessScopeItem(s),
		)
	}

	return resp, nil
}

func (h *Handler) handleGetNetworkInsightsAccessScopeContent(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("NetworkInsightsAccessScopeId")
	scopes := h.Backend.DescribeNetworkInsightsAccessScopes([]string{id})

	if len(scopes) == 0 {
		return nil, ErrNetworkInsightsAccessScopeNF
	}

	return &getNetworkInsightsAccessScopeContentResponse{
		RequestID:                  reqID,
		NetworkInsightsAccessScope: toNetworkInsightsAccessScopeItem(scopes[0]),
	}, nil
}

// ---- Network Insights Access Scope Analysis handlers ----

func toNetworkInsightsAccessScopeAnalysisItem(
	a *NetworkInsightsAccessScopeAnalysis,
) networkInsightsAccessScopeAnalysisItem {
	return networkInsightsAccessScopeAnalysisItem{
		NetworkInsightsAccessScopeAnalysisID: a.NetworkInsightsAccessScopeAnalysisID,
		NetworkInsightsAccessScopeID:         a.NetworkInsightsAccessScopeID,
		Status:                               a.Status,
		AnalyzedEniCount:                     a.AnalyzedEniCount,
	}
}

func (h *Handler) handleStartNetworkInsightsAccessScopeAnalysis(
	vals url.Values,
	reqID string,
) (any, error) {
	scopeID := vals.Get("NetworkInsightsAccessScopeId")

	a, err := h.Backend.StartNetworkInsightsAccessScopeAnalysis(scopeID)
	if err != nil {
		return nil, err
	}

	return &startNetworkInsightsAccessScopeAnalysisResponse{
		RequestID: reqID,
		Analysis:  toNetworkInsightsAccessScopeAnalysisItem(a),
	}, nil
}

func (h *Handler) handleDeleteNetworkInsightsAccessScopeAnalysis(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("NetworkInsightsAccessScopeAnalysisId")
	if err := h.Backend.DeleteNetworkInsightsAccessScopeAnalysis(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsAccessScopeAnalysisResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeNetworkInsightsAccessScopeAnalyses(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "NetworkInsightsAccessScopeAnalysisId")
	analyses := h.Backend.DescribeNetworkInsightsAccessScopeAnalyses(ids)

	resp := &describeNetworkInsightsAccessScopeAnalysesResponse{RequestID: reqID}
	for _, a := range analyses {
		resp.NetworkInsightsAccessScopeAnalyses.Items = append(
			resp.NetworkInsightsAccessScopeAnalyses.Items,
			toNetworkInsightsAccessScopeAnalysisItem(a),
		)
	}

	return resp, nil
}

func (h *Handler) handleGetNetworkInsightsAccessScopeAnalysisFindings(
	vals url.Values,
	reqID string,
) (any, error) {
	analysisID := vals.Get("NetworkInsightsAccessScopeAnalysisId")

	return &getNetworkInsightsAccessScopeAnalysisFindingsResponse{
		RequestID:      reqID,
		AnalysisID:     analysisID,
		AnalysisStatus: "succeeded",
	}, nil
}

// ---- BYOIP handlers ----

func (h *Handler) handleProvisionByoipCidr(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("Cidr")
	description := vals.Get("Description")

	entry, err := h.Backend.ProvisionByoipCidr(cidr, description)
	if err != nil {
		return nil, err
	}

	return &provisionByoipCidrResponse{
		RequestID: reqID,
		ByoipCidr: byoipCidrItem{
			Cidr:          entry.Cidr,
			State:         entry.State,
			StatusMessage: entry.StatusMessage,
		},
	}, nil
}

func (h *Handler) handleDeprovisionByoipCidr(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("Cidr")

	entry, err := h.Backend.DeprovisionByoipCidr(cidr)
	if err != nil {
		return nil, err
	}

	return &deprovisionByoipCidrResponse{
		RequestID: reqID,
		ByoipCidr: byoipCidrItem{
			Cidr:  entry.Cidr,
			State: entry.State,
		},
	}, nil
}

func (h *Handler) handleWithdrawByoipCidr(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("Cidr")

	entry, err := h.Backend.WithdrawByoipCidr(cidr)
	if err != nil {
		return nil, err
	}

	return &withdrawByoipCidrResponse{
		RequestID: reqID,
		ByoipCidr: byoipCidrItem{
			Cidr:  entry.Cidr,
			State: entry.State,
		},
	}, nil
}

// ---- Carrier Gateway handlers ----

func toCarrierGatewayItem(gw *CarrierGateway) carrierGatewayItem {
	return carrierGatewayItem{
		CarrierGatewayID: gw.CarrierGatewayID,
		VpcID:            gw.VpcID,
		State:            gw.State,
		OwnerID:          gw.OwnerID,
	}
}

func (h *Handler) handleCreateCarrierGateway(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")

	gw, err := h.Backend.CreateCarrierGateway(vpcID)
	if err != nil {
		return nil, err
	}

	return &createCarrierGatewayResponse{
		RequestID:      reqID,
		CarrierGateway: toCarrierGatewayItem(gw),
	}, nil
}

func (h *Handler) handleDeleteCarrierGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("CarrierGatewayId")
	if err := h.Backend.DeleteCarrierGateway(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteCarrierGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeCarrierGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CarrierGatewayId")
	gateways := h.Backend.DescribeCarrierGateways(ids)

	resp := &describeCarrierGatewaysResponse{RequestID: reqID}
	for _, gw := range gateways {
		resp.CarrierGateways.Items = append(resp.CarrierGateways.Items, toCarrierGatewayItem(gw))
	}

	return resp, nil
}

// ---- Reserved Instances handlers ----

func toReservedInstanceItem(ri *ReservedInstance) reservedInstanceItem {
	return reservedInstanceItem{
		ReservedInstancesID: ri.ReservedInstancesID,
		InstanceType:        ri.InstanceType,
		AvailabilityZone:    ri.AvailabilityZone,
		InstanceCount:       ri.InstanceCount,
		ProductDescription:  ri.ProductDescription,
		State:               ri.State,
		OfferingType:        ri.OfferingType,
		Duration:            ri.Duration,
		FixedPrice:          ri.FixedPrice,
		UsagePrice:          ri.UsagePrice,
	}
}

func toReservedInstancesOfferingItem(o *ReservedInstancesOffering) reservedInstancesOfferingItem {
	return reservedInstancesOfferingItem{
		ReservedInstancesOfferingID: o.ReservedInstancesOfferingID,
		InstanceType:                o.InstanceType,
		AvailabilityZone:            o.AvailabilityZone,
		ProductDescription:          o.ProductDescription,
		OfferingType:                o.OfferingType,
		Duration:                    o.Duration,
		FixedPrice:                  o.FixedPrice,
		UsagePrice:                  o.UsagePrice,
	}
}

func toReservedInstancesListingItem(l *ReservedInstancesListing) reservedInstancesListingItem {
	return reservedInstancesListingItem{
		ReservedInstancesListingID: l.ReservedInstancesListingID,
		ReservedInstancesID:        l.ReservedInstancesID,
		Status:                     l.Status,
		StatusMessage:              l.StatusMessage,
	}
}

func toReservedInstancesModificationItem(
	m *ReservedInstancesModification,
) reservedInstancesModificationItem {
	return reservedInstancesModificationItem{
		ReservedInstancesModificationID: m.ReservedInstancesModificationID,
		Status:                          m.Status,
		StatusMessage:                   m.StatusMessage,
	}
}

func (h *Handler) handleDescribeReservedInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReservedInstancesId")
	ris := h.Backend.DescribeReservedInstances(ids)

	resp := &describeReservedInstancesResponse{RequestID: reqID}
	for _, ri := range ris {
		resp.ReservedInstancesSet.Items = append(
			resp.ReservedInstancesSet.Items,
			toReservedInstanceItem(ri),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeReservedInstancesOfferings(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceType := vals.Get("InstanceType")
	az := vals.Get("AvailabilityZone")
	productDesc := vals.Get("ProductDescription")

	offerings := h.Backend.DescribeReservedInstancesOfferings(instanceType, az, productDesc)

	resp := &describeReservedInstancesOfferingsResponse{RequestID: reqID}
	for _, o := range offerings {
		resp.ReservedInstancesOfferingsSet.Items = append(
			resp.ReservedInstancesOfferingsSet.Items,
			toReservedInstancesOfferingItem(o),
		)
	}

	return resp, nil
}

func (h *Handler) handlePurchaseReservedInstancesOffering(
	vals url.Values,
	reqID string,
) (any, error) {
	offeringID := vals.Get("ReservedInstancesOfferingId")

	instanceCount := 1
	parseIntValue(vals.Get("InstanceCount"), &instanceCount)

	ri, err := h.Backend.PurchaseReservedInstancesOffering(offeringID, instanceCount)
	if err != nil {
		return nil, err
	}

	return &purchaseReservedInstancesOfferingResponse{
		RequestID:           reqID,
		ReservedInstancesID: ri.ReservedInstancesID,
	}, nil
}

func (h *Handler) handleCreateReservedInstancesListing(vals url.Values, reqID string) (any, error) {
	riID := vals.Get("ReservedInstancesId")

	instanceCount := 1
	parseIntValue(vals.Get("InstanceCount"), &instanceCount)

	listing, err := h.Backend.CreateReservedInstancesListing(riID, instanceCount)
	if err != nil {
		return nil, err
	}

	resp := &createReservedInstancesListingResponse{RequestID: reqID}
	resp.ReservedInstancesListingsSet.Items = append(
		resp.ReservedInstancesListingsSet.Items,
		toReservedInstancesListingItem(listing),
	)

	return resp, nil
}

func (h *Handler) handleCancelReservedInstancesListing(vals url.Values, reqID string) (any, error) {
	id := vals.Get("ReservedInstancesListingId")
	if err := h.Backend.CancelReservedInstancesListing(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelReservedInstancesListingResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeReservedInstancesListings(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ReservedInstancesListingId")
	listings := h.Backend.DescribeReservedInstancesListings(ids)

	resp := &describeReservedInstancesListingsResponse{RequestID: reqID}
	for _, l := range listings {
		resp.ReservedInstancesListingsSet.Items = append(
			resp.ReservedInstancesListingsSet.Items,
			toReservedInstancesListingItem(l),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeReservedInstancesModifications(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ReservedInstancesModificationId")
	mods := h.Backend.DescribeReservedInstancesModifications(ids)

	resp := &describeReservedInstancesModificationsResponse{RequestID: reqID}
	for _, m := range mods {
		resp.ReservedInstancesModificationsSet.Items = append(
			resp.ReservedInstancesModificationsSet.Items,
			toReservedInstancesModificationItem(m),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyReservedInstances(vals url.Values, reqID string) (any, error) {
	riIDs := parseMemberList(vals, "ReservedInstancesId")
	targetInstanceType := vals.Get("ReservedInstancesConfigurationSetItemType.1.InstanceType")

	targetCount := 0
	parseIntValue(
		vals.Get("ReservedInstancesConfigurationSetItemType.1.InstanceCount"),
		&targetCount,
	)

	mod, err := h.Backend.ModifyReservedInstances(riIDs, targetInstanceType, targetCount)
	if err != nil {
		return nil, err
	}

	return &modifyReservedInstancesResponse{
		RequestID:                       reqID,
		ReservedInstancesModificationID: mod.ReservedInstancesModificationID,
	}, nil
}

func (h *Handler) handleDeleteQueuedReservedInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReservedInstancesId")
	h.Backend.DeleteQueuedReservedInstances(ids)

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteQueuedReservedInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleGetReservedInstancesExchangeQuote(_ url.Values, reqID string) (any, error) {
	return &getReservedInstancesExchangeQuoteResponse{
		RequestID:       reqID,
		IsValidExchange: true,
	}, nil
}
