package ec2

import (
	"encoding/xml"
	"net/url"
)

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

// networkInsightsAccessScopeContentItem matches the real
// NetworkInsightsAccessScopeContent shape (networkInsightsAccessScopeId plus
// matchPathSet/excludePathSet). This backend does not track match/exclude
// paths, so those lists are always empty; that is a modeling gap, not this
// wrapper-key bug.
type networkInsightsAccessScopeContentItem struct {
	NetworkInsightsAccessScopeID string `xml:"networkInsightsAccessScopeId,omitempty"`
}

type getNetworkInsightsAccessScopeContentResponse struct {
	XMLName                    xml.Name                              `xml:"GetNetworkInsightsAccessScopeContentResponse"`
	RequestID                  string                                `xml:"requestId"`
	NetworkInsightsAccessScope networkInsightsAccessScopeContentItem `xml:"networkInsightsAccessScopeContent"`
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
	AnalysisID     string   `xml:"networkInsightsAccessScopeAnalysisId,omitempty"`
	AnalysisStatus string   `xml:"analysisStatus,omitempty"`
	Findings       struct {
		Items []struct{} `xml:"item"`
	} `xml:"analysisFindingSet"`
}

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
	sourceID := vals.Get("Source")
	destinationID := vals.Get("Destination")
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

type deleteNetworkInsightsPathResponse struct {
	XMLName               xml.Name `xml:"DeleteNetworkInsightsPathResponse"`
	RequestID             string   `xml:"requestId"`
	NetworkInsightsPathID string   `xml:"networkInsightsPathId"`
}

func (h *Handler) handleDeleteNetworkInsightsPath(vals url.Values, reqID string) (any, error) {
	id := vals.Get("NetworkInsightsPathId")
	if err := h.Backend.DeleteNetworkInsightsPath(id); err != nil {
		return nil, err
	}

	return &deleteNetworkInsightsPathResponse{
		RequestID:             reqID,
		NetworkInsightsPathID: id,
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

type deleteNetworkInsightsAnalysisResponse struct {
	XMLName                   xml.Name `xml:"DeleteNetworkInsightsAnalysisResponse"`
	RequestID                 string   `xml:"requestId"`
	NetworkInsightsAnalysisID string   `xml:"networkInsightsAnalysisId"`
}

func (h *Handler) handleDeleteNetworkInsightsAnalysis(vals url.Values, reqID string) (any, error) {
	id := vals.Get("NetworkInsightsAnalysisId")
	if err := h.Backend.DeleteNetworkInsightsAnalysis(id); err != nil {
		return nil, err
	}

	return &deleteNetworkInsightsAnalysisResponse{
		RequestID:                 reqID,
		NetworkInsightsAnalysisID: id,
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

	return &deleteNetworkInsightsAccessScopeResponse{
		RequestID:                    reqID,
		NetworkInsightsAccessScopeID: id,
	}, nil
}

type deleteNetworkInsightsAccessScopeResponse struct {
	XMLName                      xml.Name `xml:"DeleteNetworkInsightsAccessScopeResponse"`
	RequestID                    string   `xml:"requestId"`
	NetworkInsightsAccessScopeID string   `xml:"networkInsightsAccessScopeId"`
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
		RequestID: reqID,
		NetworkInsightsAccessScope: networkInsightsAccessScopeContentItem{
			NetworkInsightsAccessScopeID: scopes[0].NetworkInsightsAccessScopeID,
		},
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

	return &deleteNetworkInsightsAccessScopeAnalysisResponse{
		RequestID:                            reqID,
		NetworkInsightsAccessScopeAnalysisID: id,
	}, nil
}

type deleteNetworkInsightsAccessScopeAnalysisResponse struct {
	XMLName                              xml.Name `xml:"DeleteNetworkInsightsAccessScopeAnalysisResponse"`
	RequestID                            string   `xml:"requestId"`
	NetworkInsightsAccessScopeAnalysisID string   `xml:"networkInsightsAccessScopeAnalysisId"`
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

type enableReachabilityAnalyzerOrgSharingResponse struct {
	XMLName     xml.Name `xml:"EnableReachabilityAnalyzerOrganizationSharingResponse"`
	RequestID   string   `xml:"requestId"`
	ReturnValue bool     `xml:"returnValue"`
}

func (h *Handler) handleEnableReachabilityAnalyzerOrganizationSharing(_ url.Values, reqID string) (any, error) {
	ok := h.Backend.EnableReachabilityAnalyzerOrganizationSharing()

	return &enableReachabilityAnalyzerOrgSharingResponse{
		RequestID:   reqID,
		ReturnValue: ok,
	}, nil
}

type elasticGpuItem struct {
	ElasticGpuID   string `xml:"elasticGpuId,omitempty"`
	InstanceID     string `xml:"instanceId,omitempty"`
	ElasticGpuType string `xml:"elasticGpuType,omitempty"`
}

// registerNetworkInsightsOps registers the NetworkInsights operation handlers.
func registerNetworkInsightsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateNetworkInsightsPath"] = h.handleCreateNetworkInsightsPath
	ops["DeleteNetworkInsightsPath"] = h.handleDeleteNetworkInsightsPath
	ops["DescribeNetworkInsightsPaths"] = h.handleDescribeNetworkInsightsPaths
	ops["StartNetworkInsightsAnalysis"] = h.handleStartNetworkInsightsAnalysis
	ops["DeleteNetworkInsightsAnalysis"] = h.handleDeleteNetworkInsightsAnalysis
	ops["DescribeNetworkInsightsAnalyses"] = h.handleDescribeNetworkInsightsAnalyses
	ops["CreateNetworkInsightsAccessScope"] = h.handleCreateNetworkInsightsAccessScope
	ops["DeleteNetworkInsightsAccessScope"] = h.handleDeleteNetworkInsightsAccessScope
	ops["DescribeNetworkInsightsAccessScopes"] = h.handleDescribeNetworkInsightsAccessScopes
	ops["GetNetworkInsightsAccessScopeContent"] = h.handleGetNetworkInsightsAccessScopeContent
	ops["StartNetworkInsightsAccessScopeAnalysis"] = h.handleStartNetworkInsightsAccessScopeAnalysis
	ops["DeleteNetworkInsightsAccessScopeAnalysis"] = h.handleDeleteNetworkInsightsAccessScopeAnalysis
	ops["DescribeNetworkInsightsAccessScopeAnalyses"] = h.handleDescribeNetworkInsightsAccessScopeAnalyses
	ops["GetNetworkInsightsAccessScopeAnalysisFindings"] = h.handleGetNetworkInsightsAccessScopeAnalysisFindings
	ops["EnableReachabilityAnalyzerOrganizationSharing"] = h.handleEnableReachabilityAnalyzerOrganizationSharing
}

// networkInsightsSupportedOperations lists the operation names registered by
// registerNetworkInsightsOps, for GetSupportedOperations().
func networkInsightsSupportedOperations() []string {
	return []string{
		"EnableReachabilityAnalyzerOrganizationSharing",
	}
}
