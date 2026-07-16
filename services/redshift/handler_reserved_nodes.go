package redshift

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- Reserved node XML types ----

type xmlReservedNodeOffering struct {
	ReservedNodeOfferingID   string  `xml:"ReservedNodeOfferingId"`
	ReservedNodeOfferingType string  `xml:"ReservedNodeOfferingType,omitempty"`
	NodeType                 string  `xml:"NodeType"`
	CurrencyCode             string  `xml:"CurrencyCode"`
	OfferingType             string  `xml:"OfferingType"`
	Duration                 int     `xml:"Duration"`
	FixedPrice               float64 `xml:"FixedPrice"`
	UsagePrice               float64 `xml:"UsagePrice"`
}

type xmlReservedNodeOfferingList struct {
	Members []xmlReservedNodeOffering `xml:"ReservedNodeOffering"`
}

type xmlReservedNodeList struct {
	Members []xmlReservedNode `xml:"ReservedNode"`
}

// ---- DescribeReservedNodes ----

type describeReservedNodesResponse struct {
	XMLName       xml.Name            `xml:"DescribeReservedNodesResponse"`
	Xmlns         string              `xml:"xmlns,attr"`
	ReservedNodes xmlReservedNodeList `xml:"DescribeReservedNodesResult>ReservedNodes"`
}

func (h *Handler) handleDescribeReservedNodes(vals url.Values) (any, error) {
	nodeID := vals.Get("ReservedNodeId")

	nodes, err := h.Backend.DescribeReservedNodes(nodeID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlReservedNode, 0, len(nodes))
	for _, n := range nodes {
		np := n
		members = append(members, reservedNodeToXML(&np))
	}

	return &describeReservedNodesResponse{
		Xmlns:         redshiftXMLNS,
		ReservedNodes: xmlReservedNodeList{Members: members},
	}, nil
}

func reservedNodeToXML(n *ReservedNode) xmlReservedNode {
	return xmlReservedNode{
		ReservedNodeID:         n.ReservedNodeID,
		ReservedNodeOfferingID: n.ReservedNodeOfferingID,
		NodeType:               n.NodeType,
		CurrencyCode:           n.CurrencyCode,
		State:                  n.State,
		OfferingType:           n.OfferingType,
		Duration:               n.Duration,
		FixedPrice:             n.FixedPrice,
		UsagePrice:             n.UsagePrice,
		NodeCount:              n.NodeCount,
	}
}

// ---- DescribeReservedNodeOfferings ----

type describeReservedNodeOfferingsResponse struct {
	XMLName               xml.Name                    `xml:"DescribeReservedNodeOfferingsResponse"`
	Xmlns                 string                      `xml:"xmlns,attr"`
	ReservedNodeOfferings xmlReservedNodeOfferingList `xml:"DescribeReservedNodeOfferingsResult>ReservedNodeOfferings"`
}

func (h *Handler) handleDescribeReservedNodeOfferings(vals url.Values) (any, error) {
	offeringID := vals.Get("ReservedNodeOfferingId")

	offerings, err := h.Backend.DescribeReservedNodeOfferings(offeringID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlReservedNodeOffering, 0, len(offerings))
	for _, o := range offerings {
		members = append(members, xmlReservedNodeOffering(o))
	}

	return &describeReservedNodeOfferingsResponse{
		Xmlns:                 redshiftXMLNS,
		ReservedNodeOfferings: xmlReservedNodeOfferingList{Members: members},
	}, nil
}

// ---- PurchaseReservedNodeOffering ----

type purchaseReservedNodeOfferingResponse struct {
	XMLName      xml.Name        `xml:"PurchaseReservedNodeOfferingResponse"`
	Xmlns        string          `xml:"xmlns,attr"`
	ReservedNode xmlReservedNode `xml:"PurchaseReservedNodeOfferingResult>ReservedNode"`
}

func (h *Handler) handlePurchaseReservedNodeOffering(vals url.Values) (any, error) {
	offeringID := vals.Get("ReservedNodeOfferingId")
	reservedNodeID := vals.Get("ReservedNodeId")
	nodeCountStr := vals.Get("NodeCount")

	nodeCount := 1

	if nodeCountStr != "" {
		n, err := strconv.Atoi(nodeCountStr)
		if err != nil {
			return nil, fmt.Errorf("%w: NodeCount must be an integer", ErrInvalidParameter)
		}

		nodeCount = n
	}

	node, err := h.Backend.PurchaseReservedNodeOffering(offeringID, reservedNodeID, nodeCount)
	if err != nil {
		return nil, err
	}

	return &purchaseReservedNodeOfferingResponse{
		Xmlns:        redshiftXMLNS,
		ReservedNode: reservedNodeToXML(node),
	}, nil
}

// ---- DescribeReservedNodeExchangeStatus ----

type xmlReservedNodeExchangeStatus struct {
	ExchangeRequestID string `xml:"ReservedNodeExchangeRequestId,omitempty"`
	Status            string `xml:"Status"`
}

type xmlReservedNodeExchangeStatusDetails struct {
	Members []xmlReservedNodeExchangeStatus `xml:"ReservedNodeExchangeStatus"`
}

type xmlReservedNodeExchangeStatusResult struct {
	ReservedNodeExchangeStatusDetails xmlReservedNodeExchangeStatusDetails `xml:"ReservedNodeExchangeStatusDetails"`
}

type describeReservedNodeExchangeStatusResponse struct {
	XMLName xml.Name                            `xml:"DescribeReservedNodeExchangeStatusResponse"`
	Xmlns   string                              `xml:"xmlns,attr"`
	Result  xmlReservedNodeExchangeStatusResult `xml:"DescribeReservedNodeExchangeStatusResult"`
}

func (h *Handler) handleDescribeReservedNodeExchangeStatus(vals url.Values) (any, error) {
	nodeID := vals.Get("ReservedNodeId")

	status, err := h.Backend.DescribeReservedNodeExchangeStatus(nodeID)
	if err != nil {
		return nil, err
	}

	return &describeReservedNodeExchangeStatusResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlReservedNodeExchangeStatusResult{
			ReservedNodeExchangeStatusDetails: xmlReservedNodeExchangeStatusDetails{
				Members: []xmlReservedNodeExchangeStatus{{Status: status}},
			},
		},
	}, nil
}

// ---- GetReservedNodeExchangeOfferings ----

type getReservedNodeExchangeOfferingsResponse struct {
	XMLName               xml.Name                    `xml:"GetReservedNodeExchangeOfferingsResponse"`
	Xmlns                 string                      `xml:"xmlns,attr"`
	ReservedNodeOfferings xmlReservedNodeOfferingList `xml:"GetReservedNodeExchangeOfferingsResult>ReservedNodeOfferings"`
}

func (h *Handler) handleGetReservedNodeExchangeOfferings(vals url.Values) (any, error) {
	nodeID := vals.Get("ReservedNodeId")

	offerings, err := h.Backend.GetReservedNodeExchangeOfferings(nodeID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlReservedNodeOffering, 0, len(offerings))
	for _, o := range offerings {
		members = append(members, xmlReservedNodeOffering(o))
	}

	return &getReservedNodeExchangeOfferingsResponse{
		Xmlns:                 redshiftXMLNS,
		ReservedNodeOfferings: xmlReservedNodeOfferingList{Members: members},
	}, nil
}

// ---- GetReservedNodeExchangeConfigurationOptions ----

type getReservedNodeExchangeConfigurationOptionsResponse struct {
	XMLName xml.Name `xml:"GetReservedNodeExchangeConfigurationOptionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleGetReservedNodeExchangeConfigurationOptions(_ url.Values) (any, error) {
	return &getReservedNodeExchangeConfigurationOptionsResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- AcceptReservedNodeExchange ----

type xmlReservedNode struct {
	ReservedNodeID         string  `xml:"ReservedNodeId"`
	ReservedNodeOfferingID string  `xml:"ReservedNodeOfferingId"`
	NodeType               string  `xml:"NodeType"`
	CurrencyCode           string  `xml:"CurrencyCode"`
	State                  string  `xml:"State"`
	OfferingType           string  `xml:"OfferingType"`
	Duration               int     `xml:"Duration"`
	FixedPrice             float64 `xml:"FixedPrice"`
	UsagePrice             float64 `xml:"UsagePrice"`
	NodeCount              int     `xml:"NodeCount"`
}

type acceptReservedNodeExchangeResponse struct {
	XMLName       xml.Name        `xml:"AcceptReservedNodeExchangeResponse"`
	Xmlns         string          `xml:"xmlns,attr"`
	ExchangedNode xmlReservedNode `xml:"AcceptReservedNodeExchangeResult>ExchangedReservedNode"`
}

func (h *Handler) handleAcceptReservedNodeExchange(vals url.Values) (any, error) {
	reservedNodeID := vals.Get("ReservedNodeId")
	targetOfferingID := vals.Get("TargetReservedNodeOfferingId")

	node, err := h.Backend.AcceptReservedNodeExchange(reservedNodeID, targetOfferingID)
	if err != nil {
		return nil, err
	}

	return &acceptReservedNodeExchangeResponse{
		Xmlns: redshiftXMLNS,
		ExchangedNode: xmlReservedNode{
			ReservedNodeID:         node.ReservedNodeID,
			ReservedNodeOfferingID: node.ReservedNodeOfferingID,
			NodeType:               node.NodeType,
			Duration:               node.Duration,
			FixedPrice:             node.FixedPrice,
			UsagePrice:             node.UsagePrice,
			CurrencyCode:           node.CurrencyCode,
			NodeCount:              node.NodeCount,
			State:                  node.State,
			OfferingType:           node.OfferingType,
		},
	}, nil
}
