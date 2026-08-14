package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

type igwAttachmentItem struct {
	VPCID string `xml:"vpcId"`
	State string `xml:"state"`
}

type igwItem struct {
	InternetGatewayID string              `xml:"internetGatewayId"`
	AttachmentSet     []igwAttachmentItem `xml:"attachmentSet>item"`
	TagSet            []simpleTagItem     `xml:"tagSet>item"`
}

type igwItemSet struct {
	Items []igwItem `xml:"item"`
}

type describeInternetGatewaysResponse struct {
	XMLName            xml.Name   `xml:"DescribeInternetGatewaysResponse"`
	Xmlns              string     `xml:"xmlns,attr"`
	RequestID          string     `xml:"requestId"`
	InternetGatewaySet igwItemSet `xml:"internetGatewaySet"`
}

type createInternetGatewayResponse struct {
	XMLName         xml.Name `xml:"CreateInternetGatewayResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	RequestID       string   `xml:"requestId"`
	InternetGateway igwItem  `xml:"internetGateway"`
}

type deleteInternetGatewayResponse struct {
	XMLName   xml.Name `xml:"DeleteInternetGatewayResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type attachInternetGatewayResponse struct {
	XMLName   xml.Name `xml:"AttachInternetGatewayResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type detachInternetGatewayResponse struct {
	XMLName   xml.Name `xml:"DetachInternetGatewayResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func toIGWItem(igw *InternetGateway, tags map[string]string) igwItem {
	atts := make([]igwAttachmentItem, 0, len(igw.Attachments))
	for _, att := range igw.Attachments {
		atts = append(atts, igwAttachmentItem(att))
	}

	return igwItem{
		InternetGatewayID: igw.ID,
		AttachmentSet:     atts,
		TagSet:            tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateInternetGateway(_ url.Values, reqID string) (any, error) {
	igw, err := h.Backend.CreateInternetGateway()
	if err != nil {
		return nil, err
	}

	return &createInternetGatewayResponse{
		Xmlns:           ec2XMLNS,
		RequestID:       reqID,
		InternetGateway: toIGWItem(igw, nil),
	}, nil
}

func (h *Handler) handleDeleteInternetGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InternetGatewayId")
	if id == "" {
		return nil, fmt.Errorf("%w: InternetGatewayId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteInternetGateway(id); err != nil {
		return nil, err
	}

	return &deleteInternetGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeInternetGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InternetGatewayId")
	igws := h.Backend.DescribeInternetGateways(ids)

	filters := parseEC2Filters(vals)
	igws = applyIGWFilters(igws, filters, h.Backend)

	items := make([]igwItem, 0, len(igws))
	for _, igw := range igws {
		items = append(items, toIGWItem(igw, h.Backend.TagsForResource(igw.ID)))
	}

	return &describeInternetGatewaysResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		InternetGatewaySet: igwItemSet{Items: items},
	}, nil
}

func (h *Handler) handleAttachInternetGateway(vals url.Values, reqID string) (any, error) {
	igwID := vals.Get("InternetGatewayId")
	vpcID := vals.Get("VpcId")

	if igwID == "" || vpcID == "" {
		return nil, fmt.Errorf("%w: InternetGatewayId and VpcId are required", ErrInvalidParameter)
	}

	if err := h.Backend.AttachInternetGateway(igwID, vpcID); err != nil {
		return nil, err
	}

	return &attachInternetGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDetachInternetGateway(vals url.Values, reqID string) (any, error) {
	igwID := vals.Get("InternetGatewayId")
	vpcID := vals.Get("VpcId")

	if igwID == "" || vpcID == "" {
		return nil, fmt.Errorf("%w: InternetGatewayId and VpcId are required", ErrInvalidParameter)
	}

	if err := h.Backend.DetachInternetGateway(igwID, vpcID); err != nil {
		return nil, err
	}

	return &detachInternetGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}
