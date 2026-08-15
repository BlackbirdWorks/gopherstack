package ec2

import (
	"encoding/xml"
	"net/url"
)

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
	ReservedInstancesID string          `xml:"reservedInstancesId"`
	InstanceType        string          `xml:"instanceType,omitempty"`
	AvailabilityZone    string          `xml:"availabilityZone,omitempty"`
	ProductDescription  string          `xml:"productDescription,omitempty"`
	State               string          `xml:"state,omitempty"`
	OfferingType        string          `xml:"offeringType,omitempty"`
	TagSet              []simpleTagItem `xml:"tagSet>item"`
	InstanceCount       int             `xml:"instanceCount,omitempty"`
	Duration            int64           `xml:"duration"`
	FixedPrice          float64         `xml:"fixedPrice"`
	UsagePrice          float64         `xml:"usagePrice"`
}

func toCarrierGatewayItem(gw *CarrierGateway, tags map[string]string) carrierGatewayItem {
	return carrierGatewayItem{
		CarrierGatewayID: gw.CarrierGatewayID,
		VpcID:            gw.VpcID,
		State:            gw.State,
		OwnerID:          gw.OwnerID,
		TagSet:           tagItemsFromMap(tags),
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
		CarrierGateway: toCarrierGatewayItem(gw, nil),
	}, nil
}

type deleteCarrierGatewayResponse struct {
	XMLName        xml.Name           `xml:"DeleteCarrierGatewayResponse"`
	RequestID      string             `xml:"requestId"`
	CarrierGateway carrierGatewayItem `xml:"carrierGateway"`
}

func (h *Handler) handleDeleteCarrierGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("CarrierGatewayId")
	gw, err := h.Backend.DeleteCarrierGateway(id)
	if err != nil {
		return nil, err
	}

	return &deleteCarrierGatewayResponse{
		RequestID:      reqID,
		CarrierGateway: toCarrierGatewayItem(gw, nil),
	}, nil
}

func (h *Handler) handleDescribeCarrierGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CarrierGatewayId")
	gateways := h.Backend.DescribeCarrierGateways(ids)

	resp := &describeCarrierGatewaysResponse{RequestID: reqID}
	for _, gw := range gateways {
		resp.CarrierGateways.Items = append(
			resp.CarrierGateways.Items, toCarrierGatewayItem(gw, h.Backend.TagsForResource(gw.CarrierGatewayID)),
		)
	}

	return resp, nil
}

// ---- Reserved Instances handlers ----

// registerCarrierGatewaysOps registers the CarrierGateways operation handlers.
func registerCarrierGatewaysOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateCarrierGateway"] = h.handleCreateCarrierGateway
	ops["DeleteCarrierGateway"] = h.handleDeleteCarrierGateway
	ops["DescribeCarrierGateways"] = h.handleDescribeCarrierGateways
}
