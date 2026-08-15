package ec2

import (
	"encoding/xml"
	"net/url"
)

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
	CarrierGatewayID string          `xml:"carrierGatewayId"`
	VpcID            string          `xml:"vpcId,omitempty"`
	State            string          `xml:"state,omitempty"`
	OwnerID          string          `xml:"ownerId,omitempty"`
	TagSet           []simpleTagItem `xml:"tagSet>item"`
}

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

// registerByoipOps registers the Byoip operation handlers.
func registerByoipOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ProvisionByoipCidr"] = h.handleProvisionByoipCidr
	ops["DeprovisionByoipCidr"] = h.handleDeprovisionByoipCidr
	ops["WithdrawByoipCidr"] = h.handleWithdrawByoipCidr
}
