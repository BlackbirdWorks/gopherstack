package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

type describeAddressesAttributeResponse struct {
	XMLName    xml.Name `xml:"DescribeAddressesAttributeResponse"`
	RequestID  string   `xml:"requestId"`
	AddressSet struct {
		Items []addressAttributeItem `xml:"item"`
	} `xml:"addressSet"`
}

type modifyAddressAttributeResponse struct {
	XMLName   xml.Name             `xml:"ModifyAddressAttributeResponse"`
	RequestID string               `xml:"requestId"`
	Address   addressAttributeItem `xml:"address"`
}

func (h *Handler) handleDescribeAddressesAttribute(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "AllocationId")
	attrs := h.Backend.DescribeAddressesAttribute(ids)

	resp := &describeAddressesAttributeResponse{RequestID: reqID}
	for _, attr := range attrs {
		resp.AddressSet.Items = append(
			resp.AddressSet.Items,
			addressAttributeItem{ //nolint:staticcheck // xml tags differ from backend type
				AllocationID: attr.AllocationID,
				PublicIP:     attr.PublicIP,
				DomainName:   attr.DomainName,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyAddressAttribute(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	domainName := vals.Get("DomainName")
	if err := h.Backend.ModifyAddressAttribute(allocationID, domainName); err != nil {
		return nil, err
	}

	return &modifyAddressAttributeResponse{
		RequestID: reqID,
		Address: addressAttributeItem{
			AllocationID: allocationID,
			DomainName:   domainName,
		},
	}, nil
}

func (h *Handler) handleResetAddressAttribute(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	if err := h.Backend.ResetAddressAttribute(allocationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetAddressAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type enableAddressTransferResponse struct {
	XMLName         xml.Name                  `xml:"EnableAddressTransferResponse"`
	RequestID       string                    `xml:"requestId"`
	AddressTransfer addressTransferDetailItem `xml:"addressTransfer"`
}

type describeAddressTransfersResponse struct {
	XMLName            xml.Name `xml:"DescribeAddressTransfersResponse"`
	RequestID          string   `xml:"requestId"`
	AddressTransferSet struct {
		Items []addressTransferDetailItem `xml:"item"`
	} `xml:"addressTransferSet"`
}

type subnetCidrReservationItem struct {
	SubnetCidrReservationID string `xml:"subnetCidrReservationId"`
	SubnetID                string `xml:"subnetId"`
	Cidr                    string `xml:"cidr"`
	ReservationType         string `xml:"reservationType"`
	Description             string `xml:"description,omitempty"`
	OwnerID                 string `xml:"ownerId"`
	State                   string `xml:"state,omitempty"`
}

func toAddressTransferDetailItem(t *AddressTransfer) addressTransferDetailItem {
	return addressTransferDetailItem{
		AllocationID:        t.AllocationID,
		PublicIP:            t.PublicIP,
		TransferAccountID:   t.TransferAccountID,
		TransferOfferStatus: t.TransferOfferStatus,
		TransferOfferExpiry: t.TransferOfferExpiry.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

func (h *Handler) handleEnableAddressTransfer(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	transferAccountID := vals.Get("TransferAccountId")

	transfer, err := h.Backend.EnableAddressTransfer(allocationID, transferAccountID)
	if err != nil {
		return nil, err
	}

	return &enableAddressTransferResponse{
		RequestID:       reqID,
		AddressTransfer: toAddressTransferDetailItem(transfer),
	}, nil
}

func (h *Handler) handleDisableAddressTransfer(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	if err := h.Backend.DisableAddressTransfer(allocationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableAddressTransferResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeAddressTransfers(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "AllocationId")
	transfers := h.Backend.DescribeAddressTransfers(ids)

	resp := &describeAddressTransfersResponse{RequestID: reqID}
	for _, t := range transfers {
		resp.AddressTransferSet.Items = append(
			resp.AddressTransferSet.Items,
			toAddressTransferDetailItem(t),
		)
	}

	return resp, nil
}

func (h *Handler) handleRestoreAddressToClassic(vals url.Values, reqID string) (any, error) {
	publicIP := vals.Get("PublicIp")
	if err := h.Backend.RestoreAddressToClassic(publicIP); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreAddressToClassicResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type describeMovingAddressesResponse struct {
	XMLName                xml.Name `xml:"DescribeMovingAddressesResponse"`
	RequestID              string   `xml:"requestId"`
	MovingAddressStatusSet struct {
		Items []movingAddressStatusItem `xml:"item"`
	} `xml:"movingAddressStatusSet"`
}

func (h *Handler) handleDescribeMovingAddresses(vals url.Values, reqID string) (any, error) {
	publicIPs := parseMemberList(vals, "PublicIp")

	statuses := h.Backend.DescribeMovingAddresses(publicIPs)

	resp := &describeMovingAddressesResponse{RequestID: reqID}
	for _, st := range statuses {
		resp.MovingAddressStatusSet.Items = append(resp.MovingAddressStatusSet.Items, movingAddressStatusItem{
			PublicIP:   st.PublicIP,
			MoveStatus: st.MoveStatus,
		})
	}

	return resp, nil
}

type moveAddressToVpcResponse struct {
	XMLName      xml.Name `xml:"MoveAddressToVpcResponse"`
	RequestID    string   `xml:"requestId"`
	AllocationID string   `xml:"allocationId,omitempty"`
	Status       string   `xml:"status,omitempty"`
}

func (h *Handler) handleMoveAddressToVpc(vals url.Values, reqID string) (any, error) {
	publicIP := vals.Get("PublicIp")

	addr, err := h.Backend.MoveAddressToVpc(publicIP)
	if err != nil {
		return nil, err
	}

	return &moveAddressToVpcResponse{
		RequestID:    reqID,
		AllocationID: addr.AllocationID,
		Status:       "MoveInProgress",
	}, nil
}

// registerElasticIpsOps registers the ElasticIps operation handlers.
func registerElasticIpsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DescribeAddressesAttribute"] = h.handleDescribeAddressesAttribute
	ops["ModifyAddressAttribute"] = h.handleModifyAddressAttribute
	ops["ResetAddressAttribute"] = h.handleResetAddressAttribute
	ops["EnableAddressTransfer"] = h.handleEnableAddressTransfer
	ops["DisableAddressTransfer"] = h.handleDisableAddressTransfer
	ops["DescribeAddressTransfers"] = h.handleDescribeAddressTransfers
	ops["RestoreAddressToClassic"] = h.handleRestoreAddressToClassic
	ops["DescribeMovingAddresses"] = h.handleDescribeMovingAddresses
	ops["MoveAddressToVpc"] = h.handleMoveAddressToVpc
}

// elasticIpsSupportedOperations lists the operation names registered by
// registerElasticIpsOps, for GetSupportedOperations().
func elasticIpsSupportedOperations() []string {
	return []string{
		"DescribeAddressesAttribute",
		"ModifyAddressAttribute",
		"ResetAddressAttribute",
		"EnableAddressTransfer",
		"DisableAddressTransfer",
		"DescribeAddressTransfers",
		"RestoreAddressToClassic",
		"DescribeMovingAddresses",
		"MoveAddressToVpc",
	}
}

type movingAddressStatusItem struct {
	PublicIP   string `xml:"publicIp"`
	MoveStatus string `xml:"moveStatus,omitempty"`
}

type addressItem struct {
	AllocationID  string          `xml:"allocationId"`
	AssociationID string          `xml:"associationId,omitempty"`
	PublicIP      string          `xml:"publicIp"`
	InstanceID    string          `xml:"instanceId,omitempty"`
	Domain        string          `xml:"domain"`
	TagSet        []simpleTagItem `xml:"tagSet>item"`
}

type addressItemSet struct {
	Items []addressItem `xml:"item"`
}

type describeAddressesResponse struct {
	XMLName      xml.Name       `xml:"DescribeAddressesResponse"`
	Xmlns        string         `xml:"xmlns,attr"`
	RequestID    string         `xml:"requestId"`
	AddressesSet addressItemSet `xml:"addressesSet"`
}

type allocateAddressResponse struct {
	XMLName      xml.Name `xml:"AllocateAddressResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	PublicIP     string   `xml:"publicIp"`
	AllocationID string   `xml:"allocationId"`
	Domain       string   `xml:"domain"`
}

type associateAddressResponse struct {
	XMLName       xml.Name `xml:"AssociateAddressResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	AssociationID string   `xml:"associationId"`
	Return        bool     `xml:"return"`
}

type disassociateAddressResponse struct {
	XMLName   xml.Name `xml:"DisassociateAddressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type releaseAddressResponse struct {
	XMLName   xml.Name `xml:"ReleaseAddressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleAllocateAddress(_ url.Values, reqID string) (any, error) {
	addr, err := h.Backend.AllocateAddress()
	if err != nil {
		return nil, err
	}

	return &allocateAddressResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		PublicIP:     addr.PublicIP,
		AllocationID: addr.AllocationID,
		Domain:       resourceTypeVPC,
	}, nil
}

func (h *Handler) handleAssociateAddress(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	instanceID := vals.Get("InstanceId")
	networkInterfaceID := vals.Get("NetworkInterfaceId")

	if allocationID == "" {
		return nil, fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	// Accept either InstanceId or NetworkInterfaceId as the association target.
	// Real AWS also allows association via NetworkInterfaceId (e.g. for non-instance ENIs).
	targetID := instanceID
	if targetID == "" {
		targetID = networkInterfaceID
	}

	if targetID == "" {
		return nil, fmt.Errorf(
			"%w: InstanceId or NetworkInterfaceId is required",
			ErrInvalidParameter,
		)
	}

	assocID, err := h.Backend.AssociateAddress(allocationID, targetID)
	if err != nil {
		return nil, err
	}

	return &associateAddressResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		Return:        true,
		AssociationID: assocID,
	}, nil
}

func (h *Handler) handleDisassociateAddress(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	if assocID == "" {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateAddress(assocID); err != nil {
		return nil, err
	}

	return &disassociateAddressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleReleaseAddress(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	if allocationID == "" {
		return nil, fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.ReleaseAddress(allocationID); err != nil {
		return nil, err
	}

	return &releaseAddressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeAddresses(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "AllocationId")
	addrs := h.Backend.DescribeAddresses(ids)

	filters := parseEC2Filters(vals)
	addrs = applyAddressFilters(addrs, filters, h.Backend)

	items := make([]addressItem, 0, len(addrs))
	for _, addr := range addrs {
		items = append(items, addressItem{
			AllocationID:  addr.AllocationID,
			AssociationID: addr.AssociationID,
			PublicIP:      addr.PublicIP,
			InstanceID:    addr.InstanceID,
			Domain:        resourceTypeVPC,
			TagSet:        tagItemsFromMap(h.Backend.TagsForResource(addr.AllocationID)),
		})
	}

	return &describeAddressesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		AddressesSet: addressItemSet{Items: items},
	}, nil
}
