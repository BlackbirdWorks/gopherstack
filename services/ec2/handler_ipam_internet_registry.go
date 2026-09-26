package ec2

import (
	"encoding/xml"
	"net/url"
)

// ---- IPAM internet registry association handlers ----
//
// Wire element casing verified against ec2@v1.329.0 deserializers.go's
// awsEc2query_deserializeDocumentIpamInternetRegistryAssociation and the per-op
// awsEc2query_deserializeOpDocument<Op>Output functions.

type ipamInternetRegistryAssociationItem struct {
	IpamInternetRegistryAssociationID  string          `xml:"ipamInternetRegistryAssociationId"`
	IpamInternetRegistryAssociationARN string          `xml:"ipamInternetRegistryAssociationArn,omitempty"`
	IpamID                             string          `xml:"ipamId"`
	IpamRegion                         string          `xml:"ipamRegion,omitempty"`
	OrganizationHandle                 string          `xml:"organizationHandle,omitempty"`
	Rir                                string          `xml:"rir,omitempty"`
	Description                        string          `xml:"description,omitempty"`
	OwnerID                            string          `xml:"ownerId,omitempty"`
	State                              string          `xml:"state"`
	StateMessage                       string          `xml:"stateMessage,omitempty"`
	ChildRequestXML                    string          `xml:"childRequestXml,omitempty"`
	TagSet                             []simpleTagItem `xml:"tagSet>item,omitempty"`
}

func (h *Handler) toIpamInternetRegistryAssociationItem(
	a *IpamInternetRegistryAssociation,
) ipamInternetRegistryAssociationItem {
	return ipamInternetRegistryAssociationItem{
		IpamInternetRegistryAssociationID:  a.IpamInternetRegistryAssociationID,
		IpamInternetRegistryAssociationARN: a.IpamInternetRegistryAssociationARN,
		IpamID:                             a.IpamID,
		IpamRegion:                         a.IpamRegion,
		OrganizationHandle:                 a.OrganizationHandle,
		Rir:                                a.Rir,
		Description:                        a.Description,
		OwnerID:                            a.OwnerID,
		State:                              a.State,
		StateMessage:                       a.StateMessage,
		ChildRequestXML:                    a.ChildRequestXML,
		TagSet: tagItemsFromMap(
			h.Backend.TagsForResource(a.IpamInternetRegistryAssociationID),
		),
	}
}

type createIpamInternetRegistryAssociationResponse struct {
	XMLName   xml.Name `xml:"CreateIpamInternetRegistryAssociationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`

	IpamInternetRegistryAssociation ipamInternetRegistryAssociationItem `xml:"ipamInternetRegistryAssociation"`
}

func (h *Handler) handleCreateIpamInternetRegistryAssociation(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.CreateIpamInternetRegistryAssociation(
		vals.Get("IpamId"), vals.Get("OrganizationHandle"), vals.Get("Rir"), vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "ipam-internet-registry-association")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{assoc.IpamInternetRegistryAssociationID}, tags); err != nil {
			return nil, err
		}
	}

	return &createIpamInternetRegistryAssociationResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		IpamInternetRegistryAssociation: h.toIpamInternetRegistryAssociationItem(assoc),
	}, nil
}

type describeIpamInternetRegistryAssociationsResponse struct {
	XMLName                            xml.Name `xml:"DescribeIpamInternetRegistryAssociationsResponse"`
	Xmlns                              string   `xml:"xmlns,attr"`
	RequestID                          string   `xml:"requestId"`
	NextToken                          string   `xml:"nextToken,omitempty"`
	IpamInternetRegistryAssociationSet struct {
		Items []ipamInternetRegistryAssociationItem `xml:"item"`
	} `xml:"ipamInternetRegistryAssociationSet"`
}

func (h *Handler) handleDescribeIpamInternetRegistryAssociations(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamInternetRegistryAssociationId")
	filters := parseEC2Filters(vals)
	assocs := h.Backend.DescribeIpamInternetRegistryAssociations(ids, filters)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(assocs, offset, maxResults)

	resp := &describeIpamInternetRegistryAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}
	for _, a := range page {
		resp.IpamInternetRegistryAssociationSet.Items = append(
			resp.IpamInternetRegistryAssociationSet.Items, h.toIpamInternetRegistryAssociationItem(a),
		)
	}

	return resp, nil
}

type enableIpamInternetRegistryAssociationResponse struct {
	XMLName   xml.Name `xml:"EnableIpamInternetRegistryAssociationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`

	IpamInternetRegistryAssociation ipamInternetRegistryAssociationItem `xml:"ipamInternetRegistryAssociation"`
}

func (h *Handler) handleEnableIpamInternetRegistryAssociation(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.EnableIpamInternetRegistryAssociation(
		vals.Get("IpamInternetRegistryAssociationId"),
		vals.Get("ChildHandle"), vals.Get("ParentBpkiTa"), vals.Get("ParentHandle"),
		vals.Get("RpkiVersion"), vals.Get("ServiceUri"),
	)
	if err != nil {
		return nil, err
	}

	return &enableIpamInternetRegistryAssociationResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		IpamInternetRegistryAssociation: h.toIpamInternetRegistryAssociationItem(assoc),
	}, nil
}

type deleteIpamInternetRegistryAssociationResponse struct {
	XMLName   xml.Name `xml:"DeleteIpamInternetRegistryAssociationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`

	IpamInternetRegistryAssociation ipamInternetRegistryAssociationItem `xml:"ipamInternetRegistryAssociation"`
}

func (h *Handler) handleDeleteIpamInternetRegistryAssociation(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.DeleteIpamInternetRegistryAssociation(vals.Get("IpamInternetRegistryAssociationId"))
	if err != nil {
		return nil, err
	}

	return &deleteIpamInternetRegistryAssociationResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		IpamInternetRegistryAssociation: h.toIpamInternetRegistryAssociationItem(assoc),
	}, nil
}

type ipamInternetRegistryAssociationAsnItem struct {
	Asn            string `xml:"asn"`
	LastObservedAt string `xml:"lastObservedAt,omitempty"`
}

type getIpamInternetRegistryAssociationAsnsResponse struct {
	XMLName                               xml.Name `xml:"GetIpamInternetRegistryAssociationAsnsResponse"`
	Xmlns                                 string   `xml:"xmlns,attr"`
	RequestID                             string   `xml:"requestId"`
	NextToken                             string   `xml:"nextToken,omitempty"`
	IpamInternetRegistryAssociationAsnSet struct {
		Items []ipamInternetRegistryAssociationAsnItem `xml:"item"`
	} `xml:"ipamInternetRegistryAssociationAsnSet"`
}

func (h *Handler) handleGetIpamInternetRegistryAssociationAsns(vals url.Values, reqID string) (any, error) {
	asns, err := h.Backend.GetIpamInternetRegistryAssociationAsns(vals.Get("IpamInternetRegistryAssociationId"))
	if err != nil {
		return nil, err
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(asns, offset, maxResults)

	resp := &getIpamInternetRegistryAssociationAsnsResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}
	for _, a := range page {
		resp.IpamInternetRegistryAssociationAsnSet.Items = append(
			resp.IpamInternetRegistryAssociationAsnSet.Items, ipamInternetRegistryAssociationAsnItem{Asn: a.Asn},
		)
	}

	return resp, nil
}

type ipamInternetRegistryAssociationCidrItem struct {
	Cidr           string `xml:"cidr"`
	LastObservedAt string `xml:"lastObservedAt,omitempty"`
}

type getIpamInternetRegistryAssociationCidrsResponse struct {
	XMLName                                xml.Name `xml:"GetIpamInternetRegistryAssociationCidrsResponse"`
	Xmlns                                  string   `xml:"xmlns,attr"`
	RequestID                              string   `xml:"requestId"`
	NextToken                              string   `xml:"nextToken,omitempty"`
	IpamInternetRegistryAssociationCidrSet struct {
		Items []ipamInternetRegistryAssociationCidrItem `xml:"item"`
	} `xml:"ipamInternetRegistryAssociationCidrSet"`
}

func (h *Handler) handleGetIpamInternetRegistryAssociationCidrs(vals url.Values, reqID string) (any, error) {
	cidrs, err := h.Backend.GetIpamInternetRegistryAssociationCidrs(vals.Get("IpamInternetRegistryAssociationId"))
	if err != nil {
		return nil, err
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(cidrs, offset, maxResults)

	resp := &getIpamInternetRegistryAssociationCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}
	for _, c := range page {
		resp.IpamInternetRegistryAssociationCidrSet.Items = append(
			resp.IpamInternetRegistryAssociationCidrSet.Items, ipamInternetRegistryAssociationCidrItem{Cidr: c.Cidr},
		)
	}

	return resp, nil
}
