package ec2

import (
	"encoding/xml"
	"net/url"
)

// handler_secondary_net.go implements the HTTP handlers for the Secondary
// Network / Secondary Subnet / Secondary Interface family plus the read-only
// Outpost LAG / Service Link Virtual Interface family, backed by
// secondary_net.go.

func registerSecondaryNetOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateSecondaryNetwork"] = h.handleCreateSecondaryNetwork
	ops["DeleteSecondaryNetwork"] = h.handleDeleteSecondaryNetwork
	ops["DescribeSecondaryNetworks"] = h.handleDescribeSecondaryNetworks
	ops["CreateSecondarySubnet"] = h.handleCreateSecondarySubnet
	ops["DeleteSecondarySubnet"] = h.handleDeleteSecondarySubnet
	ops["DescribeSecondarySubnets"] = h.handleDescribeSecondarySubnets
	ops["DescribeSecondaryInterfaces"] = h.handleDescribeSecondaryInterfaces
	ops["DescribeServiceLinkVirtualInterfaces"] = h.handleDescribeServiceLinkVirtualInterfaces
	ops["DescribeOutpostLags"] = h.handleDescribeOutpostLags
}

// secondaryNetSupportedOperations lists the operation names registered by
// registerSecondaryNetOps, for GetSupportedOperations().
func secondaryNetSupportedOperations() []string {
	return []string{
		"CreateSecondaryNetwork",
		"DeleteSecondaryNetwork",
		"DescribeSecondaryNetworks",
		"CreateSecondarySubnet",
		"DeleteSecondarySubnet",
		"DescribeSecondarySubnets",
		"DescribeSecondaryInterfaces",
		"DescribeServiceLinkVirtualInterfaces",
		"DescribeOutpostLags",
	}
}

// ---- Response shapes: Secondary Networks ----

type secondaryNetworkCidrItem struct {
	AssociationID string `xml:"associationId,omitempty"`
	CidrBlock     string `xml:"cidrBlock,omitempty"`
	State         string `xml:"state,omitempty"`
}

type secondaryNetworkItem struct {
	SecondaryNetworkID          string `xml:"secondaryNetworkId,omitempty"`
	SecondaryNetworkArn         string `xml:"secondaryNetworkArn,omitempty"`
	OwnerID                     string `xml:"ownerId,omitempty"`
	Type                        string `xml:"type,omitempty"`
	State                       string `xml:"state,omitempty"`
	Ipv4CidrBlockAssociationSet struct {
		Items []secondaryNetworkCidrItem `xml:"item"`
	} `xml:"ipv4CidrBlockAssociationSet"`
	TagSet []simpleTagItem `xml:"tagSet>item"`
}

func toSecondaryNetworkItem(n *SecondaryNetwork, tags map[string]string) secondaryNetworkItem {
	item := secondaryNetworkItem{
		SecondaryNetworkID:  n.SecondaryNetworkID,
		SecondaryNetworkArn: n.SecondaryNetworkArn,
		OwnerID:             n.OwnerID,
		Type:                n.Type,
		State:               n.State,
		TagSet:              tagItemsFromMap(tags),
	}
	for _, c := range n.Ipv4CidrBlockAssociations {
		item.Ipv4CidrBlockAssociationSet.Items = append(
			item.Ipv4CidrBlockAssociationSet.Items, secondaryNetworkCidrItem(c),
		)
	}

	return item
}

type createSecondaryNetworkResponse struct {
	XMLName          xml.Name             `xml:"CreateSecondaryNetworkResponse"`
	Xmlns            string               `xml:"xmlns,attr"`
	RequestID        string               `xml:"requestId"`
	SecondaryNetwork secondaryNetworkItem `xml:"secondaryNetwork"`
}

type deleteSecondaryNetworkResponse struct {
	XMLName          xml.Name             `xml:"DeleteSecondaryNetworkResponse"`
	Xmlns            string               `xml:"xmlns,attr"`
	RequestID        string               `xml:"requestId"`
	SecondaryNetwork secondaryNetworkItem `xml:"secondaryNetwork"`
}

type describeSecondaryNetworksResponse struct {
	XMLName             xml.Name `xml:"DescribeSecondaryNetworksResponse"`
	Xmlns               string   `xml:"xmlns,attr"`
	RequestID           string   `xml:"requestId"`
	SecondaryNetworkSet struct {
		Items []secondaryNetworkItem `xml:"item"`
	} `xml:"secondaryNetworkSet"`
}

// ---- Response shapes: Secondary Subnets ----

type secondarySubnetCidrItem struct {
	AssociationID string `xml:"associationId,omitempty"`
	CidrBlock     string `xml:"cidrBlock,omitempty"`
	State         string `xml:"state,omitempty"`
}

type secondarySubnetItem struct {
	SecondarySubnetID           string `xml:"secondarySubnetId,omitempty"`
	SecondarySubnetArn          string `xml:"secondarySubnetArn,omitempty"`
	SecondaryNetworkID          string `xml:"secondaryNetworkId,omitempty"`
	SecondaryNetworkType        string `xml:"secondaryNetworkType,omitempty"`
	OwnerID                     string `xml:"ownerId,omitempty"`
	AvailabilityZone            string `xml:"availabilityZone,omitempty"`
	AvailabilityZoneID          string `xml:"availabilityZoneId,omitempty"`
	State                       string `xml:"state,omitempty"`
	Ipv4CidrBlockAssociationSet struct {
		Items []secondarySubnetCidrItem `xml:"item"`
	} `xml:"ipv4CidrBlockAssociationSet"`
	TagSet []simpleTagItem `xml:"tagSet>item"`
}

func toSecondarySubnetItem(s *SecondarySubnet, tags map[string]string) secondarySubnetItem {
	item := secondarySubnetItem{
		SecondarySubnetID:    s.SecondarySubnetID,
		SecondarySubnetArn:   s.SecondarySubnetArn,
		SecondaryNetworkID:   s.SecondaryNetworkID,
		SecondaryNetworkType: s.SecondaryNetworkType,
		OwnerID:              s.OwnerID,
		AvailabilityZone:     s.AvailabilityZone,
		AvailabilityZoneID:   s.AvailabilityZoneID,
		State:                s.State,
		TagSet:               tagItemsFromMap(tags),
	}
	for _, c := range s.Ipv4CidrBlockAssociations {
		item.Ipv4CidrBlockAssociationSet.Items = append(
			item.Ipv4CidrBlockAssociationSet.Items, secondarySubnetCidrItem(c),
		)
	}

	return item
}

type createSecondarySubnetResponse struct {
	XMLName         xml.Name            `xml:"CreateSecondarySubnetResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	RequestID       string              `xml:"requestId"`
	SecondarySubnet secondarySubnetItem `xml:"secondarySubnet"`
}

type deleteSecondarySubnetResponse struct {
	XMLName         xml.Name            `xml:"DeleteSecondarySubnetResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	RequestID       string              `xml:"requestId"`
	SecondarySubnet secondarySubnetItem `xml:"secondarySubnet"`
}

type describeSecondarySubnetsResponse struct {
	XMLName            xml.Name `xml:"DescribeSecondarySubnetsResponse"`
	Xmlns              string   `xml:"xmlns,attr"`
	RequestID          string   `xml:"requestId"`
	SecondarySubnetSet struct {
		Items []secondarySubnetItem `xml:"item"`
	} `xml:"secondarySubnetSet"`
}

// ---- Response shapes: Secondary Interfaces ----

type secondaryInterfaceItem struct {
	SecondaryInterfaceID   string          `xml:"secondaryInterfaceId,omitempty"`
	Status                 string          `xml:"status,omitempty"`
	SecondaryInterfaceType string          `xml:"secondaryInterfaceType,omitempty"`
	SecondaryNetworkID     string          `xml:"secondaryNetworkId,omitempty"`
	SecondaryNetworkType   string          `xml:"secondaryNetworkType,omitempty"`
	SecondarySubnetID      string          `xml:"secondarySubnetId,omitempty"`
	AvailabilityZoneID     string          `xml:"availabilityZoneId,omitempty"`
	AvailabilityZone       string          `xml:"availabilityZone,omitempty"`
	SecondaryInterfaceArn  string          `xml:"secondaryInterfaceArn,omitempty"`
	MacAddress             string          `xml:"macAddress,omitempty"`
	OwnerID                string          `xml:"ownerId,omitempty"`
	PrivateIpv4Addresses   []string        `xml:"privateIpv4AddressSet>item,omitempty"`
	TagSet                 []simpleTagItem `xml:"tagSet>item"`
	SourceDestCheck        bool            `xml:"sourceDestCheck,omitempty"`
}

func toSecondaryInterfaceItem(si *SecondaryInterface, tags map[string]string) secondaryInterfaceItem {
	return secondaryInterfaceItem{
		SecondaryInterfaceID:   si.SecondaryInterfaceID,
		SecondaryInterfaceArn:  si.SecondaryInterfaceArn,
		SecondaryInterfaceType: si.SecondaryInterfaceType,
		SecondaryNetworkID:     si.SecondaryNetworkID,
		SecondaryNetworkType:   si.SecondaryNetworkType,
		SecondarySubnetID:      si.SecondarySubnetID,
		OwnerID:                si.OwnerID,
		AvailabilityZone:       si.AvailabilityZone,
		AvailabilityZoneID:     si.AvailabilityZoneID,
		MacAddress:             si.MacAddress,
		Status:                 si.Status,
		SourceDestCheck:        si.SourceDestCheck,
		PrivateIpv4Addresses:   si.PrivateIpv4Addresses,
		TagSet:                 tagItemsFromMap(tags),
	}
}

type describeSecondaryInterfacesResponse struct {
	XMLName               xml.Name `xml:"DescribeSecondaryInterfacesResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	RequestID             string   `xml:"requestId"`
	SecondaryInterfaceSet struct {
		Items []secondaryInterfaceItem `xml:"item"`
	} `xml:"secondaryInterfaceSet"`
}

// ---- Response shapes: Service Link Virtual Interfaces / Outpost LAGs ----

type serviceLinkVirtualInterfaceItem struct {
	ServiceLinkVirtualInterfaceID  string          `xml:"serviceLinkVirtualInterfaceId,omitempty"`
	ServiceLinkVirtualInterfaceArn string          `xml:"serviceLinkVirtualInterfaceArn,omitempty"`
	OutpostID                      string          `xml:"outpostId,omitempty"`
	OutpostArn                     string          `xml:"outpostArn,omitempty"`
	OutpostLagID                   string          `xml:"outpostLagId,omitempty"`
	OwnerID                        string          `xml:"ownerId,omitempty"`
	LocalAddress                   string          `xml:"localAddress,omitempty"`
	PeerAddress                    string          `xml:"peerAddress,omitempty"`
	ConfigurationState             string          `xml:"configurationState,omitempty"`
	TagSet                         []simpleTagItem `xml:"tagSet>item"`
	PeerBgpAsn                     int64           `xml:"peerBgpAsn,omitempty"`
	Vlan                           int32           `xml:"vlan,omitempty"`
}

func toServiceLinkVirtualInterfaceItem(
	v *ServiceLinkVirtualInterface, tags map[string]string,
) serviceLinkVirtualInterfaceItem {
	return serviceLinkVirtualInterfaceItem{
		ServiceLinkVirtualInterfaceID:  v.ServiceLinkVirtualInterfaceID,
		ServiceLinkVirtualInterfaceArn: v.ServiceLinkVirtualInterfaceArn,
		OutpostID:                      v.OutpostID,
		OutpostArn:                     v.OutpostArn,
		OutpostLagID:                   v.OutpostLagID,
		OwnerID:                        v.OwnerID,
		LocalAddress:                   v.LocalAddress,
		PeerAddress:                    v.PeerAddress,
		PeerBgpAsn:                     v.PeerBgpAsn,
		ConfigurationState:             v.ConfigurationState,
		Vlan:                           v.Vlan,
		TagSet:                         tagItemsFromMap(tags),
	}
}

type describeServiceLinkVirtualInterfacesResponse struct {
	XMLName                        xml.Name `xml:"DescribeServiceLinkVirtualInterfacesResponse"`
	Xmlns                          string   `xml:"xmlns,attr"`
	RequestID                      string   `xml:"requestId"`
	ServiceLinkVirtualInterfaceSet struct {
		Items []serviceLinkVirtualInterfaceItem `xml:"item"`
	} `xml:"serviceLinkVirtualInterfaceSet"`
}

type outpostLagItem struct {
	OutpostLagID                    string          `xml:"outpostLagId,omitempty"`
	OutpostArn                      string          `xml:"outpostArn,omitempty"`
	OwnerID                         string          `xml:"ownerId,omitempty"`
	State                           string          `xml:"state,omitempty"`
	LocalGatewayVirtualInterfaceIDs []string        `xml:"localGatewayVirtualInterfaceIdSet>item,omitempty"`
	ServiceLinkVirtualInterfaceIDs  []string        `xml:"serviceLinkVirtualInterfaceIdSet>item,omitempty"`
	TagSet                          []simpleTagItem `xml:"tagSet>item"`
}

func toOutpostLagItem(l *OutpostLag, tags map[string]string) outpostLagItem {
	return outpostLagItem{
		OutpostLagID:                    l.OutpostLagID,
		OutpostArn:                      l.OutpostArn,
		OwnerID:                         l.OwnerID,
		State:                           l.State,
		LocalGatewayVirtualInterfaceIDs: l.LocalGatewayVirtualInterfaceIDs,
		ServiceLinkVirtualInterfaceIDs:  l.ServiceLinkVirtualInterfaceIDs,
		TagSet:                          tagItemsFromMap(tags),
	}
}

type describeOutpostLagsResponse struct {
	XMLName       xml.Name `xml:"DescribeOutpostLagsResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	OutpostLagSet struct {
		Items []outpostLagItem `xml:"item"`
	} `xml:"outpostLagSet"`
}

// ---- Handlers: Secondary Networks ----

func (h *Handler) handleCreateSecondaryNetwork(vals url.Values, reqID string) (any, error) {
	net, err := h.Backend.CreateSecondaryNetwork(
		vals.Get("Ipv4CidrBlock"), vals.Get("NetworkType"), parseTagSpecification(vals, "secondary-network"),
	)
	if err != nil {
		return nil, err
	}

	return &createSecondaryNetworkResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		SecondaryNetwork: toSecondaryNetworkItem(net, h.Backend.TagsForResource(net.SecondaryNetworkID)),
	}, nil
}

func (h *Handler) handleDeleteSecondaryNetwork(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SecondaryNetworkId")
	tags := h.Backend.TagsForResource(id)

	net, err := h.Backend.DeleteSecondaryNetwork(id)
	if err != nil {
		return nil, err
	}

	return &deleteSecondaryNetworkResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, SecondaryNetwork: toSecondaryNetworkItem(net, tags),
	}, nil
}

func (h *Handler) handleDescribeSecondaryNetworks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SecondaryNetworkId")
	nets := h.Backend.DescribeSecondaryNetworks(ids)

	resp := &describeSecondaryNetworksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, n := range nets {
		resp.SecondaryNetworkSet.Items = append(
			resp.SecondaryNetworkSet.Items,
			toSecondaryNetworkItem(n, h.Backend.TagsForResource(n.SecondaryNetworkID)),
		)
	}

	return resp, nil
}

// ---- Handlers: Secondary Subnets ----

func (h *Handler) handleCreateSecondarySubnet(vals url.Values, reqID string) (any, error) {
	sub, err := h.Backend.CreateSecondarySubnet(
		vals.Get("Ipv4CidrBlock"),
		vals.Get("SecondaryNetworkId"),
		vals.Get("AvailabilityZone"),
		vals.Get("AvailabilityZoneId"),
		parseTagSpecification(vals, "secondary-subnet"),
	)
	if err != nil {
		return nil, err
	}

	return &createSecondarySubnetResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		SecondarySubnet: toSecondarySubnetItem(sub, h.Backend.TagsForResource(sub.SecondarySubnetID)),
	}, nil
}

func (h *Handler) handleDeleteSecondarySubnet(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SecondarySubnetId")
	tags := h.Backend.TagsForResource(id)

	sub, err := h.Backend.DeleteSecondarySubnet(id)
	if err != nil {
		return nil, err
	}

	return &deleteSecondarySubnetResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, SecondarySubnet: toSecondarySubnetItem(sub, tags),
	}, nil
}

func (h *Handler) handleDescribeSecondarySubnets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SecondarySubnetId")
	subs := h.Backend.DescribeSecondarySubnets(ids)

	resp := &describeSecondarySubnetsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, s := range subs {
		resp.SecondarySubnetSet.Items = append(
			resp.SecondarySubnetSet.Items,
			toSecondarySubnetItem(s, h.Backend.TagsForResource(s.SecondarySubnetID)),
		)
	}

	return resp, nil
}

// ---- Handlers: Secondary Interfaces / Outpost LAGs / Service Link VIFs ----

func (h *Handler) handleDescribeSecondaryInterfaces(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SecondaryInterfaceId")
	sis := h.Backend.DescribeSecondaryInterfaces(ids)

	resp := &describeSecondaryInterfacesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, si := range sis {
		resp.SecondaryInterfaceSet.Items = append(
			resp.SecondaryInterfaceSet.Items,
			toSecondaryInterfaceItem(si, h.Backend.TagsForResource(si.SecondaryInterfaceID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeServiceLinkVirtualInterfaces(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ServiceLinkVirtualInterfaceId")
	vifs := h.Backend.DescribeServiceLinkVirtualInterfaces(ids)

	resp := &describeServiceLinkVirtualInterfacesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, v := range vifs {
		resp.ServiceLinkVirtualInterfaceSet.Items = append(
			resp.ServiceLinkVirtualInterfaceSet.Items,
			toServiceLinkVirtualInterfaceItem(v, h.Backend.TagsForResource(v.ServiceLinkVirtualInterfaceID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeOutpostLags(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "OutpostLagId")
	lags := h.Backend.DescribeOutpostLags(ids)

	resp := &describeOutpostLagsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, l := range lags {
		resp.OutpostLagSet.Items = append(
			resp.OutpostLagSet.Items,
			toOutpostLagItem(l, h.Backend.TagsForResource(l.OutpostLagID)),
		)
	}

	return resp, nil
}
