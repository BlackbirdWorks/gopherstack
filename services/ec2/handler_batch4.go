package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// ---- Registration ----

func registerBatch4Ops(h *Handler, ops map[string]ec2ActionFn) {
	// ManagedPrefixList
	ops["CreateManagedPrefixList"] = h.handleCreateManagedPrefixList
	ops["DeleteManagedPrefixList"] = h.handleDeleteManagedPrefixList
	ops["DescribeManagedPrefixLists"] = h.handleDescribeManagedPrefixLists
	ops["GetManagedPrefixListEntries"] = h.handleGetManagedPrefixListEntries
	ops["GetManagedPrefixListAssociations"] = h.handleGetManagedPrefixListAssociations
	ops["ModifyManagedPrefixList"] = h.handleModifyManagedPrefixList
	ops["RestoreManagedPrefixListVersion"] = h.handleRestoreManagedPrefixListVersion
	// ClientVPN
	ops["CreateClientVpnEndpoint"] = h.handleCreateClientVpnEndpoint
	ops["DeleteClientVpnEndpoint"] = h.handleDeleteClientVpnEndpoint
	ops["DescribeClientVpnEndpoints"] = h.handleDescribeClientVpnEndpoints
	ops["AssociateClientVpnTargetNetwork"] = h.handleAssociateClientVpnTargetNetwork
	ops["DescribeClientVpnTargetNetworks"] = h.handleDescribeClientVpnTargetNetworks
	ops["CreateClientVpnRoute"] = h.handleCreateClientVpnRoute
	ops["DeleteClientVpnRoute"] = h.handleDeleteClientVpnRoute
	ops["DescribeClientVpnRoutes"] = h.handleDescribeClientVpnRoutes
	ops["AuthorizeClientVpnIngress"] = h.handleAuthorizeClientVpnIngress
	ops["DescribeClientVpnAuthorizationRules"] = h.handleDescribeClientVpnAuthorizationRules
	// ClientVPN extra
	ops["TerminateClientVpnConnections"] = h.handleTerminateClientVpnConnections
	ops["DescribeClientVpnConnections"] = h.handleDescribeClientVpnConnections
	ops["ApplySecurityGroupsToClientVpnTargetNetwork"] = h.handleApplySecurityGroupsToClientVpnTargetNetwork
	ops["RevokeClientVpnIngress"] = h.handleRevokeClientVpnIngress
	ops["ModifyClientVpnEndpoint"] = h.handleModifyClientVpnEndpoint
	ops["DisassociateClientVpnTargetNetwork"] = h.handleDisassociateClientVpnTargetNetwork
	// TGW Peering
	ops["CreateTransitGatewayPeeringAttachment"] = h.handleCreateTransitGatewayPeeringAttachment
	ops["DeleteTransitGatewayPeeringAttachment"] = h.handleDeleteTransitGatewayPeeringAttachment
	ops["DescribeTransitGatewayPeeringAttachments"] = h.handleDescribeTransitGatewayPeeringAttachments
	// TGW Connect
	ops["CreateTransitGatewayConnect"] = h.handleCreateTransitGatewayConnect
	ops["DeleteTransitGatewayConnect"] = h.handleDeleteTransitGatewayConnect
	ops["DescribeTransitGatewayConnects"] = h.handleDescribeTransitGatewayConnects
	ops["CreateTransitGatewayConnectPeer"] = h.handleCreateTransitGatewayConnectPeer
	ops["DeleteTransitGatewayConnectPeer"] = h.handleDeleteTransitGatewayConnectPeer
	ops["DescribeTransitGatewayConnectPeers"] = h.handleDescribeTransitGatewayConnectPeers
	// TGW PrefixListRef
	ops["CreateTransitGatewayPrefixListReference"] = h.handleCreateTransitGatewayPrefixListReference
	ops["DeleteTransitGatewayPrefixListReference"] = h.handleDeleteTransitGatewayPrefixListReference
	ops["GetTransitGatewayPrefixListReferences"] = h.handleGetTransitGatewayPrefixListReferences
	// VerifiedAccess
	ops["CreateVerifiedAccessEndpoint"] = h.handleCreateVerifiedAccessEndpoint
	ops["DeleteVerifiedAccessEndpoint"] = h.handleDeleteVerifiedAccessEndpoint
	ops["DescribeVerifiedAccessEndpoints"] = h.handleDescribeVerifiedAccessEndpoints
	ops["ModifyVerifiedAccessEndpoint"] = h.handleModifyVerifiedAccessEndpoint
	ops["CreateVerifiedAccessGroup"] = h.handleCreateVerifiedAccessGroup
	ops["DeleteVerifiedAccessGroup"] = h.handleDeleteVerifiedAccessGroup
	ops["DescribeVerifiedAccessGroups"] = h.handleDescribeVerifiedAccessGroups
	ops["CreateVerifiedAccessInstance"] = h.handleCreateVerifiedAccessInstance
	ops["DeleteVerifiedAccessInstance"] = h.handleDeleteVerifiedAccessInstance
	ops["DescribeVerifiedAccessInstances"] = h.handleDescribeVerifiedAccessInstances
	ops["CreateVerifiedAccessTrustProvider"] = h.handleCreateVerifiedAccessTrustProvider
	ops["DeleteVerifiedAccessTrustProvider"] = h.handleDeleteVerifiedAccessTrustProvider
	ops["DescribeVerifiedAccessTrustProviders"] = h.handleDescribeVerifiedAccessTrustProviders
	ops["AttachVerifiedAccessTrustProvider"] = h.handleAttachVerifiedAccessTrustProvider
	ops["DetachVerifiedAccessTrustProvider"] = h.handleDetachVerifiedAccessTrustProvider
}

func batch4SupportedOperations() []string {
	return []string{
		// ManagedPrefixList
		"CreateManagedPrefixList",
		"DeleteManagedPrefixList",
		"DescribeManagedPrefixLists",
		"GetManagedPrefixListEntries",
		"GetManagedPrefixListAssociations",
		"ModifyManagedPrefixList",
		"RestoreManagedPrefixListVersion",
		// ClientVPN
		"CreateClientVpnEndpoint",
		"DeleteClientVpnEndpoint",
		"DescribeClientVpnEndpoints",
		"AssociateClientVpnTargetNetwork",
		"DescribeClientVpnTargetNetworks",
		"CreateClientVpnRoute",
		"DeleteClientVpnRoute",
		"DescribeClientVpnRoutes",
		"AuthorizeClientVpnIngress",
		"DescribeClientVpnAuthorizationRules",
		// ClientVPN extra
		"TerminateClientVpnConnections",
		"DescribeClientVpnConnections",
		"ApplySecurityGroupsToClientVpnTargetNetwork",
		"RevokeClientVpnIngress",
		"ModifyClientVpnEndpoint",
		"DisassociateClientVpnTargetNetwork",
		// TGW Peering
		"CreateTransitGatewayPeeringAttachment",
		"DeleteTransitGatewayPeeringAttachment",
		"DescribeTransitGatewayPeeringAttachments",
		// TGW Connect
		"CreateTransitGatewayConnect",
		"DeleteTransitGatewayConnect",
		"DescribeTransitGatewayConnects",
		"CreateTransitGatewayConnectPeer",
		"DeleteTransitGatewayConnectPeer",
		"DescribeTransitGatewayConnectPeers",
		// TGW PrefixListRef
		"CreateTransitGatewayPrefixListReference",
		"DeleteTransitGatewayPrefixListReference",
		"GetTransitGatewayPrefixListReferences",
		// VerifiedAccess
		"CreateVerifiedAccessEndpoint",
		"DeleteVerifiedAccessEndpoint",
		"DescribeVerifiedAccessEndpoints",
		"ModifyVerifiedAccessEndpoint",
		"CreateVerifiedAccessGroup",
		"DeleteVerifiedAccessGroup",
		"DescribeVerifiedAccessGroups",
		"CreateVerifiedAccessInstance",
		"DeleteVerifiedAccessInstance",
		"DescribeVerifiedAccessInstances",
		"CreateVerifiedAccessTrustProvider",
		"DeleteVerifiedAccessTrustProvider",
		"DescribeVerifiedAccessTrustProviders",
		"AttachVerifiedAccessTrustProvider",
		"DetachVerifiedAccessTrustProvider",
	}
}

// ---- XML response types ----

type managedPrefixListItem struct {
	PrefixListID   string `xml:"prefixListId"`
	PrefixListName string `xml:"prefixListName"`
	PrefixListArn  string `xml:"prefixListArn"`
	AddressFamily  string `xml:"addressFamily"`
	State          string `xml:"state"`
	OwnerID        string `xml:"ownerId"`
	Version        int64  `xml:"version"`
	MaxEntries     int    `xml:"maxEntries"`
}

type createManagedPrefixListResponse struct {
	XMLName    xml.Name              `xml:"CreateManagedPrefixListResponse"`
	RequestID  string                `xml:"requestId"`
	PrefixList managedPrefixListItem `xml:"prefixList"`
}

type describeManagedPrefixListsResponse struct {
	XMLName       xml.Name `xml:"DescribeManagedPrefixListsResponse"`
	RequestID     string   `xml:"requestId"`
	PrefixListSet struct {
		Items []managedPrefixListItem `xml:"item"`
	} `xml:"prefixListSet"`
}

type prefixListEntryItem struct {
	Cidr        string `xml:"cidr"`
	Description string `xml:"description,omitempty"`
}

type getManagedPrefixListEntriesResponse struct {
	XMLName   xml.Name `xml:"GetManagedPrefixListEntriesResponse"`
	RequestID string   `xml:"requestId"`
	EntrySet  struct {
		Items []prefixListEntryItem `xml:"item"`
	} `xml:"entrySet"`
}

type getManagedPrefixListAssociationsResponse struct {
	XMLName        xml.Name `xml:"GetManagedPrefixListAssociationsResponse"`
	RequestID      string   `xml:"requestId"`
	AssociationSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"associationSet"`
}

type clientVpnEndpointItem struct {
	ClientVpnEndpointID string `xml:"clientVpnEndpointId"`
	DNSName             string `xml:"dnsName"`
	Status              string `xml:"status"`
	Description         string `xml:"description,omitempty"`
	ClientCidrBlock     string `xml:"clientCidrBlock"`
}

type createClientVpnEndpointResponse struct {
	XMLName           xml.Name              `xml:"CreateClientVpnEndpointResponse"`
	RequestID         string                `xml:"requestId"`
	ClientVpnEndpoint clientVpnEndpointItem `xml:"clientVpnEndpoint"`
}

type describeClientVpnEndpointsResponse struct {
	XMLName              xml.Name `xml:"DescribeClientVpnEndpointsResponse"`
	RequestID            string   `xml:"requestId"`
	ClientVpnEndpointSet struct {
		Items []clientVpnEndpointItem `xml:"item"`
	} `xml:"clientVpnEndpointSet"`
}

type clientVpnTargetNetworkItem struct {
	AssociationID       string `xml:"associationId"`
	SubnetID            string `xml:"subnetId"`
	ClientVpnEndpointID string `xml:"clientVpnEndpointId"`
	Status              string `xml:"status"`
}

type describeClientVpnTargetNetworksResponse struct {
	XMLName                 xml.Name `xml:"DescribeClientVpnTargetNetworksResponse"`
	RequestID               string   `xml:"requestId"`
	ClientVpnTargetNetworks struct {
		Items []clientVpnTargetNetworkItem `xml:"item"`
	} `xml:"clientVpnTargetNetworks"`
}

type clientVpnRouteItem struct {
	DestinationCidr string `xml:"destinationCidrBlock"`
	Status          string `xml:"status"`
	Description     string `xml:"description,omitempty"`
}

type describeClientVpnRoutesResponse struct {
	XMLName   xml.Name `xml:"DescribeClientVpnRoutesResponse"`
	RequestID string   `xml:"requestId"`
	Routes    struct {
		Items []clientVpnRouteItem `xml:"item"`
	} `xml:"clientVpnRouteSet"`
}

type clientVpnAuthRuleItem struct {
	Cidr        string `xml:"cidr"`
	Status      string `xml:"status"`
	Description string `xml:"description,omitempty"`
}

type describeClientVpnAuthorizationRulesResponse struct {
	XMLName            xml.Name `xml:"DescribeClientVpnAuthorizationRulesResponse"`
	RequestID          string   `xml:"requestId"`
	AuthorizationRules struct {
		Items []clientVpnAuthRuleItem `xml:"item"`
	} `xml:"authorizationRules"`
}

type describeClientVpnConnectionsResponse struct {
	XMLName     xml.Name `xml:"DescribeClientVpnConnectionsResponse"`
	RequestID   string   `xml:"requestId"`
	Connections struct {
		Items []struct{} `xml:"item"`
	} `xml:"connections"`
}

type createTransitGatewayPeeringAttachmentResponse struct {
	XMLName                         xml.Name                 `xml:"CreateTransitGatewayPeeringAttachmentResponse"`
	RequestID                       string                   `xml:"requestId"`
	TransitGatewayPeeringAttachment tgwPeeringAttachmentItem `xml:"transitGatewayPeeringAttachment"`
}

type describeTransitGatewayPeeringAttachmentsResponse struct {
	XMLName                          xml.Name `xml:"DescribeTransitGatewayPeeringAttachmentsResponse"`
	RequestID                        string   `xml:"requestId"`
	TransitGatewayPeeringAttachments struct {
		Items []tgwPeeringAttachmentItem `xml:"item"`
	} `xml:"transitGatewayPeeringAttachments"`
}

type tgwConnectItem struct {
	TransitGatewayAttachmentID          string `xml:"transitGatewayAttachmentId"`
	TransportTransitGatewayAttachmentID string `xml:"transportTransitGatewayAttachmentId"`
	TransitGatewayID                    string `xml:"transitGatewayId"`
	State                               string `xml:"state"`
}

type createTransitGatewayConnectResponse struct {
	XMLName               xml.Name       `xml:"CreateTransitGatewayConnectResponse"`
	RequestID             string         `xml:"requestId"`
	TransitGatewayConnect tgwConnectItem `xml:"transitGatewayConnect"`
}

type describeTransitGatewayConnectsResponse struct {
	XMLName                xml.Name `xml:"DescribeTransitGatewayConnectsResponse"`
	RequestID              string   `xml:"requestId"`
	TransitGatewayConnects struct {
		Items []tgwConnectItem `xml:"item"`
	} `xml:"transitGatewayConnects"`
}

type tgwConnectPeerItem struct {
	TransitGatewayConnectPeerID string `xml:"transitGatewayConnectPeerId"`
	TransitGatewayAttachmentID  string `xml:"transitGatewayAttachmentId"`
	State                       string `xml:"state"`
	PeerAddress                 string `xml:"peerAddress"`
}

type createTransitGatewayConnectPeerResponse struct {
	XMLName                   xml.Name           `xml:"CreateTransitGatewayConnectPeerResponse"`
	RequestID                 string             `xml:"requestId"`
	TransitGatewayConnectPeer tgwConnectPeerItem `xml:"transitGatewayConnectPeer"`
}

type describeTransitGatewayConnectPeersResponse struct {
	XMLName                    xml.Name `xml:"DescribeTransitGatewayConnectPeersResponse"`
	RequestID                  string   `xml:"requestId"`
	TransitGatewayConnectPeers struct {
		Items []tgwConnectPeerItem `xml:"item"`
	} `xml:"transitGatewayConnectPeers"`
}

type tgwPrefixListRefItem struct {
	PrefixListID               string `xml:"prefixListId"`
	TransitGatewayRouteTableID string `xml:"transitGatewayRouteTableId"`
	State                      string `xml:"state"`
	Blackhole                  bool   `xml:"blackhole"`
}

type createTransitGatewayPrefixListReferenceResponse struct {
	XMLName                           xml.Name             `xml:"CreateTransitGatewayPrefixListReferenceResponse"`
	RequestID                         string               `xml:"requestId"`
	TransitGatewayPrefixListReference tgwPrefixListRefItem `xml:"transitGatewayPrefixListReference"`
}

type getTransitGatewayPrefixListReferencesResponse struct {
	XMLName                              xml.Name `xml:"GetTransitGatewayPrefixListReferencesResponse"`
	RequestID                            string   `xml:"requestId"`
	TransitGatewayPrefixListReferenceSet struct {
		Items []tgwPrefixListRefItem `xml:"item"`
	} `xml:"transitGatewayPrefixListReferenceSet"`
}

type verifiedAccessEndpointItem struct {
	VerifiedAccessEndpointID string `xml:"verifiedAccessEndpointId"`
	VerifiedAccessGroupID    string `xml:"verifiedAccessGroupId"`
	Status                   string `xml:"status"`
	Description              string `xml:"description,omitempty"`
	EndpointType             string `xml:"endpointType,omitempty"`
}

type createVerifiedAccessEndpointResponse struct {
	XMLName                xml.Name                   `xml:"CreateVerifiedAccessEndpointResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessEndpoint verifiedAccessEndpointItem `xml:"verifiedAccessEndpoint"`
}

type describeVerifiedAccessEndpointsResponse struct {
	XMLName                   xml.Name `xml:"DescribeVerifiedAccessEndpointsResponse"`
	RequestID                 string   `xml:"requestId"`
	VerifiedAccessEndpointSet struct {
		Items []verifiedAccessEndpointItem `xml:"item"`
	} `xml:"verifiedAccessEndpointSet"`
}

type verifiedAccessGroupItem struct {
	VerifiedAccessGroupID    string `xml:"verifiedAccessGroupId"`
	VerifiedAccessInstanceID string `xml:"verifiedAccessInstanceId"`
	Status                   string `xml:"status"`
	Description              string `xml:"description,omitempty"`
}

type createVerifiedAccessGroupResponse struct {
	XMLName             xml.Name                `xml:"CreateVerifiedAccessGroupResponse"`
	RequestID           string                  `xml:"requestId"`
	VerifiedAccessGroup verifiedAccessGroupItem `xml:"verifiedAccessGroup"`
}

type describeVerifiedAccessGroupsResponse struct {
	XMLName                xml.Name `xml:"DescribeVerifiedAccessGroupsResponse"`
	RequestID              string   `xml:"requestId"`
	VerifiedAccessGroupSet struct {
		Items []verifiedAccessGroupItem `xml:"item"`
	} `xml:"verifiedAccessGroupSet"`
}

type verifiedAccessInstanceItem struct {
	VerifiedAccessInstanceID string `xml:"verifiedAccessInstanceId"`
	Status                   string `xml:"status"`
	Description              string `xml:"description,omitempty"`
}

type createVerifiedAccessInstanceResponse struct {
	XMLName                xml.Name                   `xml:"CreateVerifiedAccessInstanceResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessInstance verifiedAccessInstanceItem `xml:"verifiedAccessInstance"`
}

type describeVerifiedAccessInstancesResponse struct {
	XMLName                   xml.Name `xml:"DescribeVerifiedAccessInstancesResponse"`
	RequestID                 string   `xml:"requestId"`
	VerifiedAccessInstanceSet struct {
		Items []verifiedAccessInstanceItem `xml:"item"`
	} `xml:"verifiedAccessInstanceSet"`
}

type verifiedAccessTrustProviderItem struct {
	VerifiedAccessTrustProviderID string `xml:"verifiedAccessTrustProviderId"`
	TrustProviderType             string `xml:"trustProviderType"`
	Status                        string `xml:"status"`
	Description                   string `xml:"description,omitempty"`
}

type createVerifiedAccessTrustProviderResponse struct {
	XMLName                     xml.Name                        `xml:"CreateVerifiedAccessTrustProviderResponse"`
	RequestID                   string                          `xml:"requestId"`
	VerifiedAccessTrustProvider verifiedAccessTrustProviderItem `xml:"verifiedAccessTrustProvider"`
}

type describeVerifiedAccessTrustProvidersResponse struct {
	XMLName                        xml.Name `xml:"DescribeVerifiedAccessTrustProvidersResponse"`
	RequestID                      string   `xml:"requestId"`
	VerifiedAccessTrustProviderSet struct {
		Items []verifiedAccessTrustProviderItem `xml:"item"`
	} `xml:"verifiedAccessTrustProviderSet"`
}

// ---- ManagedPrefixList handlers ----

func toManagedPrefixListItem(pl *ManagedPrefixList) managedPrefixListItem {
	return managedPrefixListItem{
		PrefixListID:   pl.PrefixListID,
		PrefixListName: pl.PrefixListName,
		PrefixListArn:  pl.PrefixListArn,
		AddressFamily:  pl.AddressFamily,
		State:          pl.State,
		MaxEntries:     pl.MaxEntries,
		Version:        pl.Version,
		OwnerID:        pl.OwnerID,
	}
}

func (h *Handler) handleCreateManagedPrefixList(vals url.Values, reqID string) (any, error) {
	name := vals.Get("PrefixListName")
	af := vals.Get("AddressFamily")
	maxEntries := 0
	if v := vals.Get("MaxEntries"); v != "" {
		parseIntValue(v, &maxEntries)
	}

	pl, err := h.Backend.CreateManagedPrefixList(name, af, maxEntries)
	if err != nil {
		return nil, err
	}

	return &createManagedPrefixListResponse{
		RequestID:  reqID,
		PrefixList: toManagedPrefixListItem(pl),
	}, nil
}

func (h *Handler) handleDeleteManagedPrefixList(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	if err := h.Backend.DeleteManagedPrefixList(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteManagedPrefixListResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeManagedPrefixLists(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "PrefixListId")
	pls := h.Backend.DescribeManagedPrefixLists(ids)

	resp := &describeManagedPrefixListsResponse{RequestID: reqID}
	for _, pl := range pls {
		resp.PrefixListSet.Items = append(resp.PrefixListSet.Items, toManagedPrefixListItem(pl))
	}

	return resp, nil
}

func (h *Handler) handleGetManagedPrefixListEntries(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	entries, err := h.Backend.GetManagedPrefixListEntries(id)
	if err != nil {
		return nil, err
	}

	resp := &getManagedPrefixListEntriesResponse{RequestID: reqID}
	for _, e := range entries {
		resp.EntrySet.Items = append(resp.EntrySet.Items, prefixListEntryItem(e))
	}

	return resp, nil
}

func (h *Handler) handleGetManagedPrefixListAssociations(_ url.Values, reqID string) (any, error) {
	return &getManagedPrefixListAssociationsResponse{RequestID: reqID}, nil
}

func (h *Handler) handleModifyManagedPrefixList(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	// Parse AddEntry.N.Cidr and RemoveEntry.N.Cidr
	var addEntries, removeEntries []PrefixListEntry
	for i := 1; ; i++ {
		cidr := vals.Get("AddEntry." + itoa(i) + ".Cidr")
		if cidr == "" {
			break
		}
		addEntries = append(addEntries, PrefixListEntry{
			Cidr:        cidr,
			Description: vals.Get("AddEntry." + itoa(i) + ".Description"),
		})
	}
	for i := 1; ; i++ {
		cidr := vals.Get("RemoveEntry." + itoa(i) + ".Cidr")
		if cidr == "" {
			break
		}
		removeEntries = append(removeEntries, PrefixListEntry{Cidr: cidr})
	}

	if err := h.Backend.ModifyManagedPrefixList(id, addEntries, removeEntries); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyManagedPrefixListResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRestoreManagedPrefixListVersion(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	version := 0
	parseIntValue(vals.Get("PreviousVersion"), &version)
	if err := h.Backend.RestoreManagedPrefixListVersion(id, int64(version)); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreManagedPrefixListVersionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- ClientVPN handlers ----

func toClientVpnEndpointItem(ep *ClientVpnEndpoint) clientVpnEndpointItem {
	return clientVpnEndpointItem{
		ClientVpnEndpointID: ep.ClientVpnEndpointID,
		DNSName:             ep.DNSName,
		Status:              ep.Status,
		Description:         ep.Description,
		ClientCidrBlock:     ep.ClientCidrBlock,
	}
}

func (h *Handler) handleCreateClientVpnEndpoint(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("ClientCidrBlock")
	description := vals.Get("Description")
	dnsServers := parseMemberList(vals, "DnsServers")

	ep, err := h.Backend.CreateClientVpnEndpoint(cidr, description, dnsServers)
	if err != nil {
		return nil, err
	}

	return &createClientVpnEndpointResponse{
		RequestID:         reqID,
		ClientVpnEndpoint: toClientVpnEndpointItem(ep),
	}, nil
}

func (h *Handler) handleDeleteClientVpnEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("ClientVpnEndpointId")
	if err := h.Backend.DeleteClientVpnEndpoint(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteClientVpnEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeClientVpnEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ClientVpnEndpointId")
	eps := h.Backend.DescribeClientVpnEndpoints(ids)

	resp := &describeClientVpnEndpointsResponse{RequestID: reqID}
	for _, ep := range eps {
		resp.ClientVpnEndpointSet.Items = append(resp.ClientVpnEndpointSet.Items, toClientVpnEndpointItem(ep))
	}

	return resp, nil
}

type associateClientVpnTargetNetworkResponse struct {
	XMLName     xml.Name `xml:"AssociateClientVpnTargetNetworkResponse"`
	RequestID   string   `xml:"requestId"`
	AssocStatus struct {
		AssociationID string `xml:"associationId"`
		Status        string `xml:"status"`
	} `xml:"associationStatus"`
}

func (h *Handler) handleAssociateClientVpnTargetNetwork(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	subnetID := vals.Get("SubnetId")
	assocID, err := h.Backend.AssociateClientVpnTargetNetwork(endpointID, subnetID)
	if err != nil {
		return nil, err
	}

	resp := &associateClientVpnTargetNetworkResponse{RequestID: reqID}
	resp.AssocStatus.AssociationID = assocID
	resp.AssocStatus.Status = "associating"

	return resp, nil
}

func (h *Handler) handleDisassociateClientVpnTargetNetwork(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	assocID := vals.Get("AssociationId")
	if err := h.Backend.DisassociateClientVpnTargetNetwork(endpointID, assocID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateClientVpnTargetNetworkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeClientVpnTargetNetworks(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	networks, err := h.Backend.DescribeClientVpnTargetNetworks(endpointID)
	if err != nil {
		return nil, err
	}

	resp := &describeClientVpnTargetNetworksResponse{RequestID: reqID}
	for _, tn := range networks {
		resp.ClientVpnTargetNetworks.Items = append(
			resp.ClientVpnTargetNetworks.Items,
			clientVpnTargetNetworkItem{
				AssociationID:       tn.AssociationID,
				SubnetID:            tn.SubnetID,
				ClientVpnEndpointID: tn.ClientVpnEndpointID,
				Status:              tn.Status,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateClientVpnRoute(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	cidr := vals.Get("DestinationCidrBlock")
	description := vals.Get("Description")
	if err := h.Backend.CreateClientVpnRoute(endpointID, cidr, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateClientVpnRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDeleteClientVpnRoute(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	cidr := vals.Get("DestinationCidrBlock")
	if err := h.Backend.DeleteClientVpnRoute(endpointID, cidr); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteClientVpnRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeClientVpnRoutes(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	routes, err := h.Backend.DescribeClientVpnRoutes(endpointID)
	if err != nil {
		return nil, err
	}

	resp := &describeClientVpnRoutesResponse{RequestID: reqID}
	for _, r := range routes {
		resp.Routes.Items = append(resp.Routes.Items, clientVpnRouteItem(r))
	}

	return resp, nil
}

func (h *Handler) handleAuthorizeClientVpnIngress(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	cidr := vals.Get("AuthorizeAllGroups")
	if cidr == "" {
		cidr = vals.Get("TargetNetworkCidr")
	}
	description := vals.Get("Description")
	if err := h.Backend.AuthorizeClientVpnIngress(endpointID, cidr, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AuthorizeClientVpnIngressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRevokeClientVpnIngress(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	cidr := vals.Get("TargetNetworkCidr")
	if err := h.Backend.RevokeClientVpnIngress(endpointID, cidr); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RevokeClientVpnIngressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeClientVpnAuthorizationRules(
	vals url.Values,
	reqID string,
) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	rules, err := h.Backend.DescribeClientVpnAuthorizationRules(endpointID)
	if err != nil {
		return nil, err
	}

	resp := &describeClientVpnAuthorizationRulesResponse{RequestID: reqID}
	for _, r := range rules {
		resp.AuthorizationRules.Items = append(resp.AuthorizationRules.Items, clientVpnAuthRuleItem(r))
	}

	return resp, nil
}

func (h *Handler) handleModifyClientVpnEndpoint(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	description := vals.Get("Description")
	dnsServers := parseMemberList(vals, "DnsServers")
	if err := h.Backend.ModifyClientVpnEndpoint(endpointID, description, dnsServers); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyClientVpnEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleApplySecurityGroupsToClientVpnTargetNetwork(
	vals url.Values,
	reqID string,
) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	sgs := parseMemberList(vals, "SecurityGroupId")
	if err := h.Backend.ApplySecurityGroupsToClientVpnTargetNetwork(endpointID, sgs); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ApplySecurityGroupsToClientVpnTargetNetworkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeClientVpnConnections(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	_, err := h.Backend.DescribeClientVpnConnections(endpointID)
	if err != nil {
		return nil, err
	}

	return &describeClientVpnConnectionsResponse{RequestID: reqID}, nil
}

func (h *Handler) handleTerminateClientVpnConnections(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("ClientVpnEndpointId")
	if err := h.Backend.TerminateClientVpnConnections(endpointID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "TerminateClientVpnConnectionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- TGW Peering handlers ----

func toTGWPeeringAttachmentItem(att *TransitGatewayPeeringAttachment) tgwPeeringAttachmentItem {
	return tgwPeeringAttachmentItem{
		TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
		RequesterTransitGatewayID:  att.RequesterTransitGatewayID,
		AccepterTransitGatewayID:   att.AccepterTransitGatewayID,
		State:                      att.State,
	}
}

func (h *Handler) handleCreateTransitGatewayPeeringAttachment(
	vals url.Values,
	reqID string,
) (any, error) {
	tgwID := vals.Get("TransitGatewayId")
	peerTGWID := vals.Get("PeerTransitGatewayId")
	peerRegion := vals.Get("PeerRegion")

	att, err := h.Backend.CreateTransitGatewayPeeringAttachment(tgwID, peerTGWID, peerRegion)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayPeeringAttachmentResponse{
		RequestID:                       reqID,
		TransitGatewayPeeringAttachment: toTGWPeeringAttachmentItem(att),
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayPeeringAttachment(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("TransitGatewayAttachmentId")
	if err := h.Backend.DeleteTransitGatewayPeeringAttachment(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayPeeringAttachmentResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayPeeringAttachments(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayAttachmentId")
	atts := h.Backend.DescribeTransitGatewayPeeringAttachments(ids)

	resp := &describeTransitGatewayPeeringAttachmentsResponse{RequestID: reqID}
	for _, att := range atts {
		resp.TransitGatewayPeeringAttachments.Items = append(
			resp.TransitGatewayPeeringAttachments.Items,
			toTGWPeeringAttachmentItem(att),
		)
	}

	return resp, nil
}

// ---- TGW Connect handlers ----

func toTGWConnectItem(conn *TransitGatewayConnect) tgwConnectItem {
	return tgwConnectItem{
		TransitGatewayAttachmentID:          conn.TransitGatewayAttachmentID,
		TransportTransitGatewayAttachmentID: conn.TransportTransitGatewayAttachmentID,
		TransitGatewayID:                    conn.TransitGatewayID,
		State:                               conn.State,
	}
}

func (h *Handler) handleCreateTransitGatewayConnect(vals url.Values, reqID string) (any, error) {
	transportID := vals.Get("TransportTransitGatewayAttachmentId")
	tgwID := vals.Get("TransitGatewayId")

	conn, err := h.Backend.CreateTransitGatewayConnect(transportID, tgwID)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayConnectResponse{
		RequestID:             reqID,
		TransitGatewayConnect: toTGWConnectItem(conn),
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayConnect(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayAttachmentId")
	if err := h.Backend.DeleteTransitGatewayConnect(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayConnectResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayConnects(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayAttachmentId")
	conns := h.Backend.DescribeTransitGatewayConnects(ids)

	resp := &describeTransitGatewayConnectsResponse{RequestID: reqID}
	for _, conn := range conns {
		resp.TransitGatewayConnects.Items = append(resp.TransitGatewayConnects.Items, toTGWConnectItem(conn))
	}

	return resp, nil
}

func (h *Handler) handleCreateTransitGatewayConnectPeer(vals url.Values, reqID string) (any, error) {
	connectID := vals.Get("TransitGatewayAttachmentId")
	peerAddress := vals.Get("PeerAddress")
	insideCidrs := parseMemberList(vals, "InsideCidrBlocks")

	peer, err := h.Backend.CreateTransitGatewayConnectPeer(connectID, peerAddress, insideCidrs)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayConnectPeerResponse{
		RequestID: reqID,
		TransitGatewayConnectPeer: tgwConnectPeerItem{
			TransitGatewayConnectPeerID: peer.TransitGatewayConnectPeerID,
			TransitGatewayAttachmentID:  peer.TransitGatewayAttachmentID,
			State:                       peer.State,
			PeerAddress:                 peer.PeerAddress,
		},
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayConnectPeer(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayConnectPeerId")
	if err := h.Backend.DeleteTransitGatewayConnectPeer(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayConnectPeerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayConnectPeers(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayConnectPeerId")
	peers := h.Backend.DescribeTransitGatewayConnectPeers(ids)

	resp := &describeTransitGatewayConnectPeersResponse{RequestID: reqID}
	for _, peer := range peers {
		resp.TransitGatewayConnectPeers.Items = append(
			resp.TransitGatewayConnectPeers.Items,
			tgwConnectPeerItem{
				TransitGatewayConnectPeerID: peer.TransitGatewayConnectPeerID,
				TransitGatewayAttachmentID:  peer.TransitGatewayAttachmentID,
				State:                       peer.State,
				PeerAddress:                 peer.PeerAddress,
			},
		)
	}

	return resp, nil
}

// ---- TGW PrefixListRef handlers ----

func (h *Handler) handleCreateTransitGatewayPrefixListReference(
	vals url.Values,
	reqID string,
) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	prefixListID := vals.Get("PrefixListId")
	blackhole := vals.Get("Blackhole") == ec2BooleanTrue

	ref, err := h.Backend.CreateTransitGatewayPrefixListReference(routeTableID, prefixListID, blackhole)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayPrefixListReferenceResponse{
		RequestID: reqID,
		TransitGatewayPrefixListReference: tgwPrefixListRefItem{
			PrefixListID:               ref.PrefixListID,
			TransitGatewayRouteTableID: ref.TransitGatewayRouteTableID,
			State:                      ref.State,
			Blackhole:                  ref.Blackhole,
		},
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayPrefixListReference(
	vals url.Values,
	reqID string,
) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	prefixListID := vals.Get("PrefixListId")
	if err := h.Backend.DeleteTransitGatewayPrefixListReference(routeTableID, prefixListID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayPrefixListReferenceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleGetTransitGatewayPrefixListReferences(
	vals url.Values,
	reqID string,
) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	refs, err := h.Backend.GetTransitGatewayPrefixListReferences(routeTableID)
	if err != nil {
		return nil, err
	}

	resp := &getTransitGatewayPrefixListReferencesResponse{RequestID: reqID}
	for _, ref := range refs {
		resp.TransitGatewayPrefixListReferenceSet.Items = append(
			resp.TransitGatewayPrefixListReferenceSet.Items,
			tgwPrefixListRefItem{
				PrefixListID:               ref.PrefixListID,
				TransitGatewayRouteTableID: ref.TransitGatewayRouteTableID,
				State:                      ref.State,
				Blackhole:                  ref.Blackhole,
			},
		)
	}

	return resp, nil
}

// ---- VerifiedAccess handlers ----

func (h *Handler) handleCreateVerifiedAccessEndpoint(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("VerifiedAccessGroupId")
	endpointType := vals.Get("EndpointType")
	description := vals.Get("Description")

	ep, err := h.Backend.CreateVerifiedAccessEndpoint(groupID, endpointType, description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessEndpointResponse{
		RequestID: reqID,
		VerifiedAccessEndpoint: verifiedAccessEndpointItem{
			VerifiedAccessEndpointID: ep.VerifiedAccessEndpointID,
			VerifiedAccessGroupID:    ep.VerifiedAccessGroupID,
			Status:                   ep.Status,
			Description:              ep.Description,
			EndpointType:             ep.EndpointType,
		},
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessEndpointId")
	if err := h.Backend.DeleteVerifiedAccessEndpoint(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVerifiedAccessEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeVerifiedAccessEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessEndpointId")
	eps := h.Backend.DescribeVerifiedAccessEndpoints(ids)

	resp := &describeVerifiedAccessEndpointsResponse{RequestID: reqID}
	for _, ep := range eps {
		resp.VerifiedAccessEndpointSet.Items = append(
			resp.VerifiedAccessEndpointSet.Items,
			verifiedAccessEndpointItem{
				VerifiedAccessEndpointID: ep.VerifiedAccessEndpointID,
				VerifiedAccessGroupID:    ep.VerifiedAccessGroupID,
				Status:                   ep.Status,
				Description:              ep.Description,
				EndpointType:             ep.EndpointType,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyVerifiedAccessEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessEndpointId")
	description := vals.Get("Description")
	if err := h.Backend.ModifyVerifiedAccessEndpoint(id, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleCreateVerifiedAccessGroup(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")
	description := vals.Get("Description")

	grp, err := h.Backend.CreateVerifiedAccessGroup(instanceID, description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessGroupResponse{
		RequestID: reqID,
		VerifiedAccessGroup: verifiedAccessGroupItem{
			VerifiedAccessGroupID:    grp.VerifiedAccessGroupID,
			VerifiedAccessInstanceID: grp.VerifiedAccessInstanceID,
			Status:                   grp.Status,
			Description:              grp.Description,
		},
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessGroup(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessGroupId")
	if err := h.Backend.DeleteVerifiedAccessGroup(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVerifiedAccessGroupResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeVerifiedAccessGroups(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessGroupId")
	groups := h.Backend.DescribeVerifiedAccessGroups(ids)

	resp := &describeVerifiedAccessGroupsResponse{RequestID: reqID}
	for _, grp := range groups {
		resp.VerifiedAccessGroupSet.Items = append(
			resp.VerifiedAccessGroupSet.Items,
			verifiedAccessGroupItem{
				VerifiedAccessGroupID:    grp.VerifiedAccessGroupID,
				VerifiedAccessInstanceID: grp.VerifiedAccessInstanceID,
				Status:                   grp.Status,
				Description:              grp.Description,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateVerifiedAccessInstance(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")

	inst, err := h.Backend.CreateVerifiedAccessInstance(description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessInstanceResponse{
		RequestID: reqID,
		VerifiedAccessInstance: verifiedAccessInstanceItem{
			VerifiedAccessInstanceID: inst.VerifiedAccessInstanceID,
			Status:                   inst.Status,
			Description:              inst.Description,
		},
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessInstance(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessInstanceId")
	if err := h.Backend.DeleteVerifiedAccessInstance(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVerifiedAccessInstanceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeVerifiedAccessInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessInstanceId")
	instances := h.Backend.DescribeVerifiedAccessInstances(ids)

	resp := &describeVerifiedAccessInstancesResponse{RequestID: reqID}
	for _, inst := range instances {
		resp.VerifiedAccessInstanceSet.Items = append(
			resp.VerifiedAccessInstanceSet.Items,
			verifiedAccessInstanceItem{
				VerifiedAccessInstanceID: inst.VerifiedAccessInstanceID,
				Status:                   inst.Status,
				Description:              inst.Description,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	providerType := vals.Get("TrustProviderType")
	description := vals.Get("Description")

	tp, err := h.Backend.CreateVerifiedAccessTrustProvider(providerType, description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessTrustProviderResponse{
		RequestID: reqID,
		VerifiedAccessTrustProvider: verifiedAccessTrustProviderItem{
			VerifiedAccessTrustProviderID: tp.VerifiedAccessTrustProviderID,
			TrustProviderType:             tp.TrustProviderType,
			Status:                        tp.Status,
			Description:                   tp.Description,
		},
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessTrustProviderId")
	if err := h.Backend.DeleteVerifiedAccessTrustProvider(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVerifiedAccessTrustProviderResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeVerifiedAccessTrustProviders(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessTrustProviderId")
	providers := h.Backend.DescribeVerifiedAccessTrustProviders(ids)

	resp := &describeVerifiedAccessTrustProvidersResponse{RequestID: reqID}
	for _, tp := range providers {
		resp.VerifiedAccessTrustProviderSet.Items = append(
			resp.VerifiedAccessTrustProviderSet.Items,
			verifiedAccessTrustProviderItem{
				VerifiedAccessTrustProviderID: tp.VerifiedAccessTrustProviderID,
				TrustProviderType:             tp.TrustProviderType,
				Status:                        tp.Status,
				Description:                   tp.Description,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleAttachVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")
	providerID := vals.Get("VerifiedAccessTrustProviderId")
	if err := h.Backend.AttachVerifiedAccessTrustProvider(instanceID, providerID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AttachVerifiedAccessTrustProviderResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDetachVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")
	providerID := vals.Get("VerifiedAccessTrustProviderId")
	if err := h.Backend.DetachVerifiedAccessTrustProvider(instanceID, providerID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DetachVerifiedAccessTrustProviderResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- Helpers ----

// itoa converts an int to decimal string.
func itoa(i int) string {
	return strconv.Itoa(i)
}

// parseIntValue parses s into *v. Ignores parse errors (best-effort).
func parseIntValue(s string, v *int) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return
	}
	if v != nil {
		*v = n
	}
}
