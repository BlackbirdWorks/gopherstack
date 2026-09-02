package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- Handler registration ----

func registerLocalGatewayOps(h *Handler, ops map[string]ec2ActionFn) {
	// Describe-only (no Create API — Outpost-provisioned)
	ops["DescribeLocalGateways"] = h.handleDescribeLocalGateways
	ops["DescribeLocalGatewayVirtualInterfaces"] = h.handleDescribeLocalGatewayVirtualInterfaces
	ops["DescribeLocalGatewayVirtualInterfaceGroups"] = h.handleDescribeLocalGatewayVirtualInterfaceGroups

	// Local Gateway Route Tables
	ops["CreateLocalGatewayRouteTable"] = h.handleCreateLocalGatewayRouteTable
	ops["DescribeLocalGatewayRouteTables"] = h.handleDescribeLocalGatewayRouteTables
	ops["DeleteLocalGatewayRouteTable"] = h.handleDeleteLocalGatewayRouteTable

	// Local Gateway Routes
	ops["CreateLocalGatewayRoute"] = h.handleCreateLocalGatewayRoute
	ops["DeleteLocalGatewayRoute"] = h.handleDeleteLocalGatewayRoute
	ops["ModifyLocalGatewayRoute"] = h.handleModifyLocalGatewayRoute
	ops["SearchLocalGatewayRoutes"] = h.handleSearchLocalGatewayRoutes

	// Local Gateway Route Table VPC Associations
	ops["CreateLocalGatewayRouteTableVpcAssociation"] = h.handleCreateLocalGatewayRouteTableVpcAssociation
	ops["DeleteLocalGatewayRouteTableVpcAssociation"] = h.handleDeleteLocalGatewayRouteTableVpcAssociation
	ops["DescribeLocalGatewayRouteTableVpcAssociations"] = h.handleDescribeLocalGatewayRouteTableVpcAssociations

	// Local Gateway Route Table Virtual Interface Group Associations
	ops["CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation"] = h.handleCreateLGWVifGroupAssoc
	ops["DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation"] = h.handleDeleteLGWVifGroupAssoc
	ops["DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations"] = h.handleDescribeLGWVifGroupAssocs

	// Local Gateway Virtual Interfaces / Virtual Interface Groups (Create/Delete API)
	ops["CreateLocalGatewayVirtualInterface"] = h.handleCreateLocalGatewayVirtualInterface
	ops["DeleteLocalGatewayVirtualInterface"] = h.handleDeleteLocalGatewayVirtualInterface
	ops["CreateLocalGatewayVirtualInterfaceGroup"] = h.handleCreateLocalGatewayVirtualInterfaceGroup
	ops["DeleteLocalGatewayVirtualInterfaceGroup"] = h.handleDeleteLocalGatewayVirtualInterfaceGroup
}

func localGatewaySupportedOperations() []string {
	return []string{
		"DescribeLocalGateways",
		"DescribeLocalGatewayVirtualInterfaces",
		"DescribeLocalGatewayVirtualInterfaceGroups",
		"CreateLocalGatewayRouteTable",
		"DescribeLocalGatewayRouteTables",
		"DeleteLocalGatewayRouteTable",
		"CreateLocalGatewayRoute",
		"DeleteLocalGatewayRoute",
		"ModifyLocalGatewayRoute",
		"SearchLocalGatewayRoutes",
		"CreateLocalGatewayRouteTableVpcAssociation",
		"DeleteLocalGatewayRouteTableVpcAssociation",
		"DescribeLocalGatewayRouteTableVpcAssociations",
		"CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
		"DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
		"DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations",
		"CreateLocalGatewayVirtualInterface",
		"DeleteLocalGatewayVirtualInterface",
		"CreateLocalGatewayVirtualInterfaceGroup",
		"DeleteLocalGatewayVirtualInterfaceGroup",
	}
}

// ---- XML entity types ----

type localGatewayItem struct {
	LocalGatewayID string `xml:"localGatewayId"`
	OutpostArn     string `xml:"outpostArn"`
	OwnerID        string `xml:"ownerId"`
	State          string `xml:"state"`
}

type localGatewayVifItem struct {
	LocalGatewayVirtualInterfaceID      string          `xml:"localGatewayVirtualInterfaceId"`
	LocalGatewayVirtualInterfaceArn     string          `xml:"localGatewayVirtualInterfaceArn,omitempty"`
	LocalGatewayID                      string          `xml:"localGatewayId"`
	LocalGatewayVirtualInterfaceGroupID string          `xml:"localGatewayVirtualInterfaceGroupId,omitempty"`
	OutpostLagID                        string          `xml:"outpostLagId,omitempty"`
	ConfigurationState                  string          `xml:"configurationState"`
	LocalAddress                        string          `xml:"localAddress"`
	PeerAddress                         string          `xml:"peerAddress"`
	OwnerID                             string          `xml:"ownerId"`
	TagSet                              []simpleTagItem `xml:"tagSet>item"`
	LocalBgpAsn                         int32           `xml:"localBgpAsn"`
	PeerBgpAsn                          int32           `xml:"peerBgpAsn"`
	PeerBgpAsnExtended                  int64           `xml:"peerBgpAsnExtended,omitempty"`
	Vlan                                int32           `xml:"vlan"`
}

type localGatewayVifGroupItem struct {
	LocalGatewayVirtualInterfaceGroupID  string          `xml:"localGatewayVirtualInterfaceGroupId"`
	LocalGatewayVirtualInterfaceGroupArn string          `xml:"localGatewayVirtualInterfaceGroupArn,omitempty"`
	LocalGatewayID                       string          `xml:"localGatewayId"`
	ConfigurationState                   string          `xml:"configurationState"`
	OwnerID                              string          `xml:"ownerId"`
	TagSet                               []simpleTagItem `xml:"tagSet>item"`
	VifIDs                               []string        `xml:"localGatewayVirtualInterfaceIdSet>item"`
	LocalBgpAsn                          int32           `xml:"localBgpAsn"`
	LocalBgpAsnExtended                  int64           `xml:"localBgpAsnExtended,omitempty"`
}

type localGatewayRouteTableItem struct {
	LocalGatewayRouteTableID  string `xml:"localGatewayRouteTableId"`
	LocalGatewayRouteTableArn string `xml:"localGatewayRouteTableArn"`
	LocalGatewayID            string `xml:"localGatewayId"`
	OutpostArn                string `xml:"outpostArn"`
	Mode                      string `xml:"mode"`
	State                     string `xml:"state"`
	OwnerID                   string `xml:"ownerId"`
}

type localGatewayRouteItem struct {
	DestinationCidrBlock                string `xml:"destinationCidrBlock,omitempty"`
	DestinationPrefixListID             string `xml:"destinationPrefixListId,omitempty"`
	LocalGatewayRouteTableID            string `xml:"localGatewayRouteTableId"`
	LocalGatewayRouteTableArn           string `xml:"localGatewayRouteTableArn"`
	LocalGatewayVirtualInterfaceGroupID string `xml:"localGatewayVirtualInterfaceGroupId,omitempty"`
	NetworkInterfaceID                  string `xml:"networkInterfaceId,omitempty"`
	Type                                string `xml:"type"`
	State                               string `xml:"state"`
	OwnerID                             string `xml:"ownerId"`
}

type localGatewayVpcAssociationItem struct {
	LocalGatewayRouteTableVpcAssociationID string `xml:"localGatewayRouteTableVpcAssociationId"`
	LocalGatewayRouteTableID               string `xml:"localGatewayRouteTableId"`
	LocalGatewayRouteTableArn              string `xml:"localGatewayRouteTableArn"`
	LocalGatewayID                         string `xml:"localGatewayId"`
	VpcID                                  string `xml:"vpcId"`
	State                                  string `xml:"state"`
	OwnerID                                string `xml:"ownerId"`
}

// lgwVifGroupAssocItem is the XML item for a local gateway route table virtual
// interface group association. Field names are shortened from the AWS shape name
// (LocalGatewayRouteTableVirtualInterfaceGroupAssociation...) to keep lines within
// the repo's line-length limit; the xml tags carry the exact AWS wire names.
type lgwVifGroupAssocItem struct {
	ID                                  string `xml:"localGatewayRouteTableVirtualInterfaceGroupAssociationId"`
	LocalGatewayRouteTableID            string `xml:"localGatewayRouteTableId"`
	LocalGatewayRouteTableArn           string `xml:"localGatewayRouteTableArn"`
	LocalGatewayID                      string `xml:"localGatewayId"`
	LocalGatewayVirtualInterfaceGroupID string `xml:"localGatewayVirtualInterfaceGroupId"`
	State                               string `xml:"state"`
	OwnerID                             string `xml:"ownerId"`
}

// ---- XML response types ----

type describeLocalGatewaysResponse struct {
	XMLName       xml.Name `xml:"DescribeLocalGatewaysResponse"`
	RequestID     string   `xml:"requestId"`
	LocalGateways struct {
		Items []localGatewayItem `xml:"item"`
	} `xml:"localGatewaySet"`
}

type describeLocalGatewayVirtualInterfacesResponse struct {
	XMLName                       xml.Name `xml:"DescribeLocalGatewayVirtualInterfacesResponse"`
	RequestID                     string   `xml:"requestId"`
	LocalGatewayVirtualInterfaces struct {
		Items []localGatewayVifItem `xml:"item"`
	} `xml:"localGatewayVirtualInterfaceSet"`
}

type describeLocalGatewayVirtualInterfaceGroupsResponse struct {
	XMLName                            xml.Name `xml:"DescribeLocalGatewayVirtualInterfaceGroupsResponse"`
	RequestID                          string   `xml:"requestId"`
	LocalGatewayVirtualInterfaceGroups struct {
		Items []localGatewayVifGroupItem `xml:"item"`
	} `xml:"localGatewayVirtualInterfaceGroupSet"`
}

type createLocalGatewayRouteTableResponse struct {
	XMLName                xml.Name                   `xml:"CreateLocalGatewayRouteTableResponse"`
	RequestID              string                     `xml:"requestId"`
	LocalGatewayRouteTable localGatewayRouteTableItem `xml:"localGatewayRouteTable"`
}

type describeLocalGatewayRouteTablesResponse struct {
	XMLName                 xml.Name `xml:"DescribeLocalGatewayRouteTablesResponse"`
	RequestID               string   `xml:"requestId"`
	LocalGatewayRouteTables struct {
		Items []localGatewayRouteTableItem `xml:"item"`
	} `xml:"localGatewayRouteTableSet"`
}

type deleteLocalGatewayRouteTableResponse struct {
	XMLName                xml.Name                   `xml:"DeleteLocalGatewayRouteTableResponse"`
	RequestID              string                     `xml:"requestId"`
	LocalGatewayRouteTable localGatewayRouteTableItem `xml:"localGatewayRouteTable"`
}

type createLocalGatewayRouteResponse struct {
	XMLName   xml.Name              `xml:"CreateLocalGatewayRouteResponse"`
	RequestID string                `xml:"requestId"`
	Route     localGatewayRouteItem `xml:"route"`
}

type deleteLocalGatewayRouteResponse struct {
	XMLName   xml.Name              `xml:"DeleteLocalGatewayRouteResponse"`
	RequestID string                `xml:"requestId"`
	Route     localGatewayRouteItem `xml:"route"`
}

type modifyLocalGatewayRouteResponse struct {
	XMLName   xml.Name              `xml:"ModifyLocalGatewayRouteResponse"`
	RequestID string                `xml:"requestId"`
	Route     localGatewayRouteItem `xml:"route"`
}

type searchLocalGatewayRoutesResponse struct {
	XMLName   xml.Name `xml:"SearchLocalGatewayRoutesResponse"`
	RequestID string   `xml:"requestId"`
	Routes    struct {
		Items []localGatewayRouteItem `xml:"item"`
	} `xml:"routeSet"`
}

type createLocalGatewayRouteTableVpcAssociationResponse struct {
	XMLName   xml.Name                       `xml:"CreateLocalGatewayRouteTableVpcAssociationResponse"`
	RequestID string                         `xml:"requestId"`
	Assoc     localGatewayVpcAssociationItem `xml:"localGatewayRouteTableVpcAssociation"`
}

type deleteLocalGatewayRouteTableVpcAssociationResponse struct {
	XMLName   xml.Name                       `xml:"DeleteLocalGatewayRouteTableVpcAssociationResponse"`
	RequestID string                         `xml:"requestId"`
	Assoc     localGatewayVpcAssociationItem `xml:"localGatewayRouteTableVpcAssociation"`
}

type describeLocalGatewayRouteTableVpcAssociationsResponse struct {
	XMLName                               xml.Name `xml:"DescribeLocalGatewayRouteTableVpcAssociationsResponse"`
	RequestID                             string   `xml:"requestId"`
	LocalGatewayRouteTableVpcAssociations struct {
		Items []localGatewayVpcAssociationItem `xml:"item"`
	} `xml:"localGatewayRouteTableVpcAssociationSet"`
}

type createLGWVifGroupAssocResponse struct {
	XMLName   xml.Name             `xml:"CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociationResponse"`
	RequestID string               `xml:"requestId"`
	Assoc     lgwVifGroupAssocItem `xml:"localGatewayRouteTableVirtualInterfaceGroupAssociation"`
}

type deleteLGWVifGroupAssocResponse struct {
	XMLName   xml.Name             `xml:"DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociationResponse"`
	RequestID string               `xml:"requestId"`
	Assoc     lgwVifGroupAssocItem `xml:"localGatewayRouteTableVirtualInterfaceGroupAssociation"`
}

type describeLGWVifGroupAssocsResponse struct {
	XMLName   xml.Name `xml:"DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociationsResponse"`
	RequestID string   `xml:"requestId"`
	Assocs    struct {
		Items []lgwVifGroupAssocItem `xml:"item"`
	} `xml:"localGatewayRouteTableVirtualInterfaceGroupAssociationSet"`
}

// ---- item conversion helpers ----

func localGatewayToItem(lg *LocalGateway) localGatewayItem {
	return localGatewayItem{
		LocalGatewayID: lg.LocalGatewayID,
		OutpostArn:     lg.OutpostArn,
		OwnerID:        lg.OwnerID,
		State:          lg.State,
	}
}

func localGatewayVifToItem(vif *LocalGatewayVirtualInterface, tags map[string]string) localGatewayVifItem {
	return localGatewayVifItem{
		LocalGatewayVirtualInterfaceID:      vif.LocalGatewayVirtualInterfaceID,
		LocalGatewayVirtualInterfaceArn:     vif.LocalGatewayVirtualInterfaceArn,
		LocalGatewayID:                      vif.LocalGatewayID,
		LocalGatewayVirtualInterfaceGroupID: vif.LocalGatewayVirtualInterfaceGroupID,
		OutpostLagID:                        vif.OutpostLagID,
		ConfigurationState:                  vif.ConfigurationState,
		LocalAddress:                        vif.LocalAddress,
		PeerAddress:                         vif.PeerAddress,
		LocalBgpAsn:                         vif.LocalBgpAsn,
		PeerBgpAsn:                          vif.PeerBgpAsn,
		PeerBgpAsnExtended:                  vif.PeerBgpAsnExtended,
		Vlan:                                vif.Vlan,
		OwnerID:                             vif.OwnerID,
		TagSet:                              tagItemsFromMap(tags),
	}
}

func localGatewayVifGroupToItem(
	group *LocalGatewayVirtualInterfaceGroup,
	tags map[string]string,
) localGatewayVifGroupItem {
	return localGatewayVifGroupItem{
		LocalGatewayVirtualInterfaceGroupID:  group.LocalGatewayVirtualInterfaceGroupID,
		LocalGatewayVirtualInterfaceGroupArn: group.LocalGatewayVirtualInterfaceGroupArn,
		LocalGatewayID:                       group.LocalGatewayID,
		LocalBgpAsn:                          group.LocalBgpAsn,
		LocalBgpAsnExtended:                  group.LocalBgpAsnExtended,
		ConfigurationState:                   group.ConfigurationState,
		VifIDs:                               group.LocalGatewayVirtualInterfaceIDs,
		OwnerID:                              group.OwnerID,
		TagSet:                               tagItemsFromMap(tags),
	}
}

func localGatewayRouteTableToItem(rt *LocalGatewayRouteTable) localGatewayRouteTableItem {
	return localGatewayRouteTableItem{
		LocalGatewayRouteTableID:  rt.LocalGatewayRouteTableID,
		LocalGatewayRouteTableArn: rt.LocalGatewayRouteTableArn,
		LocalGatewayID:            rt.LocalGatewayID,
		OutpostArn:                rt.OutpostArn,
		Mode:                      rt.Mode,
		State:                     rt.State,
		OwnerID:                   rt.OwnerID,
	}
}

func localGatewayRouteToItem(r *LocalGatewayRoute) localGatewayRouteItem {
	return localGatewayRouteItem{
		DestinationCidrBlock:                r.DestinationCidrBlock,
		DestinationPrefixListID:             r.DestinationPrefixListID,
		LocalGatewayRouteTableID:            r.LocalGatewayRouteTableID,
		LocalGatewayRouteTableArn:           r.LocalGatewayRouteTableArn,
		LocalGatewayVirtualInterfaceGroupID: r.LocalGatewayVirtualInterfaceGroupID,
		NetworkInterfaceID:                  r.NetworkInterfaceID,
		Type:                                r.Type,
		State:                               r.State,
		OwnerID:                             r.OwnerID,
	}
}

func localGatewayVpcAssociationToItem(a *LocalGatewayRouteTableVpcAssociation) localGatewayVpcAssociationItem {
	return localGatewayVpcAssociationItem{
		LocalGatewayRouteTableVpcAssociationID: a.LocalGatewayRouteTableVpcAssociationID,
		LocalGatewayRouteTableID:               a.LocalGatewayRouteTableID,
		LocalGatewayRouteTableArn:              a.LocalGatewayRouteTableArn,
		LocalGatewayID:                         a.LocalGatewayID,
		VpcID:                                  a.VpcID,
		State:                                  a.State,
		OwnerID:                                a.OwnerID,
	}
}

func lgwVifGroupAssocToItem(
	a *LocalGatewayRouteTableVirtualInterfaceGroupAssociation,
) lgwVifGroupAssocItem {
	return lgwVifGroupAssocItem{
		ID:                                  a.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID,
		LocalGatewayRouteTableID:            a.LocalGatewayRouteTableID,
		LocalGatewayRouteTableArn:           a.LocalGatewayRouteTableArn,
		LocalGatewayID:                      a.LocalGatewayID,
		LocalGatewayVirtualInterfaceGroupID: a.LocalGatewayVirtualInterfaceGroupID,
		State:                               a.State,
		OwnerID:                             a.OwnerID,
	}
}

// ---- handlers: Describe (no Create API) ----

func (h *Handler) handleDescribeLocalGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "LocalGatewayId")
	lgws := h.Backend.DescribeLocalGateways(ids)

	resp := &describeLocalGatewaysResponse{RequestID: reqID}
	for _, lg := range lgws {
		resp.LocalGateways.Items = append(resp.LocalGateways.Items, localGatewayToItem(lg))
	}

	return resp, nil
}

func (h *Handler) handleDescribeLocalGatewayVirtualInterfaces(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "LocalGatewayVirtualInterfaceId")
	vifs := h.Backend.DescribeLocalGatewayVirtualInterfaces(ids)

	resp := &describeLocalGatewayVirtualInterfacesResponse{RequestID: reqID}
	for _, vif := range vifs {
		resp.LocalGatewayVirtualInterfaces.Items = append(
			resp.LocalGatewayVirtualInterfaces.Items,
			localGatewayVifToItem(vif, h.Backend.TagsForResource(vif.LocalGatewayVirtualInterfaceID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeLocalGatewayVirtualInterfaceGroups(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "LocalGatewayVirtualInterfaceGroupId")
	groups := h.Backend.DescribeLocalGatewayVirtualInterfaceGroups(ids)

	resp := &describeLocalGatewayVirtualInterfaceGroupsResponse{RequestID: reqID}
	for _, group := range groups {
		resp.LocalGatewayVirtualInterfaceGroups.Items = append(
			resp.LocalGatewayVirtualInterfaceGroups.Items,
			localGatewayVifGroupToItem(group, h.Backend.TagsForResource(group.LocalGatewayVirtualInterfaceGroupID)),
		)
	}

	return resp, nil
}

// ---- handlers: Local Gateway Route Tables ----

func (h *Handler) handleCreateLocalGatewayRouteTable(vals url.Values, reqID string) (any, error) {
	localGatewayID := vals.Get("LocalGatewayId")
	mode := vals.Get("Mode")

	rt, err := h.Backend.CreateLocalGatewayRouteTable(localGatewayID, mode)
	if err != nil {
		return nil, err
	}

	return &createLocalGatewayRouteTableResponse{
		RequestID:              reqID,
		LocalGatewayRouteTable: localGatewayRouteTableToItem(rt),
	}, nil
}

func (h *Handler) handleDescribeLocalGatewayRouteTables(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "LocalGatewayRouteTableId")
	rts := h.Backend.DescribeLocalGatewayRouteTables(ids)

	resp := &describeLocalGatewayRouteTablesResponse{RequestID: reqID}
	for _, rt := range rts {
		resp.LocalGatewayRouteTables.Items = append(
			resp.LocalGatewayRouteTables.Items,
			localGatewayRouteTableToItem(rt),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteLocalGatewayRouteTable(vals url.Values, reqID string) (any, error) {
	id := vals.Get("LocalGatewayRouteTableId")

	existing := h.Backend.DescribeLocalGatewayRouteTables([]string{id})

	if err := h.Backend.DeleteLocalGatewayRouteTable(id); err != nil {
		return nil, err
	}

	var item localGatewayRouteTableItem
	if len(existing) > 0 {
		item = localGatewayRouteTableToItem(existing[0])
		item.State = localGatewayRouteStateDeleted
	}

	return &deleteLocalGatewayRouteTableResponse{
		RequestID:              reqID,
		LocalGatewayRouteTable: item,
	}, nil
}

// ---- handlers: Local Gateway Routes ----

func (h *Handler) handleCreateLocalGatewayRoute(vals url.Values, reqID string) (any, error) {
	route, err := h.Backend.CreateLocalGatewayRoute(
		vals.Get("LocalGatewayRouteTableId"),
		vals.Get("DestinationCidrBlock"),
		vals.Get("DestinationPrefixListId"),
		vals.Get("LocalGatewayVirtualInterfaceGroupId"),
		vals.Get("NetworkInterfaceId"),
	)
	if err != nil {
		return nil, err
	}

	return &createLocalGatewayRouteResponse{
		RequestID: reqID,
		Route:     localGatewayRouteToItem(route),
	}, nil
}

func (h *Handler) handleDeleteLocalGatewayRoute(vals url.Values, reqID string) (any, error) {
	route, err := h.Backend.DeleteLocalGatewayRoute(
		vals.Get("LocalGatewayRouteTableId"),
		vals.Get("DestinationCidrBlock"),
		vals.Get("DestinationPrefixListId"),
	)
	if err != nil {
		return nil, err
	}

	return &deleteLocalGatewayRouteResponse{
		RequestID: reqID,
		Route:     localGatewayRouteToItem(route),
	}, nil
}

func (h *Handler) handleModifyLocalGatewayRoute(vals url.Values, reqID string) (any, error) {
	route, err := h.Backend.ModifyLocalGatewayRoute(
		vals.Get("LocalGatewayRouteTableId"),
		vals.Get("DestinationCidrBlock"),
		vals.Get("DestinationPrefixListId"),
		vals.Get("LocalGatewayVirtualInterfaceGroupId"),
		vals.Get("NetworkInterfaceId"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyLocalGatewayRouteResponse{
		RequestID: reqID,
		Route:     localGatewayRouteToItem(route),
	}, nil
}

// searchLocalGatewayRouteStates extracts the "state" filter's values from a
// SearchLocalGatewayRoutes request's Filter.N.Name/Value.M form fields.
// "route-search.exact-match" is a distinct, separately-documented filter
// name (api_op_SearchLocalGatewayRoutes.go) and must not be folded in here.
func searchLocalGatewayRouteStates(vals url.Values) []string {
	var states []string

	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}

		if name != "state" {
			continue
		}

		for j := 1; ; j++ {
			v := vals.Get(fmt.Sprintf("Filter.%d.Value.%d", i, j))
			if v == "" {
				break
			}

			states = append(states, v)
		}
	}

	return states
}

func (h *Handler) handleSearchLocalGatewayRoutes(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("LocalGatewayRouteTableId")
	states := searchLocalGatewayRouteStates(vals)

	routes, err := h.Backend.SearchLocalGatewayRoutes(routeTableID, states)
	if err != nil {
		return nil, err
	}

	resp := &searchLocalGatewayRoutesResponse{RequestID: reqID}
	for _, r := range routes {
		resp.Routes.Items = append(resp.Routes.Items, localGatewayRouteToItem(r))
	}

	return resp, nil
}

// ---- handlers: Local Gateway Route Table VPC Associations ----

func (h *Handler) handleCreateLocalGatewayRouteTableVpcAssociation(
	vals url.Values,
	reqID string,
) (any, error) {
	assoc, err := h.Backend.CreateLocalGatewayRouteTableVpcAssociation(
		vals.Get("LocalGatewayRouteTableId"),
		vals.Get("VpcId"),
	)
	if err != nil {
		return nil, err
	}

	return &createLocalGatewayRouteTableVpcAssociationResponse{
		RequestID: reqID,
		Assoc:     localGatewayVpcAssociationToItem(assoc),
	}, nil
}

func (h *Handler) handleDeleteLocalGatewayRouteTableVpcAssociation(
	vals url.Values,
	reqID string,
) (any, error) {
	assoc, err := h.Backend.DeleteLocalGatewayRouteTableVpcAssociation(
		vals.Get("LocalGatewayRouteTableVpcAssociationId"),
	)
	if err != nil {
		return nil, err
	}

	return &deleteLocalGatewayRouteTableVpcAssociationResponse{
		RequestID: reqID,
		Assoc:     localGatewayVpcAssociationToItem(assoc),
	}, nil
}

func (h *Handler) handleDescribeLocalGatewayRouteTableVpcAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "LocalGatewayRouteTableVpcAssociationId")
	assocs := h.Backend.DescribeLocalGatewayRouteTableVpcAssociations(ids)

	resp := &describeLocalGatewayRouteTableVpcAssociationsResponse{RequestID: reqID}
	for _, a := range assocs {
		resp.LocalGatewayRouteTableVpcAssociations.Items = append(
			resp.LocalGatewayRouteTableVpcAssociations.Items,
			localGatewayVpcAssociationToItem(a),
		)
	}

	return resp, nil
}

// ---- handlers: Local Gateway Route Table Virtual Interface Group Associations ----

func (h *Handler) handleCreateLGWVifGroupAssoc(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		vals.Get("LocalGatewayRouteTableId"),
		vals.Get("LocalGatewayVirtualInterfaceGroupId"),
	)
	if err != nil {
		return nil, err
	}

	return &createLGWVifGroupAssocResponse{
		RequestID: reqID,
		Assoc:     lgwVifGroupAssocToItem(assoc),
	}, nil
}

func (h *Handler) handleDeleteLGWVifGroupAssoc(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		vals.Get("LocalGatewayRouteTableVirtualInterfaceGroupAssociationId"),
	)
	if err != nil {
		return nil, err
	}

	return &deleteLGWVifGroupAssocResponse{
		RequestID: reqID,
		Assoc:     lgwVifGroupAssocToItem(assoc),
	}, nil
}

func (h *Handler) handleDescribeLGWVifGroupAssocs(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "LocalGatewayRouteTableVirtualInterfaceGroupAssociationId")
	assocs := h.Backend.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(ids)

	resp := &describeLGWVifGroupAssocsResponse{RequestID: reqID}
	for _, a := range assocs {
		resp.Assocs.Items = append(resp.Assocs.Items, lgwVifGroupAssocToItem(a))
	}

	return resp, nil
}

// ---- Local Gateway Virtual Interfaces / Virtual Interface Groups (Create/Delete API) ----

type createLocalGatewayVirtualInterfaceResponse struct {
	XMLName                      xml.Name            `xml:"CreateLocalGatewayVirtualInterfaceResponse"`
	RequestID                    string              `xml:"requestId"`
	LocalGatewayVirtualInterface localGatewayVifItem `xml:"localGatewayVirtualInterface"`
}

type deleteLocalGatewayVirtualInterfaceResponse struct {
	XMLName                      xml.Name            `xml:"DeleteLocalGatewayVirtualInterfaceResponse"`
	RequestID                    string              `xml:"requestId"`
	LocalGatewayVirtualInterface localGatewayVifItem `xml:"localGatewayVirtualInterface"`
}

type createLocalGatewayVirtualInterfaceGroupResponse struct {
	XMLName                           xml.Name                 `xml:"CreateLocalGatewayVirtualInterfaceGroupResponse"`
	RequestID                         string                   `xml:"requestId"`
	LocalGatewayVirtualInterfaceGroup localGatewayVifGroupItem `xml:"localGatewayVirtualInterfaceGroup"`
}

type deleteLocalGatewayVirtualInterfaceGroupResponse struct {
	XMLName                           xml.Name                 `xml:"DeleteLocalGatewayVirtualInterfaceGroupResponse"`
	RequestID                         string                   `xml:"requestId"`
	LocalGatewayVirtualInterfaceGroup localGatewayVifGroupItem `xml:"localGatewayVirtualInterfaceGroup"`
}

func (h *Handler) handleCreateLocalGatewayVirtualInterface(vals url.Values, reqID string) (any, error) {
	var peerBgpAsn, vlan int64

	peerBgpAsn, _ = strconv.ParseInt(vals.Get("PeerBgpAsn"), 10, 32)
	vlan, _ = strconv.ParseInt(vals.Get("Vlan"), 10, 32)
	peerBgpAsnExtended, _ := strconv.ParseInt(
		vals.Get("PeerBgpAsnExtended"), 10, 64,
	)

	tags := parseTagSpecification(vals, "local-gateway-virtual-interface")

	vif, err := h.Backend.CreateLocalGatewayVirtualInterface(LocalGatewayVirtualInterfaceParams{
		LocalGatewayVirtualInterfaceGroupID: vals.Get("LocalGatewayVirtualInterfaceGroupId"),
		OutpostLagID:                        vals.Get("OutpostLagId"),
		LocalAddress:                        vals.Get("LocalAddress"),
		PeerAddress:                         vals.Get("PeerAddress"),
		Vlan:                                int32(vlan),
		PeerBgpAsn:                          int32(peerBgpAsn),
		PeerBgpAsnExtended:                  peerBgpAsnExtended,
		Tags:                                tags,
	})
	if err != nil {
		return nil, err
	}

	return &createLocalGatewayVirtualInterfaceResponse{
		RequestID: reqID,
		LocalGatewayVirtualInterface: localGatewayVifToItem(
			vif, h.Backend.TagsForResource(vif.LocalGatewayVirtualInterfaceID),
		),
	}, nil
}

func (h *Handler) handleDeleteLocalGatewayVirtualInterface(vals url.Values, reqID string) (any, error) {
	id := vals.Get("LocalGatewayVirtualInterfaceId")
	tags := h.Backend.TagsForResource(id)

	vif, err := h.Backend.DeleteLocalGatewayVirtualInterface(id)
	if err != nil {
		return nil, err
	}

	return &deleteLocalGatewayVirtualInterfaceResponse{
		RequestID:                    reqID,
		LocalGatewayVirtualInterface: localGatewayVifToItem(vif, tags),
	}, nil
}

func (h *Handler) handleCreateLocalGatewayVirtualInterfaceGroup(vals url.Values, reqID string) (any, error) {
	localBgpAsn, _ := strconv.ParseInt(vals.Get("LocalBgpAsn"), 10, 32)
	localBgpAsnExtended, _ := strconv.ParseInt(
		vals.Get("LocalBgpAsnExtended"), 10, 64,
	)

	tags := parseTagSpecification(vals, "local-gateway-virtual-interface-group")

	group, err := h.Backend.CreateLocalGatewayVirtualInterfaceGroup(
		vals.Get("LocalGatewayId"),
		int32(localBgpAsn),
		localBgpAsnExtended,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocalGatewayVirtualInterfaceGroupResponse{
		RequestID: reqID,
		LocalGatewayVirtualInterfaceGroup: localGatewayVifGroupToItem(
			group, h.Backend.TagsForResource(group.LocalGatewayVirtualInterfaceGroupID),
		),
	}, nil
}

func (h *Handler) handleDeleteLocalGatewayVirtualInterfaceGroup(vals url.Values, reqID string) (any, error) {
	id := vals.Get("LocalGatewayVirtualInterfaceGroupId")
	tags := h.Backend.TagsForResource(id)

	group, err := h.Backend.DeleteLocalGatewayVirtualInterfaceGroup(id)
	if err != nil {
		return nil, err
	}

	return &deleteLocalGatewayVirtualInterfaceGroupResponse{
		RequestID:                         reqID,
		LocalGatewayVirtualInterfaceGroup: localGatewayVifGroupToItem(group, tags),
	}, nil
}
