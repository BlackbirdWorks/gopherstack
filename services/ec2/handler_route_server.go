package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// ---- Handler registration ----

func registerRouteServerOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateRouteServer"] = h.handleCreateRouteServer
	ops["DescribeRouteServers"] = h.handleDescribeRouteServers
	ops["DeleteRouteServer"] = h.handleDeleteRouteServer
	ops["ModifyRouteServer"] = h.handleModifyRouteServer

	ops["CreateRouteServerEndpoint"] = h.handleCreateRouteServerEndpoint
	ops["DescribeRouteServerEndpoints"] = h.handleDescribeRouteServerEndpoints
	ops["DeleteRouteServerEndpoint"] = h.handleDeleteRouteServerEndpoint

	ops["CreateRouteServerPeer"] = h.handleCreateRouteServerPeer
	ops["DescribeRouteServerPeers"] = h.handleDescribeRouteServerPeers
	ops["DeleteRouteServerPeer"] = h.handleDeleteRouteServerPeer

	ops["AssociateRouteServer"] = h.handleAssociateRouteServer
	ops["DisassociateRouteServer"] = h.handleDisassociateRouteServer
	ops["GetRouteServerAssociations"] = h.handleGetRouteServerAssociations

	ops["EnableRouteServerPropagation"] = h.handleEnableRouteServerPropagation
	ops["DisableRouteServerPropagation"] = h.handleDisableRouteServerPropagation
	ops["GetRouteServerPropagations"] = h.handleGetRouteServerPropagations

	ops["GetRouteServerRoutingDatabase"] = h.handleGetRouteServerRoutingDatabase
}

// ---- XML response types ----

type routeServerItem struct {
	RouteServerID           string          `xml:"routeServerId"`
	State                   string          `xml:"state,omitempty"`
	SnsTopicArn             string          `xml:"snsTopicArn,omitempty"`
	PersistRoutesState      string          `xml:"persistRoutesState,omitempty"`
	TagSet                  []simpleTagItem `xml:"tagSet>item"`
	AmazonSideAsn           int64           `xml:"amazonSideAsn,omitempty"`
	PersistRoutesDuration   int64           `xml:"persistRoutesDuration,omitempty"`
	SnsNotificationsEnabled bool            `xml:"snsNotificationsEnabled"`
}

func toRouteServerItem(rs *RouteServer, tags map[string]string) routeServerItem {
	return routeServerItem{
		RouteServerID:           rs.RouteServerID,
		AmazonSideAsn:           rs.AmazonSideAsn,
		State:                   rs.State,
		SnsNotificationsEnabled: rs.SnsNotificationsEnabled,
		SnsTopicArn:             rs.SnsTopicArn,
		PersistRoutesState:      rs.PersistRoutesState,
		PersistRoutesDuration:   rs.PersistRoutesDuration,
		TagSet:                  tagItemsFromMap(tags),
	}
}

type createRouteServerResponse struct {
	XMLName     xml.Name        `xml:"CreateRouteServerResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	RequestID   string          `xml:"requestId"`
	RouteServer routeServerItem `xml:"routeServer"`
}

type describeRouteServersResponse struct {
	XMLName      xml.Name `xml:"DescribeRouteServersResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	RouteServers struct {
		Items []routeServerItem `xml:"item"`
	} `xml:"routeServerSet"`
}

type deleteRouteServerResponse struct {
	XMLName     xml.Name        `xml:"DeleteRouteServerResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	RequestID   string          `xml:"requestId"`
	RouteServer routeServerItem `xml:"routeServer"`
}

type modifyRouteServerResponse struct {
	XMLName     xml.Name        `xml:"ModifyRouteServerResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	RequestID   string          `xml:"requestId"`
	RouteServer routeServerItem `xml:"routeServer"`
}

// routeServerEndpointItem.FailureReason is a flat scalar (ec2@v1.319.1
// deserializers.go awsEc2query_deserializeDocumentRouteServerEndpoint reads
// it via decoder.Value()), not a nested <code>/<message> element -- a real
// client fails outright ("expected value for failureReason element, got
// xml.StartElement") the first time this backend populates a failure reason.
type routeServerEndpointItem struct {
	RouteServerEndpointID string          `xml:"routeServerEndpointId"`
	RouteServerID         string          `xml:"routeServerId,omitempty"`
	SubnetID              string          `xml:"subnetId,omitempty"`
	VpcID                 string          `xml:"vpcId,omitempty"`
	EniID                 string          `xml:"eniId,omitempty"`
	EniAddress            string          `xml:"eniAddress,omitempty"`
	State                 string          `xml:"state,omitempty"`
	FailureReason         string          `xml:"failureReason,omitempty"`
	TagSet                []simpleTagItem `xml:"tagSet>item"`
}

func toRouteServerEndpointItem(ep *RouteServerEndpoint, tags map[string]string) routeServerEndpointItem {
	return routeServerEndpointItem{
		RouteServerEndpointID: ep.RouteServerEndpointID,
		RouteServerID:         ep.RouteServerID,
		SubnetID:              ep.SubnetID,
		VpcID:                 ep.VpcID,
		EniID:                 ep.EniID,
		EniAddress:            ep.EniAddress,
		State:                 ep.State,
		FailureReason:         joinStateReason(ep.StateReasonCode, ep.StateReasonMessage),
		TagSet:                tagItemsFromMap(tags),
	}
}

type createRouteServerEndpointResponse struct {
	XMLName             xml.Name                `xml:"CreateRouteServerEndpointResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	RouteServerEndpoint routeServerEndpointItem `xml:"routeServerEndpoint"`
}

type describeRouteServerEndpointsResponse struct {
	XMLName              xml.Name `xml:"DescribeRouteServerEndpointsResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	RequestID            string   `xml:"requestId"`
	RouteServerEndpoints struct {
		Items []routeServerEndpointItem `xml:"item"`
	} `xml:"routeServerEndpointSet"`
}

type deleteRouteServerEndpointResponse struct {
	XMLName             xml.Name                `xml:"DeleteRouteServerEndpointResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	RouteServerEndpoint routeServerEndpointItem `xml:"routeServerEndpoint"`
}

type routeServerBGPOptionsItem struct {
	PeerLivenessDetection string `xml:"peerLivenessDetection,omitempty"`
	PeerAsn               int64  `xml:"peerAsn,omitempty"`
}

type routeServerBGPStatusItem struct {
	Status       string `xml:"status,omitempty"`
	BgpPeerState string `xml:"bgpPeerState,omitempty"`
}

// routeServerPeerItem.FailureReason is a flat scalar for the same reason as
// routeServerEndpointItem.FailureReason above (ec2@v1.319.1 deserializers.go
// awsEc2query_deserializeDocumentRouteServerPeer, "failureReason" case).
type routeServerPeerItem struct {
	BgpStatus             routeServerBGPStatusItem  `xml:"bgpStatus"`
	FailureReason         string                    `xml:"failureReason,omitempty"`
	RouteServerPeerID     string                    `xml:"routeServerPeerId"`
	RouteServerEndpointID string                    `xml:"routeServerEndpointId,omitempty"`
	RouteServerID         string                    `xml:"routeServerId,omitempty"`
	SubnetID              string                    `xml:"subnetId,omitempty"`
	VpcID                 string                    `xml:"vpcId,omitempty"`
	State                 string                    `xml:"state,omitempty"`
	EniID                 string                    `xml:"endpointEniId,omitempty"`
	EniAddress            string                    `xml:"endpointEniAddress,omitempty"`
	PeerAddress           string                    `xml:"peerAddress,omitempty"`
	BgpOptions            routeServerBGPOptionsItem `xml:"bgpOptions"`
	TagSet                []simpleTagItem           `xml:"tagSet>item"`
}

// joinStateReason combines a resource's state-reason code and message into
// the single flat string several route-server response fields expect.
func joinStateReason(code, message string) string {
	switch {
	case code == "":
		return message
	case message == "":
		return code
	default:
		return code + ": " + message
	}
}

func toRouteServerPeerItem(p *RouteServerPeer, tags map[string]string) routeServerPeerItem {
	return routeServerPeerItem{
		RouteServerPeerID:     p.RouteServerPeerID,
		RouteServerEndpointID: p.RouteServerEndpointID,
		RouteServerID:         p.RouteServerID,
		SubnetID:              p.SubnetID,
		VpcID:                 p.VpcID,
		State:                 p.State,
		EniID:                 p.EniID,
		EniAddress:            p.EniAddress,
		PeerAddress:           p.PeerAddress,
		FailureReason:         joinStateReason(p.StateReasonCode, p.StateReasonMessage),
		TagSet:                tagItemsFromMap(tags),
		BgpOptions: routeServerBGPOptionsItem{
			PeerAsn:               p.BgpPeerAsn,
			PeerLivenessDetection: p.BgpPeerLivenessDetectionMode,
		},
		BgpStatus: routeServerBGPStatusItem{
			Status:       p.BgpStatus,
			BgpPeerState: p.BgpStatusPeerState,
		},
	}
}

type createRouteServerPeerResponse struct {
	XMLName         xml.Name            `xml:"CreateRouteServerPeerResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	RequestID       string              `xml:"requestId"`
	RouteServerPeer routeServerPeerItem `xml:"routeServerPeer"`
}

type describeRouteServerPeersResponse struct {
	XMLName          xml.Name `xml:"DescribeRouteServerPeersResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	RequestID        string   `xml:"requestId"`
	RouteServerPeers struct {
		Items []routeServerPeerItem `xml:"item"`
	} `xml:"routeServerPeerSet"`
}

type deleteRouteServerPeerResponse struct {
	XMLName         xml.Name            `xml:"DeleteRouteServerPeerResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	RequestID       string              `xml:"requestId"`
	RouteServerPeer routeServerPeerItem `xml:"routeServerPeer"`
}

type routeServerAssociationItem struct {
	RouteServerID string `xml:"routeServerId"`
	VpcID         string `xml:"vpcId,omitempty"`
	State         string `xml:"state,omitempty"`
}

func toRouteServerAssociationItem(a *RouteServerAssociation) routeServerAssociationItem {
	return routeServerAssociationItem{
		RouteServerID: a.RouteServerID,
		VpcID:         a.VpcID,
		State:         a.State,
	}
}

type associateRouteServerResponse struct {
	XMLName                xml.Name                   `xml:"AssociateRouteServerResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	RequestID              string                     `xml:"requestId"`
	RouteServerAssociation routeServerAssociationItem `xml:"routeServerAssociation"`
}

type disassociateRouteServerResponse struct {
	XMLName                xml.Name                   `xml:"DisassociateRouteServerResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	RequestID              string                     `xml:"requestId"`
	RouteServerAssociation routeServerAssociationItem `xml:"routeServerAssociation"`
}

type getRouteServerAssociationsResponse struct {
	XMLName                 xml.Name `xml:"GetRouteServerAssociationsResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	RequestID               string   `xml:"requestId"`
	RouteServerAssociations struct {
		Items []routeServerAssociationItem `xml:"item"`
	} `xml:"routeServerAssociationSet"`
}

type routeServerPropagationItem struct {
	RouteServerID string `xml:"routeServerId"`
	RouteTableID  string `xml:"routeTableId,omitempty"`
	State         string `xml:"state,omitempty"`
}

func toRouteServerPropagationItem(p *RouteServerPropagation) routeServerPropagationItem {
	return routeServerPropagationItem{
		RouteServerID: p.RouteServerID,
		RouteTableID:  p.RouteTableID,
		State:         p.State,
	}
}

type enableRouteServerPropagationResponse struct {
	XMLName                xml.Name                   `xml:"EnableRouteServerPropagationResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	RequestID              string                     `xml:"requestId"`
	RouteServerPropagation routeServerPropagationItem `xml:"routeServerPropagation"`
}

type disableRouteServerPropagationResponse struct {
	XMLName                xml.Name                   `xml:"DisableRouteServerPropagationResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	RequestID              string                     `xml:"requestId"`
	RouteServerPropagation routeServerPropagationItem `xml:"routeServerPropagation"`
}

type getRouteServerPropagationsResponse struct {
	XMLName                 xml.Name `xml:"GetRouteServerPropagationsResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	RequestID               string   `xml:"requestId"`
	RouteServerPropagations struct {
		Items []routeServerPropagationItem `xml:"item"`
	} `xml:"routeServerPropagationSet"`
}

type routeServerRouteInstallationDetailItem struct {
	RouteTableID                  string `xml:"routeTableId,omitempty"`
	RouteInstallationStatus       string `xml:"routeInstallationStatus,omitempty"`
	RouteInstallationStatusReason string `xml:"routeInstallationStatusReason,omitempty"`
}

type routeServerRouteItem struct {
	RouteServerEndpointID    string                                   `xml:"routeServerEndpointId,omitempty"`
	RouteServerPeerID        string                                   `xml:"routeServerPeerId,omitempty"`
	Prefix                   string                                   `xml:"prefix,omitempty"`
	NextHopIP                string                                   `xml:"nextHopIp,omitempty"`
	RouteStatus              string                                   `xml:"routeStatus,omitempty"`
	AsPaths                  []string                                 `xml:"asPathSet>item"`
	RouteInstallationDetails []routeServerRouteInstallationDetailItem `xml:"routeInstallationDetailSet>item"`
	Med                      int64                                    `xml:"med,omitempty"`
}

func toRouteServerRouteItem(r *RouteServerRoute) routeServerRouteItem {
	item := routeServerRouteItem{
		RouteServerEndpointID: r.RouteServerEndpointID,
		RouteServerPeerID:     r.RouteServerPeerID,
		Prefix:                r.Prefix,
		NextHopIP:             r.NextHopIP,
		RouteStatus:           r.RouteStatus,
		AsPaths:               r.AsPaths,
		Med:                   r.Med,
	}

	for _, d := range r.RouteInstallationDetails {
		item.RouteInstallationDetails = append(
			item.RouteInstallationDetails, routeServerRouteInstallationDetailItem(d),
		)
	}

	return item
}

// getRouteServerRoutingDatabaseResponse matches
// GetRouteServerRoutingDatabaseOutput (ec2@v1.319.1
// api_op_GetRouteServerRoutingDatabase.go): areRoutesPersisted/nextToken/
// routeSet only -- there is no routeServerId member on the response.
type getRouteServerRoutingDatabaseResponse struct {
	XMLName   xml.Name `xml:"GetRouteServerRoutingDatabaseResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Routes    struct {
		Items []routeServerRouteItem `xml:"item"`
	} `xml:"routeSet"`
	AreRoutesPersisted bool `xml:"areRoutesPersisted,omitempty"`
}

// ---- Route Server handlers ----

const (
	routeServerPersistRoutesStateEnabled  = "enabled"
	routeServerPersistRoutesStateDisabled = "disabled"
)

// routeServerPersistRoutesStateFromAction translates the request-side
// RouteServerPersistRoutesAction ("enable"/"disable"/"reset") into the
// response-side RouteServerPersistRoutesState ("enabled"/"disabled"/...).
// The two are distinct real enums (ec2@v1.319.1 types/enums.go) with
// different wire values for the same verb; storing the action string
// unnormalized would make DescribeRouteServers/GetRouteServerRoutingDatabase
// emit "enable" instead of the real "enabled".
func routeServerPersistRoutesStateFromAction(action string) string {
	switch action {
	case "enable":
		return routeServerPersistRoutesStateEnabled
	case "disable", "reset":
		return routeServerPersistRoutesStateDisabled
	default:
		return action
	}
}

func (h *Handler) handleCreateRouteServer(vals url.Values, reqID string) (any, error) {
	amazonSideAsn, _ := strconv.ParseInt(vals.Get("AmazonSideAsn"), 10, 64)
	persistRoutesState := routeServerPersistRoutesStateFromAction(vals.Get("PersistRoutes"))

	persistRoutesDuration, _ := strconv.ParseInt(vals.Get("PersistRoutesDuration"), 10, 64)
	snsNotificationsEnabled, _ := strconv.ParseBool(vals.Get("SnsNotificationsEnabled"))

	rs, err := h.Backend.CreateRouteServer(
		amazonSideAsn,
		persistRoutesState,
		persistRoutesDuration,
		snsNotificationsEnabled,
	)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "route-server")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{rs.RouteServerID}, tags); err != nil {
			return nil, err
		}
	}

	return &createRouteServerResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		RouteServer: toRouteServerItem(rs, tags),
	}, nil
}

func (h *Handler) handleDescribeRouteServers(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteServerId")
	servers := h.Backend.DescribeRouteServers(ids)

	resp := &describeRouteServersResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, rs := range servers {
		resp.RouteServers.Items = append(
			resp.RouteServers.Items, toRouteServerItem(rs, h.Backend.TagsForResource(rs.RouteServerID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteRouteServer(vals url.Values, reqID string) (any, error) {
	id := vals.Get("RouteServerId")

	rs, err := h.Backend.DeleteRouteServer(id)
	if err != nil {
		return nil, err
	}

	return &deleteRouteServerResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		RouteServer: toRouteServerItem(rs, nil),
	}, nil
}

func (h *Handler) handleModifyRouteServer(vals url.Values, reqID string) (any, error) {
	id := vals.Get("RouteServerId")
	persistRoutesState := routeServerPersistRoutesStateFromAction(vals.Get("PersistRoutes"))

	persistRoutesDuration, _ := strconv.ParseInt(vals.Get("PersistRoutesDuration"), 10, 64)
	snsNotificationsEnabled, _ := strconv.ParseBool(vals.Get("SnsNotificationsEnabled"))

	rs, err := h.Backend.ModifyRouteServer(id, persistRoutesState, persistRoutesDuration, snsNotificationsEnabled)
	if err != nil {
		return nil, err
	}

	return &modifyRouteServerResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		RouteServer: toRouteServerItem(rs, h.Backend.TagsForResource(rs.RouteServerID)),
	}, nil
}

func (h *Handler) handleCreateRouteServerEndpoint(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	subnetID := vals.Get("SubnetId")

	ep, err := h.Backend.CreateRouteServerEndpoint(routeServerID, subnetID)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "route-server-endpoint")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{ep.RouteServerEndpointID}, tags); err != nil {
			return nil, err
		}
	}

	return &createRouteServerEndpointResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		RouteServerEndpoint: toRouteServerEndpointItem(ep, tags),
	}, nil
}

func (h *Handler) handleDescribeRouteServerEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteServerEndpointId")
	endpoints := h.Backend.DescribeRouteServerEndpoints(ids)

	resp := &describeRouteServerEndpointsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, ep := range endpoints {
		resp.RouteServerEndpoints.Items = append(
			resp.RouteServerEndpoints.Items,
			toRouteServerEndpointItem(ep, h.Backend.TagsForResource(ep.RouteServerEndpointID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteRouteServerEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("RouteServerEndpointId")

	ep, err := h.Backend.DeleteRouteServerEndpoint(id)
	if err != nil {
		return nil, err
	}

	return &deleteRouteServerEndpointResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		RouteServerEndpoint: toRouteServerEndpointItem(ep, nil),
	}, nil
}

func (h *Handler) handleCreateRouteServerPeer(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("RouteServerEndpointId")
	peerAddress := vals.Get("PeerAddress")
	bgpPeerAsn, _ := strconv.ParseInt(vals.Get("BgpOptions.PeerAsn"), 10, 64)
	livenessDetection := vals.Get("BgpOptions.PeerLivenessDetection")

	peer, err := h.Backend.CreateRouteServerPeer(endpointID, peerAddress, bgpPeerAsn, livenessDetection)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "route-server-peer")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{peer.RouteServerPeerID}, tags); err != nil {
			return nil, err
		}
	}

	return &createRouteServerPeerResponse{
		Xmlns:           ec2XMLNS,
		RequestID:       reqID,
		RouteServerPeer: toRouteServerPeerItem(peer, tags),
	}, nil
}

func (h *Handler) handleDescribeRouteServerPeers(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteServerPeerId")
	peers := h.Backend.DescribeRouteServerPeers(ids)

	resp := &describeRouteServerPeersResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range peers {
		resp.RouteServerPeers.Items = append(
			resp.RouteServerPeers.Items, toRouteServerPeerItem(p, h.Backend.TagsForResource(p.RouteServerPeerID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteRouteServerPeer(vals url.Values, reqID string) (any, error) {
	id := vals.Get("RouteServerPeerId")

	peer, err := h.Backend.DeleteRouteServerPeer(id)
	if err != nil {
		return nil, err
	}

	return &deleteRouteServerPeerResponse{
		Xmlns:           ec2XMLNS,
		RequestID:       reqID,
		RouteServerPeer: toRouteServerPeerItem(peer, nil),
	}, nil
}

func (h *Handler) handleAssociateRouteServer(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	vpcID := vals.Get("VpcId")

	assoc, err := h.Backend.AssociateRouteServer(routeServerID, vpcID)
	if err != nil {
		return nil, err
	}

	return &associateRouteServerResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		RouteServerAssociation: toRouteServerAssociationItem(assoc),
	}, nil
}

func (h *Handler) handleDisassociateRouteServer(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	vpcID := vals.Get("VpcId")

	assoc, err := h.Backend.DisassociateRouteServer(routeServerID, vpcID)
	if err != nil {
		return nil, err
	}

	return &disassociateRouteServerResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		RouteServerAssociation: toRouteServerAssociationItem(assoc),
	}, nil
}

func (h *Handler) handleGetRouteServerAssociations(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	assocs := h.Backend.GetRouteServerAssociations(routeServerID)

	resp := &getRouteServerAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.RouteServerAssociations.Items = append(resp.RouteServerAssociations.Items, toRouteServerAssociationItem(a))
	}

	return resp, nil
}

func (h *Handler) handleEnableRouteServerPropagation(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	routeTableID := vals.Get("RouteTableId")

	prop, err := h.Backend.EnableRouteServerPropagation(routeServerID, routeTableID)
	if err != nil {
		return nil, err
	}

	return &enableRouteServerPropagationResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		RouteServerPropagation: toRouteServerPropagationItem(prop),
	}, nil
}

func (h *Handler) handleDisableRouteServerPropagation(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	routeTableID := vals.Get("RouteTableId")

	prop, err := h.Backend.DisableRouteServerPropagation(routeServerID, routeTableID)
	if err != nil {
		return nil, err
	}

	return &disableRouteServerPropagationResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		RouteServerPropagation: toRouteServerPropagationItem(prop),
	}, nil
}

func (h *Handler) handleGetRouteServerPropagations(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	props := h.Backend.GetRouteServerPropagations(routeServerID)

	resp := &getRouteServerPropagationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range props {
		resp.RouteServerPropagations.Items = append(resp.RouteServerPropagations.Items, toRouteServerPropagationItem(p))
	}

	return resp, nil
}

func (h *Handler) handleGetRouteServerRoutingDatabase(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")

	routes, err := h.Backend.GetRouteServerRoutingDatabase(routeServerID)
	if err != nil {
		return nil, err
	}

	var arePersisted bool
	if servers := h.Backend.DescribeRouteServers([]string{routeServerID}); len(servers) == 1 {
		arePersisted = servers[0].PersistRoutesState == routeServerPersistRoutesStateEnabled
	}

	resp := &getRouteServerRoutingDatabaseResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, AreRoutesPersisted: arePersisted,
	}
	for _, r := range routes {
		resp.Routes.Items = append(resp.Routes.Items, toRouteServerRouteItem(r))
	}

	return resp, nil
}
