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
	RouteServerID           string `xml:"routeServerId"`
	State                   string `xml:"state,omitempty"`
	SnsTopicArn             string `xml:"snsTopicArn,omitempty"`
	PersistRoutesState      string `xml:"persistRoutesState,omitempty"`
	AmazonSideAsn           int64  `xml:"amazonSideAsn,omitempty"`
	PersistRoutesDuration   int64  `xml:"persistRoutesDuration,omitempty"`
	SnsNotificationsEnabled bool   `xml:"snsNotificationsEnabled"`
}

func toRouteServerItem(rs *RouteServer) routeServerItem {
	return routeServerItem{
		RouteServerID:           rs.RouteServerID,
		AmazonSideAsn:           rs.AmazonSideAsn,
		State:                   rs.State,
		SnsNotificationsEnabled: rs.SnsNotificationsEnabled,
		SnsTopicArn:             rs.SnsTopicArn,
		PersistRoutesState:      rs.PersistRoutesState,
		PersistRoutesDuration:   rs.PersistRoutesDuration,
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

type routeServerEndpointItem struct {
	RouteServerEndpointID string `xml:"routeServerEndpointId"`
	RouteServerID         string `xml:"routeServerId,omitempty"`
	SubnetID              string `xml:"subnetId,omitempty"`
	VpcID                 string `xml:"vpcId,omitempty"`
	EniID                 string `xml:"eniId,omitempty"`
	EniAddress            string `xml:"eniAddress,omitempty"`
	State                 string `xml:"state,omitempty"`
	FailureReason         struct {
		Code    string `xml:"code,omitempty"`
		Message string `xml:"message,omitempty"`
	} `xml:"failureReason"`
}

func toRouteServerEndpointItem(ep *RouteServerEndpoint) routeServerEndpointItem {
	item := routeServerEndpointItem{
		RouteServerEndpointID: ep.RouteServerEndpointID,
		RouteServerID:         ep.RouteServerID,
		SubnetID:              ep.SubnetID,
		VpcID:                 ep.VpcID,
		EniID:                 ep.EniID,
		EniAddress:            ep.EniAddress,
		State:                 ep.State,
	}
	item.FailureReason.Code = ep.StateReasonCode
	item.FailureReason.Message = ep.StateReasonMessage

	return item
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

type routeServerPeerItem struct {
	BgpStatus     routeServerBGPStatusItem `xml:"bgpStatus"`
	FailureReason struct {
		Code    string `xml:"code,omitempty"`
		Message string `xml:"message,omitempty"`
	} `xml:"failureReason"`
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
}

func toRouteServerPeerItem(p *RouteServerPeer) routeServerPeerItem {
	item := routeServerPeerItem{
		RouteServerPeerID:     p.RouteServerPeerID,
		RouteServerEndpointID: p.RouteServerEndpointID,
		RouteServerID:         p.RouteServerID,
		SubnetID:              p.SubnetID,
		VpcID:                 p.VpcID,
		State:                 p.State,
		EniID:                 p.EniID,
		EniAddress:            p.EniAddress,
		PeerAddress:           p.PeerAddress,
		BgpOptions: routeServerBGPOptionsItem{
			PeerAsn:               p.BgpPeerAsn,
			PeerLivenessDetection: p.BgpPeerLivenessDetectionMode,
		},
		BgpStatus: routeServerBGPStatusItem{
			Status:       p.BgpStatus,
			BgpPeerState: p.BgpStatusPeerState,
		},
	}
	item.FailureReason.Code = p.StateReasonCode
	item.FailureReason.Message = p.StateReasonMessage

	return item
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

type routeServerRouteItem struct {
	RouteServerEndpointID string  `xml:"routeServerEndpointId,omitempty"`
	RouteServerPeerID     string  `xml:"routeServerPeerId,omitempty"`
	Prefix                string  `xml:"prefix,omitempty"`
	AsPaths               []int64 `xml:"asPathSet>item"`
	Med                   int64   `xml:"med,omitempty"`
	RouteInstalled        bool    `xml:"routeInstalled,omitempty"`
}

type getRouteServerRoutingDatabaseResponse struct {
	XMLName       xml.Name `xml:"GetRouteServerRoutingDatabaseResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	RouteServerID string   `xml:"routeServerId,omitempty"`
	Routes        struct {
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

	return &createRouteServerResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		RouteServer: toRouteServerItem(rs),
	}, nil
}

func (h *Handler) handleDescribeRouteServers(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteServerId")
	servers := h.Backend.DescribeRouteServers(ids)

	resp := &describeRouteServersResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, rs := range servers {
		resp.RouteServers.Items = append(resp.RouteServers.Items, toRouteServerItem(rs))
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
		RouteServer: toRouteServerItem(rs),
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
		RouteServer: toRouteServerItem(rs),
	}, nil
}

func (h *Handler) handleCreateRouteServerEndpoint(vals url.Values, reqID string) (any, error) {
	routeServerID := vals.Get("RouteServerId")
	subnetID := vals.Get("SubnetId")

	ep, err := h.Backend.CreateRouteServerEndpoint(routeServerID, subnetID)
	if err != nil {
		return nil, err
	}

	return &createRouteServerEndpointResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		RouteServerEndpoint: toRouteServerEndpointItem(ep),
	}, nil
}

func (h *Handler) handleDescribeRouteServerEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteServerEndpointId")
	endpoints := h.Backend.DescribeRouteServerEndpoints(ids)

	resp := &describeRouteServerEndpointsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, ep := range endpoints {
		resp.RouteServerEndpoints.Items = append(resp.RouteServerEndpoints.Items, toRouteServerEndpointItem(ep))
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
		RouteServerEndpoint: toRouteServerEndpointItem(ep),
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

	return &createRouteServerPeerResponse{
		Xmlns:           ec2XMLNS,
		RequestID:       reqID,
		RouteServerPeer: toRouteServerPeerItem(peer),
	}, nil
}

func (h *Handler) handleDescribeRouteServerPeers(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteServerPeerId")
	peers := h.Backend.DescribeRouteServerPeers(ids)

	resp := &describeRouteServerPeersResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range peers {
		resp.RouteServerPeers.Items = append(resp.RouteServerPeers.Items, toRouteServerPeerItem(p))
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
		RouteServerPeer: toRouteServerPeerItem(peer),
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
		Xmlns: ec2XMLNS, RequestID: reqID, RouteServerID: routeServerID, AreRoutesPersisted: arePersisted,
	}
	for _, r := range routes {
		resp.Routes.Items = append(resp.Routes.Items, routeServerRouteItem{
			RouteServerEndpointID: r.RouteServerEndpointID,
			RouteServerPeerID:     r.RouteServerPeerID,
			Prefix:                r.Prefix,
			AsPaths:               r.AsPaths,
			Med:                   r.Med,
			RouteInstalled:        r.RouteInstalled,
		})
	}

	return resp, nil
}
