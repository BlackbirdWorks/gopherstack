package ec2

import (
	"encoding/xml"
	"net/url"
	"time"
)

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
	TransitGatewayAttachmentID          string          `xml:"transitGatewayAttachmentId"`
	TransportTransitGatewayAttachmentID string          `xml:"transportTransitGatewayAttachmentId"`
	TransitGatewayID                    string          `xml:"transitGatewayId"`
	State                               string          `xml:"state"`
	TagSet                              []simpleTagItem `xml:"tagSet>item"`
}

type createTransitGatewayConnectResponse struct {
	XMLName               xml.Name       `xml:"CreateTransitGatewayConnectResponse"`
	RequestID             string         `xml:"requestId"`
	TransitGatewayConnect tgwConnectItem `xml:"transitGatewayConnect"`
}

// describeTransitGatewayConnectsResponse's wrapper key is
// "transitGatewayConnectSet" (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDescribeTransitGatewayConnectsOutput) -
// not "transitGatewayConnects", which the real deserializer never matches,
// so a real client always saw an empty TransitGatewayConnects slice.
type describeTransitGatewayConnectsResponse struct {
	XMLName                xml.Name `xml:"DescribeTransitGatewayConnectsResponse"`
	RequestID              string   `xml:"requestId"`
	TransitGatewayConnects struct {
		Items []tgwConnectItem `xml:"item"`
	} `xml:"transitGatewayConnectSet"`
}

// tgwConnectPeerConfigurationItem mirrors the real
// TransitGatewayConnectPeerConfiguration wire shape: PeerAddress and
// InsideCidrBlocks nest under connectPeerConfiguration, not flat top-level
// fields (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentTransitGatewayConnectPeerConfiguration).
type tgwConnectPeerConfigurationItem struct {
	PeerAddress      string   `xml:"peerAddress,omitempty"`
	InsideCidrBlocks []string `xml:"insideCidrBlocks>item,omitempty"`
}

type tgwConnectPeerItem struct {
	TransitGatewayConnectPeerID string                          `xml:"transitGatewayConnectPeerId"`
	TransitGatewayAttachmentID  string                          `xml:"transitGatewayAttachmentId"`
	State                       string                          `xml:"state"`
	ConnectPeerConfiguration    tgwConnectPeerConfigurationItem `xml:"connectPeerConfiguration"`
	TagSet                      []simpleTagItem                 `xml:"tagSet>item"`
}

type createTransitGatewayConnectPeerResponse struct {
	XMLName                   xml.Name           `xml:"CreateTransitGatewayConnectPeerResponse"`
	RequestID                 string             `xml:"requestId"`
	TransitGatewayConnectPeer tgwConnectPeerItem `xml:"transitGatewayConnectPeer"`
}

// describeTransitGatewayConnectPeersResponse's wrapper key is
// "transitGatewayConnectPeerSet" (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDescribeTransitGatewayConnectPeersOutput)
// - not "transitGatewayConnectPeers", which the real deserializer never
// matches, so a real client always saw an empty TransitGatewayConnectPeers
// slice.
type describeTransitGatewayConnectPeersResponse struct {
	XMLName                    xml.Name `xml:"DescribeTransitGatewayConnectPeersResponse"`
	RequestID                  string   `xml:"requestId"`
	TransitGatewayConnectPeers struct {
		Items []tgwConnectPeerItem `xml:"item"`
	} `xml:"transitGatewayConnectPeerSet"`
}

type tgwPrefixListRefAttachmentItem struct {
	TransitGatewayAttachmentID string `xml:"transitGatewayAttachmentId,omitempty"`
}

type tgwPrefixListRefItem struct {
	TransitGatewayAttachment   *tgwPrefixListRefAttachmentItem `xml:"transitGatewayAttachment,omitempty"`
	PrefixListID               string                          `xml:"prefixListId"`
	TransitGatewayRouteTableID string                          `xml:"transitGatewayRouteTableId"`
	State                      string                          `xml:"state"`
	Blackhole                  bool                            `xml:"blackhole"`
}

// tgwPrefixListRefToItem converts a backend TGW prefix list reference into its
// XML wire item, nesting the attachment ID under transitGatewayAttachment as
// AWS does, when one is set.

// tgwPrefixListRefToItem converts a backend TGW prefix list reference into its
// XML wire item, nesting the attachment ID under transitGatewayAttachment as
// AWS does, when one is set.
func tgwPrefixListRefToItem(ref *TransitGatewayPrefixListReference) tgwPrefixListRefItem {
	item := tgwPrefixListRefItem{
		PrefixListID:               ref.PrefixListID,
		TransitGatewayRouteTableID: ref.TransitGatewayRouteTableID,
		State:                      ref.State,
		Blackhole:                  ref.Blackhole,
	}

	if ref.TransitGatewayAttachmentID != "" {
		item.TransitGatewayAttachment = &tgwPrefixListRefAttachmentItem{
			TransitGatewayAttachmentID: ref.TransitGatewayAttachmentID,
		}
	}

	return item
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

func toTGWPeeringAttachmentItem(
	att *TransitGatewayPeeringAttachment, tags map[string]string,
) tgwPeeringAttachmentItem {
	item := tgwPeeringAttachmentItem{
		TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
		RequesterTgwInfo:           peeringTgwInfoItem{TransitGatewayID: att.RequesterTransitGatewayID},
		AccepterTgwInfo:            peeringTgwInfoItem{TransitGatewayID: att.AccepterTransitGatewayID},
		State:                      att.State,
		TagSet:                     tagItemsFromMap(tags),
	}
	if !att.CreationTime.IsZero() {
		item.CreationTime = att.CreationTime.Format(time.RFC3339)
	}

	return item
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
		TransitGatewayPeeringAttachment: toTGWPeeringAttachmentItem(att, nil),
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
			toTGWPeeringAttachmentItem(att, h.Backend.TagsForResource(att.TransitGatewayAttachmentID)),
		)
	}

	return resp, nil
}

// ---- TGW Connect handlers ----

func toTGWConnectItem(conn *TransitGatewayConnect, tags map[string]string) tgwConnectItem {
	return tgwConnectItem{
		TransitGatewayAttachmentID:          conn.TransitGatewayAttachmentID,
		TransportTransitGatewayAttachmentID: conn.TransportTransitGatewayAttachmentID,
		TransitGatewayID:                    conn.TransitGatewayID,
		State:                               conn.State,
		TagSet:                              tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateTransitGatewayConnect(vals url.Values, reqID string) (any, error) {
	transportID := vals.Get("TransportTransitGatewayAttachmentId")

	// The real CreateTransitGatewayConnectInput has no TransitGatewayId
	// parameter at all (api_op_CreateTransitGatewayConnect.go) - it is
	// derived from the transport attachment. Prefer an explicit
	// TransitGatewayId if a caller sends one (back-compat), otherwise look
	// it up from the transport VPC attachment.
	tgwID := vals.Get("TransitGatewayId")
	if tgwID == "" {
		transportAtts := h.Backend.DescribeTransitGatewayVpcAttachments([]string{transportID})
		if len(transportAtts) == 1 {
			tgwID = transportAtts[0].TransitGatewayID
		}
	}

	conn, err := h.Backend.CreateTransitGatewayConnect(transportID, tgwID)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayConnectResponse{
		RequestID:             reqID,
		TransitGatewayConnect: toTGWConnectItem(conn, nil),
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
		resp.TransitGatewayConnects.Items = append(
			resp.TransitGatewayConnects.Items,
			toTGWConnectItem(conn, h.Backend.TagsForResource(conn.TransitGatewayAttachmentID)),
		)
	}

	return resp, nil
}

func toTGWConnectPeerItem(peer *TransitGatewayConnectPeer, tags map[string]string) tgwConnectPeerItem {
	return tgwConnectPeerItem{
		TransitGatewayConnectPeerID: peer.TransitGatewayConnectPeerID,
		TransitGatewayAttachmentID:  peer.TransitGatewayAttachmentID,
		State:                       peer.State,
		ConnectPeerConfiguration: tgwConnectPeerConfigurationItem{
			PeerAddress:      peer.PeerAddress,
			InsideCidrBlocks: peer.InsideCidrBlocks,
		},
		TagSet: tagItemsFromMap(tags),
	}
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
		RequestID:                 reqID,
		TransitGatewayConnectPeer: toTGWConnectPeerItem(peer, nil),
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
			toTGWConnectPeerItem(peer, h.Backend.TagsForResource(peer.TransitGatewayConnectPeerID)),
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
		RequestID:                         reqID,
		TransitGatewayPrefixListReference: tgwPrefixListRefToItem(ref),
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
			tgwPrefixListRefToItem(ref),
		)
	}

	return resp, nil
}

// ---- VerifiedAccess handlers ----

// registerTransitGatewayPeeringOps registers the TransitGatewayPeering operation handlers.
func registerTransitGatewayPeeringOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateTransitGatewayPeeringAttachment"] = h.handleCreateTransitGatewayPeeringAttachment
	ops["DeleteTransitGatewayPeeringAttachment"] = h.handleDeleteTransitGatewayPeeringAttachment
	ops["DescribeTransitGatewayPeeringAttachments"] = h.handleDescribeTransitGatewayPeeringAttachments
	ops["CreateTransitGatewayConnect"] = h.handleCreateTransitGatewayConnect
	ops["DeleteTransitGatewayConnect"] = h.handleDeleteTransitGatewayConnect
	ops["DescribeTransitGatewayConnects"] = h.handleDescribeTransitGatewayConnects
	ops["CreateTransitGatewayConnectPeer"] = h.handleCreateTransitGatewayConnectPeer
	ops["DeleteTransitGatewayConnectPeer"] = h.handleDeleteTransitGatewayConnectPeer
	ops["DescribeTransitGatewayConnectPeers"] = h.handleDescribeTransitGatewayConnectPeers
	ops["CreateTransitGatewayPrefixListReference"] = h.handleCreateTransitGatewayPrefixListReference
	ops["DeleteTransitGatewayPrefixListReference"] = h.handleDeleteTransitGatewayPrefixListReference
	ops["GetTransitGatewayPrefixListReferences"] = h.handleGetTransitGatewayPrefixListReferences
}

// transitGatewayPeeringSupportedOperations lists the operation names registered by
// registerTransitGatewayPeeringOps, for GetSupportedOperations().
func transitGatewayPeeringSupportedOperations() []string {
	return []string{
		"CreateTransitGatewayPeeringAttachment",
		"DeleteTransitGatewayPeeringAttachment",
		"DescribeTransitGatewayPeeringAttachments",
		"CreateTransitGatewayConnect",
		"DeleteTransitGatewayConnect",
		"DescribeTransitGatewayConnects",
		"CreateTransitGatewayConnectPeer",
		"DeleteTransitGatewayConnectPeer",
		"DescribeTransitGatewayConnectPeers",
		"CreateTransitGatewayPrefixListReference",
		"DeleteTransitGatewayPrefixListReference",
		"GetTransitGatewayPrefixListReferences",
	}
}
