package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

// ---- Route Server errors ----

var (
	ErrRouteServerNotFound            = errors.New("InvalidRouteServerId.NotFound")
	ErrRouteServerEndpointNotFound    = errors.New("InvalidRouteServerEndpointId.NotFound")
	ErrRouteServerPeerNotFound        = errors.New("InvalidRouteServerPeerId.NotFound")
	ErrRouteServerAssociationNotFound = errors.New("InvalidRouteServerAssociation.NotFound")
	ErrRouteServerPropagationNotFound = errors.New("InvalidRouteServerPropagation.NotFound")
)

// ---- Route Server types ----

// RouteServer holds a VPC Route Server, a managed BGP speaker that VPC
// resources can peer with to exchange dynamic routes.
type RouteServer struct {
	RouteServerID           string `json:"routeServerId,omitempty"`
	State                   string `json:"state,omitempty"`
	SnsTopicArn             string `json:"snsTopicArn,omitempty"`
	PersistRoutesState      string `json:"persistRoutesState,omitempty"`
	AmazonSideAsn           int64  `json:"amazonSideAsn,omitempty"`
	PersistRoutesDuration   int64  `json:"persistRoutesDuration,omitempty"`
	SnsNotificationsEnabled bool   `json:"snsNotificationsEnabled,omitempty"`
}

// RouteServerEndpoint holds a network interface, in a given subnet, through
// which a route server exchanges routes with VPC resources.
type RouteServerEndpoint struct {
	RouteServerEndpointID string `json:"routeServerEndpointId,omitempty"`
	RouteServerID         string `json:"routeServerId,omitempty"`
	SubnetID              string `json:"subnetId,omitempty"`
	VpcID                 string `json:"vpcId,omitempty"`
	EniID                 string `json:"eniId,omitempty"`
	EniAddress            string `json:"eniAddress,omitempty"`
	State                 string `json:"state,omitempty"`
	StateReasonCode       string `json:"stateReasonCode,omitempty"`
	StateReasonMessage    string `json:"stateReasonMessage,omitempty"`
}

// RouteServerPeer holds a BGP peer configured on a route server endpoint.
type RouteServerPeer struct {
	RouteServerPeerID            string `json:"routeServerPeerId,omitempty"`
	RouteServerEndpointID        string `json:"routeServerEndpointId,omitempty"`
	RouteServerID                string `json:"routeServerId,omitempty"`
	SubnetID                     string `json:"subnetId,omitempty"`
	VpcID                        string `json:"vpcId,omitempty"`
	State                        string `json:"state,omitempty"`
	StateReasonCode              string `json:"stateReasonCode,omitempty"`
	StateReasonMessage           string `json:"stateReasonMessage,omitempty"`
	EniID                        string `json:"eniId,omitempty"`
	EniAddress                   string `json:"eniAddress,omitempty"`
	PeerAddress                  string `json:"peerAddress,omitempty"`
	BgpPeerLivenessDetectionMode string `json:"bgpPeerLivenessDetectionMode,omitempty"`
	BgpStatus                    string `json:"bgpStatus,omitempty"`
	BgpStatusPeerState           string `json:"bgpStatusPeerState,omitempty"`
	BgpPeerAsn                   int64  `json:"bgpPeerAsn,omitempty"`
}

// RouteServerAssociation holds the association between a route server and a VPC.
type RouteServerAssociation struct {
	RouteServerID string `json:"routeServerId,omitempty"`
	VpcID         string `json:"vpcId,omitempty"`
	State         string `json:"state,omitempty"`
}

// RouteServerPropagation holds route propagation from a route server into a route table.
type RouteServerPropagation struct {
	RouteServerID string `json:"routeServerId,omitempty"`
	RouteTableID  string `json:"routeTableId,omitempty"`
	State         string `json:"state,omitempty"`
}

// RouteServerRoute holds a single route in a route server's routing database.
type RouteServerRoute struct {
	RouteServerEndpointID string  `json:"routeServerEndpointId,omitempty"`
	RouteServerPeerID     string  `json:"routeServerPeerId,omitempty"`
	Prefix                string  `json:"prefix,omitempty"`
	AsPaths               []int64 `json:"asPaths,omitempty"`
	Med                   int64   `json:"med,omitempty"`
	RouteInstalled        bool    `json:"routeInstalled,omitempty"`
}

const (
	routeServerStateAvailable  = "available"
	associationStateAssociated = "associated"
	propagationStateEnabled    = "enabled"
	stateDeleting              = "deleting"
)

// ---- Route Server backend methods ----

// CreateRouteServer creates a new route server with the given Amazon-side ASN.
func (b *InMemoryBackend) CreateRouteServer(
	amazonSideAsn int64,
	persistRoutesState string,
	persistRoutesDuration int64,
	snsNotificationsEnabled bool,
) (*RouteServer, error) {
	if amazonSideAsn == 0 {
		return nil, fmt.Errorf("%w: AmazonSideAsn is required", ErrInvalidParameter)
	}

	if persistRoutesState == "" {
		persistRoutesState = "disabled"
	}

	b.mu.Lock("CreateRouteServer")
	defer b.mu.Unlock()

	id := "rs-" + uuid.New().String()[:8]
	rs := &RouteServer{
		RouteServerID:           id,
		AmazonSideAsn:           amazonSideAsn,
		State:                   routeServerStateAvailable,
		SnsNotificationsEnabled: snsNotificationsEnabled,
		PersistRoutesState:      persistRoutesState,
		PersistRoutesDuration:   persistRoutesDuration,
	}
	b.routeServers.Put(rs)

	cp := *rs

	return &cp, nil
}

// DescribeRouteServers returns route servers, optionally filtered by ID.
func (b *InMemoryBackend) DescribeRouteServers(ids []string) []*RouteServer {
	b.mu.RLock("DescribeRouteServers")
	defer b.mu.RUnlock()

	var result []*RouteServer

	for _, rs := range b.routeServers.All() {
		if len(ids) > 0 && !slices.Contains(ids, rs.RouteServerID) {
			continue
		}

		cp := *rs
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].RouteServerID < result[j].RouteServerID })

	return result
}

// DeleteRouteServer deletes a route server by ID.
func (b *InMemoryBackend) DeleteRouteServer(id string) (*RouteServer, error) {
	b.mu.Lock("DeleteRouteServer")
	defer b.mu.Unlock()

	rs, ok := b.routeServers.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerNotFound, id)
	}

	cp := *rs
	cp.State = stateDeleting
	b.routeServers.Delete(id)
	delete(b.tags, id)

	return &cp, nil
}

// ModifyRouteServer updates mutable fields of an existing route server.
func (b *InMemoryBackend) ModifyRouteServer(
	id string,
	persistRoutesState string,
	persistRoutesDuration int64,
	snsNotificationsEnabled bool,
) (*RouteServer, error) {
	b.mu.Lock("ModifyRouteServer")
	defer b.mu.Unlock()

	rs, ok := b.routeServers.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerNotFound, id)
	}

	if persistRoutesState != "" {
		rs.PersistRoutesState = persistRoutesState
	}

	if persistRoutesDuration != 0 {
		rs.PersistRoutesDuration = persistRoutesDuration
	}

	rs.SnsNotificationsEnabled = snsNotificationsEnabled

	cp := *rs

	return &cp, nil
}

// CreateRouteServerEndpoint creates an endpoint for a route server in a subnet.
func (b *InMemoryBackend) CreateRouteServerEndpoint(routeServerID, subnetID string) (*RouteServerEndpoint, error) {
	if routeServerID == "" {
		return nil, fmt.Errorf("%w: RouteServerId is required", ErrInvalidParameter)
	}

	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRouteServerEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.routeServers.Get(routeServerID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerNotFound, routeServerID)
	}

	subnet, ok := b.subnets.Get(subnetID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	id := "rse-" + uuid.New().String()[:8]
	eniID := "eni-" + uuid.New().String()[:8]
	ep := &RouteServerEndpoint{
		RouteServerEndpointID: id,
		RouteServerID:         routeServerID,
		SubnetID:              subnetID,
		VpcID:                 subnet.VPCID,
		EniID:                 eniID,
		EniAddress:            b.allocPrivateIP(),
		State:                 routeServerStateAvailable,
	}
	b.routeServerEndpoints.Put(ep)

	cp := *ep

	return &cp, nil
}

// DescribeRouteServerEndpoints returns route server endpoints, optionally filtered by ID.
func (b *InMemoryBackend) DescribeRouteServerEndpoints(ids []string) []*RouteServerEndpoint {
	b.mu.RLock("DescribeRouteServerEndpoints")
	defer b.mu.RUnlock()

	var result []*RouteServerEndpoint

	for _, ep := range b.routeServerEndpoints.All() {
		if len(ids) > 0 && !slices.Contains(ids, ep.RouteServerEndpointID) {
			continue
		}

		cp := *ep
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RouteServerEndpointID < result[j].RouteServerEndpointID
	})

	return result
}

// DeleteRouteServerEndpoint deletes a route server endpoint by ID.
func (b *InMemoryBackend) DeleteRouteServerEndpoint(id string) (*RouteServerEndpoint, error) {
	b.mu.Lock("DeleteRouteServerEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.routeServerEndpoints.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerEndpointNotFound, id)
	}

	cp := *ep
	cp.State = stateDeleting
	b.routeServerEndpoints.Delete(id)
	delete(b.tags, id)

	return &cp, nil
}

// CreateRouteServerPeer creates a BGP peer on a route server endpoint.
func (b *InMemoryBackend) CreateRouteServerPeer(
	endpointID, peerAddress string,
	bgpPeerAsn int64,
	livenessDetection string,
) (*RouteServerPeer, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: RouteServerEndpointId is required", ErrInvalidParameter)
	}

	if peerAddress == "" {
		return nil, fmt.Errorf("%w: PeerAddress is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRouteServerPeer")
	defer b.mu.Unlock()

	ep, ok := b.routeServerEndpoints.Get(endpointID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerEndpointNotFound, endpointID)
	}

	if livenessDetection == "" {
		livenessDetection = "bfd"
	}

	id := "rsp-" + uuid.New().String()[:8]
	eniID := "eni-" + uuid.New().String()[:8]
	peer := &RouteServerPeer{
		RouteServerPeerID:            id,
		RouteServerEndpointID:        endpointID,
		RouteServerID:                ep.RouteServerID,
		SubnetID:                     ep.SubnetID,
		VpcID:                        ep.VpcID,
		State:                        routeServerStateAvailable,
		EniID:                        eniID,
		EniAddress:                   b.allocPrivateIP(),
		PeerAddress:                  peerAddress,
		BgpPeerAsn:                   bgpPeerAsn,
		BgpPeerLivenessDetectionMode: livenessDetection,
		BgpStatus:                    "down",
		BgpStatusPeerState:           "idle",
	}
	b.routeServerPeers.Put(peer)

	cp := *peer

	return &cp, nil
}

// DescribeRouteServerPeers returns route server peers, optionally filtered by ID.
func (b *InMemoryBackend) DescribeRouteServerPeers(ids []string) []*RouteServerPeer {
	b.mu.RLock("DescribeRouteServerPeers")
	defer b.mu.RUnlock()

	var result []*RouteServerPeer

	for _, p := range b.routeServerPeers.All() {
		if len(ids) > 0 && !slices.Contains(ids, p.RouteServerPeerID) {
			continue
		}

		cp := *p
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].RouteServerPeerID < result[j].RouteServerPeerID })

	return result
}

// DeleteRouteServerPeer deletes a route server peer by ID.
func (b *InMemoryBackend) DeleteRouteServerPeer(id string) (*RouteServerPeer, error) {
	b.mu.Lock("DeleteRouteServerPeer")
	defer b.mu.Unlock()

	p, ok := b.routeServerPeers.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerPeerNotFound, id)
	}

	cp := *p
	cp.State = stateDeleting
	b.routeServerPeers.Delete(id)
	delete(b.tags, id)

	return &cp, nil
}

// AssociateRouteServer associates a route server with a VPC.
func (b *InMemoryBackend) AssociateRouteServer(routeServerID, vpcID string) (*RouteServerAssociation, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateRouteServer")
	defer b.mu.Unlock()

	if _, ok := b.routeServers.Get(routeServerID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerNotFound, routeServerID)
	}

	if _, ok := b.vpcs.Get(vpcID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	assoc := &RouteServerAssociation{
		RouteServerID: routeServerID,
		VpcID:         vpcID,
		State:         associationStateAssociated,
	}
	b.routeServerAssociations.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// DisassociateRouteServer removes the association between a route server and a VPC.
func (b *InMemoryBackend) DisassociateRouteServer(routeServerID, vpcID string) (*RouteServerAssociation, error) {
	b.mu.Lock("DisassociateRouteServer")
	defer b.mu.Unlock()

	key := routeServerID + "/" + vpcID
	assoc, ok := b.routeServerAssociations.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrRouteServerAssociationNotFound, routeServerID, vpcID)
	}

	cp := *assoc
	cp.State = "disassociating"
	b.routeServerAssociations.Delete(key)

	return &cp, nil
}

// GetRouteServerAssociations returns the VPC associations for a route server.
func (b *InMemoryBackend) GetRouteServerAssociations(routeServerID string) []*RouteServerAssociation {
	b.mu.RLock("GetRouteServerAssociations")
	defer b.mu.RUnlock()

	var result []*RouteServerAssociation

	for _, a := range b.routeServerAssociations.All() {
		if a.RouteServerID != routeServerID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].VpcID < result[j].VpcID })

	return result
}

// EnableRouteServerPropagation enables route propagation from a route server into a route table.
func (b *InMemoryBackend) EnableRouteServerPropagation(
	routeServerID, routeTableID string,
) (*RouteServerPropagation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: RouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableRouteServerPropagation")
	defer b.mu.Unlock()

	if _, ok := b.routeServers.Get(routeServerID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerNotFound, routeServerID)
	}

	if _, ok := b.routeTables.Get(routeTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteTableNotFound, routeTableID)
	}

	prop := &RouteServerPropagation{
		RouteServerID: routeServerID,
		RouteTableID:  routeTableID,
		State:         propagationStateEnabled,
	}
	b.routeServerPropagations.Put(prop)

	cp := *prop

	return &cp, nil
}

// DisableRouteServerPropagation disables route propagation from a route server into a route table.
func (b *InMemoryBackend) DisableRouteServerPropagation(
	routeServerID, routeTableID string,
) (*RouteServerPropagation, error) {
	b.mu.Lock("DisableRouteServerPropagation")
	defer b.mu.Unlock()

	key := routeServerID + "/" + routeTableID
	prop, ok := b.routeServerPropagations.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrRouteServerPropagationNotFound, routeServerID, routeTableID)
	}

	cp := *prop
	cp.State = "disabling"
	b.routeServerPropagations.Delete(key)

	return &cp, nil
}

// GetRouteServerPropagations returns the route table propagations for a route server.
func (b *InMemoryBackend) GetRouteServerPropagations(routeServerID string) []*RouteServerPropagation {
	b.mu.RLock("GetRouteServerPropagations")
	defer b.mu.RUnlock()

	var result []*RouteServerPropagation

	for _, p := range b.routeServerPropagations.All() {
		if p.RouteServerID != routeServerID {
			continue
		}

		cp := *p
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].RouteTableID < result[j].RouteTableID })

	return result
}

// GetRouteServerRoutingDatabase returns the current computed routing database
// for a route server, i.e. the routes learned across all of its BGP peers.
// This emulator does not run a real BGP speaker, so the database reflects the
// (currently empty, since no peer session has exchanged routes) set of
// installed routes rather than fabricated entries.
func (b *InMemoryBackend) GetRouteServerRoutingDatabase(routeServerID string) ([]*RouteServerRoute, error) {
	b.mu.RLock("GetRouteServerRoutingDatabase")
	defer b.mu.RUnlock()

	if _, ok := b.routeServers.Get(routeServerID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrRouteServerNotFound, routeServerID)
	}

	return nil, nil
}
