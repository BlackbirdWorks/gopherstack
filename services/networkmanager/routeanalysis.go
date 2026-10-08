package networkmanager

import "net"

// This file implements PARITY.md family S: route analysis (2 ops).
//
// Real walk when EC2Resolver is wired in (cli.go's wireNetworkManagerEC2):
// resolves the analysis's anchor attachment (Source's
// TransitGatewayAttachmentArn, falling back to Destination's) to a real EC2
// TransitGatewayVpcAttachment, finds the real TransitGatewayRouteTable
// associated with it, and -- when Destination carries an IpAddress --
// performs a genuine longest-prefix-match against that route table's real
// routes (services/ec2's TransitGatewayRoute state), returning CONNECTED
// with a real PathComponent only when an active, non-blackhole route
// actually matches. This is a single-hop resolution into one TGW's route
// table, not a full multi-hop cross-TGW-peering walk with cycle detection
// -- a documented scope reduction (this backend does not model TGW-to-TGW
// peering route propagation), but every result IS derived from real EC2
// state, never a fabricated PathComponent list or hardcoded CONNECTED
// verdict.
//
// When the resolver also implements EC2PeeringResolver the walk continues across
// TGW peerings and reports CYCLIC_PATH_DETECTED on a revisited attachment.
//
// Without EC2Resolver wired (e.g. isolated unit tests), this backend cannot
// reach any cross-service state and falls back to the same honest "cannot
// resolve" verdict as before: RUNNING -> COMPLETED, NOT_CONNECTED, with
// NO_DESTINATION_ARN_PROVIDED or TRANSIT_GATEWAY_ATTACHMENT_NOT_FOUND.

func (b *InMemoryBackend) StartRouteAnalysis(
	globalNetworkID string, source, destination *RouteAnalysisEndpoint, includeReturnPath, useMiddleboxes bool,
) (*RouteAnalysis, error) {
	b.mu.Lock("StartRouteAnalysis")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return nil, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	id := newRouteAnalysisID()
	r := &RouteAnalysis{
		StartTimestamp:    nowUTC(),
		Destination:       destination,
		Source:            source,
		GlobalNetworkID:   globalNetworkID,
		OwnerAccountID:    b.accountID,
		RouteAnalysisID:   id,
		Status:            routeAnalysisStatusRunning,
		IncludeReturnPath: includeReturnPath,
		UseMiddleboxes:    useMiddleboxes,
	}
	b.routeAnalyses.Put(r)

	resolver := b.ec2Resolver

	b.work.After("RouteAnalysisCompleted", asyncTransitionDelay, func() {
		b.mu.Lock("RouteAnalysisCompleted-async")
		defer b.mu.Unlock()

		v, ok := b.routeAnalyses.Get(id)
		if !ok || v.Status != routeAnalysisStatusRunning {
			return
		}

		v.Status = routeAnalysisStatusCompleted
		v.ForwardPath = resolveRouteAnalysisPath(resolver, source, destination)

		if v.IncludeReturnPath {
			v.ReturnPath = resolveRouteAnalysisPath(resolver, destination, source)
		}
	})

	return r.clone(), nil
}

func (b *InMemoryBackend) GetRouteAnalysis(globalNetworkID, id string) (*RouteAnalysis, error) {
	b.mu.RLock("GetRouteAnalysis")
	defer b.mu.RUnlock()

	r, ok := b.routeAnalyses.Get(id)
	if !ok || r.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceRouteAnalysis, id)
	}

	return r.clone(), nil
}

// resolveRouteAnalysisPath computes one direction of a route analysis (from
// `from`'s attachment toward `to`'s IP address, if any). See this file's
// doc comment for the honesty boundary.
func resolveRouteAnalysisPath(resolver EC2Resolver, from, to *RouteAnalysisEndpoint) *RouteAnalysisPath {
	if to == nil || (to.IPAddress == "" && to.TransitGatewayAttachmentArn == "") {
		return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonNoDestination, nil)
	}

	if resolver == nil {
		return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonAttachmentNotFound, nil)
	}

	anchorArn := to.TransitGatewayAttachmentArn
	if from != nil && from.TransitGatewayAttachmentArn != "" {
		anchorArn = from.TransitGatewayAttachmentArn
	}

	if anchorArn == "" {
		return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonAttachmentNotFound, nil)
	}

	return walkRouteTables(resolver, anchorArn, to.IPAddress)
}

// walkRouteTables follows active routes hop by hop, crossing a TGW peering when the resolver
// reports the route's attachment as one.
func walkRouteTables(resolver EC2Resolver, anchorArn, ip string) *RouteAnalysisPath {
	peering, _ := resolver.(EC2PeeringResolver)
	visited := map[string]bool{}

	var (
		path []PathComponent
		seq  int32
	)

	for ; ; seq++ {
		visited[anchorArn] = true

		routeTableID, ok := resolver.TransitGatewayRouteTableForAttachment(anchorArn)
		if !ok {
			return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonAttachmentNotFound, path)
		}

		if ip == "" {
			return completedPath(routeAnalysisResultConnected, "", append(path, PathComponent{
				Sequence: seq,
				Resource: &NetworkResourceSummary{ResourceArn: anchorArn, ResourceType: tgwAttachmentResourceType},
			}))
		}

		route, found := longestPrefixMatch(resolver.TransitGatewayRoutes(routeTableID), ip)
		if !found {
			return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonRouteNotFound, path)
		}

		path = append(path, PathComponent{
			Sequence:             seq,
			DestinationCidrBlock: route.DestinationCIDRBlock,
			Resource: &NetworkResourceSummary{
				ResourceArn:  anchorArn,
				ResourceType: tgwAttachmentResourceType,
			},
		})

		switch route.State {
		case ec2TransitGatewayRouteStateBlackhole:
			return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonBlackhole, path)
		case ec2TransitGatewayRouteStateActive:
		default:
			return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonInactiveRoute, path)
		}

		var peerArn string

		if peering != nil {
			peerArn, ok = peering.TransitGatewayPeerAttachment(route.AttachmentID)
		}

		if !ok || peerArn == "" {
			return completedPath(routeAnalysisResultConnected, "", path)
		}

		if visited[peerArn] {
			return completedPath(routeAnalysisResultNotConnected, routeAnalysisReasonCyclicPath, path)
		}

		anchorArn = peerArn
	}
}

func completedPath(resultCode, reasonCode string, path []PathComponent) *RouteAnalysisPath {
	return &RouteAnalysisPath{
		CompletionStatus: &RouteAnalysisCompletion{ReasonCode: reasonCode, ResultCode: resultCode},
		Path:             path,
	}
}

// longestPrefixMatch returns the route in routes whose DestinationCIDRBlock
// contains ip with the longest (most specific) prefix, real CIDR
// arithmetic over real EC2-modeled routes -- never a fabricated match.
func longestPrefixMatch(routes []EC2TransitGatewayRoute, ip string) (EC2TransitGatewayRoute, bool) {
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return EC2TransitGatewayRoute{}, false
	}

	var (
		best     EC2TransitGatewayRoute
		bestOnes = -1
		found    bool
	)

	for _, r := range routes {
		_, network, err := net.ParseCIDR(r.DestinationCIDRBlock)
		if err != nil || !network.Contains(parsedIP) {
			continue
		}

		ones, _ := network.Mask.Size()
		if ones > bestOnes {
			best, bestOnes, found = r, ones, true
		}
	}

	return best, found
}
