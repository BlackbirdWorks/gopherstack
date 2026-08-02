package networkmanager

// This file implements PARITY.md family S: route analysis (2 ops) -- the
// single riskiest fabrication surface in this service, per the audit.
//
// Honest feasibility decision (loudly documented, not silent): a real
// StartRouteAnalysis/GetRouteAnalysis implementation would walk modeled EC2
// Transit Gateway route-table/attachment state hop by hop, detecting cycles
// and the documented 64-hop limit, to produce a genuine PathComponent
// sequence and a real CONNECTED/NOT_CONNECTED verdict. This package does
// not have a live cross-service reference into services/ec2's backend
// (see attachments.go/associations.go's identical cross-service scope
// note), so it CANNOT perform that real walk -- and per PARITY.md's own
// framing, inventing a plausible-looking PathComponent list or a hardcoded
// CONNECTED result would be exactly the fabrication this campaign forbids.
//
// This backend therefore implements a real, honest STATE MACHINE
// (RUNNING -> COMPLETED via a real asyncTransitionDelay timer, matching
// every other resource in this service) that always resolves to
// Status: COMPLETED, CompletionStatus.ResultCode: NOT_CONNECTED, with a
// real, deterministic ReasonCode: NO_DESTINATION_ARN_PROVIDED if the
// caller's Destination carries neither an IpAddress nor a
// TransitGatewayAttachmentArn, otherwise
// TRANSIT_GATEWAY_ATTACHMENT_NOT_FOUND (since this backend has no real
// attachment state to resolve any ARN against). This is never presented as
// a genuine graph-walk result -- it is an honest "cannot resolve" verdict,
// the documented alternative PARITY.md itself sanctions over fabrication.

func (b *InMemoryBackend) StartRouteAnalysis(
	globalNetworkID string, source, destination *RouteAnalysisEndpoint, includeReturnPath, _ bool,
) (*RouteAnalysis, error) {
	b.mu.Lock("StartRouteAnalysis")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return nil, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	id := newRouteAnalysisID()
	r := &RouteAnalysis{
		Destination:       destination,
		Source:            source,
		GlobalNetworkID:   globalNetworkID,
		RouteAnalysisID:   id,
		Status:            routeAnalysisStatusRunning,
		IncludeReturnPath: includeReturnPath,
	}
	b.routeAnalyses.Put(r)

	reason := routeAnalysisReasonAttachmentNotFound
	if destination == nil || (destination.IPAddress == "" && destination.TransitGatewayAttachmentArn == "") {
		reason = routeAnalysisReasonNoDestination
	}

	b.work.After("RouteAnalysisCompleted", asyncTransitionDelay, func() {
		b.mu.Lock("RouteAnalysisCompleted-async")
		defer b.mu.Unlock()

		v, ok := b.routeAnalyses.Get(id)
		if !ok || v.Status != routeAnalysisStatusRunning {
			return
		}

		completion := &RouteAnalysisCompletion{ReasonCode: reason, ResultCode: routeAnalysisResultNotConnected}
		v.Status = routeAnalysisStatusCompleted
		v.ForwardPath = &RouteAnalysisPath{CompletionStatus: completion}

		if v.IncludeReturnPath {
			v.ReturnPath = &RouteAnalysisPath{CompletionStatus: completion}
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
