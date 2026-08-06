package networkmanager

import (
	"context"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// coreNetworksRoutes wires PARITY.md families L, M, N, O, P (20 ops). Split
// across one helper per sub-family to keep this function's own length
// under funlen's limit.
func (h *Handler) coreNetworksRoutes() []route {
	return concatRoutes(
		h.coreNetworkCoreRoutes(),
		h.corePolicyRoutes(),
		h.prefixListRoutes(),
		h.routingInfoRoutes(),
		h.routingPolicyLabelRoutes(),
	)
}

func (h *Handler) coreNetworkCoreRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segCoreNetworks},
			op:      "CreateCoreNetwork",
			fn:      h.dispatchCreateCoreNetwork,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segCoreNetworks, paramCoreNetworkID},
			op:      "UpdateCoreNetwork",
			fn:      h.dispatchUpdateCoreNetwork,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segCoreNetworks, paramCoreNetworkID},
			op:      "DeleteCoreNetwork",
			fn:      h.dispatchDeleteCoreNetwork,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segCoreNetworks, paramCoreNetworkID},
			op:      "GetCoreNetwork",
			fn:      h.dispatchGetCoreNetwork,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segCoreNetworks},
			op:      "ListCoreNetworks",
			fn:      h.dispatchListCoreNetworks,
		},
	}
}

func (h *Handler) corePolicyRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segCoreNetworks, paramCoreNetworkID, "core-network-policy"},
			op:      "PutCoreNetworkPolicy",
			fn:      h.dispatchPutCoreNetworkPolicy,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segCoreNetworks, paramCoreNetworkID, "core-network-policy"},
			op:      "GetCoreNetworkPolicy",
			fn:      h.dispatchGetCoreNetworkPolicy,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segCoreNetworks, paramCoreNetworkID, segPolicyVersions},
			op:      "ListCoreNetworkPolicyVersions",
			fn:      h.dispatchListCoreNetworkPolicyVersions,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segCoreNetworks, paramCoreNetworkID, segPolicyVersions, paramPolicyVersionID},
			op:      "DeleteCoreNetworkPolicyVersion",
			fn:      h.dispatchDeleteCoreNetworkPolicyVersion,
		},
		{
			method: http.MethodPost,
			pattern: []string{
				segCoreNetworks,
				paramCoreNetworkID,
				segPolicyVersions,
				paramPolicyVersionID,
				"restore",
			},
			op: "RestoreCoreNetworkPolicyVersion",
			fn: h.dispatchRestoreCoreNetworkPolicyVersion,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segCoreNetworks, paramCoreNetworkID, "core-network-change-sets", paramPolicyVersionID},
			op:      "GetCoreNetworkChangeSet",
			fn:      h.dispatchGetCoreNetworkChangeSet,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segCoreNetworks, paramCoreNetworkID, "core-network-change-events", paramPolicyVersionID},
			op:      "GetCoreNetworkChangeEvents",
			fn:      h.dispatchGetCoreNetworkChangeEvents,
		},
		{
			method: http.MethodPost,
			pattern: []string{
				segCoreNetworks,
				paramCoreNetworkID,
				"core-network-change-sets",
				paramPolicyVersionID,
				"execute",
			},
			op: "ExecuteCoreNetworkChangeSet",
			fn: h.dispatchExecuteCoreNetworkChangeSet,
		},
	}
}

func (h *Handler) prefixListRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segPrefixList},
			op:      "CreateCoreNetworkPrefixListAssociation",
			fn:      h.dispatchCreateCoreNetworkPrefixListAssociation,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segPrefixList, ":PrefixListArn", segCoreNetworkSeg, paramCoreNetworkID},
			op:      "DeleteCoreNetworkPrefixListAssociation",
			fn:      h.dispatchDeleteCoreNetworkPrefixListAssociation,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segPrefixList, segCoreNetworkSeg, paramCoreNetworkID},
			op:      "ListCoreNetworkPrefixListAssociations",
			fn:      h.dispatchListCoreNetworkPrefixListAssociations,
		},
	}
}

func (h *Handler) routingInfoRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segCoreNetworks, paramCoreNetworkID, "core-network-routing-information"},
			op:      "ListCoreNetworkRoutingInformation",
			fn:      h.dispatchListCoreNetworkRoutingInformation,
		},
	}
}

func (h *Handler) routingPolicyLabelRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segRoutingPolicyLabel},
			op:      "PutAttachmentRoutingPolicyLabel",
			fn:      h.dispatchPutAttachmentRoutingPolicyLabel,
		},
		{
			method: http.MethodDelete,
			pattern: []string{
				segRoutingPolicyLabel,
				segCoreNetworkSeg,
				paramCoreNetworkID,
				"attachment",
				paramAttachmentID,
			},
			op: "RemoveAttachmentRoutingPolicyLabel",
			fn: h.dispatchRemoveAttachmentRoutingPolicyLabel,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segRoutingPolicyLabel, segCoreNetworkSeg, paramCoreNetworkID},
			op:      "ListAttachmentRoutingPolicyAssociations",
			fn:      h.dispatchListAttachmentRoutingPolicyAssociations,
		},
	}
}

func parsePolicyVersionID(s string) int32 {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}

// ---- Core Network ----

func (h *Handler) dispatchCreateCoreNetwork(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createCoreNetworkReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.CreateCoreNetwork(
		req.GlobalNetworkID,
		req.Description,
		req.PolicyDocument,
		tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkEnvelope{CoreNetwork: toCoreNetworkWire(c)})
}

func (h *Handler) dispatchUpdateCoreNetwork(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateCoreNetworkReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.UpdateCoreNetwork(params["CoreNetworkId"], req.Description)
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkEnvelope{CoreNetwork: toCoreNetworkWire(c)})
}

func (h *Handler) dispatchDeleteCoreNetwork(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	c, err := h.Backend.DeleteCoreNetwork(params["CoreNetworkId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkEnvelope{CoreNetwork: toCoreNetworkWire(c)})
}

func (h *Handler) dispatchGetCoreNetwork(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	c, err := h.Backend.GetCoreNetwork(params["CoreNetworkId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkEnvelope{CoreNetwork: toCoreNetworkWire(c)})
}

func (h *Handler) dispatchListCoreNetworks(
	_ context.Context,
	r *http.Request,
	_ routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()
	p := h.Backend.ListCoreNetworks(queryNextToken(q), queryMaxResults(q))

	out := make([]coreNetworkSummaryWire, len(p.Data))
	for i, c := range p.Data {
		out[i] = toCoreNetworkSummaryWire(c)
	}

	return marshalResponse(listCoreNetworksResponse{CoreNetworks: out, NextToken: p.Next})
}

// ---- Core Network Policy lifecycle ----

func (h *Handler) dispatchPutCoreNetworkPolicy(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req putCoreNetworkPolicyReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	_, v, err := h.Backend.PutCoreNetworkPolicy(
		params["CoreNetworkId"],
		req.PolicyDocument,
		req.Description,
		req.LatestVersionID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkPolicyEnvelope{CoreNetworkPolicy: toCoreNetworkPolicyWire(v, policyAliasLatest)})
}

func (h *Handler) dispatchGetCoreNetworkPolicy(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	v, alias, err := h.Backend.GetCoreNetworkPolicy(
		params["CoreNetworkId"],
		q.Get("alias"),
		parsePolicyVersionID(q.Get("policyVersionId")),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkPolicyEnvelope{CoreNetworkPolicy: toCoreNetworkPolicyWire(v, alias)})
}

func (h *Handler) dispatchListCoreNetworkPolicyVersions(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, aliases, err := h.Backend.ListCoreNetworkPolicyVersions(
		params["CoreNetworkId"],
		queryNextToken(q),
		queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]coreNetworkPolicyVersionWire, len(p.Data))
	for i, v := range p.Data {
		out[i] = toCoreNetworkPolicyVersionWire(v, aliases[v.PolicyVersionID])
	}

	return marshalResponse(listCoreNetworkPolicyVersionsResponse{CoreNetworkPolicyVersions: out, NextToken: p.Next})
}

func (h *Handler) dispatchDeleteCoreNetworkPolicyVersion(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	v, alias, err := h.Backend.DeleteCoreNetworkPolicyVersion(
		params["CoreNetworkId"],
		parsePolicyVersionID(params["PolicyVersionId"]),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkPolicyEnvelope{CoreNetworkPolicy: toCoreNetworkPolicyWire(v, alias)})
}

func (h *Handler) dispatchRestoreCoreNetworkPolicyVersion(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	v, err := h.Backend.RestoreCoreNetworkPolicyVersion(
		params["CoreNetworkId"],
		parsePolicyVersionID(params["PolicyVersionId"]),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(coreNetworkPolicyEnvelope{CoreNetworkPolicy: toCoreNetworkPolicyWire(v, policyAliasLatest)})
}

func (h *Handler) dispatchGetCoreNetworkChangeSet(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	changes, err := h.Backend.GetCoreNetworkChangeSet(
		params["CoreNetworkId"], parsePolicyVersionID(params["PolicyVersionId"]),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(getCoreNetworkChangeSetResponse{CoreNetworkChanges: toCoreNetworkChangesWire(changes)})
}

func (h *Handler) dispatchGetCoreNetworkChangeEvents(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	events, err := h.Backend.GetCoreNetworkChangeEvents(
		params["CoreNetworkId"],
		parsePolicyVersionID(params["PolicyVersionId"]),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		getCoreNetworkChangeEventsResponse{CoreNetworkChangeEvents: toCoreNetworkChangeEventsWire(events)},
	)
}

func (h *Handler) dispatchExecuteCoreNetworkChangeSet(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	err := h.Backend.ExecuteCoreNetworkChangeSet(
		params["CoreNetworkId"],
		parsePolicyVersionID(params["PolicyVersionId"]),
	)
	if err != nil {
		return nil, err
	}

	return []byte("{}"), nil
}

// ---- Core Network Prefix List Association ----

func (h *Handler) dispatchCreateCoreNetworkPrefixListAssociation(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createCoreNetworkPrefixListAssociationReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateCoreNetworkPrefixListAssociation(
		req.CoreNetworkID,
		req.PrefixListAlias,
		req.PrefixListArn,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toPrefixListAssociationWire(a))
}

func (h *Handler) dispatchDeleteCoreNetworkPrefixListAssociation(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.DeleteCoreNetworkPrefixListAssociation(params["CoreNetworkId"], params["PrefixListArn"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(toPrefixListAssociationWire(a))
}

func (h *Handler) dispatchListCoreNetworkPrefixListAssociations(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.ListCoreNetworkPrefixListAssociations(
		params["CoreNetworkId"], q.Get("prefixListArn"), queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]prefixListAssociationWire, len(p.Data))
	for i, a := range p.Data {
		out[i] = toPrefixListAssociationWire(a)
	}

	return marshalResponse(
		listCoreNetworkPrefixListAssociationsResponse{PrefixListAssociations: out, NextToken: p.Next},
	)
}

// ---- Core Network Routing Information ----

func (h *Handler) dispatchListCoreNetworkRoutingInformation(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req listCoreNetworkRoutingInformationReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	err := h.Backend.ListCoreNetworkRoutingInformation(params["CoreNetworkId"], req.EdgeLocation, req.SegmentName)
	if err != nil {
		return nil, err
	}

	return marshalResponse(listCoreNetworkRoutingInformationResponse{CoreNetworkRoutingInformation: []struct{}{}})
}

// ---- Attachment Routing Policy labels ----

func (h *Handler) dispatchPutAttachmentRoutingPolicyLabel(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req putAttachmentRoutingPolicyLabelReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	l, err := h.Backend.PutAttachmentRoutingPolicyLabel(req.CoreNetworkID, req.AttachmentID, req.RoutingPolicyLabel)
	if err != nil {
		return nil, err
	}

	return marshalResponse(routingPolicyLabelResponse{
		AttachmentID: l.AttachmentID, CoreNetworkID: l.CoreNetworkID, RoutingPolicyLabel: l.RoutingPolicyLabel,
	})
}

func (h *Handler) dispatchRemoveAttachmentRoutingPolicyLabel(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	l, err := h.Backend.RemoveAttachmentRoutingPolicyLabel(params["CoreNetworkId"], params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(routingPolicyLabelResponse{
		AttachmentID: l.AttachmentID, CoreNetworkID: l.CoreNetworkID, RoutingPolicyLabel: l.RoutingPolicyLabel,
	})
}

func (h *Handler) dispatchListAttachmentRoutingPolicyAssociations(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.ListAttachmentRoutingPolicyAssociations(
		params["CoreNetworkId"], q.Get("attachmentId"), queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]attachmentRoutingPolicyAssociationSummaryWire, len(p.Data))
	for i, l := range p.Data {
		out[i] = attachmentRoutingPolicyAssociationSummaryWire{
			AssociatedRoutingPolicies: []string{l.RoutingPolicyLabel},
			AttachmentID:              l.AttachmentID,
			RoutingPolicyLabel:        l.RoutingPolicyLabel,
		}
	}

	return marshalResponse(listAttachmentRoutingPolicyAssociationsResponse{
		AttachmentRoutingPolicyAssociations: out, NextToken: p.Next,
	})
}
