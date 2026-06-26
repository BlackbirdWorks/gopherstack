package managedblockchain

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opCreateProposal      = "CreateProposal"
	opDeleteAccessor      = "DeleteAccessor"
	opDeleteMember        = "DeleteMember"
	opDeleteNode          = "DeleteNode"
	opGetAccessor         = "GetAccessor"
	opGetMember           = "GetMember"
	opGetNetwork          = "GetNetwork"
	opGetNode             = "GetNode"
	opGetProposal         = "GetProposal"
	opListAccessors       = "ListAccessors"
	opListInvitations     = "ListInvitations"
	opListMembers         = "ListMembers"
	opListNetworks        = "ListNetworks"
	opListNodes           = "ListNodes"
	opListProposalVotes   = "ListProposalVotes"
	opListProposals       = "ListProposals"
	opListTagsForResource = "ListTagsForResource"
	opRejectInvitation    = "RejectInvitation"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opUpdateMember        = "UpdateMember"
	opUpdateNode          = "UpdateNode"
	opVoteOnProposal      = "VoteOnProposal"
)

const (
	managedblockchainService       = "managedblockchain"
	managedblockchainMatchPriority = 87

	opCreateAccessor = "CreateAccessor"
	opCreateMember   = "CreateMember"
	opCreateNetwork  = "CreateNetwork"
	opCreateNode     = "CreateNode"
)

// Handler is the HTTP handler for the Managed Blockchain REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Managed Blockchain handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ManagedBlockchain" }

// GetSupportedOperations returns the list of supported Managed Blockchain operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateAccessor,
		opCreateMember,
		opCreateNetwork,
		opCreateNode,
		opCreateProposal,
		opDeleteAccessor,
		opDeleteMember,
		opDeleteNode,
		opGetAccessor,
		opGetMember,
		opGetNetwork,
		opGetNode,
		opGetProposal,
		opListAccessors,
		opListInvitations,
		opListMembers,
		opListNetworks,
		opListNodes,
		opListProposalVotes,
		opListProposals,
		opListTagsForResource,
		opRejectInvitation,
		opTagResource,
		opUntagResource,
		opUpdateMember,
		opUpdateNode,
		opVoteOnProposal,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return managedblockchainService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Managed Blockchain REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if path == "/networks" || strings.HasPrefix(path, "/networks/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == managedblockchainService
		}

		if strings.HasPrefix(path, "/tags/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == managedblockchainService
		}

		if path == "/accessors" || strings.HasPrefix(path, "/accessors/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == managedblockchainService
		}

		if path == "/invitations" || strings.HasPrefix(path, "/invitations/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == managedblockchainService
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return managedblockchainMatchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parsePath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource ID from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parsePath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function for Managed Blockchain requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		method := c.Request().Method
		path := c.Request().URL.Path

		op, resource := parsePath(method, path)
		if op == "" {
			return writeError(c, http.StatusNotFound, "ResourceNotFoundException", "resource not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "managedblockchain: failed to read request body", "error", err)

			return writeError(c,
				http.StatusInternalServerError,
				"InternalServiceErrorException",
				"failed to read request body",
			)
		}

		log.DebugContext(ctx, "managedblockchain request", "op", op, "resource", resource)

		return h.dispatch(c, op, resource, body, c.Request().URL.Query())
	}
}

const (
	// maxPathParts is the maximum number of segments to split when parsing paths.
	maxPathParts = 7

	// networkIDSegment is the index of the network ID in the path parts.
	networkIDSegment = 2
)

// parsePath maps a method+path to an (operation, resource) pair.
//
// Supported path shapes:
//
//	POST   /networks                                                               → CreateNetwork, ""
//	GET    /networks                                                               → ListNetworks, ""
//	GET    /networks/{networkId}                                                   → GetNetwork, networkId
//	POST   /networks/{networkId}/members                                           → CreateMember, networkId
//	GET    /networks/{networkId}/members                                           → ListMembers, networkId
//	GET    /networks/{networkId}/members/{memberId}                                → GetMember, networkId/memberId
//	DELETE /networks/{networkId}/members/{memberId}                                → DeleteMember, networkId/memberId
//	POST   /networks/{networkId}/members/{memberId}/nodes                         → CreateNode, networkId/memberId
//	GET    /networks/{networkId}/members/{memberId}/nodes                         → ListNodes, networkId/memberId
//	GET    /networks/{networkId}/members/{memberId}/nodes/{nodeId}                → GetNode, networkId/memberId/nodeId
//	DELETE /networks/{networkId}/members/{memberId}/nodes/{nodeId}                → DeleteNode, networkId/memberId/nodeId
//	POST   /networks/{networkId}/proposals                                         → CreateProposal, networkId
//	GET    /networks/{networkId}/proposals                                         → ListProposals, networkId
//	GET    /networks/{networkId}/proposals/{proposalId}   → GetProposal, networkId/proposalId
//	GET    /networks/{networkId}/proposals/{proposalId}/votes → ListProposalVotes, networkId/proposalId
//	GET    /tags/{resourceArn}                                                     → ListTagsForResource, arn
//	POST   /tags/{resourceArn}                                                     → TagResource, arn
//	DELETE /tags/{resourceArn}                                                     → UntagResource, arn
//	POST   /accessors                                                              → CreateAccessor, ""
//	GET    /accessors                                                              → ListAccessors, ""
//	GET    /accessors/{accessorId}                                                 → GetAccessor, accessorId
//	DELETE /accessors/{accessorId}                                                 → DeleteAccessor, accessorId
//	GET    /invitations                                                            → ListInvitations, ""
//	DELETE /invitations/{invitationId}                                             → RejectInvitation, invitationId
func parsePath(method, path string) (string, string) {
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.SplitN(trimmed, "/", maxPathParts)

	if len(parts) == 0 {
		return "", ""
	}

	base := parts[0]

	switch base {
	case "tags":
		if len(parts) < 2 || parts[1] == "" {
			return "", ""
		}

		arnEncoded := strings.Join(parts[1:], "/")

		switch method {
		case http.MethodGet:
			return opListTagsForResource, arnEncoded
		case http.MethodPost:
			return opTagResource, arnEncoded
		case http.MethodDelete:
			return opUntagResource, arnEncoded
		}

		return "", ""

	case "networks":
		return parseNetworksPath(method, parts)

	case "accessors":
		return parseAccessorsPath(method, parts)

	case "invitations":
		return parseInvitationsPath(method, parts)
	}

	return "", ""
}

// parseNetworksPath handles routing for /networks and /networks/{id}/... paths.
func parseNetworksPath(method string, parts []string) (string, string) {
	// /networks or /networks/
	if len(parts) == 1 || (len(parts) == networkIDSegment && parts[1] == "") {
		return parseRootNetworksMethod(method)
	}

	networkID := parts[1]

	// /networks/{networkId}
	if len(parts) == networkIDSegment {
		if method == http.MethodGet {
			return opGetNetwork, networkID
		}

		return "", ""
	}

	// /networks/{networkId}/members/...
	if parts[2] == "members" {
		return parseMembersPath(method, parts, networkID)
	}

	// /networks/{networkId}/proposals/...
	if parts[2] == "proposals" {
		return parseProposalsPath(method, parts, networkID)
	}

	return "", ""
}

// parseRootNetworksMethod returns the operation for POST/GET /networks.
func parseRootNetworksMethod(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateNetwork, ""
	case http.MethodGet:
		return opListNetworks, ""
	}

	return "", ""
}

// parseMembersPath handles routing for /networks/{networkId}/members/... paths.
func parseMembersPath(method string, parts []string, networkID string) (string, string) {
	if len(parts) == 3 || (len(parts) == 4 && parts[3] == "") {
		switch method {
		case http.MethodPost:
			return opCreateMember, networkID
		case http.MethodGet:
			return opListMembers, networkID
		}

		return "", ""
	}

	// /networks/{networkId}/members/{memberId}[/nodes[/{nodeId}]]
	if len(parts) >= 4 && parts[3] != "" {
		memberID := parts[3]

		// /networks/{networkId}/members/{memberId}/nodes[/{nodeId}]
		if len(parts) >= 5 && parts[4] == "nodes" {
			return parseNodesPath(method, parts, networkID, memberID)
		}

		resource := networkID + "/" + memberID

		switch method {
		case http.MethodGet:
			return opGetMember, resource
		case http.MethodDelete:
			return opDeleteMember, resource
		case http.MethodPatch:
			return opUpdateMember, resource
		}
	}

	return "", ""
}

// parseNodesPath handles routing for /networks/{networkId}/members/{memberId}/nodes/... paths.
func parseNodesPath(method string, parts []string, networkID, memberID string) (string, string) {
	resource := networkID + "/" + memberID

	// /networks/{networkId}/members/{memberId}/nodes
	if len(parts) == 5 || (len(parts) == 6 && parts[5] == "") {
		switch method {
		case http.MethodPost:
			return opCreateNode, resource
		case http.MethodGet:
			return opListNodes, resource
		}

		return "", ""
	}

	// /networks/{networkId}/members/{memberId}/nodes/{nodeId}
	if len(parts) >= 6 && parts[5] != "" {
		nodeID := parts[5]
		nodeResource := resource + "/" + nodeID

		switch method {
		case http.MethodGet:
			return opGetNode, nodeResource
		case http.MethodDelete:
			return opDeleteNode, nodeResource
		case http.MethodPatch:
			return opUpdateNode, nodeResource
		}
	}

	return "", ""
}

// parseProposalsPath handles routing for /networks/{networkId}/proposals/... paths.
func parseProposalsPath(method string, parts []string, networkID string) (string, string) {
	// /networks/{networkId}/proposals  or  /networks/{networkId}/proposals/
	if len(parts) == 3 || (len(parts) == 4 && parts[3] == "") {
		switch method {
		case http.MethodPost:
			return opCreateProposal, networkID
		case http.MethodGet:
			return opListProposals, networkID
		}

		return "", ""
	}

	// /networks/{networkId}/proposals/{proposalId}[/votes]
	if len(parts) >= 4 && parts[3] != "" {
		return parseProposalIDPath(method, parts, networkID)
	}

	return "", ""
}

// parseProposalIDPath handles routing for /networks/{networkId}/proposals/{proposalId}[/votes].
func parseProposalIDPath(method string, parts []string, networkID string) (string, string) {
	proposalID := parts[3]
	resource := networkID + "/" + proposalID

	// /networks/{networkId}/proposals/{proposalId}/votes
	if len(parts) >= 5 && parts[4] == "votes" {
		switch method {
		case http.MethodGet:
			return opListProposalVotes, resource
		case http.MethodPost:
			return opVoteOnProposal, resource
		}
	}

	// /networks/{networkId}/proposals/{proposalId}
	if len(parts) == 4 || (len(parts) == 5 && parts[4] == "") {
		if method == http.MethodGet {
			return opGetProposal, resource
		}
	}

	return "", ""
}

// parseAccessorsPath handles routing for /accessors and /accessors/{id} paths.
func parseAccessorsPath(method string, parts []string) (string, string) {
	// /accessors  or  /accessors/
	if len(parts) == 1 || (len(parts) == 2 && parts[1] == "") {
		switch method {
		case http.MethodPost:
			return opCreateAccessor, ""
		case http.MethodGet:
			return opListAccessors, ""
		}

		return "", ""
	}

	// /accessors/{accessorId}
	if len(parts) >= 2 && parts[1] != "" {
		accessorID := parts[1]

		switch method {
		case http.MethodGet:
			return opGetAccessor, accessorID
		case http.MethodDelete:
			return opDeleteAccessor, accessorID
		}
	}

	return "", ""
}

// parseInvitationsPath handles routing for /invitations and /invitations/{id} paths.
func parseInvitationsPath(method string, parts []string) (string, string) {
	// /invitations  or  /invitations/
	if len(parts) == 1 || (len(parts) == 2 && parts[1] == "") {
		if method == http.MethodGet {
			return opListInvitations, ""
		}

		return "", ""
	}

	// /invitations/{invitationId}
	if len(parts) >= 2 && parts[1] != "" {
		invitationID := parts[1]

		if method == http.MethodDelete {
			return opRejectInvitation, invitationID
		}
	}

	return "", ""
}

// dispatch routes to the appropriate handler based on the operation name.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte, query url.Values) error {
	if err := h.dispatchNetworkOps(c, op, resource, body, query); !errors.Is(err, errUnknownOp) {
		return err
	}

	if err := h.dispatchAccessorOps(c, op, resource, body); !errors.Is(err, errUnknownOp) {
		return err
	}

	if err := h.dispatchProposalOps(c, op, resource, body); !errors.Is(err, errUnknownOp) {
		return err
	}

	if err := h.dispatchInvitationOps(c, op, resource); !errors.Is(err, errUnknownOp) {
		return err
	}

	return writeError(c, http.StatusNotFound, "ResourceNotFoundException", "unknown operation")
}

// errUnknownOp is a sentinel returned by sub-dispatch helpers when the operation is not handled.
var errUnknownOp = errors.New("unknown operation")

// dispatchNetworkOps handles network, member, node, and tag operations.
func (h *Handler) dispatchNetworkOps(
	c *echo.Context, op, resource string, body []byte, query url.Values,
) error {
	switch op {
	case opCreateNetwork:
		return h.handleCreateNetwork(c, body)
	case opGetNetwork:
		return h.handleGetNetwork(c, resource)
	case opListNetworks:
		return h.handleListNetworks(c)
	case opListTagsForResource:
		return h.handleListTagsForResource(c, resource)
	case opTagResource:
		return h.handleTagResource(c, resource, body)
	case opUntagResource:
		return h.handleUntagResource(c, resource, query)
	}

	if err := h.dispatchMemberNodeOps(c, op, resource, body); !errors.Is(err, errUnknownOp) {
		return err
	}

	return errUnknownOp
}

// dispatchMemberNodeOps handles member and node operations.
func (h *Handler) dispatchMemberNodeOps(c *echo.Context, op, resource string, body []byte) error {
	switch op {
	case opCreateMember:
		return h.handleCreateMember(c, resource, body)
	case opGetMember:
		return h.handleGetMember(c, resource)
	case opListMembers:
		return h.handleListMembers(c, resource)
	case opDeleteMember:
		return h.handleDeleteMember(c, resource)
	case opUpdateMember:
		return h.handleUpdateMember(c, resource, body)
	case opCreateNode:
		return h.handleCreateNode(c, resource, body)
	case opGetNode:
		return h.handleGetNode(c, resource)
	case opListNodes:
		return h.handleListNodes(c, resource)
	case opDeleteNode:
		return h.handleDeleteNode(c, resource)
	case opUpdateNode:
		return h.handleUpdateNode(c, resource, body)
	}

	return errUnknownOp
}

// dispatchAccessorOps handles accessor operations.
func (h *Handler) dispatchAccessorOps(c *echo.Context, op, resource string, body []byte) error {
	switch op {
	case opCreateAccessor:
		return h.handleCreateAccessor(c, body)
	case opGetAccessor:
		return h.handleGetAccessor(c, resource)
	case opDeleteAccessor:
		return h.handleDeleteAccessor(c, resource)
	case opListAccessors:
		return h.handleListAccessors(c)
	}

	return errUnknownOp
}

// dispatchProposalOps handles proposal operations.
func (h *Handler) dispatchProposalOps(c *echo.Context, op, resource string, body []byte) error {
	switch op {
	case opCreateProposal:
		return h.handleCreateProposal(c, resource, body)
	case opGetProposal:
		return h.handleGetProposal(c, resource)
	case opListProposals:
		return h.handleListProposals(c, resource)
	case opListProposalVotes:
		return h.handleListProposalVotes(c, resource)
	case opVoteOnProposal:
		return h.handleVoteOnProposal(c, resource, body)
	}

	return errUnknownOp
}

// dispatchInvitationOps handles invitation operations.
func (h *Handler) dispatchInvitationOps(c *echo.Context, op, resource string) error {
	switch op {
	case opListInvitations:
		return h.handleListInvitations(c)
	case opRejectInvitation:
		return h.handleRejectInvitation(c, resource)
	}

	return errUnknownOp
}

func (h *Handler) handleCreateNetwork(c *echo.Context, body []byte) error {
	var req createNetworkRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.Name == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkName.Error())
	}

	if req.MemberConfiguration.Name == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingMemberName.Error())
	}

	var votingPolicy *VotingPolicy

	if req.VotingPolicy != nil {
		votingPolicy = &VotingPolicy{}

		if req.VotingPolicy.ApprovalThresholdPolicy != nil {
			votingPolicy.ApprovalThresholdPolicy = &ApprovalThresholdPolicy{
				ThresholdComparator:     req.VotingPolicy.ApprovalThresholdPolicy.ThresholdComparator,
				ProposalDurationInHours: req.VotingPolicy.ApprovalThresholdPolicy.ProposalDurationInHours,
				ThresholdPercentage:     req.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage,
			}
		}
	}

	network, member, err := h.Backend.CreateNetwork(
		h.DefaultRegion,
		h.AccountID,
		req.Name,
		req.Description,
		req.Framework,
		req.FrameworkVersion,
		req.MemberConfiguration.Name,
		req.MemberConfiguration.Description,
		req.Tags,
		votingPolicy,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createNetworkResponse{
		NetworkID: network.ID,
		MemberID:  member.ID,
	})
}

func (h *Handler) handleGetNetwork(c *echo.Context, networkID string) error {
	network, err := h.Backend.GetNetwork(networkID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getNetworkResponse{
		Network: toNetworkObject(network),
	})
}

func (h *Handler) handleListNetworks(c *echo.Context) error {
	q := c.Request().URL.Query()
	filter := ListNetworksFilter{
		Name:      q.Get("name"),
		Framework: q.Get("framework"),
		Status:    q.Get("status"),
	}

	networks, err := h.Backend.ListNetworks(filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]networkSummaryObject, 0, len(networks))

	for _, n := range networks {
		summaries = append(summaries, toNetworkSummaryObject(n))
	}

	return c.JSON(http.StatusOK, listNetworksResponse{Networks: summaries})
}

func (h *Handler) handleCreateMember(c *echo.Context, networkID string, body []byte) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	var req createMemberRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.MemberConfiguration.Name == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingMemberName.Error())
	}

	member, err := h.Backend.CreateMember(
		h.DefaultRegion,
		h.AccountID,
		networkID,
		req.MemberConfiguration.Name,
		req.MemberConfiguration.Description,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createMemberResponse{MemberID: member.ID})
}

func (h *Handler) handleGetMember(c *echo.Context, resource string) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	member, err := h.Backend.GetMember(networkID, memberID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getMemberResponse{
		Member: toMemberObject(member),
	})
}

func (h *Handler) handleListMembers(c *echo.Context, networkID string) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	q := c.Request().URL.Query()
	filter := ListMembersFilter{
		Name:   q.Get("name"),
		Status: q.Get("status"),
	}

	if isOwnedStr := q.Get("isOwned"); isOwnedStr != "" {
		isOwned := isOwnedStr == "true"
		filter.IsOwned = &isOwned
	}

	members, err := h.Backend.ListMembers(networkID, filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]memberSummaryObject, 0, len(members))

	for _, m := range members {
		summaries = append(summaries, toMemberSummaryObject(m))
	}

	return c.JSON(http.StatusOK, listMembersResponse{Members: summaries})
}

func (h *Handler) handleDeleteMember(c *echo.Context, resource string) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	if err := h.Backend.DeleteMember(networkID, memberID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateNode(c *echo.Context, resource string, body []byte) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	var req createNodeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	node, err := h.Backend.CreateNode(
		h.DefaultRegion,
		h.AccountID,
		networkID,
		memberID,
		req.NodeConfiguration.InstanceType,
		req.NodeConfiguration.AvailabilityZone,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createNodeResponse{NodeID: node.ID})
}

func (h *Handler) handleGetNode(c *echo.Context, resource string) error {
	networkID, memberID, nodeID, ok := splitThreePart(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	node, err := h.Backend.GetNode(networkID, memberID, nodeID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getNodeResponse{Node: toNodeObject(node)})
}

func (h *Handler) handleListNodes(c *echo.Context, resource string) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	filter := ListNodesFilter{
		Status: c.Request().URL.Query().Get("status"),
	}

	nodes, err := h.Backend.ListNodes(networkID, memberID, filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]nodeSummaryObject, 0, len(nodes))
	for _, n := range nodes {
		summaries = append(summaries, toNodeSummaryObject(n))
	}

	return c.JSON(http.StatusOK, listNodesResponse{Nodes: summaries})
}

func (h *Handler) handleDeleteNode(c *echo.Context, resource string) error {
	networkID, memberID, nodeID, ok := splitThreePart(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	if err := h.Backend.DeleteNode(networkID, memberID, nodeID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	decoded, err := url.PathUnescape(resourceARN)
	if err != nil {
		decoded = resourceARN
	}

	tags, err := h.Backend.ListTagsForResource(decoded)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	decoded, err := url.PathUnescape(resourceARN)
	if err != nil {
		decoded = resourceARN
	}

	var req tagResourceRequest

	if parseErr := json.Unmarshal(body, &req); parseErr != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if tagErr := h.Backend.TagResource(decoded, req.Tags); tagErr != nil {
		return h.writeBackendError(c, tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, query url.Values) error {
	decoded, err := url.PathUnescape(resourceARN)
	if err != nil {
		decoded = resourceARN
	}

	tagKeys := query["tagKeys"]

	if untagErr := h.Backend.UntagResource(decoded, tagKeys); untagErr != nil {
		return h.writeBackendError(c, untagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateAccessor(c *echo.Context, body []byte) error {
	var req createAccessorRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	accessor, err := h.Backend.CreateAccessor(
		h.DefaultRegion,
		h.AccountID,
		req.AccessorType,
		req.NetworkType,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createAccessorResponse{
		AccessorID:   accessor.ID,
		BillingToken: accessor.BillingToken,
		NetworkType:  accessor.NetworkType,
	})
}

func (h *Handler) handleGetAccessor(c *echo.Context, accessorID string) error {
	accessor, err := h.Backend.GetAccessor(accessorID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getAccessorResponse{
		Accessor: toAccessorObject(accessor),
	})
}

func (h *Handler) handleDeleteAccessor(c *echo.Context, accessorID string) error {
	if err := h.Backend.DeleteAccessor(accessorID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListAccessors(c *echo.Context) error {
	filter := ListAccessorsFilter{
		NetworkType: c.Request().URL.Query().Get("networkType"),
	}

	accessors, err := h.Backend.ListAccessors(filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]accessorSummaryObject, 0, len(accessors))

	for _, a := range accessors {
		summaries = append(summaries, toAccessorSummaryObject(a))
	}

	return c.JSON(http.StatusOK, listAccessorsResponse{Accessors: summaries})
}

func (h *Handler) handleCreateProposal(c *echo.Context, networkID string, body []byte) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	var req createProposalRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.MemberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingMemberID.Error())
	}

	var actions *ProposalActions

	if req.Actions != nil {
		actions = &ProposalActions{}

		for _, inv := range req.Actions.Invitations {
			actions.Invitations = append(actions.Invitations, InviteAction(inv))
		}

		for _, rem := range req.Actions.Removals {
			actions.Removals = append(actions.Removals, RemoveAction(rem))
		}
	}

	proposal, err := h.Backend.CreateProposal(
		h.DefaultRegion,
		h.AccountID,
		networkID,
		req.MemberID,
		req.Description,
		actions,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createProposalResponse{ProposalID: proposal.ProposalID})
}

func (h *Handler) handleGetProposal(c *echo.Context, resource string) error {
	networkID, proposalID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	proposal, err := h.Backend.GetProposal(networkID, proposalID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getProposalResponse{Proposal: toProposalObject(proposal)})
}

func (h *Handler) handleListProposals(c *echo.Context, networkID string) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	statusFilter := c.Request().URL.Query().Get("status")

	proposals, err := h.Backend.ListProposals(networkID, statusFilter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]proposalSummaryObject, 0, len(proposals))

	for _, p := range proposals {
		summaries = append(summaries, toProposalSummaryObject(p))
	}

	return c.JSON(http.StatusOK, listProposalsResponse{Proposals: summaries})
}

func (h *Handler) handleListProposalVotes(c *echo.Context, resource string) error {
	networkID, proposalID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	votes, err := h.Backend.ListProposalVotes(networkID, proposalID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]voteSummaryObject, 0, len(votes))

	for _, v := range votes {
		summaries = append(summaries, voteSummaryObject{
			MemberID:   v.MemberID,
			MemberName: v.MemberName,
			Vote:       v.Vote,
		})
	}

	return c.JSON(http.StatusOK, listProposalVotesResponse{ProposalVotes: summaries})
}

func (h *Handler) handleListInvitations(c *echo.Context) error {
	invitations, err := h.Backend.ListInvitations()
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]invitationObject, 0, len(invitations))

	for _, inv := range invitations {
		objs = append(objs, toInvitationObject(inv))
	}

	return c.JSON(http.StatusOK, listInvitationsResponse{Invitations: objs})
}

func (h *Handler) handleRejectInvitation(c *echo.Context, invitationID string) error {
	if err := h.Backend.RejectInvitation(invitationID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateMember(c *echo.Context, resource string, body []byte) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	var req updateMemberRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	_, err := h.Backend.UpdateMember(networkID, memberID, buildMemberLogConfig(req.LogPublishingConfiguration))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func buildMemberLogConfig(req *memberLogPublishingConfigReq) *MemberLogPublishingConfigState {
	if req == nil {
		return nil
	}

	logConfig := &MemberLogPublishingConfigState{}

	if req.Fabric == nil {
		return logConfig
	}

	fabric := &MemberFabricLogState{}

	if req.Fabric.CaLogs != nil {
		caLogs := &LogConfigState{}
		if req.Fabric.CaLogs.CloudWatch != nil {
			caLogs.CloudWatch = &CloudWatchLogState{Enabled: req.Fabric.CaLogs.CloudWatch.Enabled}
		}
		fabric.CALogs = caLogs
	}

	logConfig.Fabric = fabric

	return logConfig
}

func (h *Handler) handleUpdateNode(c *echo.Context, resource string, body []byte) error {
	networkID, memberID, nodeID, ok := splitThreePart(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	var req updateNodeRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
		}
	}

	_, err := h.Backend.UpdateNode(networkID, memberID, nodeID, buildNodeLogConfig(req.LogPublishingConfiguration))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func buildNodeLogConfig(req *nodeLogPublishingConfigReq) *NodeLogPublishingConfigState {
	if req == nil {
		return nil
	}

	logConfig := &NodeLogPublishingConfigState{}

	if req.Fabric == nil {
		return logConfig
	}

	fabric := &NodeFabricLogState{}

	if req.Fabric.ChaincodeLogs != nil {
		cl := &LogConfigState{}
		if req.Fabric.ChaincodeLogs.CloudWatch != nil {
			cl.CloudWatch = &CloudWatchLogState{Enabled: req.Fabric.ChaincodeLogs.CloudWatch.Enabled}
		}
		fabric.ChaincodeLogs = cl
	}

	if req.Fabric.PeerLogs != nil {
		pl := &LogConfigState{}
		if req.Fabric.PeerLogs.CloudWatch != nil {
			pl.CloudWatch = &CloudWatchLogState{Enabled: req.Fabric.PeerLogs.CloudWatch.Enabled}
		}
		fabric.PeerLogs = pl
	}

	logConfig.Fabric = fabric

	return logConfig
}

func (h *Handler) handleVoteOnProposal(c *echo.Context, resource string, body []byte) error {
	networkID, proposalID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	var req voteOnProposalRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.VoterMemberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingVoterMemberID.Error())
	}

	if err := h.Backend.VoteOnProposal(networkID, proposalID, req.VoterMemberID, req.Vote); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// writeBackendError translates a backend error to an HTTP response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return writeError(c, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", err.Error())
	default:
		return writeError(c, http.StatusInternalServerError, "InternalServiceErrorException", err.Error())
	}
}

// writeError writes a JSON error response.
func writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, errorResponse{Message: message, Code: code})
}

// splitResource splits a "networkId/memberId" resource string into its parts.
func splitResource(resource string) (string, string, bool) {
	idx := strings.Index(resource, "/")
	if idx <= 0 || idx == len(resource)-1 {
		return "", "", false
	}

	return resource[:idx], resource[idx+1:], true
}

// splitThreePart splits a "a/b/c" resource string into its three parts.
func splitThreePart(resource string) (string, string, string, bool) {
	first, rest, ok := strings.Cut(resource, "/")
	if !ok || rest == "" {
		return "", "", "", false
	}

	second, third, ok := strings.Cut(rest, "/")
	if !ok || third == "" {
		return "", "", "", false
	}

	return first, second, third, true
}

// toNetworkObject converts a Network to its JSON representation.
func toNetworkObject(n *Network) networkObject {
	obj := networkObject{
		ID:               n.ID,
		Arn:              n.Arn,
		Name:             n.Name,
		Description:      n.Description,
		Framework:        n.Framework,
		FrameworkVersion: n.FrameworkVersion,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
		Tags:             n.Tags,
	}

	if n.VotingPolicy != nil {
		vp := &votingPolicyObject{}

		if n.VotingPolicy.ApprovalThresholdPolicy != nil {
			vp.ApprovalThresholdPolicy = &approvalThresholdPolicyObject{
				ThresholdComparator:     n.VotingPolicy.ApprovalThresholdPolicy.ThresholdComparator,
				ProposalDurationInHours: n.VotingPolicy.ApprovalThresholdPolicy.ProposalDurationInHours,
				ThresholdPercentage:     n.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage,
			}
		}

		obj.VotingPolicy = vp
	}

	return obj
}

// toNetworkSummaryObject converts a Network to its summary JSON representation.
func toNetworkSummaryObject(n *Network) networkSummaryObject {
	return networkSummaryObject{
		ID:               n.ID,
		Arn:              n.Arn,
		Name:             n.Name,
		Description:      n.Description,
		Framework:        n.Framework,
		FrameworkVersion: n.FrameworkVersion,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
	}
}

// toMemberObject converts a Member to its JSON representation.
func toMemberObject(m *Member) memberObject {
	obj := memberObject{
		ID:           m.ID,
		Arn:          m.Arn,
		Name:         m.Name,
		Description:  m.Description,
		NetworkID:    m.NetworkID,
		Status:       m.Status,
		CreationDate: m.CreationDate,
		Tags:         m.Tags,
		IsOwned:      m.IsOwned,
	}

	if m.LogPublishingConfiguration != nil {
		obj.LogPublishingConfiguration = toMemberLogConfigRespObj(m.LogPublishingConfiguration)
	}

	return obj
}

// toMemberLogConfigRespObj converts MemberLogPublishingConfigState to its response JSON.
func toMemberLogConfigRespObj(c *MemberLogPublishingConfigState) *memberLogPublishingConfigRespObj {
	if c == nil {
		return nil
	}

	obj := &memberLogPublishingConfigRespObj{}

	if c.Fabric != nil {
		fabric := &memberFabricLogRespObj{}

		if c.Fabric.CALogs != nil {
			fabric.CaLogs = toLogConfigRespObj(c.Fabric.CALogs)
		}

		obj.Fabric = fabric
	}

	return obj
}

// toLogConfigRespObj converts LogConfigState to its response JSON.
func toLogConfigRespObj(c *LogConfigState) *logConfigRespObj {
	if c == nil {
		return nil
	}

	obj := &logConfigRespObj{}

	if c.CloudWatch != nil {
		obj.CloudWatch = &cloudWatchLogRespObj{Enabled: c.CloudWatch.Enabled}
	}

	return obj
}

// toMemberSummaryObject converts a Member to its summary JSON representation.
func toMemberSummaryObject(m *Member) memberSummaryObject {
	return memberSummaryObject{
		ID:           m.ID,
		Arn:          m.Arn,
		Name:         m.Name,
		Description:  m.Description,
		Status:       m.Status,
		CreationDate: m.CreationDate,
		IsOwned:      m.IsOwned,
	}
}

// toNodeObject converts a Node to its JSON representation.
func toNodeObject(n *Node) nodeObject {
	obj := nodeObject{
		ID:               n.ID,
		Arn:              n.Arn,
		InstanceType:     n.InstanceType,
		AvailabilityZone: n.AvailabilityZone,
		MemberID:         n.MemberID,
		NetworkID:        n.NetworkID,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
		Tags:             n.Tags,
	}

	if n.LogPublishingConfiguration != nil {
		obj.LogPublishingConfiguration = toNodeLogConfigRespObj(n.LogPublishingConfiguration)
	}

	return obj
}

// toNodeLogConfigRespObj converts NodeLogPublishingConfigState to its response JSON.
func toNodeLogConfigRespObj(c *NodeLogPublishingConfigState) *nodeLogPublishingConfigRespObj {
	if c == nil {
		return nil
	}

	obj := &nodeLogPublishingConfigRespObj{}

	if c.Fabric != nil {
		fabric := &nodeFabricLogRespObj{}

		if c.Fabric.ChaincodeLogs != nil {
			fabric.ChaincodeLogs = toLogConfigRespObj(c.Fabric.ChaincodeLogs)
		}

		if c.Fabric.PeerLogs != nil {
			fabric.PeerLogs = toLogConfigRespObj(c.Fabric.PeerLogs)
		}

		obj.Fabric = fabric
	}

	return obj
}

// toNodeSummaryObject converts a Node to its summary JSON representation.
func toNodeSummaryObject(n *Node) nodeSummaryObject {
	return nodeSummaryObject{
		ID:               n.ID,
		Arn:              n.Arn,
		InstanceType:     n.InstanceType,
		AvailabilityZone: n.AvailabilityZone,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
	}
}

// toAccessorObject converts an Accessor to its JSON representation.
func toAccessorObject(a *Accessor) accessorObject {
	return accessorObject{
		ID:           a.ID,
		Arn:          a.Arn,
		BillingToken: a.BillingToken,
		Type:         a.Type,
		NetworkType:  a.NetworkType,
		Status:       a.Status,
		CreationDate: a.CreationDate,
		Tags:         a.Tags,
	}
}

// toAccessorSummaryObject converts an Accessor to its summary JSON representation.
func toAccessorSummaryObject(a *Accessor) accessorSummaryObject {
	return accessorSummaryObject{
		ID:           a.ID,
		Arn:          a.Arn,
		Type:         a.Type,
		NetworkType:  a.NetworkType,
		Status:       a.Status,
		CreationDate: a.CreationDate,
	}
}

// toProposalObject converts a Proposal to its JSON representation.
func toProposalObject(p *Proposal) proposalObject {
	obj := proposalObject{
		ProposalID:           p.ProposalID,
		Arn:                  p.Arn,
		NetworkID:            p.NetworkID,
		ProposedByMemberID:   p.ProposedByMemberID,
		ProposedByMemberName: p.ProposedByMemberName,
		Description:          p.Description,
		Status:               p.Status,
		CreationDate:         p.CreationDate,
		ExpirationDate:       p.ExpirationDate,
		YesVoteCount:         p.YesVoteCount,
		NoVoteCount:          p.NoVoteCount,
		OutstandingVoteCount: p.OutstandingVoteCount,
		Tags:                 p.Tags,
	}

	if p.Actions != nil {
		actObj := &proposalActionsObject{}

		for _, inv := range p.Actions.Invitations {
			actObj.Invitations = append(actObj.Invitations, inviteActionObject(inv))
		}

		for _, rem := range p.Actions.Removals {
			actObj.Removals = append(actObj.Removals, removeActionObject(rem))
		}

		obj.Actions = actObj
	}

	return obj
}

// toProposalSummaryObject converts a Proposal to its summary JSON representation.
func toProposalSummaryObject(p *Proposal) proposalSummaryObject {
	return proposalSummaryObject{
		ProposalID:           p.ProposalID,
		Arn:                  p.Arn,
		NetworkID:            p.NetworkID,
		ProposedByMemberID:   p.ProposedByMemberID,
		ProposedByMemberName: p.ProposedByMemberName,
		Description:          p.Description,
		Status:               p.Status,
		CreationDate:         p.CreationDate,
		ExpirationDate:       p.ExpirationDate,
	}
}

// toInvitationObject converts an Invitation to its JSON representation.
func toInvitationObject(inv *Invitation) invitationObject {
	obj := invitationObject{
		InvitationID:   inv.InvitationID,
		Arn:            inv.Arn,
		NetworkID:      inv.NetworkID,
		NetworkName:    inv.NetworkName,
		Status:         inv.Status,
		CreationDate:   inv.CreationDate,
		ExpirationDate: inv.ExpirationDate,
	}

	if inv.NetworkSummary != nil {
		obj.NetworkSummary = &invitationNetworkSummaryObject{
			ID:               inv.NetworkSummary.ID,
			Arn:              inv.NetworkSummary.Arn,
			Name:             inv.NetworkSummary.Name,
			Description:      inv.NetworkSummary.Description,
			Framework:        inv.NetworkSummary.Framework,
			FrameworkVersion: inv.NetworkSummary.FrameworkVersion,
			Status:           inv.NetworkSummary.Status,
			CreationDate:     inv.NetworkSummary.CreationDate,
		}
	}

	return obj
}
