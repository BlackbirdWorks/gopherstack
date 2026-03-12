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
	managedblockchainService       = "managedblockchain"
	managedblockchainMatchPriority = 87
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
		"CreateNetwork",
		"GetNetwork",
		"ListNetworks",
		"CreateMember",
		"GetMember",
		"ListMembers",
		"DeleteMember",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
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
			return writeError(c, http.StatusNotFound, "resource not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "managedblockchain: failed to read request body", "error", err)

			return writeError(c, http.StatusInternalServerError, "failed to read request body")
		}

		log.DebugContext(ctx, "managedblockchain request", "op", op, "resource", resource)

		return h.dispatch(c, op, resource, body, c.Request().URL.Query())
	}
}

const (
	// maxPathParts is the maximum number of segments to split when parsing paths.
	maxPathParts = 5

	// networkIDSegment is the index of the network ID in the path parts.
	networkIDSegment = 2
)

// parsePath maps a method+path to an (operation, resource) pair.
//
// Supported path shapes:
//
//	POST   /networks                                    → CreateNetwork, ""
//	GET    /networks                                    → ListNetworks, ""
//	GET    /networks/{networkId}                        → GetNetwork, networkId
//	POST   /networks/{networkId}/members                → CreateMember, networkId
//	GET    /networks/{networkId}/members                → ListMembers, networkId
//	GET    /networks/{networkId}/members/{memberId}     → GetMember, networkId/memberId
//	DELETE /networks/{networkId}/members/{memberId}     → DeleteMember, networkId/memberId
//	GET    /tags/{resourceArn}                          → ListTagsForResource, arn
//	POST   /tags/{resourceArn}                          → TagResource, arn
//	DELETE /tags/{resourceArn}                          → UntagResource, arn
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
			return "ListTagsForResource", arnEncoded
		case http.MethodPost:
			return "TagResource", arnEncoded
		case http.MethodDelete:
			return "UntagResource", arnEncoded
		}

		return "", ""

	case "networks":
		return parseNetworksPath(method, parts)
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
			return "GetNetwork", networkID
		}

		return "", ""
	}

	// /networks/{networkId}/members/...
	if parts[2] == "members" {
		return parseMembersPath(method, parts, networkID)
	}

	return "", ""
}

// parseRootNetworksMethod returns the operation for POST/GET /networks.
func parseRootNetworksMethod(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return "CreateNetwork", ""
	case http.MethodGet:
		return "ListNetworks", ""
	}

	return "", ""
}

// parseMembersPath handles routing for /networks/{networkId}/members/... paths.
func parseMembersPath(method string, parts []string, networkID string) (string, string) {
	if len(parts) == 3 || (len(parts) == 4 && parts[3] == "") {
		switch method {
		case http.MethodPost:
			return "CreateMember", networkID
		case http.MethodGet:
			return "ListMembers", networkID
		}

		return "", ""
	}

	// /networks/{networkId}/members/{memberId}
	if len(parts) >= 4 && parts[3] != "" {
		memberID := parts[3]
		resource := networkID + "/" + memberID

		switch method {
		case http.MethodGet:
			return "GetMember", resource
		case http.MethodDelete:
			return "DeleteMember", resource
		}
	}

	return "", ""
}

// dispatch routes to the appropriate handler based on the operation name.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte, query url.Values) error {
	_ = query

	switch op {
	case "CreateNetwork":
		return h.handleCreateNetwork(c, body)
	case "GetNetwork":
		return h.handleGetNetwork(c, resource)
	case "ListNetworks":
		return h.handleListNetworks(c)
	case "CreateMember":
		return h.handleCreateMember(c, resource, body)
	case "GetMember":
		return h.handleGetMember(c, resource)
	case "ListMembers":
		return h.handleListMembers(c, resource)
	case "DeleteMember":
		return h.handleDeleteMember(c, resource)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, resource)
	case "TagResource":
		return h.handleTagResource(c, resource, body)
	case "UntagResource":
		return h.handleUntagResource(c, resource, query)
	}

	return writeError(c, http.StatusNotFound, "unknown operation")
}

func (h *Handler) handleCreateNetwork(c *echo.Context, body []byte) error {
	var req createNetworkRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Name == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingNetworkName.Error())
	}

	if req.MemberConfiguration.Name == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingMemberName.Error())
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
		nil,
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
	networks, err := h.Backend.ListNetworks()
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
		return writeError(c, http.StatusBadRequest, ErrMissingNetworkID.Error())
	}

	var req createMemberRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.MemberConfiguration.Name == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingMemberName.Error())
	}

	member, err := h.Backend.CreateMember(
		h.DefaultRegion,
		h.AccountID,
		networkID,
		req.MemberConfiguration.Name,
		req.MemberConfiguration.Description,
		nil,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createMemberResponse{MemberID: member.ID})
}

func (h *Handler) handleGetMember(c *echo.Context, resource string) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "invalid resource path")
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
		return writeError(c, http.StatusBadRequest, ErrMissingNetworkID.Error())
	}

	members, err := h.Backend.ListMembers(networkID)
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
		return writeError(c, http.StatusBadRequest, "invalid resource path")
	}

	if err := h.Backend.DeleteMember(networkID, memberID); err != nil {
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
		return writeError(c, http.StatusBadRequest, "invalid request body")
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

// writeBackendError translates a backend error to an HTTP response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return writeError(c, http.StatusNotFound, err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return writeError(c, http.StatusConflict, err.Error())
	default:
		return writeError(c, http.StatusInternalServerError, err.Error())
	}
}

// writeError writes a JSON error response.
func writeError(c *echo.Context, status int, message string) error {
	return c.JSON(status, errorResponse{Message: message})
}

// splitResource splits a "networkId/memberId" resource string into its parts.
func splitResource(resource string) (string, string, bool) {
	idx := strings.Index(resource, "/")
	if idx <= 0 || idx == len(resource)-1 {
		return "", "", false
	}

	return resource[:idx], resource[idx+1:], true
}

// toNetworkObject converts a Network to its JSON representation.
func toNetworkObject(n *Network) networkObject {
	return networkObject{
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
	return memberObject{
		ID:           m.ID,
		Arn:          m.Arn,
		Name:         m.Name,
		Description:  m.Description,
		NetworkID:    m.NetworkID,
		Status:       m.Status,
		CreationDate: m.CreationDate,
		Tags:         m.Tags,
	}
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
	}
}
