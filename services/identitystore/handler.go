package identitystore

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	pathPrefix = "/identitystores/"
	// minSegmentsForID is the minimum number of path segments for a resource ID.
	minSegmentsForID = 2
)

// Handler is the Echo HTTP handler for the Identity Store REST API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Identity Store handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "IdentityStore" }

// GetSupportedOperations returns the list of supported Identity Store operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateUser",
		"DescribeUser",
		"ListUsers",
		"UpdateUser",
		"DeleteUser",
		"GetUserId",
		"CreateGroup",
		"DescribeGroup",
		"ListGroups",
		"UpdateGroup",
		"DeleteGroup",
		"GetGroupId",
		"CreateGroupMembership",
		"DescribeGroupMembership",
		"ListGroupMemberships",
		"DeleteGroupMembership",
		"GetGroupMembershipId",
		"ListGroupMembershipsForMember",
		"IsMemberInGroups",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "identitystore" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Identity Store instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Identity Store REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, pathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the Identity Store operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _, _ := parseIdentityStorePath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource ID from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, storeID, resourceID := parseIdentityStorePath(c.Request().Method, c.Request().URL.Path)

	if resourceID != "" {
		return resourceID
	}

	return storeID
}

// Handler returns the Echo handler function for Identity Store requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op, storeID, resourceID := parseIdentityStorePath(c.Request().Method, c.Request().URL.Path)
		if op == "" || storeID == "" {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "identitystore: failed to read request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "identitystore request", "op", op, "storeID", storeID, "resourceID", resourceID)

		return h.dispatch(c, op, storeID, resourceID, body)
	}
}

// ----------------------------------------
// Path parsing
// ----------------------------------------

// parseIdentityStorePath maps HTTP method + URL path to operation, storeID, resourceID.
//
// Path structure: /identitystores/{storeId}/{resource}[/{id}][/{sub}]
//
//nolint:cyclop // dispatch table requires many branches
func parseIdentityStorePath(method, path string) (op, storeID, resourceID string) {
	// Strip the /identitystores/ prefix.
	rest := strings.TrimPrefix(path, pathPrefix)
	if rest == path {
		return "", "", ""
	}

	// Split into segments: [storeID, resource, id?, sub?]
	parts := strings.SplitN(rest, "/", 4)
	if len(parts) < minSegmentsForID {
		return "", "", ""
	}

	storeID = parts[0]
	if storeID == "" {
		return "", "", ""
	}

	resource := parts[1]

	// Determine the resource ID and optional sub-path.
	var resID, sub string

	if len(parts) >= 3 {
		resID = parts[2]
	}

	if len(parts) >= 4 {
		sub = parts[3]
	}

	switch resource {
	case "users":
		return parseUsersPath(method, resID, sub), storeID, resID
	case "groups":
		return parseGroupsPath(method, resID, sub), storeID, resID
	case "memberships":
		return parseMembershipsPath(method, resID), storeID, resID
	case "memberships-for-member":
		if method == http.MethodPost {
			return "ListGroupMembershipsForMember", storeID, ""
		}
	case "IsMemberInGroups":
		if method == http.MethodPost {
			return "IsMemberInGroups", storeID, ""
		}
	}

	return "", storeID, ""
}

func parseUsersPath(method, resID, sub string) string {
	if resID == "" {
		// /identitystores/{storeId}/users
		switch method {
		case http.MethodPost:
			return "CreateUser"
		case http.MethodGet:
			return "ListUsers"
		}

		return ""
	}

	if resID == "id" {
		// /identitystores/{storeId}/users/id
		if method == http.MethodPost {
			return "GetUserId"
		}

		return ""
	}

	if sub == "" {
		// /identitystores/{storeId}/users/{userId}
		switch method {
		case http.MethodGet:
			return "DescribeUser"
		case http.MethodPatch:
			return "UpdateUser"
		case http.MethodDelete:
			return "DeleteUser"
		}

		return ""
	}

	return ""
}

func parseGroupsPath(method, resID, sub string) string {
	if resID == "" {
		// /identitystores/{storeId}/groups
		switch method {
		case http.MethodPost:
			return "CreateGroup"
		case http.MethodGet:
			return "ListGroups"
		}

		return ""
	}

	if resID == "id" {
		// /identitystores/{storeId}/groups/id
		if method == http.MethodPost {
			return "GetGroupId"
		}

		return ""
	}

	if sub == "" {
		// /identitystores/{storeId}/groups/{groupId}
		switch method {
		case http.MethodGet:
			return "DescribeGroup"
		case http.MethodPatch:
			return "UpdateGroup"
		case http.MethodDelete:
			return "DeleteGroup"
		}

		return ""
	}

	// /identitystores/{storeId}/groups/{groupId}/memberships
	if sub == "memberships" && method == http.MethodGet {
		return "ListGroupMemberships"
	}

	return ""
}

func parseMembershipsPath(method, resID string) string {
	if resID == "" {
		// /identitystores/{storeId}/memberships
		if method == http.MethodPost {
			return "CreateGroupMembership"
		}

		return ""
	}

	if resID == "id" {
		// /identitystores/{storeId}/memberships/id
		if method == http.MethodPost {
			return "GetGroupMembershipId"
		}

		return ""
	}

	// /identitystores/{storeId}/memberships/{membershipId}
	switch method {
	case http.MethodGet:
		return "DescribeGroupMembership"
	case http.MethodDelete:
		return "DeleteGroupMembership"
	}

	return ""
}

// ----------------------------------------
// Request/response types
// ----------------------------------------

type createUserRequest struct {
	IdentityStoreID string        `json:"IdentityStoreId"`
	UserName        string        `json:"UserName"`
	DisplayName     string        `json:"DisplayName"`
	NickName        string        `json:"NickName"`
	Title           string        `json:"Title"`
	ProfileURL      string        `json:"ProfileUrl"`
	Locale          string        `json:"Locale"`
	PreferredLang   string        `json:"PreferredLanguage"`
	Timezone        string        `json:"Timezone"`
	UserType        string        `json:"UserType"`
	Name            *Name         `json:"Name"`
	Emails          []Email       `json:"Emails"`
	Addresses       []Address     `json:"Addresses"`
	PhoneNumbers    []PhoneNumber `json:"PhoneNumbers"`
}

type createUserResponse struct {
	UserID          string `json:"UserId"`
	IdentityStoreID string `json:"IdentityStoreId"`
}

type createGroupRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	DisplayName     string `json:"DisplayName"`
	Description     string `json:"Description"`
}

type createGroupResponse struct {
	GroupID         string `json:"GroupId"`
	IdentityStoreID string `json:"IdentityStoreId"`
}

type createGroupMembershipRequest struct {
	IdentityStoreID string   `json:"IdentityStoreId"`
	GroupID         string   `json:"GroupId"`
	MemberID        MemberID `json:"MemberId"`
}

type createGroupMembershipResponse struct {
	MembershipID    string `json:"MembershipId"`
	IdentityStoreID string `json:"IdentityStoreId"`
}

type attributeOperation struct {
	AttributePath  string `json:"AttributePath"`
	AttributeValue any    `json:"AttributeValue"`
}

type updateUserRequest struct {
	Operations []attributeOperation `json:"Operations"`
}

type updateGroupRequest struct {
	Operations []attributeOperation `json:"Operations"`
}

type getUserIDRequest struct {
	IdentityStoreID     string              `json:"IdentityStoreId"`
	AlternateIdentifier alternateIdentifier `json:"AlternateIdentifier"`
}

type getGroupIDRequest struct {
	IdentityStoreID     string              `json:"IdentityStoreId"`
	AlternateIdentifier alternateIdentifier `json:"AlternateIdentifier"`
}

type getGroupMembershipIDRequest struct {
	IdentityStoreID string   `json:"IdentityStoreId"`
	GroupID         string   `json:"GroupId"`
	MemberID        MemberID `json:"MemberId"`
}

type alternateIdentifier struct {
	UniqueAttribute *uniqueAttribute `json:"UniqueAttribute"`
	ExternalID      *externalID      `json:"ExternalId"`
}

type uniqueAttribute struct {
	AttributePath  string `json:"AttributePath"`
	AttributeValue string `json:"AttributeValue"`
}

type externalID struct {
	Issuer string `json:"Issuer"`
	ID     string `json:"Id"`
}

type listGroupMembershipsForMemberRequest struct {
	IdentityStoreID string   `json:"IdentityStoreId"`
	MemberID        MemberID `json:"MemberId"`
}

type isMemberInGroupsRequest struct {
	IdentityStoreID string   `json:"IdentityStoreId"`
	MemberID        MemberID `json:"MemberId"`
	GroupIDs        []string `json:"GroupIds"`
}

type isMemberInGroupsResponse struct {
	Results []groupMembershipExistence `json:"Results"`
}

// ----------------------------------------
// Dispatch
// ----------------------------------------

//nolint:cyclop // dispatch table has necessary branches for each operation
func (h *Handler) dispatch(c *echo.Context, op, storeID, resourceID string, body []byte) error {
	switch op {
	// User operations
	case "CreateUser":
		return h.handleCreateUser(c, storeID, body)
	case "DescribeUser":
		return h.handleDescribeUser(c, storeID, resourceID)
	case "ListUsers":
		return h.handleListUsers(c, storeID)
	case "UpdateUser":
		return h.handleUpdateUser(c, storeID, resourceID, body)
	case "DeleteUser":
		return h.handleDeleteUser(c, storeID, resourceID)
	case "GetUserId":
		return h.handleGetUserID(c, storeID, body)

	// Group operations
	case "CreateGroup":
		return h.handleCreateGroup(c, storeID, body)
	case "DescribeGroup":
		return h.handleDescribeGroup(c, storeID, resourceID)
	case "ListGroups":
		return h.handleListGroups(c, storeID)
	case "UpdateGroup":
		return h.handleUpdateGroup(c, storeID, resourceID, body)
	case "DeleteGroup":
		return h.handleDeleteGroup(c, storeID, resourceID)
	case "GetGroupId":
		return h.handleGetGroupID(c, storeID, body)

	// Membership operations
	case "CreateGroupMembership":
		return h.handleCreateGroupMembership(c, storeID, body)
	case "DescribeGroupMembership":
		return h.handleDescribeGroupMembership(c, storeID, resourceID)
	case "ListGroupMemberships":
		return h.handleListGroupMemberships(c, storeID, resourceID)
	case "DeleteGroupMembership":
		return h.handleDeleteGroupMembership(c, storeID, resourceID)
	case "GetGroupMembershipId":
		return h.handleGetGroupMembershipID(c, storeID, body)
	case "ListGroupMembershipsForMember":
		return h.handleListGroupMembershipsForMember(c, storeID, body)
	case "IsMemberInGroups":
		return h.handleIsMemberInGroups(c, storeID, body)
	}

	return h.writeError(c, http.StatusBadRequest, "UnrecognizedClientException",
		"operation "+op+" is not supported")
}

// ----------------------------------------
// User handlers
// ----------------------------------------

func (h *Handler) handleCreateUser(c *echo.Context, storeID string, body []byte) error {
	var req createUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	user, err := h.Backend.CreateUser(storeID, &CreateUserRequest{
		UserName:      req.UserName,
		DisplayName:   req.DisplayName,
		NickName:      req.NickName,
		Title:         req.Title,
		ProfileURL:    req.ProfileURL,
		Locale:        req.Locale,
		PreferredLang: req.PreferredLang,
		Timezone:      req.Timezone,
		UserType:      req.UserType,
		Name:          req.Name,
		Emails:        req.Emails,
		Addresses:     req.Addresses,
		PhoneNumbers:  req.PhoneNumbers,
	})
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createUserResponse{
		UserID:          user.UserID,
		IdentityStoreID: user.IdentityStoreID,
	})
}

func (h *Handler) handleDescribeUser(c *echo.Context, storeID, userID string) error {
	user, err := h.Backend.DescribeUser(storeID, userID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, user)
}

func (h *Handler) handleListUsers(c *echo.Context, storeID string) error {
	users := h.Backend.ListUsers(storeID)

	return c.JSON(http.StatusOK, map[string]any{
		"Users":     users,
		"NextToken": nil,
	})
}

func (h *Handler) handleUpdateUser(c *echo.Context, storeID, userID string, body []byte) error {
	var req updateUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.UpdateUser(storeID, userID, req.Operations); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteUser(c *echo.Context, storeID, userID string) error {
	if err := h.Backend.DeleteUser(storeID, userID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetUserID(c *echo.Context, storeID string, body []byte) error {
	var req getUserIDRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	attrPath, attrValue := extractAlternateIdentifier(req.AlternateIdentifier)
	if attrPath == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "AlternateIdentifier is required")
	}

	userID, err := h.Backend.GetUserID(storeID, attrPath, attrValue)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"UserId":          userID,
		"IdentityStoreId": storeID,
	})
}

// ----------------------------------------
// Group handlers
// ----------------------------------------

func (h *Handler) handleCreateGroup(c *echo.Context, storeID string, body []byte) error {
	var req createGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	group, err := h.Backend.CreateGroup(storeID, &CreateGroupRequest{
		DisplayName: req.DisplayName,
		Description: req.Description,
	})
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createGroupResponse{
		GroupID:         group.GroupID,
		IdentityStoreID: group.IdentityStoreID,
	})
}

func (h *Handler) handleDescribeGroup(c *echo.Context, storeID, groupID string) error {
	group, err := h.Backend.DescribeGroup(storeID, groupID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, group)
}

func (h *Handler) handleListGroups(c *echo.Context, storeID string) error {
	groups := h.Backend.ListGroups(storeID)

	return c.JSON(http.StatusOK, map[string]any{
		"Groups":    groups,
		"NextToken": nil,
	})
}

func (h *Handler) handleUpdateGroup(c *echo.Context, storeID, groupID string, body []byte) error {
	var req updateGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.UpdateGroup(storeID, groupID, req.Operations); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteGroup(c *echo.Context, storeID, groupID string) error {
	if err := h.Backend.DeleteGroup(storeID, groupID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetGroupID(c *echo.Context, storeID string, body []byte) error {
	var req getGroupIDRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	attrPath, attrValue := extractAlternateIdentifier(req.AlternateIdentifier)
	if attrPath == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "AlternateIdentifier is required")
	}

	groupID, err := h.Backend.GetGroupID(storeID, attrPath, attrValue)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"GroupId":         groupID,
		"IdentityStoreId": storeID,
	})
}

// ----------------------------------------
// Membership handlers
// ----------------------------------------

func (h *Handler) handleCreateGroupMembership(c *echo.Context, storeID string, body []byte) error {
	var req createGroupMembershipRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	membership, err := h.Backend.CreateGroupMembership(storeID, req.GroupID, req.MemberID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createGroupMembershipResponse{
		MembershipID:    membership.MembershipID,
		IdentityStoreID: membership.IdentityStoreID,
	})
}

func (h *Handler) handleDescribeGroupMembership(c *echo.Context, storeID, membershipID string) error {
	m, err := h.Backend.DescribeGroupMembership(storeID, membershipID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, m)
}

func (h *Handler) handleListGroupMemberships(c *echo.Context, storeID, groupID string) error {
	memberships := h.Backend.ListGroupMemberships(storeID, groupID)

	return c.JSON(http.StatusOK, map[string]any{
		"GroupMemberships": memberships,
		"NextToken":        nil,
	})
}

func (h *Handler) handleDeleteGroupMembership(c *echo.Context, storeID, membershipID string) error {
	if err := h.Backend.DeleteGroupMembership(storeID, membershipID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetGroupMembershipID(c *echo.Context, storeID string, body []byte) error {
	var req getGroupMembershipIDRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	membershipID, err := h.Backend.GetGroupMembershipID(storeID, req.GroupID, req.MemberID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"MembershipId":    membershipID,
		"IdentityStoreId": storeID,
	})
}

func (h *Handler) handleListGroupMembershipsForMember(c *echo.Context, storeID string, body []byte) error {
	var req listGroupMembershipsForMemberRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	memberships := h.Backend.ListGroupMembershipsForMember(storeID, req.MemberID)

	return c.JSON(http.StatusOK, map[string]any{
		"GroupMemberships": memberships,
		"NextToken":        nil,
	})
}

func (h *Handler) handleIsMemberInGroups(c *echo.Context, storeID string, body []byte) error {
	var req isMemberInGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	results := h.Backend.IsMemberInGroups(storeID, req.MemberID, req.GroupIDs)

	return c.JSON(http.StatusOK, isMemberInGroupsResponse{Results: results})
}

// ----------------------------------------
// Error handling helpers
// ----------------------------------------

func (h *Handler) handleBackendError(c *echo.Context, err error) error {
	code := http.StatusInternalServerError
	errType := "InternalFailure"

	switch {
	case errors.Is(err, ErrUserNotFound) || errors.Is(err, ErrGroupNotFound) || errors.Is(err, ErrMembershipNotFound):
		code = http.StatusNotFound
		errType = "ResourceNotFoundException"
	case errors.Is(err, ErrConflict):
		code = http.StatusConflict
		errType = "ConflictException"
	}

	return h.writeError(c, code, errType, err.Error())
}

func (h *Handler) writeError(c *echo.Context, statusCode int, errType, message string) error {
	return c.JSON(statusCode, map[string]string{
		"__type":  errType,
		"message": message,
	})
}

// ----------------------------------------
// Helpers
// ----------------------------------------

// extractAlternateIdentifier extracts the attribute path and value from an AlternateIdentifier.
func extractAlternateIdentifier(ai alternateIdentifier) (path, value string) {
	if ai.UniqueAttribute != nil {
		return ai.UniqueAttribute.AttributePath, ai.UniqueAttribute.AttributeValue
	}

	return "", ""
}
