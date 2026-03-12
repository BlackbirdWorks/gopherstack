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
	// targetPrefix is the X-Amz-Target prefix for the Identity Store JSON protocol.
	targetPrefix = "AWSIdentityStore."
	// isMemberInGroupsOp is the operation name for the IsMemberInGroups API call.
	isMemberInGroupsOp = "IsMemberInGroups"
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

// RouteMatcher returns a function that matches Identity Store JSON protocol requests.
// The SDK uses X-Amz-Target: AWSIdentityStore.{Operation} with POST to /.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
// Uses PriorityHeaderExact since matching is by X-Amz-Target header.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Identity Store operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource extracts the IdentityStoreId from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		IdentityStoreID string `json:"IdentityStoreId"`
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.IdentityStoreID
}

// Handler returns the Echo handler function for Identity Store requests.
// The Identity Store SDK uses the JSON 1.1 protocol: POST / with X-Amz-Target header.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		target := c.Request().Header.Get("X-Amz-Target")
		op := strings.TrimPrefix(target, targetPrefix)
		if op == "" || op == target {
			return h.writeError(c, http.StatusBadRequest, "UnrecognizedClientException", "missing X-Amz-Target header")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "identitystore: failed to read request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "identitystore request", "op", op)

		return h.dispatch(c, op, body)
	}
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
	AttributeValue any    `json:"AttributeValue"`
	AttributePath  string `json:"AttributePath"`
}

type updateUserRequest struct {
	IdentityStoreID string               `json:"IdentityStoreId"`
	UserID          string               `json:"UserId"`
	Operations      []attributeOperation `json:"Operations"`
}

type updateGroupRequest struct {
	IdentityStoreID string               `json:"IdentityStoreId"`
	GroupID         string               `json:"GroupId"`
	Operations      []attributeOperation `json:"Operations"`
}

type getUserIDRequest struct {
	AlternateIdentifier alternateIdentifier `json:"AlternateIdentifier"`
	IdentityStoreID     string              `json:"IdentityStoreId"`
}

type getGroupIDRequest struct {
	AlternateIdentifier alternateIdentifier `json:"AlternateIdentifier"`
	IdentityStoreID     string              `json:"IdentityStoreId"`
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
	Results []GroupMembershipExistence `json:"Results"`
}

type describeUserRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	UserID          string `json:"UserId"`
}

type deleteUserRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	UserID          string `json:"UserId"`
}

type listUsersRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
}

type describeGroupRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	GroupID         string `json:"GroupId"`
}

type deleteGroupRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	GroupID         string `json:"GroupId"`
}

type listGroupsRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
}

type describeGroupMembershipRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	MembershipID    string `json:"MembershipId"`
}

type deleteGroupMembershipRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	MembershipID    string `json:"MembershipId"`
}

type listGroupMembershipsRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	GroupID         string `json:"GroupId"`
}

// ----------------------------------------
// Dispatch
// ----------------------------------------

//nolint:cyclop // dispatch table has necessary branches for each operation
func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	switch op {
	// User operations
	case "CreateUser":
		return h.handleCreateUser(c, body)
	case "DescribeUser":
		return h.handleDescribeUser(c, body)
	case "ListUsers":
		return h.handleListUsers(c, body)
	case "UpdateUser":
		return h.handleUpdateUser(c, body)
	case "DeleteUser":
		return h.handleDeleteUser(c, body)
	case "GetUserId":
		return h.handleGetUserID(c, body)

	// Group operations
	case "CreateGroup":
		return h.handleCreateGroup(c, body)
	case "DescribeGroup":
		return h.handleDescribeGroup(c, body)
	case "ListGroups":
		return h.handleListGroups(c, body)
	case "UpdateGroup":
		return h.handleUpdateGroup(c, body)
	case "DeleteGroup":
		return h.handleDeleteGroup(c, body)
	case "GetGroupId":
		return h.handleGetGroupID(c, body)

	// Membership operations
	case "CreateGroupMembership":
		return h.handleCreateGroupMembership(c, body)
	case "DescribeGroupMembership":
		return h.handleDescribeGroupMembership(c, body)
	case "ListGroupMemberships":
		return h.handleListGroupMemberships(c, body)
	case "DeleteGroupMembership":
		return h.handleDeleteGroupMembership(c, body)
	case "GetGroupMembershipId":
		return h.handleGetGroupMembershipID(c, body)
	case "ListGroupMembershipsForMember":
		return h.handleListGroupMembershipsForMember(c, body)
	case isMemberInGroupsOp:
		return h.handleIsMemberInGroups(c, body)
	}

	return h.writeError(c, http.StatusBadRequest, "UnrecognizedClientException",
		"operation "+op+" is not supported")
}

// ----------------------------------------
// User handlers
// ----------------------------------------

func (h *Handler) handleCreateUser(c *echo.Context, body []byte) error {
	var req createUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	user, err := h.Backend.CreateUser(req.IdentityStoreID, &CreateUserRequest{
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

func (h *Handler) handleDescribeUser(c *echo.Context, body []byte) error {
	var req describeUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	user, err := h.Backend.DescribeUser(req.IdentityStoreID, req.UserID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, user)
}

func (h *Handler) handleListUsers(c *echo.Context, body []byte) error {
	var req listUsersRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	users := h.Backend.ListUsers(req.IdentityStoreID)

	return c.JSON(http.StatusOK, map[string]any{
		"Users":     users,
		"NextToken": nil,
	})
}

func (h *Handler) handleUpdateUser(c *echo.Context, body []byte) error {
	var req updateUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.UpdateUser(req.IdentityStoreID, req.UserID, req.Operations); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteUser(c *echo.Context, body []byte) error {
	var req deleteUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteUser(req.IdentityStoreID, req.UserID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetUserID(c *echo.Context, body []byte) error {
	var req getUserIDRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	attrPath, attrValue := extractAlternateIdentifier(req.AlternateIdentifier)
	if attrPath == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "AlternateIdentifier is required")
	}

	userID, err := h.Backend.GetUserID(req.IdentityStoreID, attrPath, attrValue)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"UserId":          userID,
		"IdentityStoreId": req.IdentityStoreID,
	})
}

// ----------------------------------------
// Group handlers
// ----------------------------------------

func (h *Handler) handleCreateGroup(c *echo.Context, body []byte) error {
	var req createGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	group, err := h.Backend.CreateGroup(req.IdentityStoreID, &CreateGroupRequest{
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

func (h *Handler) handleDescribeGroup(c *echo.Context, body []byte) error {
	var req describeGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	group, err := h.Backend.DescribeGroup(req.IdentityStoreID, req.GroupID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, group)
}

func (h *Handler) handleListGroups(c *echo.Context, body []byte) error {
	var req listGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	groups := h.Backend.ListGroups(req.IdentityStoreID)

	return c.JSON(http.StatusOK, map[string]any{
		"Groups":    groups,
		"NextToken": nil,
	})
}

func (h *Handler) handleUpdateGroup(c *echo.Context, body []byte) error {
	var req updateGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.UpdateGroup(req.IdentityStoreID, req.GroupID, req.Operations); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteGroup(c *echo.Context, body []byte) error {
	var req deleteGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteGroup(req.IdentityStoreID, req.GroupID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetGroupID(c *echo.Context, body []byte) error {
	var req getGroupIDRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	attrPath, attrValue := extractAlternateIdentifier(req.AlternateIdentifier)
	if attrPath == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "AlternateIdentifier is required")
	}

	groupID, err := h.Backend.GetGroupID(req.IdentityStoreID, attrPath, attrValue)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"GroupId":         groupID,
		"IdentityStoreId": req.IdentityStoreID,
	})
}

// ----------------------------------------
// Membership handlers
// ----------------------------------------

func (h *Handler) handleCreateGroupMembership(c *echo.Context, body []byte) error {
	var req createGroupMembershipRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.MemberID.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "MemberId.UserId is required")
	}

	membership, err := h.Backend.CreateGroupMembership(req.IdentityStoreID, req.GroupID, req.MemberID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createGroupMembershipResponse{
		MembershipID:    membership.MembershipID,
		IdentityStoreID: membership.IdentityStoreID,
	})
}

func (h *Handler) handleDescribeGroupMembership(c *echo.Context, body []byte) error {
	var req describeGroupMembershipRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	m, err := h.Backend.DescribeGroupMembership(req.IdentityStoreID, req.MembershipID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, m)
}

func (h *Handler) handleListGroupMemberships(c *echo.Context, body []byte) error {
	var req listGroupMembershipsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	memberships := h.Backend.ListGroupMemberships(req.IdentityStoreID, req.GroupID)

	return c.JSON(http.StatusOK, map[string]any{
		"GroupMemberships": memberships,
		"NextToken":        nil,
	})
}

func (h *Handler) handleDeleteGroupMembership(c *echo.Context, body []byte) error {
	var req deleteGroupMembershipRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteGroupMembership(req.IdentityStoreID, req.MembershipID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetGroupMembershipID(c *echo.Context, body []byte) error {
	var req getGroupMembershipIDRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	membershipID, err := h.Backend.GetGroupMembershipID(req.IdentityStoreID, req.GroupID, req.MemberID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"MembershipId":    membershipID,
		"IdentityStoreId": req.IdentityStoreID,
	})
}

func (h *Handler) handleListGroupMembershipsForMember(c *echo.Context, body []byte) error {
	var req listGroupMembershipsForMemberRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	memberships := h.Backend.ListGroupMembershipsForMember(req.IdentityStoreID, req.MemberID)

	return c.JSON(http.StatusOK, map[string]any{
		"GroupMemberships": memberships,
		"NextToken":        nil,
	})
}

func (h *Handler) handleIsMemberInGroups(c *echo.Context, body []byte) error {
	var req isMemberInGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	results := h.Backend.IsMemberInGroups(req.IdentityStoreID, req.MemberID, req.GroupIDs)

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
func extractAlternateIdentifier(ai alternateIdentifier) (string, string) {
	if ai.UniqueAttribute != nil {
		return ai.UniqueAttribute.AttributePath, ai.UniqueAttribute.AttributeValue
	}

	if ai.ExternalID != nil {
		return "ExternalId", ai.ExternalID.ID
	}

	return "", ""
}
