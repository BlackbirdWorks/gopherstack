package identitystore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyNextToken       = "NextToken"
	keyIdentityStoreID = "IdentityStoreId"
)

const (
	// targetPrefix is the X-Amz-Target prefix for the Identity Store JSON protocol.
	targetPrefix = "AWSIdentityStore."
	// isMemberInGroupsOp is the operation name for the IsMemberInGroups API call.
	isMemberInGroupsOp = "IsMemberInGroups"
)

// Operation name constants for Identity Store.
const (
	opCreateUser                    = "CreateUser"
	opDescribeUser                  = "DescribeUser"
	opListUsers                     = "ListUsers"
	opUpdateUser                    = "UpdateUser"
	opDeleteUser                    = "DeleteUser"
	opGetUserID                     = "GetUserId"
	opCreateGroup                   = "CreateGroup"
	opDescribeGroup                 = "DescribeGroup"
	opListGroups                    = "ListGroups"
	opUpdateGroup                   = "UpdateGroup"
	opDeleteGroup                   = "DeleteGroup"
	opGetGroupID                    = "GetGroupId"
	opCreateGroupMembership         = "CreateGroupMembership"
	opDescribeGroupMembership       = "DescribeGroupMembership"
	opListGroupMemberships          = "ListGroupMemberships"
	opDeleteGroupMembership         = "DeleteGroupMembership"
	opListGroupMembershipsForMember = "ListGroupMembershipsForMember"
	opGetGroupMembershipID          = "GetGroupMembershipId"
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
		opCreateUser,
		opDescribeUser,
		opListUsers,
		opUpdateUser,
		opDeleteUser,
		opGetUserID,
		opCreateGroup,
		opDescribeGroup,
		opListGroups,
		opUpdateGroup,
		opDeleteGroup,
		opGetGroupID,
		opCreateGroupMembership,
		opDescribeGroupMembership,
		opListGroupMemberships,
		opDeleteGroupMembership,
		opGetGroupMembershipID,
		opListGroupMembershipsForMember,
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

		// Resolve per-request region from SigV4 credential scope or X-Amz-Region,
		// then attach it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx = context.WithValue(ctx, regionContextKey{}, region)

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

		return h.dispatch(ctx, c, op, body)
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
	Birthdate       string        `json:"Birthdate"`
	Website         string        `json:"Website"`
	Name            *Name         `json:"Name"`
	Emails          []Email       `json:"Emails"`
	Addresses       []Address     `json:"Addresses"`
	PhoneNumbers    []PhoneNumber `json:"PhoneNumbers"`
	Photos          []Photo       `json:"Photos"`
	Roles           []Role        `json:"Roles"`
	ExternalIDs     []ExternalID  `json:"ExternalIds"`
}

type createUserResponse struct {
	UserID          string `json:"UserId"`
	IdentityStoreID string `json:"IdentityStoreId"`
}

type createGroupRequest struct {
	IdentityStoreID string       `json:"IdentityStoreId"`
	DisplayName     string       `json:"DisplayName"`
	Description     string       `json:"Description"`
	ExternalIDs     []ExternalID `json:"ExternalIds"`
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
	NextToken       string   `json:"NextToken"`
	MaxResults      int32    `json:"MaxResults"`
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
	IdentityStoreID string       `json:"IdentityStoreId"`
	NextToken       string       `json:"NextToken"`
	Filters         []ListFilter `json:"Filters"`
	MaxResults      int32        `json:"MaxResults"`
}

type listGroupsRequest struct {
	IdentityStoreID string       `json:"IdentityStoreId"`
	NextToken       string       `json:"NextToken"`
	Filters         []ListFilter `json:"Filters"`
	MaxResults      int32        `json:"MaxResults"`
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
	NextToken       string `json:"NextToken"`
	MaxResults      int32  `json:"MaxResults"`
}

type describeGroupRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	GroupID         string `json:"GroupId"`
}

type deleteGroupRequest struct {
	IdentityStoreID string `json:"IdentityStoreId"`
	GroupID         string `json:"GroupId"`
}

// ----------------------------------------
// Dispatch
// ----------------------------------------

// identityStoreDispatch maps operation names to their handler functions.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var identityStoreDispatch = map[string]func(*Handler, context.Context, *echo.Context, []byte) error{
	// User operations
	opCreateUser:   (*Handler).handleCreateUser,
	opDescribeUser: (*Handler).handleDescribeUser,
	opListUsers:    (*Handler).handleListUsers,
	opUpdateUser:   (*Handler).handleUpdateUser,
	opDeleteUser:   (*Handler).handleDeleteUser,
	opGetUserID:    (*Handler).handleGetUserID,
	// Group operations
	opCreateGroup:   (*Handler).handleCreateGroup,
	opDescribeGroup: (*Handler).handleDescribeGroup,
	opListGroups:    (*Handler).handleListGroups,
	opUpdateGroup:   (*Handler).handleUpdateGroup,
	opDeleteGroup:   (*Handler).handleDeleteGroup,
	opGetGroupID:    (*Handler).handleGetGroupID,
	// Membership operations
	opCreateGroupMembership:         (*Handler).handleCreateGroupMembership,
	opDescribeGroupMembership:       (*Handler).handleDescribeGroupMembership,
	opListGroupMemberships:          (*Handler).handleListGroupMemberships,
	opDeleteGroupMembership:         (*Handler).handleDeleteGroupMembership,
	opGetGroupMembershipID:          (*Handler).handleGetGroupMembershipID,
	opListGroupMembershipsForMember: (*Handler).handleListGroupMembershipsForMember,
	isMemberInGroupsOp:              (*Handler).handleIsMemberInGroups,
}

func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op string, body []byte) error {
	if fn, ok := identityStoreDispatch[op]; ok {
		return fn(h, ctx, c, body)
	}

	return h.writeError(c, http.StatusBadRequest, "UnrecognizedClientException",
		"operation "+op+" is not supported")
}

// ----------------------------------------
// User handlers
// ----------------------------------------

func (h *Handler) handleCreateUser(ctx context.Context, c *echo.Context, body []byte) error {
	var req createUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	user, err := h.Backend.CreateUser(ctx, req.IdentityStoreID, &CreateUserRequest{
		UserName:      req.UserName,
		DisplayName:   req.DisplayName,
		NickName:      req.NickName,
		Title:         req.Title,
		ProfileURL:    req.ProfileURL,
		Locale:        req.Locale,
		PreferredLang: req.PreferredLang,
		Timezone:      req.Timezone,
		UserType:      req.UserType,
		Birthdate:     req.Birthdate,
		Website:       req.Website,
		Name:          req.Name,
		Emails:        req.Emails,
		Addresses:     req.Addresses,
		PhoneNumbers:  req.PhoneNumbers,
		Photos:        req.Photos,
		Roles:         req.Roles,
		ExternalIDs:   req.ExternalIDs,
	})
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createUserResponse{
		UserID:          user.UserID,
		IdentityStoreID: user.IdentityStoreID,
	})
}

func (h *Handler) handleDescribeUser(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "UserId is required")
	}

	user, err := h.Backend.DescribeUser(ctx, req.IdentityStoreID, req.UserID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, user)
}

// errMaxResultsOutOfRange is returned when a list MaxResults value falls
// outside the AWS-permitted 1-100 range.
var errMaxResultsOutOfRange = fmt.Errorf("MaxResults must be between 1 and %d", maxListPageSize)

// validateMaxResults enforces the AWS Identity Store list MaxResults bound.
// MaxResults is optional (0 = unset); when supplied it must be 1-100.
func validateMaxResults(maxResults int32) error {
	if maxResults == 0 {
		return nil
	}

	if maxResults < 1 || maxResults > maxListPageSize {
		return errMaxResultsOutOfRange
	}

	return nil
}

//nolint:dupl // structurally parallel to handleListGroups; both validate MaxResults, filter, paginate
func (h *Handler) handleListUsers(ctx context.Context, c *echo.Context, body []byte) error {
	var req listUsersRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if err := validateMaxResults(req.MaxResults); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	all := h.Backend.ListUsers(ctx, req.IdentityStoreID)
	filtered := applyUserFilters(all, req.Filters)
	page, nextToken := paginateSlice(filtered, req.MaxResults, req.NextToken)

	return c.JSON(http.StatusOK, map[string]any{
		"Users":      page,
		keyNextToken: nextToken,
	})
}

func (h *Handler) handleUpdateUser(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "UserId is required")
	}

	if err := h.Backend.UpdateUser(ctx, req.IdentityStoreID, req.UserID, req.Operations); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteUser(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "UserId is required")
	}

	if err := h.Backend.DeleteUser(ctx, req.IdentityStoreID, req.UserID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// alternateIDResult holds the parsed fields from an alternate-identifier request.
type alternateIDResult struct {
	storeID   string
	attrPath  string
	attrValue string
}

// parseAlternateIDRequest decodes a request body that contains IdentityStoreId and
// AlternateIdentifier, validates both are present, and returns the parsed values.
func (h *Handler) parseAlternateIDRequest(c *echo.Context, body []byte) (alternateIDResult, error) {
	var req struct {
		AlternateIdentifier alternateIdentifier `json:"AlternateIdentifier"`
		IdentityStoreID     string              `json:"IdentityStoreId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return alternateIDResult{}, h.writeError(
			c, http.StatusBadRequest, "ValidationException", "invalid request body",
		)
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return alternateIDResult{}, h.writeError(
			c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required",
		)
	}

	attrPath, attrValue := extractAlternateIdentifier(req.AlternateIdentifier)
	if attrPath == "" {
		return alternateIDResult{}, h.writeError(
			c, http.StatusBadRequest, "ValidationException", "AlternateIdentifier is required",
		)
	}

	return alternateIDResult{storeID: req.IdentityStoreID, attrPath: attrPath, attrValue: attrValue}, nil
}

func (h *Handler) handleGetUserID(ctx context.Context, c *echo.Context, body []byte) error {
	parsed, err := h.parseAlternateIDRequest(c, body)
	if err != nil {
		return err
	}

	userID, backendErr := h.Backend.GetUserID(ctx, parsed.storeID, parsed.attrPath, parsed.attrValue)
	if backendErr != nil {
		return h.handleBackendError(c, backendErr)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"UserId":           userID,
		keyIdentityStoreID: parsed.storeID,
	})
}

// ----------------------------------------
// Group handlers
// ----------------------------------------

func (h *Handler) handleCreateGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req createGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	// DisplayName is NOT required by the real CreateGroup API (only
	// IdentityStoreId is in the "required" list of the smithy model) -- see
	// api-2.json's CreateGroupRequest. A group may be created with no
	// DisplayName at all.
	group, err := h.Backend.CreateGroup(ctx, req.IdentityStoreID, &CreateGroupRequest{
		DisplayName: req.DisplayName,
		Description: req.Description,
		ExternalIDs: req.ExternalIDs,
	})
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createGroupResponse{
		GroupID:         group.GroupID,
		IdentityStoreID: group.IdentityStoreID,
	})
}

func (h *Handler) handleDescribeGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.GroupID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "GroupId is required")
	}

	group, err := h.Backend.DescribeGroup(ctx, req.IdentityStoreID, req.GroupID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, group)
}

//nolint:dupl // structurally parallel to handleListUsers; both validate MaxResults, filter, paginate
func (h *Handler) handleListGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req listGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if err := validateMaxResults(req.MaxResults); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	all := h.Backend.ListGroups(ctx, req.IdentityStoreID)
	filtered := applyGroupFilters(all, req.Filters)
	page, nextToken := paginateSlice(filtered, req.MaxResults, req.NextToken)

	return c.JSON(http.StatusOK, map[string]any{
		"Groups":     page,
		keyNextToken: nextToken,
	})
}

func (h *Handler) handleUpdateGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.GroupID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "GroupId is required")
	}

	if err := h.Backend.UpdateGroup(ctx, req.IdentityStoreID, req.GroupID, req.Operations); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.GroupID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "GroupId is required")
	}

	if err := h.Backend.DeleteGroup(ctx, req.IdentityStoreID, req.GroupID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetGroupID(ctx context.Context, c *echo.Context, body []byte) error {
	parsed, err := h.parseAlternateIDRequest(c, body)
	if err != nil {
		return err
	}

	groupID, backendErr := h.Backend.GetGroupID(ctx, parsed.storeID, parsed.attrPath, parsed.attrValue)
	if backendErr != nil {
		return h.handleBackendError(c, backendErr)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"GroupId":          groupID,
		keyIdentityStoreID: parsed.storeID,
	})
}

// ----------------------------------------
// Membership handlers
// ----------------------------------------

func (h *Handler) handleCreateGroupMembership(ctx context.Context, c *echo.Context, body []byte) error {
	var req createGroupMembershipRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.GroupID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "GroupId is required")
	}

	if strings.TrimSpace(req.MemberID.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "MemberId.UserId is required")
	}

	membership, err := h.Backend.CreateGroupMembership(ctx, req.IdentityStoreID, req.GroupID, req.MemberID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createGroupMembershipResponse{
		MembershipID:    membership.MembershipID,
		IdentityStoreID: membership.IdentityStoreID,
	})
}

func (h *Handler) handleDescribeGroupMembership(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeGroupMembershipRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.MembershipID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "MembershipId is required")
	}

	m, err := h.Backend.DescribeGroupMembership(ctx, req.IdentityStoreID, req.MembershipID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, m)
}

func (h *Handler) handleListGroupMemberships(ctx context.Context, c *echo.Context, body []byte) error {
	var req listGroupMembershipsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.GroupID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "GroupId is required")
	}

	if err := validateMaxResults(req.MaxResults); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	all := h.Backend.ListGroupMemberships(ctx, req.IdentityStoreID, req.GroupID)
	page, nextToken := paginateSlice(all, req.MaxResults, req.NextToken)

	return c.JSON(http.StatusOK, map[string]any{
		"GroupMemberships": page,
		keyNextToken:       nextToken,
	})
}

func (h *Handler) handleDeleteGroupMembership(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteGroupMembershipRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.MembershipID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "MembershipId is required")
	}

	if err := h.Backend.DeleteGroupMembership(ctx, req.IdentityStoreID, req.MembershipID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetGroupMembershipID(ctx context.Context, c *echo.Context, body []byte) error {
	var req getGroupMembershipIDRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.GroupID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "GroupId is required")
	}

	if strings.TrimSpace(req.MemberID.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "MemberId.UserId is required")
	}

	membershipID, err := h.Backend.GetGroupMembershipID(ctx, req.IdentityStoreID, req.GroupID, req.MemberID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"MembershipId":     membershipID,
		keyIdentityStoreID: req.IdentityStoreID,
	})
}

func (h *Handler) handleListGroupMembershipsForMember(ctx context.Context, c *echo.Context, body []byte) error {
	var req listGroupMembershipsForMemberRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.MemberID.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "MemberId.UserId is required")
	}

	if err := validateMaxResults(req.MaxResults); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	all := h.Backend.ListGroupMembershipsForMember(ctx, req.IdentityStoreID, req.MemberID)
	page, nextToken := paginateSlice(all, req.MaxResults, req.NextToken)

	return c.JSON(http.StatusOK, map[string]any{
		"GroupMemberships": page,
		keyNextToken:       nextToken,
	})
}

func (h *Handler) handleIsMemberInGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req isMemberInGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if strings.TrimSpace(req.IdentityStoreID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "IdentityStoreId is required")
	}

	if strings.TrimSpace(req.MemberID.UserID) == "" {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "MemberId.UserId is required")
	}

	if len(req.GroupIDs) == 0 {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", "GroupIds must not be empty")
	}

	if len(req.GroupIDs) > maxIsMemberInGroupsIDs {
		return h.writeError(c, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("GroupIds must not exceed %d items", maxIsMemberInGroupsIDs))
	}

	results := h.Backend.IsMemberInGroups(ctx, req.IdentityStoreID, req.MemberID, req.GroupIDs)

	return c.JSON(http.StatusOK, isMemberInGroupsResponse{Results: results})
}

// ----------------------------------------
// Error handling helpers
// ----------------------------------------

func (h *Handler) handleBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrUserNotFound):

		return h.writeResourceError(c, "ResourceNotFoundException", err.Error(), "USER")
	case errors.Is(err, ErrGroupNotFound):

		return h.writeResourceError(c, "ResourceNotFoundException", err.Error(), "GROUP")
	case errors.Is(err, ErrMembershipNotFound):

		return h.writeResourceError(c, "ResourceNotFoundException", err.Error(), "GROUP_MEMBERSHIP")
	case errors.Is(err, ErrConflict):

		return h.writeError(c, http.StatusConflict, "ConflictException", err.Error())
	case errors.Is(err, ErrValidation):

		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}

// writeResourceError writes a ResourceNotFoundException with a ResourceType field for
// AWS-compatible error responses.
func (h *Handler) writeResourceError(c *echo.Context, errType, message, resourceType string) error {
	return c.JSON(http.StatusNotFound, map[string]string{
		"__type":       errType,
		"message":      message,
		"ResourceType": resourceType,
	})
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

// externalIDSep is the separator used to encode an ExternalId compound key as a single string.
// Null byte is used because it cannot appear in valid Issuer or ID values (both are typically URLs or UUIDs).
const externalIDSep = "\x00"

// extractAlternateIdentifier extracts the attribute path and value from an AlternateIdentifier.
// For ExternalId, both Issuer and Id are encoded as a compound value separated by externalIDSep.
func extractAlternateIdentifier(ai alternateIdentifier) (string, string) {
	if ai.UniqueAttribute != nil {
		return ai.UniqueAttribute.AttributePath, ai.UniqueAttribute.AttributeValue
	}

	if ai.ExternalID != nil {
		return "ExternalId", ai.ExternalID.Issuer + externalIDSep + ai.ExternalID.ID
	}

	return "", ""
}
