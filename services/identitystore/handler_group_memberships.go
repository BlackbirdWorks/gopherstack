package identitystore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// maxIsMemberInGroupsIDs is the maximum number of GroupIds allowed per IsMemberInGroups request.
const maxIsMemberInGroupsIDs = 100

// ----------------------------------------
// Membership request/response types
// ----------------------------------------

type createGroupMembershipRequest struct {
	IdentityStoreID string   `json:"IdentityStoreId"`
	GroupID         string   `json:"GroupId"`
	MemberID        MemberID `json:"MemberId"`
}

type createGroupMembershipResponse struct {
	MembershipID    string `json:"MembershipId"`
	IdentityStoreID string `json:"IdentityStoreId"`
}

type getGroupMembershipIDRequest struct {
	IdentityStoreID string   `json:"IdentityStoreId"`
	GroupID         string   `json:"GroupId"`
	MemberID        MemberID `json:"MemberId"`
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
