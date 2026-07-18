package identitystore

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// Group request/response types
// ----------------------------------------

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

type updateGroupRequest struct {
	IdentityStoreID string               `json:"IdentityStoreId"`
	GroupID         string               `json:"GroupId"`
	Operations      []attributeOperation `json:"Operations"`
}

type listGroupsRequest struct {
	IdentityStoreID string       `json:"IdentityStoreId"`
	NextToken       string       `json:"NextToken"`
	Filters         []ListFilter `json:"Filters"`
	MaxResults      int32        `json:"MaxResults"`
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
