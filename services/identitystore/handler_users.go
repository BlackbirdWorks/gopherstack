package identitystore

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// User request/response types
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

type updateUserRequest struct {
	IdentityStoreID string               `json:"IdentityStoreId"`
	UserID          string               `json:"UserId"`
	Operations      []attributeOperation `json:"Operations"`
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

	if err := validateOperations(req.Operations); err != nil {
		return h.writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
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
