package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createOrganizationalUnitRequest struct {
	ParentID string `json:"ParentId"`
	Name     string `json:"Name"`
	Tags     []Tag  `json:"Tags,omitempty"`
}

type ouObject struct {
	ID   string `json:"Id"`
	ARN  string `json:"Arn"`
	Name string `json:"Name"`
	Path string `json:"Path,omitempty"`
}

type createOrganizationalUnitResponse struct {
	OrganizationalUnit ouObject `json:"OrganizationalUnit"`
}

type describeOrganizationalUnitRequest struct {
	OrganizationalUnitID string `json:"OrganizationalUnitId"`
}

type describeOrganizationalUnitResponse struct {
	OrganizationalUnit ouObject `json:"OrganizationalUnit"`
}

type deleteOrganizationalUnitRequest struct {
	OrganizationalUnitID string `json:"OrganizationalUnitId"`
}

type updateOrganizationalUnitRequest struct {
	OrganizationalUnitID string `json:"OrganizationalUnitId"`
	Name                 string `json:"Name"`
}

type updateOrganizationalUnitResponse struct {
	OrganizationalUnit ouObject `json:"OrganizationalUnit"`
}

type listOrganizationalUnitsForParentRequest struct {
	ParentID   string `json:"ParentId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listOrganizationalUnitsForParentResponse struct {
	NextToken           string     `json:"NextToken,omitempty"`
	OrganizationalUnits []ouObject `json:"OrganizationalUnits"`
}

type listParentsRequest struct {
	ChildID string `json:"ChildId"`
}

type listParentsResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Parents   []ParentSummary `json:"Parents"`
}

type listChildrenRequest struct {
	ParentID   string `json:"ParentId"`
	ChildType  string `json:"ChildType"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listChildrenResponse struct {
	NextToken string         `json:"NextToken,omitempty"`
	Children  []ChildSummary `json:"Children"`
}

type listAccountsForParentRequest struct {
	ParentID   string `json:"ParentId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listAccountsForParentResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Accounts  []accountObject `json:"Accounts"`
}

// dispatchOU handles OU and hierarchy operations.
func (h *Handler) dispatchOU(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "CreateOrganizationalUnit":
		return true, h.handleCreateOrganizationalUnit(c, body)
	case "DescribeOrganizationalUnit":
		return true, h.handleDescribeOrganizationalUnit(c, body)
	case "DeleteOrganizationalUnit":
		return true, h.handleDeleteOrganizationalUnit(c, body)
	case "UpdateOrganizationalUnit":
		return true, h.handleUpdateOrganizationalUnit(c, body)
	case "ListOrganizationalUnitsForParent":
		return true, h.handleListOrganizationalUnitsForParent(c, body)
	case "ListAccountsForParent":
		return true, h.handleListAccountsForParent(c, body)
	case "ListParents":
		return true, h.handleListParents(c, body)
	case "ListChildren":
		return true, h.handleListChildren(c, body)
	}

	return false, nil
}

// ----------------------------------------
// OU handlers
// ----------------------------------------

func (h *Handler) handleCreateOrganizationalUnit(c *echo.Context, body []byte) error {
	var req createOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	ou, err := h.Backend.CreateOrganizationalUnit(req.ParentID, req.Name, req.Tags)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createOrganizationalUnitResponse{OrganizationalUnit: toOUObject(ou)})
}

func (h *Handler) handleDescribeOrganizationalUnit(c *echo.Context, body []byte) error {
	var req describeOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	ou, err := h.Backend.DescribeOrganizationalUnit(req.OrganizationalUnitID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeOrganizationalUnitResponse{OrganizationalUnit: toOUObject(ou)})
}

func (h *Handler) handleDeleteOrganizationalUnit(c *echo.Context, body []byte) error {
	var req deleteOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DeleteOrganizationalUnit(req.OrganizationalUnitID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUpdateOrganizationalUnit(c *echo.Context, body []byte) error {
	var req updateOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	ou, err := h.Backend.UpdateOrganizationalUnit(req.OrganizationalUnitID, req.Name)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateOrganizationalUnitResponse{OrganizationalUnit: toOUObject(ou)})
}

func (h *Handler) handleListOrganizationalUnitsForParent(c *echo.Context, body []byte) error {
	var req listOrganizationalUnitsForParentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	ous, err := h.Backend.ListOrganizationalUnitsForParent(req.ParentID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]ouObject, 0, len(ous))
	for _, ou := range ous {
		objs = append(objs, toOUObject(ou))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(
		http.StatusOK,
		listOrganizationalUnitsForParentResponse{OrganizationalUnits: p.Data, NextToken: p.Next},
	)
}

func (h *Handler) handleListAccountsForParent(c *echo.Context, body []byte) error {
	var req listAccountsForParentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	accounts, err := h.Backend.ListAccountsForParent(req.ParentID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]accountObject, 0, len(accounts))
	for _, a := range accounts {
		objs = append(objs, toAccountObject(a))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listAccountsForParentResponse{Accounts: p.Data, NextToken: p.Next})
}

func (h *Handler) handleListParents(c *echo.Context, body []byte) error {
	var req listParentsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	parents, err := h.Backend.ListParents(req.ChildID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listParentsResponse{Parents: parents})
}

func (h *Handler) handleListChildren(c *echo.Context, body []byte) error {
	var req listChildrenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	children, err := h.Backend.ListChildren(req.ParentID, req.ChildType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	p := page.New(children, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listChildrenResponse{Children: p.Data, NextToken: p.Next})
}

func toOUObject(ou *OrganizationalUnit) ouObject {
	return ouObject{
		ID:   ou.ID,
		ARN:  ou.ARN,
		Name: ou.Name,
		Path: ou.Path,
	}
}
