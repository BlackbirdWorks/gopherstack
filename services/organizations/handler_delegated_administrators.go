package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type registerDelegatedAdministratorRequest struct {
	AccountID        string `json:"AccountId"`
	ServicePrincipal string `json:"ServicePrincipal"`
}

type deregisterDelegatedAdministratorRequest struct {
	AccountID        string `json:"AccountId"`
	ServicePrincipal string `json:"ServicePrincipal"`
}

type listDelegatedAdministratorsRequest struct {
	ServicePrincipal string `json:"ServicePrincipal,omitempty"`
	NextToken        string `json:"NextToken,omitempty"`
	MaxResults       int    `json:"MaxResults,omitempty"`
}

type delegatedAdminObject struct {
	ID             string  `json:"Id"`
	ARN            string  `json:"Arn"`
	Name           string  `json:"Name"`
	Email          string  `json:"Email"`
	Status         string  `json:"Status"`
	JoinedMethod   string  `json:"JoinedMethod"`
	JoinedAt       float64 `json:"JoinedTimestamp"`
	DelegationTime float64 `json:"DelegationEnabledDate"`
}

type listDelegatedAdministratorsResponse struct {
	NextToken               string                 `json:"NextToken,omitempty"`
	DelegatedAdministrators []delegatedAdminObject `json:"DelegatedAdministrators"`
}

// -- ListDelegatedServicesForAccount --

type listDelegatedServicesForAccountRequest struct {
	AccountID  string `json:"AccountId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type delegatedServiceObject struct {
	ServicePrincipal      string  `json:"ServicePrincipal"`
	DelegationEnabledDate float64 `json:"DelegationEnabledDate"`
}

type listDelegatedServicesForAccountResponse struct {
	NextToken         string                   `json:"NextToken,omitempty"`
	DelegatedServices []delegatedServiceObject `json:"DelegatedServices"`
}

// dispatchDelegatedAdmin handles delegated admin operations.
func (h *Handler) dispatchDelegatedAdmin(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "RegisterDelegatedAdministrator":
		return true, h.handleRegisterDelegatedAdministrator(c, body)
	case "DeregisterDelegatedAdministrator":
		return true, h.handleDeregisterDelegatedAdministrator(c, body)
	case "ListDelegatedAdministrators":
		return true, h.handleListDelegatedAdministrators(c, body)
	case "ListDelegatedServicesForAccount":
		return true, h.handleListDelegatedServicesForAccount(c, body)
	}

	return false, nil
}

// ----------------------------------------
// Delegated admin handlers
// ----------------------------------------

func (h *Handler) handleRegisterDelegatedAdministrator(c *echo.Context, body []byte) error {
	var req registerDelegatedAdministratorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.RegisterDelegatedAdministrator(req.AccountID, req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDeregisterDelegatedAdministrator(c *echo.Context, body []byte) error {
	var req deregisterDelegatedAdministratorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DeregisterDelegatedAdministrator(req.AccountID, req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListDelegatedAdministrators(c *echo.Context, body []byte) error {
	var req listDelegatedAdministratorsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	admins, err := h.Backend.ListDelegatedAdministrators(req.ServicePrincipal)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]delegatedAdminObject, 0, len(admins))
	for _, da := range admins {
		objs = append(objs, delegatedAdminObject{
			ID:             da.AccountID,
			ARN:            da.ARN,
			Name:           da.Name,
			Email:          da.Email,
			Status:         da.Status,
			JoinedMethod:   da.JoinedMethod,
			JoinedAt:       epochSeconds(da.JoinedAt),
			DelegationTime: epochSeconds(da.DelegationTime),
		})
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listDelegatedAdministratorsResponse{
		DelegatedAdministrators: p.Data,
		NextToken:               p.Next,
	})
}

func (h *Handler) handleListDelegatedServicesForAccount(c *echo.Context, body []byte) error {
	var req listDelegatedServicesForAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.AccountID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "AccountId is required")
	}

	services, err := h.Backend.ListDelegatedServicesForAccount(req.AccountID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]delegatedServiceObject, 0, len(services))
	for _, svc := range services {
		objs = append(objs, delegatedServiceObject{
			ServicePrincipal:      svc.ServicePrincipal,
			DelegationEnabledDate: epochSeconds(svc.DelegationEnabledDate),
		})
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listDelegatedServicesForAccountResponse{DelegatedServices: p.Data, NextToken: p.Next})
}
