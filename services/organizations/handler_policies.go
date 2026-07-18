package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createPolicyRequest struct {
	Name        string `json:"Name"`
	Description string `json:"Description"`
	Content     string `json:"Content"`
	Type        string `json:"Type"`
	Tags        []Tag  `json:"Tags,omitempty"`
}

type policySummaryObject struct {
	ID          string `json:"Id"`
	ARN         string `json:"Arn"`
	Name        string `json:"Name"`
	Description string `json:"Description"`
	Type        string `json:"Type"`
	AwsManaged  bool   `json:"AwsManaged"`
}

type policyObject struct {
	Content       string              `json:"Content"`
	PolicySummary policySummaryObject `json:"PolicySummary"`
}

type createPolicyResponse struct {
	Policy policyObject `json:"Policy"`
}

type describePolicyRequest struct {
	PolicyID string `json:"PolicyId"`
}

type describePolicyResponse struct {
	Policy policyObject `json:"Policy"`
}

type updatePolicyRequest struct {
	PolicyID    string `json:"PolicyId"`
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	Content     string `json:"Content,omitempty"`
}

type updatePolicyResponse struct {
	Policy policyObject `json:"Policy"`
}

type deletePolicyRequest struct {
	PolicyID string `json:"PolicyId"`
}

type listPoliciesRequest struct {
	Filter     string `json:"Filter"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listPoliciesResponse struct {
	NextToken string                `json:"NextToken,omitempty"`
	Policies  []policySummaryObject `json:"Policies"`
}

type enablePolicyTypeRequest struct {
	RootID     string `json:"RootId"`
	PolicyType string `json:"PolicyType"`
}

type enablePolicyTypeResponse struct {
	Root rootObject `json:"Root"`
}

type disablePolicyTypeRequest struct {
	RootID     string `json:"RootId"`
	PolicyType string `json:"PolicyType"`
}

type disablePolicyTypeResponse struct {
	Root rootObject `json:"Root"`
}

// dispatchPolicy handles policy operations.
func (h *Handler) dispatchPolicy(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "CreatePolicy":
		return true, h.handleCreatePolicy(c, body)
	case "DescribePolicy":
		return true, h.handleDescribePolicy(c, body)
	case "UpdatePolicy":
		return true, h.handleUpdatePolicy(c, body)
	case "DeletePolicy":
		return true, h.handleDeletePolicy(c, body)
	case "ListPolicies":
		return true, h.handleListPolicies(c, body)
	case "EnablePolicyType":
		return true, h.handleEnablePolicyType(c, body)
	case "DisablePolicyType":
		return true, h.handleDisablePolicyType(c, body)
	}

	return false, nil
}

// ----------------------------------------
// Policy handlers
// ----------------------------------------

func (h *Handler) handleCreatePolicy(c *echo.Context, body []byte) error {
	var req createPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	p, err := h.Backend.CreatePolicy(req.Name, req.Description, req.Content, req.Type, req.Tags)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createPolicyResponse{Policy: toPolicyObject(p)})
}

func (h *Handler) handleDescribePolicy(c *echo.Context, body []byte) error {
	var req describePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	p, err := h.Backend.DescribePolicy(req.PolicyID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describePolicyResponse{Policy: toPolicyObject(p)})
}

func (h *Handler) handleUpdatePolicy(c *echo.Context, body []byte) error {
	var req updatePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	p, err := h.Backend.UpdatePolicy(req.PolicyID, req.Name, req.Description, req.Content)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updatePolicyResponse{Policy: toPolicyObject(p)})
}

func (h *Handler) handleDeletePolicy(c *echo.Context, body []byte) error {
	var req deletePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DeletePolicy(req.PolicyID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListPolicies(c *echo.Context, body []byte) error {
	var req listPoliciesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	policies, err := h.Backend.ListPolicies(req.Filter)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]policySummaryObject, 0, len(policies))
	for _, p := range policies {
		objs = append(objs, toPolicySummaryObject(p))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listPoliciesResponse{Policies: p.Data, NextToken: p.Next})
}

func (h *Handler) handleEnablePolicyType(c *echo.Context, body []byte) error {
	var req enablePolicyTypeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	root, err := h.Backend.EnablePolicyType(req.RootID, req.PolicyType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, enablePolicyTypeResponse{Root: toRootObject(root)})
}

func (h *Handler) handleDisablePolicyType(c *echo.Context, body []byte) error {
	var req disablePolicyTypeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	root, err := h.Backend.DisablePolicyType(req.RootID, req.PolicyType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, disablePolicyTypeResponse{Root: toRootObject(root)})
}

func toPolicyObject(p *Policy) policyObject {
	return policyObject{
		PolicySummary: toPolicySummaryObject(p),
		Content:       p.Content,
	}
}

func toPolicySummaryObject(p *Policy) policySummaryObject {
	return policySummaryObject{
		ID:          p.PolicySummary.ID,
		ARN:         p.PolicySummary.ARN,
		Name:        p.PolicySummary.Name,
		Description: p.PolicySummary.Description,
		Type:        p.PolicySummary.Type,
		AwsManaged:  p.PolicySummary.AwsManaged,
	}
}
