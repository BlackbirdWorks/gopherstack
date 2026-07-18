package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type attachPolicyRequest struct {
	PolicyID string `json:"PolicyId"`
	TargetID string `json:"TargetId"`
}

type detachPolicyRequest struct {
	PolicyID string `json:"PolicyId"`
	TargetID string `json:"TargetId"`
}

type listPoliciesForTargetRequest struct {
	TargetID   string `json:"TargetId"`
	Filter     string `json:"Filter"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listPoliciesForTargetResponse struct {
	NextToken string                `json:"NextToken,omitempty"`
	Policies  []policySummaryObject `json:"Policies"`
}

type listTargetsForPolicyRequest struct {
	PolicyID   string `json:"PolicyId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type policyTargetObject struct {
	TargetID string `json:"TargetId"`
	ARN      string `json:"Arn"`
	Name     string `json:"Name"`
	Type     string `json:"Type"`
}

type listTargetsForPolicyResponse struct {
	NextToken string               `json:"NextToken,omitempty"`
	Targets   []policyTargetObject `json:"Targets"`
}

// dispatchPolicyAttachments handles policy attachment operations.
func (h *Handler) dispatchPolicyAttachments(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "AttachPolicy":
		return true, h.handleAttachPolicy(c, body)
	case "DetachPolicy":
		return true, h.handleDetachPolicy(c, body)
	case "ListPoliciesForTarget":
		return true, h.handleListPoliciesForTarget(c, body)
	case "ListTargetsForPolicy":
		return true, h.handleListTargetsForPolicy(c, body)
	}

	return false, nil
}

func (h *Handler) handleAttachPolicy(c *echo.Context, body []byte) error {
	var req attachPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.AttachPolicy(req.PolicyID, req.TargetID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDetachPolicy(c *echo.Context, body []byte) error {
	var req detachPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DetachPolicy(req.PolicyID, req.TargetID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListPoliciesForTarget(c *echo.Context, body []byte) error {
	var req listPoliciesForTargetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	policies, err := h.Backend.ListPoliciesForTarget(req.TargetID, req.Filter)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]policySummaryObject, 0, len(policies))
	for _, p := range policies {
		objs = append(objs, toPolicySummaryObject(p))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listPoliciesForTargetResponse{Policies: p.Data, NextToken: p.Next})
}

func (h *Handler) handleListTargetsForPolicy(c *echo.Context, body []byte) error {
	var req listTargetsForPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	targets, err := h.Backend.ListTargetsForPolicy(req.PolicyID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]policyTargetObject, 0, len(targets))
	for _, t := range targets {
		objs = append(objs, policyTargetObject(t))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listTargetsForPolicyResponse{Targets: p.Data, NextToken: p.Next})
}
