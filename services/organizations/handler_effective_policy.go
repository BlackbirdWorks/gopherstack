package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type describeEffectivePolicyRequest struct {
	PolicyType string `json:"PolicyType"`
	TargetID   string `json:"TargetId,omitempty"`
}

type effectivePolicyObject struct {
	PolicyContent        string  `json:"PolicyContent"`
	PolicyID             string  `json:"PolicyId"`
	PolicyType           string  `json:"PolicyType"`
	TargetID             string  `json:"TargetId"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp"`
}

type describeEffectivePolicyResponse struct {
	EffectivePolicy effectivePolicyObject `json:"EffectivePolicy"`
}

// -- ListEffectivePolicyValidationErrors --

type listEffectivePolicyValidationErrorsRequest struct {
	PolicyType string `json:"PolicyType"`
	TargetID   string `json:"TargetId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listEffectivePolicyValidationErrorsResponse struct {
	NextToken        string `json:"NextToken,omitempty"`
	ValidationErrors []any  `json:"ValidationErrors"`
}

// dispatchEffectivePolicy handles effective-policy operations.
func (h *Handler) dispatchEffectivePolicy(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "DescribeEffectivePolicy":
		return true, h.handleDescribeEffectivePolicy(c, body)
	case "ListEffectivePolicyValidationErrors":
		return true, h.handleListEffectivePolicyValidationErrors(c, body)
	}

	return false, nil
}

// ----------------------------------------
// EffectivePolicy handlers
// ----------------------------------------

func (h *Handler) handleDescribeEffectivePolicy(c *echo.Context, body []byte) error {
	var req describeEffectivePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.PolicyType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "PolicyType is required")
	}

	ep, err := h.Backend.DescribeEffectivePolicy(req.PolicyType, req.TargetID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeEffectivePolicyResponse{
		EffectivePolicy: effectivePolicyObject{
			LastUpdatedTimestamp: epochSeconds(ep.LastUpdatedTimestamp),
			PolicyContent:        ep.PolicyContent,
			PolicyID:             ep.PolicyID,
			PolicyType:           ep.PolicyType,
			TargetID:             ep.TargetID,
		},
	})
}

func (h *Handler) handleListEffectivePolicyValidationErrors(c *echo.Context, body []byte) error {
	var req listEffectivePolicyValidationErrorsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.PolicyType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "PolicyType is required")
	}

	errs, err := h.Backend.ListEffectivePolicyValidationErrors(req.PolicyType, req.TargetID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listEffectivePolicyValidationErrorsResponse{ValidationErrors: errs})
}
