package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type describeEffectivePolicyRequest struct {
	PolicyType string `json:"PolicyType"`
	TargetID   string `json:"TargetId,omitempty"`
}

type effectivePolicyObject struct {
	PolicyContent        string  `json:"PolicyContent"`
	PolicyType           string  `json:"PolicyType"`
	TargetID             string  `json:"TargetId"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp"`
}

type describeEffectivePolicyResponse struct {
	EffectivePolicy effectivePolicyObject `json:"EffectivePolicy"`
}

// -- ListEffectivePolicyValidationErrors --

// listEffectivePolicyValidationErrorsRequest's target member is "AccountId",
// not "TargetId" -- verified against
// awsAwsjson11_serializeOpDocumentListEffectivePolicyValidationErrorsInput
// (organizations@v1.53.5 serializers.go), unlike its sibling
// describeEffectivePolicyRequest, which genuinely uses "TargetId".
type listEffectivePolicyValidationErrorsRequest struct {
	PolicyType string `json:"PolicyType"`
	AccountID  string `json:"AccountId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listEffectivePolicyValidationErrorsResponse struct {
	NextToken                       string `json:"NextToken,omitempty"`
	EffectivePolicyValidationErrors []any  `json:"EffectivePolicyValidationErrors"`
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

	if rejected, pErr := h.checkPaging(c, req.MaxResults, req.NextToken); rejected {
		return pErr
	}

	errs, err := h.Backend.ListEffectivePolicyValidationErrors(req.PolicyType, req.AccountID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	p := page.New(errs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(
		http.StatusOK,
		listEffectivePolicyValidationErrorsResponse{EffectivePolicyValidationErrors: p.Data, NextToken: p.Next},
	)
}
