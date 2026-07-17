package glacier

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

// handleDataRetrievalPolicy handles GetDataRetrievalPolicy and SetDataRetrievalPolicy.
func (h *Handler) handleDataRetrievalPolicy(c *echo.Context, op string, body []byte) error {
	if op == opGetDataRetrievalPolicy {
		return h.handleGetDataRetrievalPolicy(c)
	}

	return h.handleSetDataRetrievalPolicy(c, body)
}

func (h *Handler) handleGetDataRetrievalPolicy(c *echo.Context) error {
	policy := h.Backend.GetDataRetrievalPolicy(h.AccountID)
	if policy == "" {
		return c.JSON(http.StatusOK, map[string]any{
			"Policy": map[string]any{
				"Rules": []map[string]string{
					{"Strategy": "FreeTier"},
				},
			},
		})
	}

	var parsed any
	if err := json.Unmarshal([]byte(policy), &parsed); err == nil {
		return c.JSON(http.StatusOK, parsed)
	}

	return c.JSON(http.StatusOK, map[string]any{"Policy": policy})
}

func (h *Handler) handleSetDataRetrievalPolicy(c *echo.Context, body []byte) error {
	var req dataRetrievalPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"invalid data retrieval policy: "+err.Error())
	}

	if vErr := validateDataRetrievalRules(req.Policy.Rules); vErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			vErr.Error(),
		)
	}

	h.Backend.SetDataRetrievalPolicy(h.AccountID, body)

	return c.NoContent(http.StatusNoContent)
}

// validateDataRetrievalRules validates the Rules slice of a data retrieval policy.
func validateDataRetrievalRules(rules []dataRetrievalRule) error {
	validStrategies := map[string]bool{"None": true, "FreeTier": true, "BytesPerHour": true}

	for _, r := range rules {
		if !validStrategies[r.Strategy] {
			return fmt.Errorf(
				"%w: %q; must be None, FreeTier, or BytesPerHour",
				ErrInvalidStrategy,
				r.Strategy,
			)
		}

		if r.Strategy == "BytesPerHour" && (r.BytesPerHour == nil || *r.BytesPerHour <= 0) {
			return ErrBytesPerHourRequired
		}
	}

	return nil
}
