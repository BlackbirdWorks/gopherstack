package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type resourcePolicySummaryObject struct {
	ARN string `json:"Arn"`
	ID  string `json:"Id"`
}

type resourcePolicyObject struct {
	Content               string                      `json:"Content"`
	ResourcePolicySummary resourcePolicySummaryObject `json:"ResourcePolicySummary"`
}

type describeResourcePolicyResponse struct {
	ResourcePolicy resourcePolicyObject `json:"ResourcePolicy"`
}

// dispatchResourcePolicy handles resource-policy operations.
func (h *Handler) dispatchResourcePolicy(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "DeleteResourcePolicy":
		return true, h.handleDeleteResourcePolicy(c, body)
	case "DescribeResourcePolicy":
		return true, h.handleDescribeResourcePolicy(c, body)
	case "PutResourcePolicy":
		return true, h.handlePutResourcePolicy(c, body)
	}

	return false, nil
}

// ----------------------------------------
// ResourcePolicy handlers
// ----------------------------------------

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, _ []byte) error {
	if err := h.Backend.DeleteResourcePolicy(); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDescribeResourcePolicy(c *echo.Context, _ []byte) error {
	rp, err := h.Backend.DescribeResourcePolicy()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeResourcePolicyResponse{
		ResourcePolicy: resourcePolicyObject{
			Content: rp.Content,
			ResourcePolicySummary: resourcePolicySummaryObject{
				ARN: rp.ARN,
				ID:  rp.ID,
			},
		},
	})
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context, body []byte) error {
	var req struct {
		Content string `json:"Content"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Content == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Content is required")
	}

	rp, err := h.Backend.PutResourcePolicy(req.Content)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeResourcePolicyResponse{
		ResourcePolicy: resourcePolicyObject{
			Content: rp.Content,
			ResourcePolicySummary: resourcePolicySummaryObject{
				ARN: rp.ARN,
				ID:  rp.ID,
			},
		},
	})
}
