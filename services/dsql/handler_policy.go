package dsql

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchPolicyOps handles cluster resource-policy operations.
func (h *Handler) dispatchPolicyOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opGetClusterPolicy:
		return true, h.handleGetClusterPolicy(c, resource)
	case opPutClusterPolicy:
		return true, h.handlePutClusterPolicy(c, resource, body)
	case opDeleteClusterPolicy:
		return true, h.handleDeleteClusterPolicy(c, resource)
	}

	return false, nil
}

func (h *Handler) handleGetClusterPolicy(c *echo.Context, identifier string) error {
	policy, err := h.Backend.GetClusterPolicy(identifier)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, policyResponse(policy))
}

func (h *Handler) handlePutClusterPolicy(c *echo.Context, identifier string, body []byte) error {
	var req putClusterPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeInvalidBody(c)
	}

	policy, err := h.Backend.PutClusterPolicy(identifier, req.Policy, req.ExpectedPolicyVersion)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, policyVersionResponse{PolicyVersion: policy.Version})
}

func (h *Handler) handleDeleteClusterPolicy(c *echo.Context, identifier string) error {
	q := c.Request().URL.Query()

	policy, err := h.Backend.DeleteClusterPolicy(identifier, q.Get("expected-policy-version"))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, policyVersionResponse{PolicyVersion: policy.Version})
}
