package kafka

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDeleteClusterPolicy(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
) error {
	if err := h.Backend.DeleteClusterPolicy(ctx, clusterArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type getClusterPolicyOutput struct {
	CurrentVersion string `json:"currentVersion,omitempty"`
	Policy         string `json:"policy"`
}

type putClusterPolicyInput struct {
	CurrentVersion string `json:"currentVersion"`
	Policy         string `json:"policy"`
}

type putClusterPolicyOutput struct {
	CurrentVersion string `json:"currentVersion"`
}

func (h *Handler) handleGetClusterPolicy(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
) error {
	policy, version, err := h.Backend.GetClusterPolicy(ctx, clusterArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getClusterPolicyOutput{Policy: policy, CurrentVersion: version})
}

func (h *Handler) handlePutClusterPolicy(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in putClusterPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	version, err := h.Backend.PutClusterPolicy(ctx, clusterArn, in.Policy, in.CurrentVersion)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, putClusterPolicyOutput{CurrentVersion: version})
}
