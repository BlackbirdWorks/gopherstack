package glacier

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleSetVaultAccessPolicy(c *echo.Context, vaultName string, body []byte) error {
	var req vaultAccessPolicy
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.SetVaultAccessPolicy(h.AccountID, h.DefaultRegion, vaultName, req.Policy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetVaultAccessPolicy(c *echo.Context, vaultName string) error {
	policy, err := h.Backend.GetVaultAccessPolicy(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	if policy == "" {
		return h.writeError(
			c,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"vault access policy not found",
		)
	}

	return c.JSON(http.StatusOK, vaultAccessPolicy{Policy: policy})
}

func (h *Handler) handleDeleteVaultAccessPolicy(c *echo.Context, vaultName string) error {
	if err := h.Backend.DeleteVaultAccessPolicy(h.AccountID, h.DefaultRegion, vaultName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
