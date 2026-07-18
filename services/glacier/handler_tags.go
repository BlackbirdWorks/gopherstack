package glacier

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleAddTagsToVault(c *echo.Context, vaultName string, body []byte) error {
	var req addTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.AddTagsToVault(h.AccountID, h.DefaultRegion, vaultName, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForVault(c *echo.Context, vaultName string) error {
	tags, err := h.Backend.ListTagsForVault(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) handleRemoveTagsFromVault(c *echo.Context, vaultName string, body []byte) error {
	var req removeTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.RemoveTagsFromVault(h.AccountID, h.DefaultRegion, vaultName, req.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
