package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- ChannelPolicy handlers ---

func (h *Handler) handlePutChannelPolicy(c *echo.Context, channelName string, body map[string]any) error {
	policy, _ := body["Policy"].(string)

	if err := h.Backend.PutChannelPolicy(channelName, policy); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetChannelPolicy(c *echo.Context, channelName string) error {
	policy, err := h.Backend.GetChannelPolicy(channelName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Policy": policy})
}

func (h *Handler) handleDeleteChannelPolicy(c *echo.Context, channelName string) error {
	if err := h.Backend.DeleteChannelPolicy(channelName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
