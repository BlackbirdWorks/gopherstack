package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Logs handlers ---

func (h *Handler) handleConfigureLogsForChannel(c *echo.Context, body map[string]any) error {
	channelName, _ := body[keyChannelName].(string)
	logTypes := extractStringSlice(body, "LogTypes")

	name, types, err := h.Backend.ConfigureLogsForChannel(channelName, logTypes)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelName: name,
		"LogTypes":     types,
	})
}

func (h *Handler) handleConfigureLogsForPlaybackConfiguration(c *echo.Context, body map[string]any) error {
	playbackConfigName, _ := body["PlaybackConfigurationName"].(string)
	pct, _ := body["PercentEnabled"].(float64)
	percentEnabled := int(pct)

	name, percent, err := h.Backend.ConfigureLogsForPlaybackConfiguration(playbackConfigName, percentEnabled)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"PlaybackConfigurationName": name,
		"PercentEnabled":            percent,
	})
}
