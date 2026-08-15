package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Logs handlers ---

func (h *Handler) handleConfigureLogsForChannel(c *echo.Context, body map[string]any) error {
	channelName, _ := body[keyChannelName].(string)
	logTypes := extractStringSlice(body, keyLogTypes)

	name, types, err := h.Backend.ConfigureLogsForChannel(channelName, logTypes)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelName: name,
		keyLogTypes:    types,
	})
}

func (h *Handler) handleConfigureLogsForPlaybackConfiguration(c *echo.Context, body map[string]any) error {
	playbackConfigName, _ := body["PlaybackConfigurationName"].(string)
	pct, _ := body["PercentEnabled"].(float64)
	percentEnabled := int(pct)
	strategies := extractStringSlice(body, "EnabledLoggingStrategies")
	adsLog, _ := body["AdsInteractionLog"].(map[string]any)
	manifestLog, _ := body["ManifestServiceInteractionLog"].(map[string]any)

	logCfg, err := h.Backend.ConfigureLogsForPlaybackConfiguration(
		playbackConfigName, percentEnabled, strategies, adsLog, manifestLog,
	)
	if err != nil {
		return respondErr(c, err)
	}

	out := toLogConfigurationOutput(logCfg)
	out["PlaybackConfigurationName"] = playbackConfigName

	return c.JSON(http.StatusOK, out)
}

func toLogConfigurationOutput(logCfg *PlaybackConfigurationLogConfiguration) map[string]any {
	out := map[string]any{
		"PercentEnabled":           logCfg.PercentEnabled,
		"EnabledLoggingStrategies": nilToEmptyStrings(logCfg.EnabledLoggingStrategies),
	}

	if logCfg.AdsInteractionLog != nil {
		out["AdsInteractionLog"] = logCfg.AdsInteractionLog
	}

	if logCfg.ManifestServiceInteractionLog != nil {
		out["ManifestServiceInteractionLog"] = logCfg.ManifestServiceInteractionLog
	}

	return out
}
