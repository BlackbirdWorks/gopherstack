package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- PlaybackConfiguration handlers ---

func (h *Handler) handlePutPlaybackConfiguration(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	adsURL, _ := body["AdDecisionServerUrl"].(string)
	videoURL, _ := body["VideoContentSourceUrl"].(string)
	tags := extractTags(body)

	cfg, err := h.Backend.PutPlaybackConfiguration(name, adsURL, videoURL, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPlaybackConfigOutput(cfg))
}

func (h *Handler) handleGetPlaybackConfiguration(c *echo.Context, name string) error {
	cfg, err := h.Backend.GetPlaybackConfiguration(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPlaybackConfigOutput(cfg))
}

func (h *Handler) handleDeletePlaybackConfiguration(c *echo.Context, name string) error {
	if err := h.Backend.DeletePlaybackConfiguration(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListPlaybackConfigurations(c *echo.Context) error {
	maxResults, nextToken := extractPaginationParams(c)
	summaries, nextToken, err := h.Backend.ListPlaybackConfigurations(maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyName:                    s.Name,
			"PlaybackConfigurationArn": s.PlaybackConfigurationARN,
			"AdDecisionServerUrl":      s.AdDecisionServerURL,
			"VideoContentSourceUrl":    s.VideoContentSourceURL,
			keyTags:                    nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toPlaybackConfigOutput(cfg *PlaybackConfiguration) map[string]any {
	out := map[string]any{
		keyName:                               cfg.Name,
		"PlaybackConfigurationArn":            cfg.PlaybackConfigurationARN,
		"AdDecisionServerUrl":                 cfg.AdDecisionServerURL,
		"VideoContentSourceUrl":               cfg.VideoContentSourceURL,
		"PlaybackEndpointPrefix":              cfg.PlaybackEndpointPrefix,
		"SessionInitializationEndpointPrefix": cfg.SessionInitializationPrefix,
		keyTags:                               nilToEmpty(cfg.Tags),
	}

	if cfg.HlsManifestEndpointPrefix != "" {
		out["HlsConfiguration"] = map[string]any{
			"ManifestEndpointPrefix": cfg.HlsManifestEndpointPrefix,
		}
	}

	return out
}
