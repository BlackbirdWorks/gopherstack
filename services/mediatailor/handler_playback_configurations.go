package mediatailor

import (
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// --- PlaybackConfiguration handlers ---

// insertionModeStitchedOnly is InsertionMode's documented default: "The
// default for players that do not specify an insertion mode is stitched."
// (mediatailor@v1.63.4 api_op_PutPlaybackConfiguration.go:99-103).
const insertionModeStitchedOnly = "STITCHED_ONLY"

// isValidInsertionMode mirrors types.InsertionMode.Values(), so it cannot
// drift from the real enum.
func isValidInsertionMode(v string) bool {
	return v == insertionModeStitchedOnly || v == "PLAYER_SELECT"
}

func (h *Handler) handlePutPlaybackConfiguration(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	adsURL, _ := body[keyAdDecisionServerURL].(string)
	videoURL, _ := body[keyVideoContentSourceURL].(string)
	tags := extractTags(body)
	extra := extractExtraConfig(body)

	insertionMode, ok := extra["InsertionMode"].(string)
	if !ok || insertionMode == "" {
		insertionMode = insertionModeStitchedOnly
	}
	if !isValidInsertionMode(insertionMode) {
		return respondErr(c, fmt.Errorf("%w: InsertionMode value %q is not valid", ErrInvalidParameter, insertionMode))
	}
	if extra == nil {
		extra = make(map[string]any)
	}
	extra["InsertionMode"] = insertionMode
	applyPlaybackConfigDefaults(extra)

	cfg, err := h.Backend.PutPlaybackConfiguration(name, adsURL, videoURL, tags, extra)
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
		// ListPlaybackConfigurationsOutput.Items is []types.PlaybackConfiguration,
		// the same full type GetPlaybackConfiguration returns, so reuse
		// toPlaybackConfigOutput rather than re-deriving a slimmer shape.
		out = append(out, toPlaybackConfigOutput(&PlaybackConfiguration{
			Name:                        s.Name,
			PlaybackConfigurationARN:    s.PlaybackConfigurationARN,
			AdDecisionServerURL:         s.AdDecisionServerURL,
			VideoContentSourceURL:       s.VideoContentSourceURL,
			Tags:                        s.Tags,
			PlaybackEndpointPrefix:      s.PlaybackEndpointPrefix,
			SessionInitializationPrefix: s.SessionInitializationPrefix,
			HlsManifestEndpointPrefix:   s.HlsManifestEndpointPrefix,
			LogConfiguration:            s.LogConfiguration,
			Extra:                       s.Extra,
		}))
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
		keyAdDecisionServerURL:                cfg.AdDecisionServerURL,
		keyVideoContentSourceURL:              cfg.VideoContentSourceURL,
		"PlaybackEndpointPrefix":              cfg.PlaybackEndpointPrefix,
		"SessionInitializationEndpointPrefix": cfg.SessionInitializationPrefix,
		keyTags:                               nilToEmpty(cfg.Tags),
	}

	if cfg.HlsManifestEndpointPrefix != "" {
		hlsCfg := map[string]any{
			"ManifestEndpointPrefix": cfg.HlsManifestEndpointPrefix,
		}

		if cfg.HlsDualStackManifestEndpointPrefix != "" {
			hlsCfg["DualStackManifestEndpointPrefix"] = cfg.HlsDualStackManifestEndpointPrefix
		}

		out["HlsConfiguration"] = hlsCfg
	}

	if cfg.LogConfiguration != nil {
		out["LogConfiguration"] = toLogConfigurationOutput(cfg.LogConfiguration)
	}

	if cfg.DualStackPlaybackEndpointPrefix != "" {
		out["DualStackPlaybackEndpointPrefix"] = cfg.DualStackPlaybackEndpointPrefix
	}

	if cfg.DualStackSessionInitializationEndpointPrefix != "" {
		out["DualStackSessionInitializationEndpointPrefix"] = cfg.DualStackSessionInitializationEndpointPrefix
	}

	mergeExtraConfig(out, cfg.Extra)
	addDashManifestEndpointPrefix(out, cfg.HlsManifestEndpointPrefix)

	return out
}

// applyPlaybackConfigDefaults fills the documented defaults of the dash,
// concurrency and timeout sub-configs (types.go:199-234, 551-580).
func applyPlaybackConfigDefaults(extra map[string]any) {
	fill := func(key string, defaults map[string]any) {
		sub, _ := extra[key].(map[string]any)
		out := make(map[string]any, len(defaults))
		maps.Copy(out, defaults)

		for k, v := range sub {
			if v != nil {
				out[k] = v
			}
		}

		extra[key] = out
	}

	fill("DashConfiguration", map[string]any{"MpdLocation": "EMT_DEFAULT", "OriginManifestType": "MULTI_PERIOD"})

	if _, ok := extra["AdsPersonalizationConcurrency"]; ok {
		fill("AdsPersonalizationConcurrency", map[string]any{
			"EnableVodVastParallelization": false, "MaxConcurrentAdsRequests": float64(1),
		})
	}

	if _, ok := extra["AdsPersonalizationTimeouts"]; ok {
		const adsRequestTimeoutMs, maxPersonalizationTimeMs = 3000, 10000

		fill("AdsPersonalizationTimeouts", map[string]any{
			"AdsRequestTimeoutMilliseconds":                 float64(adsRequestTimeoutMs),
			"LiveMaximumAdsPersonalizationTimeMilliseconds": float64(maxPersonalizationTimeMs),
			"VodMaximumAdsPersonalizationTimeMilliseconds":  float64(maxPersonalizationTimeMs),
		})
	}
}

// addDashManifestEndpointPrefix adds the output-only DASH manifest prefix,
// derived from the HLS prefix, to a stored DashConfiguration.
func addDashManifestEndpointPrefix(out map[string]any, hlsPrefix string) {
	stored, ok := out["DashConfiguration"].(map[string]any)
	if !ok || hlsPrefix == "" {
		return
	}

	dash := make(map[string]any, len(stored)+1)
	maps.Copy(dash, stored)
	dash["ManifestEndpointPrefix"] = strings.Replace(hlsPrefix, "/v1/master/", "/v1/dash/", 1)
	out["DashConfiguration"] = dash
}

// mergeExtraConfig writes every key from extra (PutPlaybackConfiguration's
// pass-through optional sub-configs) into out, so a client reads back
// exactly what it sent on the last Put.
func mergeExtraConfig(out map[string]any, extra map[string]any) {
	maps.Copy(out, extra)
}
