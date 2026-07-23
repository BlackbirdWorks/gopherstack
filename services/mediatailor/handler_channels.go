package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Channel handlers ---

func (h *Handler) handleCreateChannel(c *echo.Context, name string, body map[string]any) error {
	playbackMode, _ := body["PlaybackMode"].(string)
	tier, _ := body["Tier"].(string)
	outputs := extractOutputs(body)
	fillerSlate := extractFillerSlate(body)
	audiences := extractStringSlice(body, "Audiences")
	timeShift := extractTimeShiftConfiguration(body)
	tags := extractTags(body)

	ch, err := h.Backend.CreateChannel(name, playbackMode, tier, outputs, fillerSlate, audiences, timeShift, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDescribeChannel(c *echo.Context, name string) error {
	ch, err := h.Backend.DescribeChannel(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(c *echo.Context, name string, body map[string]any) error {
	outputs := extractOutputs(body)
	fillerSlate := extractFillerSlate(body)
	audiences := extractStringSlice(body, "Audiences")
	timeShift := extractTimeShiftConfiguration(body)

	ch, err := h.Backend.UpdateChannel(name, outputs, fillerSlate, audiences, timeShift)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDeleteChannel(c *echo.Context, name string) error {
	if err := h.Backend.DeleteChannel(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	maxResults, nextToken := extractPaginationParams(c)
	summaries, nextToken, err := h.Backend.ListChannels(maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyChannelName: s.Name,
			keyArn:         s.ARN,
			"PlaybackMode": s.PlaybackMode,
			"ChannelState": s.ChannelState,
			"Tier":         s.Tier,
			keyTags:        nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartChannel(c *echo.Context, name string) error {
	if err := h.Backend.StartChannel(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStopChannel(c *echo.Context, name string) error {
	if err := h.Backend.StopChannel(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func toChannelOutput(ch *Channel) map[string]any {
	outputs := make([]map[string]any, 0, len(ch.Outputs))
	for _, o := range ch.Outputs {
		out := map[string]any{
			"ManifestName": o.ManifestName,
			keySourceGroup: o.SourceGroup,
		}
		if o.HlsPlaylistSettings != nil {
			out["HlsPlaylistSettings"] = map[string]any{
				"ManifestWindowSeconds": o.HlsPlaylistSettings.ManifestWindowSeconds,
			}
		}
		outputs = append(outputs, out)
	}

	result := map[string]any{
		keyChannelName: ch.Name,
		keyArn:         ch.ARN,
		"PlaybackMode": ch.PlaybackMode,
		"ChannelState": ch.ChannelState,
		"Tier":         ch.Tier,
		"Outputs":      outputs,
		keyTags:        nilToEmpty(ch.Tags),
		"LogConfiguration": map[string]any{
			"LogTypes": nilToEmptyStrings(ch.LogConfiguration.LogTypes),
		},
	}

	if len(ch.Audiences) > 0 {
		result["Audiences"] = ch.Audiences
	}

	addTimestamps(result, ch.CreationTime, ch.LastModified)

	if ch.FillerSlate != nil {
		result["FillerSlate"] = map[string]any{
			keySourceLocationName: ch.FillerSlate.SourceLocationName,
			keyVodSourceName:      ch.FillerSlate.VodSourceName,
		}
	}

	if ch.TimeShift != nil {
		result["TimeShiftConfiguration"] = map[string]any{
			"MaxTimeDelaySeconds": ch.TimeShift.MaxTimeDelaySeconds,
		}
	}

	return result
}
