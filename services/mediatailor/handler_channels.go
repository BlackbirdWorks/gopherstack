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

	out := toChannelOutput(ch)
	// LogConfiguration is required on DescribeChannelOutput only --
	// Create/UpdateChannelOutput have no such member (confirmed against
	// both real structs).
	out["LogConfiguration"] = map[string]any{
		keyLogTypes: nilToEmptyStrings(ch.LogConfiguration.LogTypes),
	}

	return c.JSON(http.StatusOK, out)
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
		out = append(out, toChannelSummaryOutput(s))
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

func channelOutputsWire(outputs []OutputItem) []map[string]any {
	out := make([]map[string]any, 0, len(outputs))
	for _, o := range outputs {
		item := map[string]any{
			"ManifestName": o.ManifestName,
			keySourceGroup: o.SourceGroup,
		}
		if o.HlsPlaylistSettings != nil {
			item["HlsPlaylistSettings"] = map[string]any{
				"ManifestWindowSeconds": o.HlsPlaylistSettings.ManifestWindowSeconds,
			}
		}
		out = append(out, item)
	}

	return out
}

func fillerSlateWire(slate *SlateSource) map[string]any {
	if slate == nil {
		return nil
	}

	return map[string]any{
		keySourceLocationName: slate.SourceLocationName,
		keyVodSourceName:      slate.VodSourceName,
	}
}

// toChannelSummaryOutput builds a ListChannels item, matching the real
// types.Channel shape used by ListChannelsOutput.Items -- the SAME full
// type DescribeChannel returns (confirmed against mediatailor@v1.63.4's
// deserializeDocumentChannel), not a slimmer summary. Real types.Channel
// has LogConfiguration but no TimeShiftConfiguration, the opposite
// asymmetry from toChannelOutput's Create/Update shape.
func toChannelSummaryOutput(s *ChannelSummary) map[string]any {
	result := map[string]any{
		keyChannelName: s.Name,
		keyArn:         s.ARN,
		"PlaybackMode": s.PlaybackMode,
		"ChannelState": s.ChannelState,
		"Tier":         s.Tier,
		"Outputs":      channelOutputsWire(s.Outputs),
		keyTags:        nilToEmpty(s.Tags),
		"LogConfiguration": map[string]any{
			keyLogTypes: nilToEmptyStrings(s.LogConfiguration.LogTypes),
		},
	}

	if len(s.Audiences) > 0 {
		result["Audiences"] = s.Audiences
	}

	addTimestamps(result, s.CreationTime, s.LastModified)

	if slate := fillerSlateWire(s.FillerSlate); slate != nil {
		result["FillerSlate"] = slate
	}

	return result
}

func toChannelOutput(ch *Channel) map[string]any {
	result := map[string]any{
		keyChannelName: ch.Name,
		keyArn:         ch.ARN,
		"PlaybackMode": ch.PlaybackMode,
		"ChannelState": ch.ChannelState,
		"Tier":         ch.Tier,
		"Outputs":      channelOutputsWire(ch.Outputs),
		keyTags:        nilToEmpty(ch.Tags),
	}

	if len(ch.Audiences) > 0 {
		result["Audiences"] = ch.Audiences
	}

	addTimestamps(result, ch.CreationTime, ch.LastModified)

	if slate := fillerSlateWire(ch.FillerSlate); slate != nil {
		result["FillerSlate"] = slate
	}

	if ch.TimeShift != nil {
		result["TimeShiftConfiguration"] = map[string]any{
			"MaxTimeDelaySeconds": ch.TimeShift.MaxTimeDelaySeconds,
		}
	}

	return result
}
