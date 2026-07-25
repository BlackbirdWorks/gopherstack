package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Program handlers ---

func (h *Handler) handleCreateProgram(
	c *echo.Context,
	channelName, programName string,
	body map[string]any,
) error {
	sourceLocationName, _ := body[keySourceLocationName].(string)
	vodSourceName, _ := body[keyVodSourceName].(string)
	liveSourceName, _ := body[keyLiveSourceName].(string)
	scheduleConfig := extractScheduleConfiguration(body)
	adBreaks := extractAdBreaks(body)
	audienceMedia := extractAudienceMedia(body)
	tags := extractTags(body)

	prog, err := h.Backend.CreateProgram(
		channelName,
		programName,
		sourceLocationName,
		vodSourceName,
		liveSourceName,
		scheduleConfig,
		adBreaks,
		audienceMedia,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleDescribeProgram(c *echo.Context, channelName, programName string) error {
	prog, err := h.Backend.DescribeProgram(channelName, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleUpdateProgram(c *echo.Context, channelName, programName string, body map[string]any) error {
	scheduleConfig := extractUpdateProgramScheduleConfiguration(body)
	adBreaks := extractAdBreaks(body)
	audienceMedia := extractAudienceMedia(body)

	prog, err := h.Backend.UpdateProgram(channelName, programName, scheduleConfig, adBreaks, audienceMedia)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleDeleteProgram(c *echo.Context, channelName, programName string) error {
	if err := h.Backend.DeleteProgram(channelName, programName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetChannelSchedule(c *echo.Context, channelName string) error {
	maxResults, nextToken := extractPaginationParams(c)
	entries, nextToken, err := h.Backend.GetChannelSchedule(channelName, maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		item := map[string]any{
			keyArn:                e.ARN,
			keyChannelName:        e.ChannelName,
			"ProgramName":         e.ProgramName,
			keySourceLocationName: e.SourceLocationName,
			"ScheduleEntryType":   e.ScheduleEntryType,
		}

		if e.VodSourceName != "" {
			item[keyVodSourceName] = e.VodSourceName
		}

		if e.LiveSourceName != "" {
			item[keyLiveSourceName] = e.LiveSourceName
		}

		if len(e.Audiences) > 0 {
			item["Audiences"] = e.Audiences
		}

		if e.ApproximateDurationSeconds != 0 {
			item["ApproximateDurationSeconds"] = e.ApproximateDurationSeconds
		}

		if !e.ApproximateStartTime.IsZero() {
			item["ApproximateStartTime"] = awstime.Epoch(e.ApproximateStartTime)
		}

		out = append(out, item)
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toKeyValuePairs(kvs []KeyValuePair) []map[string]any {
	if len(kvs) == 0 {
		return nil
	}

	out := make([]map[string]any, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, map[string]any{"Key": kv.Key, "Value": kv.Value})
	}

	return out
}

func toClipRangeOutput(cr *ClipRange) map[string]any {
	if cr == nil {
		return nil
	}

	return map[string]any{
		"StartOffsetMillis": cr.StartOffsetMillis,
		"EndOffsetMillis":   cr.EndOffsetMillis,
	}
}

func toAdBreaksOutput(adBreaks []AdBreak) []map[string]any {
	if len(adBreaks) == 0 {
		return nil
	}

	out := make([]map[string]any, 0, len(adBreaks))

	for _, ab := range adBreaks {
		item := map[string]any{
			"OffsetMillis": ab.OffsetMillis,
		}

		if ab.MessageType != "" {
			item["MessageType"] = ab.MessageType
		}

		if md := toKeyValuePairs(ab.AdBreakMetadata); md != nil {
			item["AdBreakMetadata"] = md
		}

		if ab.Slate != nil {
			item["Slate"] = map[string]any{
				keySourceLocationName: ab.Slate.SourceLocationName,
				keyVodSourceName:      ab.Slate.VodSourceName,
			}
		}

		if ab.SpliceInsertMessage != nil {
			item["SpliceInsertMessage"] = map[string]any{
				"AvailNum":        ab.SpliceInsertMessage.AvailNum,
				"AvailsExpected":  ab.SpliceInsertMessage.AvailsExpected,
				"SpliceEventId":   ab.SpliceInsertMessage.SpliceEventID,
				"UniqueProgramId": ab.SpliceInsertMessage.UniqueProgramID,
			}
		}

		if ab.TimeSignalMessage != nil {
			item["TimeSignalMessage"] = map[string]any{
				"SegmentationDescriptors": ab.TimeSignalMessage.SegmentationDescriptors,
			}
		}

		out = append(out, item)
	}

	return out
}

func toAlternateMediaOutput(alts []AlternateMedia) []map[string]any {
	if len(alts) == 0 {
		return nil
	}

	out := make([]map[string]any, 0, len(alts))

	for _, alt := range alts {
		item := map[string]any{
			keySourceLocationName:      alt.SourceLocationName,
			keyVodSourceName:           alt.VodSourceName,
			keyLiveSourceName:          alt.LiveSourceName,
			"ScheduledStartTimeMillis": alt.ScheduledStartTimeMillis,
			"DurationMillis":           alt.DurationMillis,
		}

		if cr := toClipRangeOutput(alt.ClipRange); cr != nil {
			item["ClipRange"] = cr
		}

		if ab := toAdBreaksOutput(alt.AdBreaks); ab != nil {
			item["AdBreaks"] = ab
		}

		out = append(out, item)
	}

	return out
}

func toAudienceMediaOutput(am []AudienceMedia) []map[string]any {
	if len(am) == 0 {
		return nil
	}

	out := make([]map[string]any, 0, len(am))

	for _, a := range am {
		out = append(out, map[string]any{
			"Audience":       a.Audience,
			"AlternateMedia": toAlternateMediaOutput(a.AlternateMedia),
		})
	}

	return out
}

func toProgramOutput(prog *Program) map[string]any {
	out := map[string]any{
		keyArn:                prog.ARN,
		keyChannelName:        prog.ChannelName,
		"ProgramName":         prog.ProgramName,
		keySourceLocationName: prog.SourceLocationName,
		keyVodSourceName:      prog.VodSourceName,
		keyLiveSourceName:     prog.LiveSourceName,
		keyTags:               nilToEmpty(prog.Tags),
	}

	if !prog.CreationTime.IsZero() {
		out["CreationTime"] = awstime.Epoch(prog.CreationTime)
	}

	if !prog.ScheduledStartTime.IsZero() {
		out["ScheduledStartTime"] = awstime.Epoch(prog.ScheduledStartTime)
	}

	if prog.DurationMillis != 0 {
		out["DurationMillis"] = prog.DurationMillis
	}

	if cr := toClipRangeOutput(prog.ClipRange); cr != nil {
		out["ClipRange"] = cr
	}

	if ab := toAdBreaksOutput(prog.AdBreaks); ab != nil {
		out["AdBreaks"] = ab
	}

	if am := toAudienceMediaOutput(prog.AudienceMedia); am != nil {
		out["AudienceMedia"] = am
	}

	return out
}
