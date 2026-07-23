package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- PrefetchSchedule handlers ---

func (h *Handler) handleCreatePrefetchSchedule(
	c *echo.Context,
	playbackConfigName, name string,
	body map[string]any,
) error {
	retrieval := extractPrefetchRetrieval(body)
	consumption := extractPrefetchConsumption(body)
	scheduleType, _ := body["ScheduleType"].(string)
	streamID, _ := body["StreamId"].(string)
	recurringConfig, _ := body["RecurringPrefetchConfiguration"].(map[string]any)
	tags := extractTags(body)

	ps, err := h.Backend.CreatePrefetchSchedule(
		playbackConfigName, name, scheduleType, streamID, retrieval, consumption, recurringConfig, tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPrefetchScheduleOutput(ps))
}

func (h *Handler) handleGetPrefetchSchedule(c *echo.Context, playbackConfigName, name string) error {
	ps, err := h.Backend.GetPrefetchSchedule(playbackConfigName, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPrefetchScheduleOutput(ps))
}

func (h *Handler) handleDeletePrefetchSchedule(c *echo.Context, playbackConfigName, name string) error {
	if err := h.Backend.DeletePrefetchSchedule(playbackConfigName, name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListPrefetchSchedules(c *echo.Context, playbackConfigName string, body map[string]any) error {
	maxResults, nextToken := extractBodyPaginationParams(body)
	scheduleType, _ := body["ScheduleType"].(string)
	streamID, _ := body["StreamId"].(string)

	schedules, nextToken, err := h.Backend.ListPrefetchSchedules(
		playbackConfigName, scheduleType, streamID, maxResults, nextToken,
	)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(schedules))
	for _, ps := range schedules {
		out = append(out, toPrefetchScheduleOutput(ps))
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toPrefetchScheduleOutput(ps *PrefetchSchedule) map[string]any {
	out := map[string]any{
		keyArn:                      ps.ARN,
		keyName:                     ps.Name,
		"PlaybackConfigurationName": ps.PlaybackConfigurationName,
		"ScheduleType":              ps.ScheduleType,
		keyTags:                     nilToEmpty(ps.Tags),
	}

	if ps.StreamID != "" {
		out["StreamId"] = ps.StreamID
	}

	if !ps.CreationTime.IsZero() {
		out["CreationTime"] = awstime.Epoch(ps.CreationTime)
	}

	if ps.RecurringPrefetchConfiguration != nil {
		out["RecurringPrefetchConfiguration"] = ps.RecurringPrefetchConfiguration
	}

	if ps.Retrieval != nil {
		r := map[string]any{}
		if !ps.Retrieval.StartTime.IsZero() {
			r["StartTime"] = awstime.Epoch(ps.Retrieval.StartTime)
		}

		if !ps.Retrieval.EndTime.IsZero() {
			r["EndTime"] = awstime.Epoch(ps.Retrieval.EndTime)
		}

		if len(ps.Retrieval.DynamicVariables) > 0 {
			r["DynamicVariables"] = ps.Retrieval.DynamicVariables
		}

		out["Retrieval"] = r
	}

	if ps.Consumption != nil {
		c := map[string]any{}
		if !ps.Consumption.StartTime.IsZero() {
			c["StartTime"] = awstime.Epoch(ps.Consumption.StartTime)
		}

		if !ps.Consumption.EndTime.IsZero() {
			c["EndTime"] = awstime.Epoch(ps.Consumption.EndTime)
		}

		out["Consumption"] = c
	}

	return out
}
