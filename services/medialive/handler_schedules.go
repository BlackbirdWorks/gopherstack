package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Schedule handlers ---

func (h *Handler) handleDescribeSchedule(c *echo.Context, channelID string) error {
	actions, err := h.Backend.DescribeSchedule(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(actions))
	for _, a := range actions {
		out = append(out, map[string]any{keyActionName: a.ActionName})
	}

	return c.JSON(http.StatusOK, map[string]any{keyScheduleActions: out})
}

func (h *Handler) handleDeleteSchedule(c *echo.Context, channelID string) error {
	if err := h.Backend.DeleteSchedule(channelID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleBatchUpdateSchedule(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	var creates []ScheduleAction
	if rawCreates, ok := body["creates"].(map[string]any); ok {
		rawActions, hasActions := rawCreates[keyScheduleActions].([]any)
		if hasActions {
			for _, item := range rawActions {
				m, isMapped := item.(map[string]any)
				if !isMapped {
					continue
				}
				actionName, _ := m[keyActionName].(string)
				creates = append(creates, ScheduleAction{ActionName: actionName})
			}
		}
	}
	var deleteNames []string
	if rawDeletes, ok := body["deletes"].(map[string]any); ok {
		deleteNames = extractStringSlice(rawDeletes, "actionNames")
	}
	result, err := h.Backend.BatchUpdateSchedule(channelID, creates, deleteNames)
	if err != nil {
		return respondErr(c, err)
	}
	createsOut := make([]map[string]any, 0, len(result.Creates))
	for _, a := range result.Creates {
		createsOut = append(createsOut, map[string]any{keyActionName: a.ActionName})
	}
	// BatchScheduleActionDeleteResult also echoes back "scheduleActions"
	// (the full deleted actions), NOT "actionNames" -- verified against
	// the SDK deserializer (awsRestjson1_deserializeDocumentBatchScheduleActionDeleteResult).
	deletesOut := make([]map[string]any, 0, len(result.Deletes))
	for _, a := range result.Deletes {
		deletesOut = append(deletesOut, map[string]any{keyActionName: a.ActionName})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"creates": map[string]any{keyScheduleActions: createsOut},
		"deletes": map[string]any{keyScheduleActions: deletesOut},
	})
}
