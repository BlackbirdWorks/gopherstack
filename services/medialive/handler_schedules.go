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
		out = append(out, scheduleActionToResponse(a))
	}

	return c.JSON(http.StatusOK, map[string]any{keyScheduleActions: out})
}

// scheduleActionToResponse builds the real ScheduleAction wire shape.
// ActionName/ScheduleActionSettings/ScheduleActionStartSettings are all
// "This member is required" (medialive@v1.101.4 types/types.go:7277-7287).
func scheduleActionToResponse(a ScheduleAction) map[string]any {
	return map[string]any{
		keyActionName:                 a.ActionName,
		"scheduleActionSettings":      a.ScheduleActionSettings,
		"scheduleActionStartSettings": a.ScheduleActionStartSettings,
	}
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
				settings, _ := m["scheduleActionSettings"].(map[string]any)
				startSettings, _ := m["scheduleActionStartSettings"].(map[string]any)
				creates = append(creates, ScheduleAction{
					ActionName:                  actionName,
					ScheduleActionSettings:      settings,
					ScheduleActionStartSettings: startSettings,
				})
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
		createsOut = append(createsOut, scheduleActionToResponse(a))
	}
	// BatchScheduleActionDeleteResult also echoes back "scheduleActions"
	// (the full deleted actions), NOT "actionNames" -- verified against
	// the SDK deserializer (awsRestjson1_deserializeDocumentBatchScheduleActionDeleteResult).
	deletesOut := make([]map[string]any, 0, len(result.Deletes))
	for _, a := range result.Deletes {
		deletesOut = append(deletesOut, scheduleActionToResponse(a))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"creates": map[string]any{keyScheduleActions: createsOut},
		"deletes": map[string]any{keyScheduleActions: deletesOut},
	})
}
