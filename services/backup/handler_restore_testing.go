package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type restoreTestingPlanDoc struct {
	RestoreTestingPlanName string `json:"RestoreTestingPlanName"`
	ScheduleExpression     string `json:"ScheduleExpression,omitempty"`
	StartWindowHours       int64  `json:"StartWindowHours,omitempty"`
}

type createRestoreTestingPlanBody struct {
	CreatorRequestID   string                `json:"CreatorRequestId,omitempty"`
	RestoreTestingPlan restoreTestingPlanDoc `json:"RestoreTestingPlan"`
}

func (h *Handler) handleCreateRestoreTestingPlan(c *echo.Context, body []byte) error {
	var in createRestoreTestingPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.RestoreTestingPlan.RestoreTestingPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	rtp, err := h.Backend.CreateRestoreTestingPlan(
		in.RestoreTestingPlan.RestoreTestingPlanName,
		in.RestoreTestingPlan.ScheduleExpression,
		in.RestoreTestingPlan.StartWindowHours,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
		keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
		keyCreationTime:           epochSeconds(rtp.CreationTime),
	})
}

type restoreTestingSelectionDoc struct {
	RestoreTestingSelectionName string `json:"RestoreTestingSelectionName"`
	ProtectedResourceType       string `json:"ProtectedResourceType,omitempty"`
}

type createRestoreTestingSelectionBody struct {
	RestoreTestingSelection restoreTestingSelectionDoc `json:"RestoreTestingSelection"`
	CreatorRequestID        string                     `json:"CreatorRequestId,omitempty"`
}

func (h *Handler) handleCreateRestoreTestingSelection(
	c *echo.Context,
	planName string,
	body []byte,
) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	var in createRestoreTestingSelectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.RestoreTestingSelection.RestoreTestingSelectionName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingSelectionName is required"),
		)
	}

	sel, err := h.Backend.CreateRestoreTestingSelection(
		planName,
		in.RestoreTestingSelection.RestoreTestingSelectionName,
		in.RestoreTestingSelection.ProtectedResourceType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:       sel.RestoreTestingPlanArn,
		keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
		keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
		keyCreationTime:                epochSeconds(sel.CreationTime),
	})
}

func (h *Handler) handleGetRestoreTestingPlan(c *echo.Context, planName string) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	rtp, err := h.Backend.GetRestoreTestingPlan(planName)
	if err != nil {
		return h.handleError(c, err)
	}

	planDoc := map[string]any{
		keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
		keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
		"ScheduleExpression":      rtp.ScheduleExpression,
		keyCreationTime:           epochSeconds(rtp.CreationTime),
	}
	if rtp.StartWindowHours > 0 {
		planDoc["StartWindowHours"] = rtp.StartWindowHours
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingPlan": planDoc,
	})
}

func (h *Handler) handleListRestoreTestingPlans(c *echo.Context) error {
	plans := h.Backend.ListRestoreTestingPlans()
	items := make([]map[string]any, 0, len(plans))

	for _, rtp := range plans {
		item := map[string]any{
			keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
			keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
			"ScheduleExpression":      rtp.ScheduleExpression,
			keyCreationTime:           epochSeconds(rtp.CreationTime),
		}
		if rtp.StartWindowHours > 0 {
			item["StartWindowHours"] = rtp.StartWindowHours
		}
		items = append(items, item)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingPlans": items,
	})
}

type updateRestoreTestingPlanBody struct {
	RestoreTestingPlan restoreTestingPlanDoc `json:"RestoreTestingPlan"`
}

func (h *Handler) handleUpdateRestoreTestingPlan(
	c *echo.Context,
	planName string,
	body []byte,
) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	var in updateRestoreTestingPlanBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	rtp, err := h.Backend.UpdateRestoreTestingPlan(
		planName,
		in.RestoreTestingPlan.ScheduleExpression,
		in.RestoreTestingPlan.StartWindowHours,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
		keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
		keyCreationTime:           epochSeconds(rtp.CreationTime),
	})
}

func (h *Handler) handleDeleteRestoreTestingPlan(c *echo.Context, planName string) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	if err := h.Backend.DeleteRestoreTestingPlan(planName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetRestoreTestingSelection(c *echo.Context, resource string) error {
	planName, selName, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	sel, err := h.Backend.GetRestoreTestingSelection(planName, selName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingSelection": map[string]any{
			keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
			keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
			"ProtectedResourceType":        sel.ProtectedResourceType,
			keyCreationTime:                epochSeconds(sel.CreationTime),
		},
	})
}

func (h *Handler) handleListRestoreTestingSelections(c *echo.Context, planName string) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	sels, err := h.Backend.ListRestoreTestingSelections(planName)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(sels))
	for _, sel := range sels {
		items = append(items, map[string]any{
			keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
			keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
			"ProtectedResourceType":        sel.ProtectedResourceType,
			keyCreationTime:                epochSeconds(sel.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingSelections": items,
	})
}

type updateRestoreTestingSelectionBody struct {
	RestoreTestingSelection restoreTestingSelectionDoc `json:"RestoreTestingSelection"`
}

func (h *Handler) handleUpdateRestoreTestingSelection(
	c *echo.Context,
	resource string,
	body []byte,
) error {
	planName, selName, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	var in updateRestoreTestingSelectionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	sel, err := h.Backend.UpdateRestoreTestingSelection(
		planName,
		selName,
		in.RestoreTestingSelection.ProtectedResourceType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:       sel.RestoreTestingPlanArn,
		keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
		keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
		keyCreationTime:                epochSeconds(sel.CreationTime),
	})
}

func (h *Handler) handleDeleteRestoreTestingSelection(c *echo.Context, resource string) error {
	planName, selName, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	if err := h.Backend.DeleteRestoreTestingSelection(planName, selName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Framework read/update/delete handlers ---
