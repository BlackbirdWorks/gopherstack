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
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.RestoreTestingPlan.RestoreTestingPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "RestoreTestingPlanName is required"),
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

	// Real AWS: responseCode 201.
	return c.JSON(http.StatusCreated, map[string]any{
		keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
		keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
		keyCreationTime:           epochSeconds(rtp.CreationTime),
	})
}

type keyValueJSON struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type protectedResourceConditionsJSON struct {
	StringEquals    []keyValueJSON `json:"StringEquals,omitempty"`
	StringNotEquals []keyValueJSON `json:"StringNotEquals,omitempty"`
}

func protectedResourceConditionsFromJSON(in *protectedResourceConditionsJSON) *ProtectedResourceConditions {
	if in == nil {
		return nil
	}

	eq := make([]KeyValue, 0, len(in.StringEquals))
	for _, kv := range in.StringEquals {
		eq = append(eq, KeyValue(kv))
	}
	neq := make([]KeyValue, 0, len(in.StringNotEquals))
	for _, kv := range in.StringNotEquals {
		neq = append(neq, KeyValue(kv))
	}

	return &ProtectedResourceConditions{StringEquals: eq, StringNotEquals: neq}
}

func protectedResourceConditionsToJSON(in *ProtectedResourceConditions) map[string]any {
	out := map[string]any{}
	if len(in.StringEquals) > 0 {
		out["StringEquals"] = in.StringEquals
	}
	if len(in.StringNotEquals) > 0 {
		out["StringNotEquals"] = in.StringNotEquals
	}

	return out
}

// restoreTestingSelectionDoc is the wire shape shared by
// RestoreTestingSelectionForCreate/-ForUpdate (both PUT the same JSON body
// shape; Create additionally requires ProtectedResourceType, which is
// immutable and absent from -ForUpdate).
type restoreTestingSelectionDoc struct {
	ProtectedResourceConditions *protectedResourceConditionsJSON `json:"ProtectedResourceConditions,omitempty"`
	RestoreMetadataOverrides    map[string]string                `json:"RestoreMetadataOverrides,omitempty"`
	RestoreTestingSelectionName string                           `json:"RestoreTestingSelectionName"`
	ProtectedResourceType       string                           `json:"ProtectedResourceType,omitempty"`
	IamRoleArn                  string                           `json:"IamRoleArn,omitempty"`
	ProtectedResourceArns       []string                         `json:"ProtectedResourceArns,omitempty"`
	ValidationWindowHours       int64                            `json:"ValidationWindowHours,omitempty"`
}

func (d restoreTestingSelectionDoc) toInput() RestoreTestingSelectionInput {
	return RestoreTestingSelectionInput{
		ProtectedResourceType:       d.ProtectedResourceType,
		IAMRoleArn:                  d.IamRoleArn,
		ProtectedResourceArns:       d.ProtectedResourceArns,
		ProtectedResourceConditions: protectedResourceConditionsFromJSON(d.ProtectedResourceConditions),
		RestoreMetadataOverrides:    d.RestoreMetadataOverrides,
		ValidationWindowHours:       d.ValidationWindowHours,
	}
}

// restoreTestingSelectionToJSON renders the fields of a
// RestoreTestingSelection this backend tracks, matching (a subset of) the
// real types.RestoreTestingSelectionForGet wire shape.
func restoreTestingSelectionToJSON(sel *RestoreTestingSelection) map[string]any {
	resp := map[string]any{
		keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
		keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
		"ProtectedResourceType":        sel.ProtectedResourceType,
		keyCreationTime:                epochSeconds(sel.CreationTime),
	}
	setOptionalStr(resp, "IamRoleArn", sel.IAMRoleArn)
	setOptionalStr(resp, keyRestoreTestingPlanArn, sel.RestoreTestingPlanArn)
	if len(sel.ProtectedResourceArns) > 0 {
		resp["ProtectedResourceArns"] = sel.ProtectedResourceArns
	}
	if len(sel.RestoreMetadataOverrides) > 0 {
		resp["RestoreMetadataOverrides"] = sel.RestoreMetadataOverrides
	}
	if sel.ValidationWindowHours > 0 {
		resp["ValidationWindowHours"] = sel.ValidationWindowHours
	}
	if sel.ProtectedResourceConditions != nil {
		resp["ProtectedResourceConditions"] = protectedResourceConditionsToJSON(sel.ProtectedResourceConditions)
	}

	return resp
}

type createRestoreTestingSelectionBody struct {
	CreatorRequestID        string                     `json:"CreatorRequestId,omitempty"`
	RestoreTestingSelection restoreTestingSelectionDoc `json:"RestoreTestingSelection"`
}

func (h *Handler) handleCreateRestoreTestingSelection(
	c *echo.Context,
	planName string,
	body []byte,
) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "RestoreTestingPlanName is required"),
		)
	}

	var in createRestoreTestingSelectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.RestoreTestingSelection.RestoreTestingSelectionName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "RestoreTestingSelectionName is required"),
		)
	}

	sel, err := h.Backend.CreateRestoreTestingSelection(
		planName,
		in.RestoreTestingSelection.RestoreTestingSelectionName,
		in.RestoreTestingSelection.toInput(),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	// Real AWS: responseCode 201.
	return c.JSON(http.StatusCreated, map[string]any{
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
			errResp("MissingParameterValueException", "RestoreTestingPlanName is required"),
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
			errResp("MissingParameterValueException", "RestoreTestingPlanName is required"),
		)
	}

	var in updateRestoreTestingPlanBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
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
			errResp("MissingParameterValueException", "RestoreTestingPlanName is required"),
		)
	}

	if err := h.Backend.DeleteRestoreTestingPlan(planName); err != nil {
		return h.handleError(c, err)
	}

	// Real AWS: responseCode 204.
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetRestoreTestingSelection(c *echo.Context, resource string) error {
	planName, selName, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	sel, err := h.Backend.GetRestoreTestingSelection(planName, selName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingSelection": restoreTestingSelectionToJSON(sel),
	})
}

func (h *Handler) handleListRestoreTestingSelections(c *echo.Context, planName string) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "RestoreTestingPlanName is required"),
		)
	}

	sels, err := h.Backend.ListRestoreTestingSelections(planName)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(sels))
	for _, sel := range sels {
		items = append(items, restoreTestingSelectionToJSON(sel))
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
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	var in updateRestoreTestingSelectionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	sel, err := h.Backend.UpdateRestoreTestingSelection(
		planName,
		selName,
		in.RestoreTestingSelection.toInput(),
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
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	if err := h.Backend.DeleteRestoreTestingSelection(planName, selName); err != nil {
		return h.handleError(c, err)
	}

	// Real AWS: responseCode 204.
	return c.NoContent(http.StatusNoContent)
}

// --- Framework read/update/delete handlers ---
