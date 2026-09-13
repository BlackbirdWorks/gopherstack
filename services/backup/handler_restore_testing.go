package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// restoreTestingRecoveryPointSelectionJSON is the wire shape of
// types.RestoreTestingRecoveryPointSelection. None of its members are
// individually Smithy-required (only the RecoveryPointSelection pointer
// itself is, on RestoreTestingPlanForGet/-ForCreate), so a real client can
// send an empty object and every field here stays optional.
type restoreTestingRecoveryPointSelectionJSON struct {
	Algorithm           string   `json:"Algorithm,omitempty"`
	ExcludeVaults       []string `json:"ExcludeVaults,omitempty"`
	IncludeVaults       []string `json:"IncludeVaults,omitempty"`
	RecoveryPointTypes  []string `json:"RecoveryPointTypes,omitempty"`
	SelectionWindowDays int32    `json:"SelectionWindowDays,omitempty"`
}

func (j *restoreTestingRecoveryPointSelectionJSON) toModel() *RestoreTestingRecoveryPointSelection {
	if j == nil {
		return nil
	}

	return &RestoreTestingRecoveryPointSelection{
		Algorithm:           j.Algorithm,
		ExcludeVaults:       j.ExcludeVaults,
		IncludeVaults:       j.IncludeVaults,
		RecoveryPointTypes:  j.RecoveryPointTypes,
		SelectionWindowDays: j.SelectionWindowDays,
	}
}

func restoreTestingRecoveryPointSelectionToJSON(sel *RestoreTestingRecoveryPointSelection) map[string]any {
	if sel == nil {
		sel = &RestoreTestingRecoveryPointSelection{}
	}
	out := map[string]any{}
	setOptionalStr(out, "Algorithm", sel.Algorithm)
	if len(sel.ExcludeVaults) > 0 {
		out["ExcludeVaults"] = sel.ExcludeVaults
	}
	if len(sel.IncludeVaults) > 0 {
		out["IncludeVaults"] = sel.IncludeVaults
	}
	if len(sel.RecoveryPointTypes) > 0 {
		out["RecoveryPointTypes"] = sel.RecoveryPointTypes
	}
	if sel.SelectionWindowDays > 0 {
		out["SelectionWindowDays"] = sel.SelectionWindowDays
	}

	return out
}

type restoreTestingPlanDoc struct {
	RecoveryPointSelection *restoreTestingRecoveryPointSelectionJSON `json:"RecoveryPointSelection,omitempty"`
	RestoreTestingPlanName string                                    `json:"RestoreTestingPlanName"`
	ScheduleExpression     string                                    `json:"ScheduleExpression,omitempty"`
	StartWindowHours       int64                                     `json:"StartWindowHours,omitempty"`
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
		in.RestoreTestingPlan.RecoveryPointSelection.toModel(),
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
	// IamRoleArn is required on both RestoreTestingSelectionForGet and
	// RestoreTestingSelectionForList (types.go), but RestoreTestingSelectionForUpdate
	// leaves it optional -- an Update that omits it clears sel.IAMRoleArn to "", so
	// this must stay present-and-empty rather than dropped (parity-principles.md's
	// required-but-inapplicable rule).
	resp := map[string]any{
		keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
		keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
		"ProtectedResourceType":        sel.ProtectedResourceType,
		"IamRoleArn":                   sel.IAMRoleArn,
		keyCreationTime:                epochSeconds(sel.CreationTime),
	}
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
		"RecoveryPointSelection":  restoreTestingRecoveryPointSelectionToJSON(rtp.RecoveryPointSelection),
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
	q := c.Request().URL.Query()
	plans, nextToken := h.Backend.ListRestoreTestingPlans(parseInt(q.Get("MaxResults")), q.Get("NextToken"))
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

	resp := map[string]any{"RestoreTestingPlans": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
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
		in.RestoreTestingPlan.RecoveryPointSelection.toModel(),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
		keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
		keyCreationTime:           epochSeconds(rtp.CreationTime),
	}
	if rtp.UpdateTime != nil {
		resp["UpdateTime"] = epochSeconds(*rtp.UpdateTime)
	}

	return c.JSON(http.StatusOK, resp)
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

	q := c.Request().URL.Query()

	sels, nextToken, err := h.Backend.ListRestoreTestingSelections(
		planName, parseInt(q.Get("MaxResults")), q.Get("NextToken"),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(sels))
	for _, sel := range sels {
		items = append(items, restoreTestingSelectionToJSON(sel))
	}

	resp := map[string]any{"RestoreTestingSelections": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
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

	resp := map[string]any{
		keyRestoreTestingPlanArn:       sel.RestoreTestingPlanArn,
		keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
		keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
		keyCreationTime:                epochSeconds(sel.CreationTime),
	}
	if sel.UpdateTime != nil {
		resp["UpdateTime"] = epochSeconds(*sel.UpdateTime)
	}

	return c.JSON(http.StatusOK, resp)
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

// handleGetRestoreTestingInferredMetadata serves GetRestoreTestingInferredMetadata
// (GET /restore-testing/inferred-metadata, BackupVaultName/RecoveryPointArn bound as
// query params -- serializers.go:4516-4534, backup@v1.64.0). Real AWS derives a set of
// restore parameter defaults from the target recovery point; this backend has no
// restore-parameter-inference engine, so per the no-fabrication rule the only key
// populated is ResourceType, honestly sourced from the recovery point this backend
// already tracks -- the rest of the real key set stays undisclosed rather than
// invented. Before this pass the op never validated the vault/recovery point existed
// at all and always returned an empty map regardless of input.
func (h *Handler) handleGetRestoreTestingInferredMetadata(c *echo.Context) error {
	q := c.Request().URL.Query()

	vaultName := q.Get(keyBackupVaultName)
	recoveryPointArn := q.Get(keyRecoveryPointArn)

	switch {
	case vaultName == "":
		return c.JSON(http.StatusBadRequest, errResp("MissingParameterValueException", "BackupVaultName is required"))
	case recoveryPointArn == "":
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "RecoveryPointArn is required"),
		)
	}

	rp, err := h.Backend.DescribeRecoveryPoint(vaultName, recoveryPointArn)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ResourceNotFoundException", err.Error()))
	}

	metadata := map[string]string{}
	if rp.ResourceType != "" {
		metadata["ResourceType"] = rp.ResourceType
	}

	return c.JSON(http.StatusOK, map[string]any{"InferredMetadata": metadata})
}

// --- Framework read/update/delete handlers ---
