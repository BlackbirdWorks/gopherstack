package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- CloudWatch Alarm Template Group handlers ---

// toCWAlarmTemplateGroupOutput mirrors GetCloudWatchAlarmTemplateGroupOutput/
// CreateCloudWatchAlarmTemplateGroupOutput/
// UpdateCloudWatchAlarmTemplateGroupOutput. The real API's own
// Get/Create/Update responses do NOT include "templateCount" -- only the
// List response's CloudWatchAlarmTemplateGroupSummary shape does (verified
// against the SDK deserializer) -- see
// toCWAlarmTemplateGroupSummaryOutput for List's shape.
func toCWAlarmTemplateGroupOutput(g *CloudWatchAlarmTemplateGroup) map[string]any {
	tags := g.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: g.Arn, keyID: g.ID, keyName: g.Name, keyDescription: g.Description,
		keyCreatedAt: formatISO8601(g.CreatedAt), keyModifiedAt: formatISO8601(g.ModifiedAt),
		keyTags: tags,
	}
}

// toCWAlarmTemplateGroupSummaryOutput mirrors
// ListCloudWatchAlarmTemplateGroupsOutput's per-item Summary shape, which
// adds "templateCount" on top of every field toCWAlarmTemplateGroupOutput
// already emits.
func toCWAlarmTemplateGroupSummaryOutput(g *CloudWatchAlarmTemplateGroupSummary) map[string]any {
	out := toCWAlarmTemplateGroupOutput(&g.CloudWatchAlarmTemplateGroup)
	out["templateCount"] = g.TemplateCount

	return out
}

func (h *Handler) handleCreateCWAlarmTemplateGroup(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	tags := extractTags(body)
	g, err := h.Backend.CreateCloudWatchAlarmTemplateGroup(name, description, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toCWAlarmTemplateGroupOutput(g))
}

func (h *Handler) handleGetCWAlarmTemplateGroup(c *echo.Context, identifier string) error {
	g, err := h.Backend.GetCloudWatchAlarmTemplateGroup(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateGroupOutput(g))
}

func (h *Handler) handleListCWAlarmTemplateGroups(c *echo.Context) error {
	maxResults, nextTokenParam := paginationParams(c)
	items, nextToken, err := h.Backend.ListCloudWatchAlarmTemplateGroups(maxResults, nextTokenParam)
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, g := range items {
		out = append(out, toCWAlarmTemplateGroupSummaryOutput(g))
	}
	resp := map[string]any{"cloudWatchAlarmTemplateGroups": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateCWAlarmTemplateGroup(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	g, err := h.Backend.UpdateCloudWatchAlarmTemplateGroup(identifier, name, description)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateGroupOutput(g))
}

func (h *Handler) handleDeleteCWAlarmTemplateGroup(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteCloudWatchAlarmTemplateGroup(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- CloudWatch Alarm Template handlers ---

// toCWAlarmTemplateOutput mirrors GetCloudWatchAlarmTemplateOutput/
// CreateCloudWatchAlarmTemplateOutput/UpdateCloudWatchAlarmTemplateOutput
// exactly, including createdAt/modifiedAt (ISO8601 strings, see
// formatISO8601's doc comment). "namespace" and "groupIdentifier" are
// intentionally omitted: neither is a real field on this shape (verified
// against the SDK deserializer -- only "groupId" is returned).
func toCWAlarmTemplateOutput(t *CloudWatchAlarmTemplate) map[string]any {
	tags := t.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: t.Arn, keyID: t.ID, keyName: t.Name, keyDescription: t.Description,
		keyGroupID:   t.GroupID,
		"metricName": t.MetricName,
		"statistic":  t.Statistic, "comparisonOperator": t.ComparisonOperator,
		"targetResourceType": t.TargetResourceType, "treatMissingData": t.TreatMissingData,
		"threshold": t.Threshold, "evaluationPeriods": t.EvaluationPeriods,
		"datapointsToAlarm": t.DatapointsToAlarm, "period": t.Period,
		keyCreatedAt: formatISO8601(t.CreatedAt), keyModifiedAt: formatISO8601(t.ModifiedAt),
		keyTags: tags,
	}
}

func extractCWAlarmTemplateFields(
	body map[string]any,
) (string, string, string, string, string, string, string, float64, int32, int32, int32) {
	groupIdentifier, _ := body["groupIdentifier"].(string)
	metricName, _ := body["metricName"].(string)
	namespace, _ := body["namespace"].(string)
	statistic, _ := body["statistic"].(string)
	comparisonOperator, _ := body["comparisonOperator"].(string)
	targetResourceType, _ := body["targetResourceType"].(string)
	treatMissingData, _ := body["treatMissingData"].(string)
	var threshold float64
	if v, ok := body["threshold"].(float64); ok {
		threshold = v
	}
	var evalPeriods int32
	if v, ok := body["evaluationPeriods"].(float64); ok {
		evalPeriods = int32(v)
	}
	var datapointsToAlarm int32
	if v, ok := body["datapointsToAlarm"].(float64); ok {
		datapointsToAlarm = int32(v)
	}
	var period int32
	if v, ok := body["period"].(float64); ok {
		period = int32(v)
	}

	return groupIdentifier, metricName, namespace, statistic, comparisonOperator,
		targetResourceType, treatMissingData, threshold, evalPeriods, datapointsToAlarm, period
}

func (h *Handler) handleCreateCWAlarmTemplate(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	tags := extractTags(body)
	groupID, metricName, namespace, statistic, compOp,
		targetType, treatMissing, threshold,
		evalPeriods, datapointsToAlarm, period :=
		extractCWAlarmTemplateFields(body)
	t, err := h.Backend.CreateCloudWatchAlarmTemplate(
		name,
		description,
		groupID,
		metricName,
		namespace,
		statistic,
		compOp,
		targetType,
		treatMissing,
		threshold,
		evalPeriods,
		datapointsToAlarm,
		period,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toCWAlarmTemplateOutput(t))
}

func (h *Handler) handleGetCWAlarmTemplate(c *echo.Context, identifier string) error {
	t, err := h.Backend.GetCloudWatchAlarmTemplate(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateOutput(t))
}

func (h *Handler) handleListCWAlarmTemplates(c *echo.Context) error {
	maxResults, nextTokenParam := paginationParams(c)
	items, nextToken, err := h.Backend.ListCloudWatchAlarmTemplates(maxResults, nextTokenParam)
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, toCWAlarmTemplateOutput(t))
	}
	resp := map[string]any{"cloudWatchAlarmTemplates": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateCWAlarmTemplate(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	groupID, metricName, namespace, statistic, compOp,
		targetType, treatMissing, threshold,
		evalPeriods, datapointsToAlarm, period :=
		extractCWAlarmTemplateFields(body)
	t, err := h.Backend.UpdateCloudWatchAlarmTemplate(
		identifier,
		name,
		description,
		groupID,
		metricName,
		namespace,
		statistic,
		compOp,
		targetType,
		treatMissing,
		threshold,
		evalPeriods,
		datapointsToAlarm,
		period,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toCWAlarmTemplateOutput(t))
}

func (h *Handler) handleDeleteCWAlarmTemplate(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteCloudWatchAlarmTemplate(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
