package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- EventBridge Rule Template Group handlers ---

// toEBRuleTemplateGroupOutput mirrors GetEventBridgeRuleTemplateGroupOutput/
// CreateEventBridgeRuleTemplateGroupOutput/
// UpdateEventBridgeRuleTemplateGroupOutput -- same "no templateCount on the
// non-list shapes" nuance as toCWAlarmTemplateGroupOutput above (List uses
// toEBRuleTemplateGroupSummaryOutput instead).
func toEBRuleTemplateGroupOutput(g *EventBridgeRuleTemplateGroup) map[string]any {
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

// toEBRuleTemplateGroupSummaryOutput mirrors
// ListEventBridgeRuleTemplateGroupsOutput's per-item Summary shape, adding
// "templateCount" on top of toEBRuleTemplateGroupOutput.
func toEBRuleTemplateGroupSummaryOutput(g *EventBridgeRuleTemplateGroupSummary) map[string]any {
	out := toEBRuleTemplateGroupOutput(&g.EventBridgeRuleTemplateGroup)
	out["templateCount"] = g.TemplateCount

	return out
}

func (h *Handler) handleCreateEBRuleTemplateGroup(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	tags := extractTags(body)
	g, err := h.Backend.CreateEventBridgeRuleTemplateGroup(name, description, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toEBRuleTemplateGroupOutput(g))
}

func (h *Handler) handleGetEBRuleTemplateGroup(c *echo.Context, identifier string) error {
	g, err := h.Backend.GetEventBridgeRuleTemplateGroup(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateGroupOutput(g))
}

func (h *Handler) handleListEBRuleTemplateGroups(c *echo.Context) error {
	maxResults, nextTokenParam := paginationParams(c)
	signalMapIdentifier := c.QueryParam("signalMapIdentifier")
	items, nextToken, err := h.Backend.ListEventBridgeRuleTemplateGroups(
		maxResults,
		nextTokenParam,
		signalMapIdentifier,
	)
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, g := range items {
		out = append(out, toEBRuleTemplateGroupSummaryOutput(g))
	}
	resp := map[string]any{"eventBridgeRuleTemplateGroups": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateEBRuleTemplateGroup(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	g, err := h.Backend.UpdateEventBridgeRuleTemplateGroup(identifier, name, description)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateGroupOutput(g))
}

func (h *Handler) handleDeleteEBRuleTemplateGroup(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteEventBridgeRuleTemplateGroup(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- EventBridge Rule Template handlers ---

// toEBRuleTemplateOutput mirrors GetEventBridgeRuleTemplateOutput/
// CreateEventBridgeRuleTemplateOutput/UpdateEventBridgeRuleTemplateOutput
// exactly, including createdAt/modifiedAt. "groupIdentifier" is
// intentionally omitted: it isn't a real field on this shape (only
// "groupId" is returned; verified against the SDK deserializer).
func toEBRuleTemplateOutput(t *EventBridgeRuleTemplate) map[string]any {
	tags := t.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	targets := make([]map[string]any, 0, len(t.EventTargets))
	for _, tgt := range t.EventTargets {
		targets = append(targets, map[string]any{keyArn: tgt.Arn})
	}

	return map[string]any{
		keyArn: t.Arn, keyID: t.ID, keyName: t.Name, keyDescription: t.Description,
		keyGroupID:     t.GroupID,
		"eventType":    t.EventType,
		"eventTargets": targets,
		keyCreatedAt:   formatISO8601(t.CreatedAt), keyModifiedAt: formatISO8601(t.ModifiedAt),
		keyTags: tags,
	}
}

// toEBRuleTemplateSummaryOutput mirrors ListEventBridgeRuleTemplatesOutput's
// per-item Summary shape: "eventTargetCount" (an integer) instead of the
// full "eventTargets" array that toEBRuleTemplateOutput emits (verified
// against aws-sdk-go-v2/service/medialive's EventBridgeRuleTemplateSummary
// type).
func toEBRuleTemplateSummaryOutput(t *EventBridgeRuleTemplateSummary) map[string]any {
	tags := t.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: t.Arn, keyID: t.ID, keyName: t.Name, keyDescription: t.Description,
		keyGroupID:         t.GroupID,
		"eventType":        t.EventType,
		"eventTargetCount": t.EventTargetCount,
		keyCreatedAt:       formatISO8601(t.CreatedAt), keyModifiedAt: formatISO8601(t.ModifiedAt),
		keyTags: tags,
	}
}

func extractEBTargets(body map[string]any) []EventBridgeRuleTemplateTarget {
	raw, _ := body["eventTargets"].([]any)
	targets := make([]EventBridgeRuleTemplateTarget, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		arnVal, _ := m[keyArn].(string)
		if arnVal != "" {
			targets = append(targets, EventBridgeRuleTemplateTarget{Arn: arnVal})
		}
	}

	return targets
}

func (h *Handler) handleCreateEBRuleTemplate(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	groupIdentifier, _ := body["groupIdentifier"].(string)
	eventType, _ := body["eventType"].(string)
	targets := extractEBTargets(body)
	tags := extractTags(body)
	t, err := h.Backend.CreateEventBridgeRuleTemplate(
		name,
		description,
		groupIdentifier,
		eventType,
		targets,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toEBRuleTemplateOutput(t))
}

func (h *Handler) handleGetEBRuleTemplate(c *echo.Context, identifier string) error {
	t, err := h.Backend.GetEventBridgeRuleTemplate(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateOutput(t))
}

func (h *Handler) handleListEBRuleTemplates(c *echo.Context) error {
	maxResults, nextTokenParam := paginationParams(c)
	groupIdentifier := c.QueryParam("groupIdentifier")
	signalMapIdentifier := c.QueryParam("signalMapIdentifier")
	items, nextToken, err := h.Backend.ListEventBridgeRuleTemplates(
		maxResults, nextTokenParam, groupIdentifier, signalMapIdentifier,
	)
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, toEBRuleTemplateSummaryOutput(t))
	}
	resp := map[string]any{"eventBridgeRuleTemplates": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateEBRuleTemplate(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	groupIdentifier, _ := body["groupIdentifier"].(string)
	eventType, _ := body["eventType"].(string)
	targets := extractEBTargets(body)
	t, err := h.Backend.UpdateEventBridgeRuleTemplate(
		identifier,
		name,
		description,
		groupIdentifier,
		eventType,
		targets,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toEBRuleTemplateOutput(t))
}

func (h *Handler) handleDeleteEBRuleTemplate(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteEventBridgeRuleTemplate(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
