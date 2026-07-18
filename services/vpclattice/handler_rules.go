package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- Rule handlers -------

func (h *Handler) handleCreateRule(
	c *echo.Context,
	serviceID, listenerID string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: keyNameRequired})
	}

	priority := bodyInt32(body, keyPriority)
	action := extractRuleAction(body, "action")
	match := extractRuleMatch(body, "match")
	tags := extractTags(body)

	r, err := h.Backend.CreateRule(serviceID, listenerID, name, priority, action, match, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, ruleToJSON(r))
}

func (h *Handler) handleGetRule(c *echo.Context, serviceID, listenerID, ruleID string) error {
	r, err := h.Backend.GetRule(serviceID, listenerID, ruleID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, ruleToJSON(r))
}

func (h *Handler) handleUpdateRule(
	c *echo.Context,
	serviceID, listenerID, ruleID string,
	body map[string]any,
) error {
	priority := bodyInt32(body, keyPriority)
	action := extractRuleAction(body, "action")
	match := extractRuleMatch(body, "match")

	r, err := h.Backend.UpdateRule(serviceID, listenerID, ruleID, priority, action, match)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, ruleToJSON(r))
}

func (h *Handler) handleDeleteRule(c *echo.Context, serviceID, listenerID, ruleID string) error {
	if err := h.Backend.DeleteRule(serviceID, listenerID, ruleID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListRules(c *echo.Context, serviceID, listenerID string) error {
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListRules(serviceID, listenerID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, r := range items {
		summaries = append(summaries, ruleSummaryToJSON(r))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchUpdateRule(
	c *echo.Context,
	serviceID, listenerID string,
	body map[string]any,
) error {
	var updates []*RuleUpdate

	if rawUpdates, ok := body["rules"].([]any); ok {
		for _, raw := range rawUpdates {
			if m, ok2 := raw.(map[string]any); ok2 {
				u := &RuleUpdate{}
				u.RuleIdentifier, _ = m["ruleIdentifier"].(string)
				u.Priority = bodyInt32(m, keyPriority)
				u.Action = extractRuleAction(m, "action")
				u.Match = extractRuleMatch(m, "match")
				updates = append(updates, u)
			}
		}
	}

	successes, failures, err := h.Backend.BatchUpdateRule(serviceID, listenerID, updates)
	if err != nil {
		return h.handleError(c, err)
	}

	successList := make([]any, 0, len(successes))
	for _, s := range successes {
		successList = append(successList, ruleUpdateSuccessToJSON(s))
	}

	failureList := make([]any, 0, len(failures))
	for _, f := range failures {
		failureList = append(failureList, map[string]any{
			"ruleIdentifier": f.RuleIdentifier,
			"failureMessage": f.Message,
			"failureCode":    f.Code,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySuccessful:   successList,
		keyUnsuccessful: failureList,
	})
}

// ------- Rule JSON serialization -------

func ruleToJSON(r *Rule) map[string]any {
	m := map[string]any{
		keyARN:           r.ARN,
		"id":             r.ID,
		keyName:          r.Name,
		keyPriority:      r.Priority,
		keyIsDefault:     r.IsDefault,
		keyCreatedAt:     r.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt: r.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if r.Action != nil {
		m["action"] = ruleActionToJSON(r.Action)
	}

	if r.Match != nil {
		m["match"] = ruleMatchToJSON(r.Match)
	}

	return m
}

func ruleSummaryToJSON(r *RuleSummary) map[string]any {
	return map[string]any{
		keyARN:           r.ARN,
		"id":             r.ID,
		keyName:          r.Name,
		keyPriority:      r.Priority,
		keyIsDefault:     r.IsDefault,
		keyCreatedAt:     r.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt: r.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func ruleUpdateSuccessToJSON(r *RuleUpdateSuccess) map[string]any {
	m := map[string]any{
		keyARN:       r.ARN,
		"id":         r.ID,
		keyName:      r.Name,
		keyPriority:  r.Priority,
		keyIsDefault: r.IsDefault,
	}

	if r.Action != nil {
		m["action"] = ruleActionToJSON(r.Action)
	}

	if r.Match != nil {
		m["match"] = ruleMatchToJSON(r.Match)
	}

	return m
}

func ruleActionToJSON(a *RuleAction) map[string]any {
	if a.IsFixedResponse {
		return map[string]any{
			"fixedResponse": map[string]any{
				"statusCode": a.FixedResponseStatusCode,
			},
		}
	}

	wts := make([]any, 0, len(a.ForwardTargetGroups))
	for _, w := range a.ForwardTargetGroups {
		wts = append(wts, map[string]any{
			"targetGroupIdentifier": w.TargetGroupID,
			"weight":                w.Weight,
		})
	}

	return map[string]any{
		"forward": map[string]any{
			"targetGroups": wts,
		},
	}
}

func ruleMatchToJSON(m *RuleMatch) map[string]any {
	if m == nil {
		return nil
	}

	httpMatch := map[string]any{}
	if m.HTTPMethod != "" {
		httpMatch["method"] = m.HTTPMethod
	}

	if m.PathMatchType != "" {
		pathMatch := map[string]any{
			"match": map[string]any{
				m.PathMatchType: m.PathMatchValue,
			},
		}
		httpMatch["path"] = pathMatch
	}

	if len(m.HeaderMatches) > 0 {
		headers := make([]any, 0, len(m.HeaderMatches))
		for _, h := range m.HeaderMatches {
			headers = append(headers, map[string]any{
				keyName: h.Name,
				"match": map[string]any{h.MatchType: h.MatchValue},
			})
		}

		httpMatch["headerMatches"] = headers
	}

	return map[string]any{"httpMatch": httpMatch}
}

// ------- Rule body extraction helpers -------

func extractRuleAction(body map[string]any, key string) *RuleAction {
	raw, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}

	action := &RuleAction{}

	if fr, ok2 := raw["fixedResponse"].(map[string]any); ok2 {
		action.IsFixedResponse = true
		action.FixedResponseStatusCode = bodyInt32(fr, "statusCode")

		return action
	}

	if fwd, ok2 := raw["forward"].(map[string]any); ok2 {
		if tgs, ok3 := fwd["targetGroups"].([]any); ok3 {
			for _, tgRaw := range tgs {
				if tgMap, ok4 := tgRaw.(map[string]any); ok4 {
					wt := &WeightedTargetGroup{}
					wt.TargetGroupID, _ = tgMap["targetGroupIdentifier"].(string)
					wt.Weight = bodyInt32(tgMap, "weight")
					action.ForwardTargetGroups = append(action.ForwardTargetGroups, wt)
				}
			}
		}
	}

	return action
}

func extractRuleMatch(body map[string]any, key string) *RuleMatch {
	raw, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}

	match := &RuleMatch{}

	httpMatch, ok := raw["httpMatch"].(map[string]any)
	if !ok {
		return match
	}

	match.HTTPMethod, _ = httpMatch["method"].(string)
	match.PathMatchType, match.PathMatchValue = extractPathMatch(httpMatch)
	match.HeaderMatches = extractHeaderMatches(httpMatch)

	return match
}

// extractPathMatch pulls the path match type/value (e.g. "exact": "/api")
// out of an httpMatch body. If multiple keys are present in the match
// object, the last one encountered wins -- callers are expected to send
// exactly one.
func extractPathMatch(httpMatch map[string]any) (string, string) {
	pathRaw, isMap := httpMatch["path"].(map[string]any)
	if !isMap {
		return "", ""
	}

	matchRaw, isMap := pathRaw["match"].(map[string]any)
	if !isMap {
		return "", ""
	}

	var pathMatchType, pathMatchValue string

	for k, v := range matchRaw {
		if s, isString := v.(string); isString {
			pathMatchType = k
			pathMatchValue = s
		}
	}

	return pathMatchType, pathMatchValue
}

// extractHeaderMatches pulls the headerMatches list out of an httpMatch body.
func extractHeaderMatches(httpMatch map[string]any) []*HeaderMatch {
	headersRaw, isSlice := httpMatch["headerMatches"].([]any)
	if !isSlice {
		return nil
	}

	var headers []*HeaderMatch

	for _, hRaw := range headersRaw {
		hMap, isMap := hRaw.(map[string]any)
		if !isMap {
			continue
		}

		hm := &HeaderMatch{}
		hm.Name, _ = hMap[keyName].(string)

		if matchRaw, hasMatch := hMap["match"].(map[string]any); hasMatch {
			for k, v := range matchRaw {
				if s, isString := v.(string); isString {
					hm.MatchType = k
					hm.MatchValue = s
				}
			}
		}

		headers = append(headers, hm)
	}

	return headers
}
