package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyAutomationRulesPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/automationrules/list":
		return opListAutomationRules, ""
	case method == http.MethodPost && path == "/automationrules/create":
		return opCreateAutomationRule, ""
	case method == http.MethodPost && path == "/automationrules/get":
		return opBatchGetAutomationRules, ""
	case method == http.MethodPost && path == "/automationrules/delete":
		return opBatchDeleteAutoRules, ""
	case method == http.MethodPatch && path == "/automationrules/update":
		return opBatchUpdateAutoRules, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleListAutomationRules(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	rules, nextOut := h.Backend.ListAutomationRules(nextToken, maxResults)
	items := make([]map[string]any, len(rules))

	for i, r := range rules {
		items[i] = map[string]any{
			keyRuleArn:     r.RuleArn,
			"RuleStatus":   r.RuleStatus, //nolint:goconst // existing issue.
			"RuleOrder":    r.RuleOrder,  //nolint:goconst // existing issue.
			"RuleName":     r.RuleName,   //nolint:goconst // existing issue.
			keyDescription: r.Description,
			"IsTerminal":   r.IsTerminal,
			keyCreatedAt:   r.CreatedAt,
			keyUpdatedAt:   r.UpdatedAt,
			keyCreatedBy:   r.CreatedBy,
		}
	}

	resp := map[string]any{"AutomationRulesMetadata": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCreateAutomationRule(c *echo.Context, body map[string]any) error {
	ruleArn, createdAt := h.Backend.CreateAutomationRule(body)

	ruleName, _ := body["RuleName"].(string)
	ruleStatus, _ := body["RuleStatus"].(string)

	if ruleStatus == "" {
		ruleStatus = statusEnabled
	}

	ruleOrder := float64(0)
	if ro, ok := body["RuleOrder"].(float64); ok {
		ruleOrder = ro
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRuleArn:   ruleArn,
		keyCreatedAt: createdAt,
		"RuleStatus": ruleStatus,
		"RuleOrder":  ruleOrder,
		"RuleName":   ruleName,
	})
}

func (h *Handler) handleBatchGetAutomationRules(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["AutomationRulesArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	rules, unprocessed := h.Backend.BatchGetAutomationRules(arns)
	items := make([]map[string]any, len(rules))

	for i, r := range rules {
		items[i] = map[string]any{
			keyRuleArn:     r.RuleArn,
			"RuleStatus":   r.RuleStatus,
			"RuleOrder":    r.RuleOrder,
			"RuleName":     r.RuleName,
			keyDescription: r.Description,
			"IsTerminal":   r.IsTerminal,
			keyCreatedAt:   r.CreatedAt,
			keyUpdatedAt:   r.UpdatedAt,
			keyCreatedBy:   r.CreatedBy,
			"Criteria":     r.Criteria,
			"Actions":      r.Actions,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Rules":                 items,
		keyUnprocessedAutoRules: unprocessed,
	})
}

func (h *Handler) handleBatchDeleteAutomationRules(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["AutomationRulesArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	deleted, unprocessed := h.Backend.BatchDeleteAutomationRules(arns)

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedAutomationRules": deleted,
		keyUnprocessedAutoRules:    unprocessed,
	})
}

func (h *Handler) handleBatchUpdateAutomationRules(c *echo.Context, body map[string]any) error {
	rawUpdates, _ := body["UpdateAutomationRulesRequestItems"].([]any)

	var updates []map[string]any

	for _, u := range rawUpdates {
		if m, ok := u.(map[string]any); ok {
			updates = append(updates, m)
		}
	}

	processed, unprocessed := h.Backend.BatchUpdateAutomationRules(updates)

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedAutomationRules": processed,
		keyUnprocessedAutoRules:    unprocessed,
	})
}

// automationRuleV2IdentifierFromPath returns the trailing identifier of an
// /automationrulesv2/{id} path, excluding the create/list sub-paths which are
// handled as their own exact-match routes.
func automationRuleV2IdentifierFromPath(path string) (string, bool) {
	const prefix = "/automationrulesv2/"

	if !strings.HasPrefix(path, prefix) || path == prefix+"create" || path == prefix+"list" {
		return "", false
	}

	return strings.TrimPrefix(path, prefix), true
}

func classifyAutomationRulesV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/automationrulesv2/create":
		return opCreateAutomationRuleV2, ""
	case method == http.MethodGet && path == "/automationrulesv2/list":
		return opListAutomationRulesV2, ""
	}

	id, ok := automationRuleV2IdentifierFromPath(path)
	if !ok {
		return opUnknown, ""
	}

	switch method {
	case http.MethodGet:
		return opGetAutomationRuleV2, id
	case http.MethodPatch:
		return opUpdateAutomationRuleV2, id
	case http.MethodDelete:
		return opDeleteAutomationRuleV2, id
	default:
		return opUnknown, ""
	}
}

// handleCreateAutomationRuleV2 does not read an "IsTerminal" field: the real
// CreateAutomationRuleV2Input has no such member at all
// (securityhub@v1.75.4 api_op_CreateAutomationRuleV2.go) -- that concept only
// exists on the V1 AutomationRule shape (types.AutomationRulesMetadata), and
// a prior version of this handler had carried it over by mistake.
func (h *Handler) handleCreateAutomationRuleV2(c *echo.Context, body map[string]any) error {
	ruleName, _ := body["RuleName"].(string)
	ruleStatus, _ := body["RuleStatus"].(string)
	description, _ := body["Description"].(string)
	ruleOrder, _ := body["RuleOrder"].(float64)

	var criteria map[string]any

	if cr, ok := body["Criteria"].(map[string]any); ok {
		criteria = cr
	}

	var actions []map[string]any

	if raw, ok := body["Actions"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok { //nolint:govet // existing issue.
				actions = append(actions, m)
			}
		}
	}

	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if ruleStatus == "" {
		ruleStatus = statusEnabled
	}

	rule, err := h.Backend.CreateAutomationRuleV2(
		ruleName,
		ruleStatus,
		description,
		criteria,
		actions,
		ruleOrder,
		tags,
	)
	if err != nil {
		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, automationRuleV2ToResponse(rule))
}

func (h *Handler) handleGetAutomationRuleV2(c *echo.Context, identifier string) error {
	rule, err := h.Backend.GetAutomationRuleV2(identifier)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(
				c, http.StatusNotFound, "ResourceNotFoundException",
				"Automation rule V2 not found",
			)
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, automationRuleV2ToResponse(rule))
}

func (h *Handler) handleListAutomationRulesV2(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	rules, next := h.Backend.ListAutomationRulesV2(nextToken, maxResults)

	var out []map[string]any //nolint:prealloc // existing issue.

	for _, rule := range rules {
		out = append(out, automationRuleV2ToResponse(rule))
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"Rules": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateAutomationRuleV2(c *echo.Context, identifier string, body map[string]any) error {
	rule, err := h.Backend.UpdateAutomationRuleV2(identifier, body)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(
				c,
				http.StatusNotFound,
				"ResourceNotFoundException",
				"Automation rule V2 not found",
			)
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, automationRuleV2ToResponse(rule))
}

func (h *Handler) handleDeleteAutomationRuleV2(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteAutomationRuleV2(identifier); err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(
				c,
				http.StatusNotFound,
				"ResourceNotFoundException",
				"Automation rule V2 not found",
			)
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// automationRuleV2ToResponse renders the wire fields shared by
// CreateAutomationRuleV2Output/GetAutomationRuleV2Output/
// ListAutomationRulesV2Output's per-item AutomationRulesMetadataV2/
// UpdateAutomationRuleV2's echoed body (securityhub@v1.75.4
// api_op_GetAutomationRuleV2.go, types.go:905-935). The real key is
// "RuleId", not "Identifier" -- a real client's typed field was always
// empty regardless of backend state. "IsTerminal" is not emitted at all:
// it is a V1 AutomationRulesMetadata-only member (types.go:872), absent
// from every V2 automation-rule shape; this function previously carried it
// over from the V1 response builder by mistake.
func automationRuleV2ToResponse(rule *AutomationRuleV2) map[string]any {
	return map[string]any{
		"RuleId":       rule.Identifier,
		"RuleArn":      rule.RuleArn,
		"RuleName":     rule.RuleName,
		"RuleStatus":   rule.RuleStatus,
		keyDescription: rule.Description,
		keyCreatedAt:   rule.CreatedAt,
		keyUpdatedAt:   rule.UpdatedAt,
		"Criteria":     rule.Criteria,
		"Actions":      rule.Actions,
		"RuleOrder":    rule.RuleOrder,
	}
}

// automationRulesOpHandlers returns the Automation Rules (V1 + V2)
// operation dispatch table for handleREST.
func (h *Handler) automationRulesOpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opListAutomationRules:     func() error { return h.handleListAutomationRules(c) },
		opCreateAutomationRule:    func() error { return h.handleCreateAutomationRule(c, body) },
		opBatchGetAutomationRules: func() error { return h.handleBatchGetAutomationRules(c, body) },
		opBatchDeleteAutoRules:    func() error { return h.handleBatchDeleteAutomationRules(c, body) },
		opBatchUpdateAutoRules:    func() error { return h.handleBatchUpdateAutomationRules(c, body) },
		opCreateAutomationRuleV2:  func() error { return h.handleCreateAutomationRuleV2(c, body) },
		opGetAutomationRuleV2:     func() error { return h.handleGetAutomationRuleV2(c, resource) },
		opListAutomationRulesV2:   func() error { return h.handleListAutomationRulesV2(c) },
		opUpdateAutomationRuleV2:  func() error { return h.handleUpdateAutomationRuleV2(c, resource, body) },
		opDeleteAutomationRuleV2:  func() error { return h.handleDeleteAutomationRuleV2(c, resource) },
	}
}
