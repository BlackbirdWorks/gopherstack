package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// --- Classify functions ---

func classifyHubV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/hubv2":
		return opEnableSecurityHubV2, ""
	case method == http.MethodDelete && path == "/hubv2":
		return opDisableSecurityHubV2, ""
	case method == http.MethodGet && path == "/hubv2":
		return opDescribeSecurityHubV2, ""
	}

	return opUnknown, ""
}

func classifyAggregatorV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/aggregatorv2/create":
		return opCreateAggregatorV2, ""
	case method == http.MethodGet && strings.HasPrefix(path, "/aggregatorv2/get/"):
		return opGetAggregatorV2, strings.TrimPrefix(path, "/aggregatorv2/get/")
	case method == http.MethodGet && path == "/aggregatorv2/list":
		return opListAggregatorsV2, ""
	case method == http.MethodPatch && strings.HasPrefix(path, "/aggregatorv2/update/"):
		return opUpdateAggregatorV2, strings.TrimPrefix(path, "/aggregatorv2/update/")
	case method == http.MethodDelete && strings.HasPrefix(path, "/aggregatorv2/delete/"):
		return opDeleteAggregatorV2, strings.TrimPrefix(path, "/aggregatorv2/delete/")
	}

	return opUnknown, ""
}

func classifyAutomationRulesV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/automationrulesv2/create":
		return opCreateAutomationRuleV2, ""
	case method == http.MethodGet && path == "/automationrulesv2/list":
		return opListAutomationRulesV2, ""
	case method == http.MethodGet && strings.HasPrefix(path, "/automationrulesv2/") &&
		path != "/automationrulesv2/create" && path != "/automationrulesv2/list":
		return opGetAutomationRuleV2, strings.TrimPrefix(path, "/automationrulesv2/")
	case method == http.MethodPatch && strings.HasPrefix(path, "/automationrulesv2/") &&
		path != "/automationrulesv2/create" && path != "/automationrulesv2/list":
		return opUpdateAutomationRuleV2, strings.TrimPrefix(path, "/automationrulesv2/")
	case method == http.MethodDelete && strings.HasPrefix(path, "/automationrulesv2/") &&
		path != "/automationrulesv2/create" && path != "/automationrulesv2/list":
		return opDeleteAutomationRuleV2, strings.TrimPrefix(path, "/automationrulesv2/")
	}

	return opUnknown, ""
}

func classifyConnectorsV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/connectorsv2":
		return opCreateConnectorV2, ""
	case method == http.MethodGet && path == "/connectorsv2":
		return opListConnectorsV2, ""
	case method == http.MethodPost && path == "/connectorsv2/register":
		return opRegisterConnectorV2, ""
	case method == http.MethodGet && strings.HasPrefix(path, "/connectorsv2/") &&
		path != "/connectorsv2/register":
		return opGetConnectorV2, strings.TrimPrefix(path, "/connectorsv2/")
	case method == http.MethodPatch && strings.HasPrefix(path, "/connectorsv2/"):
		return opUpdateConnectorV2, strings.TrimPrefix(path, "/connectorsv2/")
	case method == http.MethodDelete && strings.HasPrefix(path, "/connectorsv2/") &&
		path != "/connectorsv2/register":
		return opDeleteConnectorV2, strings.TrimPrefix(path, "/connectorsv2/")
	}

	return opUnknown, ""
}

func classifyTicketsV2Path(method, path string) (string, string) {
	if method == http.MethodPost && path == "/ticketsv2" {
		return opCreateTicketV2, ""
	}

	return opUnknown, ""
}

func classifyFindingsV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/findingsv2":
		return opGetFindingsV2, ""
	case method == http.MethodPatch && path == "/findingsv2/batchupdatev2":
		return opBatchUpdateFindingsV2, ""
	case method == http.MethodPost && path == "/findingsv2/statistics":
		return opGetFindingStatisticsV2, ""
	case method == http.MethodPost && path == "/findingsTrendsv2":
		return opGetFindingsTrendsV2, ""
	}

	return opUnknown, ""
}

func classifyResourcesV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/resourcesv2":
		return opGetResourcesV2, ""
	case method == http.MethodPost && path == "/resourcesv2/statistics":
		return opGetResourcesStatisticsV2, ""
	case method == http.MethodPost && path == "/resourcesTrendsv2":
		return opGetResourcesTrendsV2, ""
	}

	return opUnknown, ""
}

func classifyProductsV2Path(method, path string) (string, string) {
	if method == http.MethodGet && path == "/productsV2" {
		return opDescribeProductsV2, ""
	}

	return opUnknown, ""
}

func classifyRecommendedPolicyV2Path(method, path string) (string, string) {
	prefix := "/recommendedPolicyV2/"
	switch {
	case method == http.MethodPost && strings.HasPrefix(path, prefix):
		return opGenerateRecommendedPolicyV2, strings.TrimPrefix(path, prefix)
	case method == http.MethodGet && strings.HasPrefix(path, prefix):
		return opGetRecommendedPolicyV2, strings.TrimPrefix(path, prefix)
	}

	return opUnknown, ""
}

// --- Handler functions: Hub V2 ---

func (h *Handler) handleEnableSecurityHubV2(c *echo.Context, body map[string]any) error {
	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if err := h.Backend.EnableSecurityHubV2(tags); err != nil {
		if errors.Is(err, ErrHubAlreadyExists) {
			return c.JSON(http.StatusConflict, map[string]any{
				keyMessage: "SecurityHub V2 is already enabled",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableSecurityHubV2(c *echo.Context) error {
	if err := h.Backend.DisableSecurityHubV2(); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeSecurityHubV2(c *echo.Context) error {
	hub, err := h.Backend.DescribeSecurityHubV2()
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "SecurityHub V2 is not enabled"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"HubV2Arn":  hub.HubV2Arn,
		"CreatedAt": hub.CreatedAt,
		"UpdatedAt": hub.UpdatedAt,
	})
}

// --- Handler functions: Aggregator V2 ---

func (h *Handler) handleCreateAggregatorV2(c *echo.Context, body map[string]any) error {
	regionLinkingMode, _ := body["RegionLinkingMode"].(string)

	var regions []string

	if raw, ok := body["Regions"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				regions = append(regions, s)
			}
		}
	}

	agg, err := h.Backend.CreateAggregatorV2(regionLinkingMode, regions)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, aggregatorV2ToResponse(agg))
}

func (h *Handler) handleGetAggregatorV2(c *echo.Context, arn string) error {
	agg, err := h.Backend.GetAggregatorV2(arn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Aggregator V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, aggregatorV2ToResponse(agg))
}

func (h *Handler) handleListAggregatorsV2(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	aggs, next := h.Backend.ListAggregatorsV2(nextToken, maxResults)

	var out []map[string]any

	for _, agg := range aggs {
		out = append(out, aggregatorV2ToResponse(agg))
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"Aggregators": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateAggregatorV2(c *echo.Context, arn string, body map[string]any) error {
	regionLinkingMode, _ := body["RegionLinkingMode"].(string)

	var regions []string

	if raw, ok := body["Regions"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				regions = append(regions, s)
			}
		}
	}

	agg, err := h.Backend.UpdateAggregatorV2(arn, regionLinkingMode, regions)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Aggregator V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, aggregatorV2ToResponse(agg))
}

func (h *Handler) handleDeleteAggregatorV2(c *echo.Context, arn string) error {
	if err := h.Backend.DeleteAggregatorV2(arn); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Aggregator V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func aggregatorV2ToResponse(agg *AggregatorV2) map[string]any {
	return map[string]any{
		"AggregatorV2Arn":   agg.AggregatorV2Arn,
		"AggregationRegion": agg.AggregationRegion,
		"RegionLinkingMode": agg.RegionLinkingMode,
		"Regions":           agg.Regions,
		"CreatedAt":         agg.CreatedAt,
		"UpdatedAt":         agg.UpdatedAt,
	}
}

// --- Handler functions: Automation Rules V2 ---

func (h *Handler) handleCreateAutomationRuleV2(c *echo.Context, body map[string]any) error {
	ruleName, _ := body["RuleName"].(string)
	ruleStatus, _ := body["RuleStatus"].(string)
	description, _ := body["Description"].(string)
	ruleOrder, _ := body["RuleOrder"].(float64)
	isTerminal, _ := body["IsTerminal"].(bool)

	var criteria map[string]any

	if cr, ok := body["Criteria"].(map[string]any); ok {
		criteria = cr
	}

	var actions []map[string]any

	if raw, ok := body["Actions"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
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
		ruleStatus = "ENABLED"
	}

	rule, err := h.Backend.CreateAutomationRuleV2(
		ruleName,
		ruleStatus,
		description,
		criteria,
		actions,
		ruleOrder,
		isTerminal,
		tags,
	)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, automationRuleV2ToResponse(rule))
}

func (h *Handler) handleGetAutomationRuleV2(c *echo.Context, identifier string) error {
	rule, err := h.Backend.GetAutomationRuleV2(identifier)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Automation rule V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
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

	var out []map[string]any

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
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Automation rule V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, automationRuleV2ToResponse(rule))
}

func (h *Handler) handleDeleteAutomationRuleV2(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteAutomationRuleV2(identifier); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Automation rule V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func automationRuleV2ToResponse(rule *AutomationRuleV2) map[string]any {
	return map[string]any{
		"Identifier":  rule.Identifier,
		"RuleArn":     rule.RuleArn,
		"RuleName":    rule.RuleName,
		"RuleStatus":  rule.RuleStatus,
		"Description": rule.Description,
		"CreatedAt":   rule.CreatedAt,
		"UpdatedAt":   rule.UpdatedAt,
		"Criteria":    rule.Criteria,
		"Actions":     rule.Actions,
		"RuleOrder":   rule.RuleOrder,
		"IsTerminal":  rule.IsTerminal,
	}
}

// --- Handler functions: Connectors V2 ---

func (h *Handler) handleCreateConnectorV2(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

	var provider map[string]any

	if p, ok := body["Provider"].(map[string]any); ok {
		provider = p
	}

	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	conn, err := h.Backend.CreateConnectorV2(name, description, provider, tags)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, connectorV2ToResponse(conn))
}

func (h *Handler) handleGetConnectorV2(c *echo.Context, connectorID string) error {
	conn, err := h.Backend.GetConnectorV2(connectorID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Connector V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, connectorV2ToResponse(conn))
}

func (h *Handler) handleListConnectorsV2(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	connectors, next := h.Backend.ListConnectorsV2(nextToken, maxResults)

	var out []map[string]any

	for _, conn := range connectors {
		out = append(out, connectorV2ToResponse(conn))
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"Connectors": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateConnectorV2(c *echo.Context, connectorID string, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

	var provider map[string]any

	if p, ok := body["Provider"].(map[string]any); ok {
		provider = p
	}

	conn, err := h.Backend.UpdateConnectorV2(connectorID, name, description, provider)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Connector V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, connectorV2ToResponse(conn))
}

func (h *Handler) handleDeleteConnectorV2(c *echo.Context, connectorID string) error {
	if err := h.Backend.DeleteConnectorV2(connectorID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Connector V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRegisterConnectorV2(c *echo.Context, body map[string]any) error {
	connectorID, _ := body["ConnectorId"].(string)

	var provider map[string]any

	if p, ok := body["Provider"].(map[string]any); ok {
		provider = p
	}

	conn, err := h.Backend.RegisterConnectorV2(connectorID, provider)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Connector V2 not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, connectorV2ToResponse(conn))
}

func connectorV2ToResponse(conn *ConnectorV2) map[string]any {
	return map[string]any{
		"ConnectorId":     conn.ConnectorId,
		"ConnectorArn":    conn.ConnectorArn,
		"Name":            conn.Name,
		"Description":     conn.Description,
		"CreatedAt":       conn.CreatedAt,
		"UpdatedAt":       conn.UpdatedAt,
		"ConnectorStatus": conn.ConnectorStatus,
		"Provider":        conn.Provider,
	}
}

// --- Handler functions: Tickets V2 ---

func (h *Handler) handleCreateTicketV2(c *echo.Context, body map[string]any) error {
	var ticketConfig map[string]any

	if tc, ok := body["TicketConfiguration"].(map[string]any); ok {
		ticketConfig = tc
	}

	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	ticket, err := h.Backend.CreateTicketV2(ticketConfig, tags)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TicketConfigurationArn": ticket.TicketConfigurationArn,
		"CreatedAt":              ticket.CreatedAt,
	})
}

// --- Handler functions: Findings V2 ---

func (h *Handler) handleGetFindingsV2(c *echo.Context, body map[string]any) error {
	var filters map[string]any

	if f, ok := body["Filters"].(map[string]any); ok {
		filters = f
	}

	var sortCriteria []map[string]any

	if raw, ok := body["SortCriteria"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
				sortCriteria = append(sortCriteria, m)
			}
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := 0

	if v, ok := body[keyMaxResults].(float64); ok {
		maxResults = int(v)
	}

	findings, next := h.Backend.GetFindingsV2(filters, sortCriteria, nextToken, maxResults)

	if findings == nil {
		findings = []map[string]any{}
	}

	resp := map[string]any{"Findings": findings}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchUpdateFindingsV2(c *echo.Context, body map[string]any) error {
	var findingIdentifiers []map[string]any

	if raw, ok := body["FindingIdentifiers"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
				findingIdentifiers = append(findingIdentifiers, m)
			}
		}
	}

	var updates map[string]any

	if u, ok := body["FindingFieldsUpdate"].(map[string]any); ok {
		updates = u
	}

	processed, unprocessed := h.Backend.BatchUpdateFindingsV2(findingIdentifiers, updates)

	if processed == nil {
		processed = []map[string]any{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedFindings":   processed,
		"UnprocessedFindings": unprocessed,
	})
}

func (h *Handler) handleGetFindingStatisticsV2(c *echo.Context, body map[string]any) error {
	var groupByAttributes []string

	if raw, ok := body["GroupByAttributes"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				groupByAttributes = append(groupByAttributes, s)
			}
		}
	}

	stats := h.Backend.GetFindingStatisticsV2(groupByAttributes)

	if stats == nil {
		stats = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingStatistics": stats,
	})
}

func (h *Handler) handleGetFindingsTrendsV2(c *echo.Context, body map[string]any) error {
	groupByAttribute, _ := body["GroupByAttribute"].(string)
	startTime, _ := body["StartTime"].(string)
	endTime, _ := body["EndTime"].(string)

	trends := h.Backend.GetFindingsTrendsV2(groupByAttribute, startTime, endTime)

	if trends == nil {
		trends = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingsTrends": trends,
	})
}

// --- Handler functions: Resources V2 ---

func (h *Handler) handleGetResourcesV2(c *echo.Context, body map[string]any) error {
	var filters map[string]any

	if f, ok := body["Filters"].(map[string]any); ok {
		filters = f
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := 0

	if v, ok := body[keyMaxResults].(float64); ok {
		maxResults = int(v)
	}

	resources, next := h.Backend.GetResourcesV2(filters, nextToken, maxResults)

	if resources == nil {
		resources = []map[string]any{}
	}

	resp := map[string]any{"Resources": resources}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetResourcesStatisticsV2(c *echo.Context, body map[string]any) error {
	var groupByAttributes []string

	if raw, ok := body["GroupByAttributes"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				groupByAttributes = append(groupByAttributes, s)
			}
		}
	}

	stats := h.Backend.GetResourcesStatisticsV2(groupByAttributes)

	if stats == nil {
		stats = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResourceStatistics": stats,
	})
}

func (h *Handler) handleGetResourcesTrendsV2(c *echo.Context, body map[string]any) error {
	groupByAttribute, _ := body["GroupByAttribute"].(string)
	startTime, _ := body["StartTime"].(string)
	endTime, _ := body["EndTime"].(string)

	trends := h.Backend.GetResourcesTrendsV2(groupByAttribute, startTime, endTime)

	if trends == nil {
		trends = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResourcesTrends": trends,
	})
}

// --- Handler functions: Products V2 ---

func (h *Handler) handleDescribeProductsV2(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	products, next := h.Backend.DescribeProductsV2(nextToken, maxResults)

	var out []map[string]any

	for _, p := range products {
		out = append(out, map[string]any{
			"ProductArn":       p.ProductArn,
			"ProductName":      p.ProductName,
			"CompanyName":      p.CompanyName,
			"Description":      p.Description,
			"Categories":       p.Categories,
			"IntegrationTypes": p.IntegrationTypes,
			"MarketplaceUrl":   p.MarketplaceURL,
			"ActivationUrl":    p.ActivationURL,
		})
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"Products": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Handler functions: Recommended Policy V2 ---

func (h *Handler) handleGenerateRecommendedPolicyV2(c *echo.Context, metadataUID string, _ map[string]any) error {
	rec, err := h.Backend.GenerateRecommendedPolicyV2(metadataUID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MetadataUid":    rec.MetadataUid,
		"Policy":         rec.Policy,
		"GenerationTime": rec.GenerationTime,
	})
}

func (h *Handler) handleGetRecommendedPolicyV2(c *echo.Context, metadataUID string) error {
	rec, err := h.Backend.GetRecommendedPolicyV2(metadataUID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Recommended policy not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MetadataUid":    rec.MetadataUid,
		"Policy":         rec.Policy,
		"GenerationTime": rec.GenerationTime,
	})
}

// avoid unused import warning
var _ = strings.HasPrefix
