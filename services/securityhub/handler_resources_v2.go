package securityhub

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

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
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
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

// resourcesV2OpHandlers returns the Resources V2 operation dispatch table
// for handleREST.
func (h *Handler) resourcesV2OpHandlers(c *echo.Context, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opGetResourcesV2:           func() error { return h.handleGetResourcesV2(c, body) },
		opGetResourcesStatisticsV2: func() error { return h.handleGetResourcesStatisticsV2(c, body) },
		opGetResourcesTrendsV2:     func() error { return h.handleGetResourcesTrendsV2(c, body) },
	}
}
