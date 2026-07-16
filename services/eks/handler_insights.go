package eks

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchInsightsOps handles cluster insights and insights-refresh operations.
func (h *Handler) dispatchInsightsOps(c *echo.Context, route eksRoute, _ []byte) (bool, error) {
	switch route.operation {
	case opDescribeInsight:
		return true, h.handleDescribeInsight(c, route.clusterName, route.nodegroupName)
	case opListInsights:
		return true, h.handleListInsights(c, route.clusterName)
	case opStartInsightsRefresh:
		return true, h.handleStartInsightsRefresh(c, route.clusterName)
	case opDescribeInsightsRefresh:
		return true, h.handleDescribeInsightsRefresh(c, route.clusterName)
	}

	return false, nil
}

// parseInsightsRoute returns the route for /clusters/{name}/insights[/{id}].
// ListInsights is POST (it carries an optional filter body), not GET --
// verified against the SDK serializer.
func parseInsightsRoute(method, clusterName string, parts []string) eksRoute {
	const insightsParts = 2

	if len(parts) == insightsParts {
		if method == http.MethodPost {
			return eksRoute{operation: opListInsights, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	if method == http.MethodGet {
		return eksRoute{operation: opDescribeInsight, clusterName: clusterName, nodegroupName: parts[2]}
	}

	return eksRoute{operation: opUnknown}
}

// parseInsightsRefreshRoute returns the route for
// /clusters/{name}/insights-refresh. This is a cluster-level singleton (no
// per-refresh id in the real API): POST starts a refresh, GET describes its
// status -- verified against the SDK serializer.
func parseInsightsRefreshRoute(method, clusterName string, parts []string) eksRoute {
	const insightsRefreshParts = 2

	if len(parts) != insightsRefreshParts {
		return eksRoute{operation: opUnknown}
	}

	switch method {
	case http.MethodPost:
		return eksRoute{operation: opStartInsightsRefresh, clusterName: clusterName}
	case http.MethodGet:
		return eksRoute{operation: opDescribeInsightsRefresh, clusterName: clusterName}
	}

	return eksRoute{operation: opUnknown}
}

func (h *Handler) handleDescribeInsight(c *echo.Context, clusterName, insightID string) error {
	insight, err := h.Backend.DescribeInsight(clusterName, insightID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"insight": insightToJSON(insight),
	})
}

func (h *Handler) handleListInsights(c *echo.Context, clusterName string) error {
	insights, err := h.Backend.ListInsights(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	result := make([]map[string]any, len(insights))
	for i, ins := range insights {
		result[i] = insightToJSON(ins)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"insights": result,
	})
}

// Both StartInsightsRefresh and DescribeInsightsRefresh return their fields
// directly at the response root (message, status, startedAt, endedAt) -- NOT
// nested under an "insightsRefresh" envelope key -- verified against the SDK
// deserializer.
func (h *Handler) handleStartInsightsRefresh(c *echo.Context, clusterName string) error {
	refresh, err := h.Backend.StartInsightsRefresh(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, insightsRefreshToJSON(refresh))
}

func (h *Handler) handleDescribeInsightsRefresh(c *echo.Context, clusterName string) error {
	refresh, err := h.Backend.DescribeInsightsRefresh(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, insightsRefreshToJSON(refresh))
}

func insightsRefreshToJSON(refresh *InsightsRefresh) map[string]any {
	m := map[string]any{
		keyStatusField: refresh.Status,
		"startedAt":    refresh.StartedAt.Unix(),
	}

	if refresh.Message != "" {
		m["message"] = refresh.Message
	}

	if !refresh.EndedAt.IsZero() {
		m["endedAt"] = refresh.EndedAt.Unix()
	}

	return m
}

func insightToJSON(ins *Insight) map[string]any {
	m := map[string]any{
		"id":                 ins.ID,
		keyClusterName:       ins.ClusterName,
		"category":           ins.Category,
		"insightStatus":      map[string]any{"status": ins.Status, "reason": ins.Recommendation},
		"lastRefreshTime":    ins.LastRefreshTime.Unix(),
		"lastTransitionTime": ins.LastTransition.Unix(),
	}

	if ins.Description != "" {
		m["description"] = ins.Description
	}

	if ins.Recommendation != "" {
		m["recommendation"] = ins.Recommendation
	}

	return m
}
