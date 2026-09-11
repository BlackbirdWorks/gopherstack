package eks

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dispatchInsightsOps handles cluster insights and insights-refresh operations.
func (h *Handler) dispatchInsightsOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDescribeInsight:
		return true, h.handleDescribeInsight(c, route.clusterName, route.nodegroupName)
	case opListInsights:
		return true, h.handleListInsights(c, route.clusterName, body)
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

type listInsightsFilterBody struct {
	Categories         []string `json:"categories"`
	Statuses           []string `json:"statuses"`
	KubernetesVersions []string `json:"kubernetesVersions"`
}

type listInsightsBody struct {
	Filter     *listInsightsFilterBody `json:"filter"`
	NextToken  string                  `json:"nextToken"`
	MaxResults int                     `json:"maxResults"`
}

func (h *Handler) handleListInsights(c *echo.Context, clusterName string, body []byte) error {
	insights, err := h.Backend.ListInsights(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	var in listInsightsBody
	if len(body) > 0 {
		// A malformed body is tolerated (ListInsights' filter/pagination
		// fields are all optional) rather than rejected, matching the
		// permissive parsing used by every other optional-body op here.
		_ = json.Unmarshal(body, &in)
	}

	if in.Filter != nil {
		if len(in.Filter.Categories) > 0 {
			insights = slices.DeleteFunc(insights, func(ins *Insight) bool {
				return !slices.Contains(in.Filter.Categories, ins.Category)
			})
		}

		if len(in.Filter.Statuses) > 0 {
			insights = slices.DeleteFunc(insights, func(ins *Insight) bool {
				return !slices.Contains(in.Filter.Statuses, ins.Status)
			})
		}

		if len(in.Filter.KubernetesVersions) > 0 {
			insights = slices.DeleteFunc(insights, func(ins *Insight) bool {
				return !slices.Contains(in.Filter.KubernetesVersions, ins.KubernetesVersion)
			})
		}
	}

	result := make([]map[string]any, len(insights))
	for i, ins := range insights {
		result[i] = insightToSummaryJSON(ins)
	}

	p := page.New(result, in.NextToken, in.MaxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("insights", p))
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

// insightStatusToJSON mirrors types.InsightStatus (status/reason) --
// gopherstack-wf8f item 2 fixed the prior bug where reason echoed
// Recommendation (remediation advice) instead of the actual reasoning for
// the status.
func insightStatusToJSON(ins *Insight) map[string]any {
	m := map[string]any{"status": ins.Status}
	if ins.StatusReason != "" {
		m["reason"] = ins.StatusReason
	}

	return m
}

// insightToJSON mirrors types.Insight (eks@v1.98.0 types.go:1645). No
// clusterName: neither Insight nor InsightSummary carries it on the wire
// (the cluster is already identified by the URL path) -- fixed alongside
// ListInsights' identical prior fix (gopherstack-uult).
// additionalInfo/categorySpecificSummary/resources are omitted rather than
// fabricated: this backend has no Kubernetes API server to source deprecated-
// API usage or per-resource detail from -- see PARITY.md gaps.
func insightToJSON(ins *Insight) map[string]any {
	m := map[string]any{
		"id":                 ins.ID,
		"category":           ins.Category,
		"insightStatus":      insightStatusToJSON(ins),
		"lastRefreshTime":    ins.LastRefreshTime.Unix(),
		"lastTransitionTime": ins.LastTransition.Unix(),
	}

	if ins.Name != "" {
		m["name"] = ins.Name
	}

	if ins.KubernetesVersion != "" {
		m["kubernetesVersion"] = ins.KubernetesVersion
	}

	if ins.Description != "" {
		m["description"] = ins.Description
	}

	if ins.Recommendation != "" {
		m["recommendation"] = ins.Recommendation
	}

	return m
}

// insightToSummaryJSON mirrors types.InsightSummary (eks@v1.98.0
// types.go:1755): category, description, id, insightStatus,
// kubernetesVersion, lastRefreshTime, lastTransitionTime, name. No
// recommendation -- that's DescribeInsight-only (types.Insight adds it,
// along with additionalInfo/categorySpecificSummary/resources). No
// clusterName either, same reasoning as insightToJSON.
func insightToSummaryJSON(ins *Insight) map[string]any {
	m := map[string]any{
		"id":                 ins.ID,
		"category":           ins.Category,
		"insightStatus":      insightStatusToJSON(ins),
		"lastRefreshTime":    ins.LastRefreshTime.Unix(),
		"lastTransitionTime": ins.LastTransition.Unix(),
	}

	if ins.Name != "" {
		m["name"] = ins.Name
	}

	if ins.KubernetesVersion != "" {
		m["kubernetesVersion"] = ins.KubernetesVersion
	}

	if ins.Description != "" {
		m["description"] = ins.Description
	}

	return m
}
