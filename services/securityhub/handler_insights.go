package securityhub

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyInsightsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/insights":
		return opCreateInsight, ""
	case method == http.MethodPost && path == "/insights/get":
		return opGetInsights, ""
	case strings.HasPrefix(path, "/insights/results/") && method == http.MethodGet:
		return opGetInsightResults, strings.TrimPrefix(path, "/insights/results/")
	case strings.HasPrefix(path, "/insights/") && method == http.MethodPatch:
		return opUpdateInsight, strings.TrimPrefix(path, "/insights/")
	case strings.HasPrefix(path, "/insights/") && method == http.MethodDelete:
		return opDeleteInsight, strings.TrimPrefix(path, "/insights/")
	}

	return opUnknown, ""
}

func (h *Handler) handleCreateInsight(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	groupByAttribute, _ := body["GroupByAttribute"].(string)
	filters, _ := body["Filters"].(map[string]any)

	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: msgNameRequired,
		})
	}

	if groupByAttribute == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: "GroupByAttribute is required",
		})
	}

	arn, err := h.Backend.CreateInsight(name, groupByAttribute, filters)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyInsightArn: arn})
}

func (h *Handler) handleGetInsights(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["InsightArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	insights, nextOut, err := h.Backend.GetInsights(arns, nextToken, maxResults)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	items := make([]map[string]any, len(insights))

	for i, ins := range insights {
		items[i] = map[string]any{
			keyInsightArn:      ins.InsightArn,
			keyName:            ins.Name,
			"GroupByAttribute": ins.GroupByAttribute, //nolint:goconst // existing issue.
			"Filters":          ins.Filters,
		}
	}

	resp := map[string]any{"Insights": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetInsightResults(c *echo.Context, insightArn string) error {
	results, err := h.Backend.GetInsightResults(insightArn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{
				keyMessage: msgInsightNotFound,
			})
		}

		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"InsightResults": map[string]any{
			keyInsightArn:      results.InsightArn,
			"GroupByAttribute": results.GroupByAttribute,
			"ResultValues":     results.ResultValues,
		},
	})
}

func (h *Handler) handleUpdateInsight(c *echo.Context, insightArn string, body map[string]any) error {
	name, _ := body["Name"].(string)
	groupByAttribute, _ := body["GroupByAttribute"].(string)
	filters, _ := body["Filters"].(map[string]any)

	if err := h.Backend.UpdateInsight(insightArn, name, groupByAttribute, filters); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: msgInsightNotFound})
		}

		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteInsight(c *echo.Context, insightArn string) error {
	deletedArn, err := h.Backend.DeleteInsight(insightArn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: msgInsightNotFound})
		}

		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyInsightArn: deletedArn})
}

// insightsOpHandlers returns the Insights operation dispatch table for
// handleREST.
func (h *Handler) insightsOpHandlers(c *echo.Context, resource string, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opCreateInsight:     func() error { return h.handleCreateInsight(c, body) },
		opGetInsights:       func() error { return h.handleGetInsights(c, body) },
		opGetInsightResults: func() error { return h.handleGetInsightResults(c, resource) },
		opUpdateInsight:     func() error { return h.handleUpdateInsight(c, resource, body) },
		opDeleteInsight:     func() error { return h.handleDeleteInsight(c, resource) },
	}
}
