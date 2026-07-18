package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyFindingAggregatorPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/findingAggregator/create":
		return opCreateFindingAggregator, ""
	case method == http.MethodGet && strings.HasPrefix(path, "/findingAggregator/get/"):
		return opGetFindingAggregator, strings.TrimPrefix(path, "/findingAggregator/get/")
	case method == http.MethodGet && path == "/findingAggregator/list":
		return opListFindingAggregators, ""
	case method == http.MethodPatch && path == "/findingAggregator/update":
		return opUpdateFindingAggregator, ""
	case method == http.MethodDelete && strings.HasPrefix(path, "/findingAggregator/delete/"):
		return opDeleteFindingAggregator, strings.TrimPrefix(path, "/findingAggregator/delete/")
	}

	return opUnknown, ""
}

func (h *Handler) handleCreateFindingAggregator(c *echo.Context, body map[string]any) error {
	regionLinkingMode, _ := body["RegionLinkingMode"].(string)

	var regions []string

	if raw, ok := body["Regions"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				regions = append(regions, s)
			}
		}
	}

	agg, err := h.Backend.CreateFindingAggregator(regionLinkingMode, regions)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingAggregatorArn":     agg.FindingAggregatorArn,     //nolint:goconst // existing issue.
		"FindingAggregationRegion": agg.FindingAggregationRegion, //nolint:goconst // existing issue.
		"RegionLinkingMode":        agg.RegionLinkingMode,        //nolint:goconst // existing issue.
		"Regions":                  agg.Regions,                  //nolint:goconst // existing issue.
	})
}

func (h *Handler) handleGetFindingAggregator(c *echo.Context, arn string) error {
	agg, err := h.Backend.GetFindingAggregator(arn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]any{keyMessage: "Finding aggregator not found"}, //nolint:goconst // existing issue.
			)
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingAggregatorArn":     agg.FindingAggregatorArn,
		"FindingAggregationRegion": agg.FindingAggregationRegion,
		"RegionLinkingMode":        agg.RegionLinkingMode,
		"Regions":                  agg.Regions,
	})
}

func (h *Handler) handleListFindingAggregators(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	aggs, next := h.Backend.ListFindingAggregators(nextToken, maxResults)

	var out []map[string]any //nolint:prealloc // existing issue.

	for _, agg := range aggs {
		out = append(out, map[string]any{
			"FindingAggregatorArn": agg.FindingAggregatorArn,
		})
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"FindingAggregators": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateFindingAggregator(c *echo.Context, body map[string]any) error {
	arn, _ := body["FindingAggregatorArn"].(string)
	regionLinkingMode, _ := body["RegionLinkingMode"].(string)

	var regions []string

	if raw, ok := body["Regions"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				regions = append(regions, s)
			}
		}
	}

	agg, err := h.Backend.UpdateFindingAggregator(arn, regionLinkingMode, regions)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Finding aggregator not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingAggregatorArn":     agg.FindingAggregatorArn,
		"FindingAggregationRegion": agg.FindingAggregationRegion,
		"RegionLinkingMode":        agg.RegionLinkingMode,
		"Regions":                  agg.Regions,
	})
}

func (h *Handler) handleDeleteFindingAggregator(c *echo.Context, arn string) error {
	if err := h.Backend.DeleteFindingAggregator(arn); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Finding aggregator not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// findingAggregatorsOpHandlers returns the Finding Aggregators operation
// dispatch table for handleREST.
func (h *Handler) findingAggregatorsOpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opCreateFindingAggregator: func() error { return h.handleCreateFindingAggregator(c, body) },
		opGetFindingAggregator:    func() error { return h.handleGetFindingAggregator(c, resource) },
		opListFindingAggregators:  func() error { return h.handleListFindingAggregators(c) },
		opUpdateFindingAggregator: func() error { return h.handleUpdateFindingAggregator(c, body) },
		opDeleteFindingAggregator: func() error { return h.handleDeleteFindingAggregator(c, resource) },
	}
}
