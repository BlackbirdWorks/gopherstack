package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

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

func (h *Handler) handleCreateAggregatorV2(c *echo.Context, body map[string]any) error {
	regionLinkingMode, _ := body["RegionLinkingMode"].(string)
	regions := stringListFromBody(body, "LinkedRegions")

	agg, err := h.Backend.CreateAggregatorV2(regionLinkingMode, regions)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	if tags, ok := body["Tags"].(map[string]any); ok {
		strTags := make(map[string]string, len(tags))
		for k, v := range tags {
			if s, isStr := v.(string); isStr {
				strTags[k] = s
			}
		}

		_ = h.Backend.TagResource(agg.AggregatorV2Arn, strTags)
	}

	return c.JSON(http.StatusOK, aggregatorV2ToResponse(agg))
}

// stringListFromBody extracts a []string from a JSON-decoded request body's
// []any field, skipping non-string entries.
func stringListFromBody(body map[string]any, field string) []string {
	var out []string

	if raw, ok := body[field].([]any); ok {
		for _, v := range raw {
			if s, isStr := v.(string); isStr {
				out = append(out, s)
			}
		}
	}

	return out
}

func (h *Handler) handleGetAggregatorV2(c *echo.Context, arn string) error {
	agg, err := h.Backend.GetAggregatorV2(arn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(
				http.StatusNotFound,
				map[string]any{keyMessage: "Aggregator V2 not found"}, //nolint:goconst // existing issue.
			)
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

	var out []map[string]any //nolint:prealloc // existing issue.

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
	regions := stringListFromBody(body, "LinkedRegions")

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

// aggregatorV2ToResponse renders the wire fields for CreateAggregatorV2Output/
// GetAggregatorV2Output/UpdateAggregatorV2Output. The region list's wire name
// is "LinkedRegions" (confirmed against aws-sdk-go-v2 securityhub@v1.75.4
// api_op_CreateAggregatorV2.go:39 and deserializers.go:9258), not "Regions".
func aggregatorV2ToResponse(agg *AggregatorV2) map[string]any {
	return map[string]any{
		"AggregatorV2Arn":   agg.AggregatorV2Arn,
		"AggregationRegion": agg.AggregationRegion,
		"RegionLinkingMode": agg.RegionLinkingMode, //nolint:goconst // existing issue.
		"LinkedRegions":     agg.Regions,
		keyCreatedAt:        agg.CreatedAt,
		keyUpdatedAt:        agg.UpdatedAt,
	}
}

// aggregatorsV2OpHandlers returns the Aggregator V2 operation dispatch
// table for handleREST.
func (h *Handler) aggregatorsV2OpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opCreateAggregatorV2: func() error { return h.handleCreateAggregatorV2(c, body) },
		opGetAggregatorV2:    func() error { return h.handleGetAggregatorV2(c, resource) },
		opListAggregatorsV2:  func() error { return h.handleListAggregatorsV2(c) },
		opUpdateAggregatorV2: func() error { return h.handleUpdateAggregatorV2(c, resource, body) },
		opDeleteAggregatorV2: func() error { return h.handleDeleteAggregatorV2(c, resource) },
	}
}
