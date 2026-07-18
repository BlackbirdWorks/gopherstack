package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opListCoverage           = "ListCoverage"
	opListCoverageStatistics = "ListCoverageStatistics"

	pathCoverageList           = "/coverage/list"
	pathCoverageStatisticsList = "/coverage/statistics/list"
)

func (h *Handler) handleListCoverage(c *echo.Context) error {
	req, ok := decodeFilterListRequest(c)
	if !ok {
		return nil
	}

	entries, nextToken, listErr := h.Backend.ListCoverage(req.FilterCriteria, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	resp := map[string]any{"coveredResources": entries}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListCoverageStatistics(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	stats, statsErr := h.Backend.ListCoverageStatistics(req.FilterCriteria)
	if statsErr != nil {
		return h.mapError(c, statsErr)
	}

	return c.JSON(http.StatusOK, stats)
}
