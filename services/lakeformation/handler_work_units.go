package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetQueryState(_ context.Context, c *echo.Context, body []byte) error {
	var in getQueryStateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	state, err := h.Backend.GetQueryState(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getQueryStateOutput{State: state})
}

func (h *Handler) handleGetQueryStatistics(_ context.Context, c *echo.Context, body []byte) error {
	var in getQueryStatisticsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	exec, plan, err := h.Backend.GetQueryStatistics(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getQueryStatisticsOutput{ExecutionStatistics: exec, PlanningStatistics: plan})
}

func (h *Handler) handleGetWorkUnitResults(_ context.Context, c *echo.Context, body []byte) error {
	var in getWorkUnitResultsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	result, err := h.Backend.GetWorkUnitResults(in.QueryID, in.WorkUnitID, in.WorkUnitToken)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.Blob(http.StatusOK, "application/json", []byte(result))
}

func (h *Handler) handleGetWorkUnits(_ context.Context, c *echo.Context, body []byte) error {
	var in getWorkUnitsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	ranges, nextToken, err := h.Backend.GetWorkUnits(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getWorkUnitsOutput{QueryID: in.QueryID, WorkUnitRanges: ranges, NextToken: nextToken})
}

func (h *Handler) handleStartQueryPlanning(_ context.Context, c *echo.Context, body []byte) error {
	var in startQueryPlanningInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if strings.TrimSpace(in.QueryString) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "QueryString is required")
	}
	if strings.TrimSpace(in.QueryPlanningContext.DatabaseName) == "" {
		return h.writeError(
			c, http.StatusBadRequest, "InvalidInputException", "QueryPlanningContext.DatabaseName is required",
		)
	}
	queryID := h.Backend.StartQueryPlanning(in.QueryString)

	return c.JSON(http.StatusOK, startQueryPlanningOutput{QueryID: queryID})
}
