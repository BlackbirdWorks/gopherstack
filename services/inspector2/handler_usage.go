package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opListUsageTotals = "ListUsageTotals"

	opBatchGetFreeTrialInfo = "BatchGetFreeTrialInfo"

	pathUsageList = "/usage/list"

	pathFreeTrialInfoBatchGet = "/freetrialinfo/batchget"
)

func (h *Handler) handleListUsageTotals(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	totals, usageErr := h.Backend.ListUsageTotals(req.AccountIDs)
	if usageErr != nil {
		return h.mapError(c, usageErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"totals": totals})
}

func (h *Handler) handleBatchGetFreeTrialInfo(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, getErr := h.Backend.BatchGetFreeTrialInfo(req.AccountIDs)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}
