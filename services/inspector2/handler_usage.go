package inspector2

import (
	"encoding/json/v2"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
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
		NextToken  string   `json:"nextToken"`
		AccountIDs []string `json:"accountIds"`
		MaxResults int32    `json:"maxResults"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req, jsonx.V1()); jsonErr != nil {
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

	page, next := pageItems(totals, mapKey(keyAccountID), req.MaxResults, req.NextToken)

	return c.JSON(http.StatusOK, withPageToken(map[string]any{"totals": page}, next))
}

func (h *Handler) handleBatchGetFreeTrialInfo(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if jsonErr := json.Unmarshal(body, &req, jsonx.V1()); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, getErr := h.Backend.BatchGetFreeTrialInfo(req.AccountIDs)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}
