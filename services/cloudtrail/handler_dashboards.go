package cloudtrail

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- CreateDashboard ---

type createDashboardBody struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleCreateDashboard(c *echo.Context, body []byte) error {
	var in createDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		kv[tag.Key] = tag.Value
	}

	d, err := h.Backend.CreateDashboard(in.Name, in.Type, kv)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDashboardArn: d.DashboardARN,
		keyName:         d.Name,
		"Type":          d.Type,
		keyStatus:       d.Status,
	})
}

// --- DeleteDashboard ---

type deleteDashboardBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleDeleteDashboard(c *echo.Context, body []byte) error {
	var in deleteDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteDashboard(in.DashboardID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- GetDashboard ---

type getDashboardBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleGetDashboard(c *echo.Context, body []byte) error {
	var in getDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.GetDashboard(in.DashboardID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, dashToMap(d))
}

// --- UpdateDashboard ---

type updateDashboardBody struct {
	DashboardID string `json:"DashboardId"`
	Name        string `json:"Name"`
}

func (h *Handler) handleUpdateDashboard(c *echo.Context, body []byte) error {
	var in updateDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.UpdateDashboard(in.DashboardID, in.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, dashToMap(d))
}

// --- ListDashboards ---

func (h *Handler) handleListDashboards(c *echo.Context, _ []byte) error {
	list := h.Backend.ListDashboards()
	items := make([]map[string]any, 0, len(list))
	for _, d := range list {
		items = append(items, dashToMap(d))
	}

	return c.JSON(http.StatusOK, map[string]any{"Dashboards": items})
}

// --- StartDashboardRefresh ---

type startDashboardRefreshBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleStartDashboardRefresh(c *echo.Context, body []byte) error {
	var in startDashboardRefreshBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.StartDashboardRefresh(in.DashboardID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDashboardArn: d.DashboardARN,
		keyStatus:       d.Status,
	})
}

// dashToMap converts a Dashboard to the JSON map used in API responses.
func dashToMap(d *Dashboard) map[string]any {
	return map[string]any{
		keyDashboardArn: d.DashboardARN,
		keyName:         d.Name,
		"Type":          d.Type,
		keyStatus:       d.Status,
	}
}
