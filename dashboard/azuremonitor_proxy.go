package dashboard

import (
	"errors"
	"net"
	"net/http"

	"github.com/labstack/echo/v5"

	monitorbackend "github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

const (
	azureMonitorPrefix    = "/dashboard/api/v1/azure/monitor"
	azureMonitorMaxQuery  = 64 << 10
	azureMonitorKeyResult = "value"
)

type azureMonitorAPI struct {
	b *monitorbackend.Backend
}

type azureMonitorQueryIn struct {
	Query    string `json:"query"`
	Timespan string `json:"timespan"`
}

// registerAzureMonitorRoutes serves the Monitor Logs page from the backend in-process. Queries run against
// the in-process workspace data only; workspace shared keys are never returned.
func (h *DashboardHandler) registerAzureMonitorRoutes() {
	api := &azureMonitorAPI{}
	if h.config.AzureMonitorOps != nil {
		api.b = h.config.AzureMonitorOps.Backend
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if api.b == nil {
				return cloudUnavailable(c, "Azure Monitor Logs")
			}

			return next(c)
		}
	}

	r := h.SubRouter
	p := azureMonitorPrefix
	r.Add(http.MethodGet, p+"/workspaces", guard(api.workspaces))
	r.Add(http.MethodGet, p+"/workspaces/:ws/tables", guard(api.tables))
	r.Add(http.MethodPost, p+"/workspaces/:ws/query", guard(api.query))
	r.Add(http.MethodGet, p+"/endpoints", guard(api.endpoints))
	r.Add(http.MethodGet, p+"/rules", guard(api.rules))
}

func requestHostname(c *echo.Context) string {
	host := c.Request().Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}

	return host
}

func (a *azureMonitorAPI) workspaces(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{azureMonitorKeyResult: a.b.ListWorkspaces()})
}

func (a *azureMonitorAPI) tables(c *echo.Context) error {
	items, err := a.b.ListWorkspaceTables(c.Param("ws"))
	if err != nil {
		if errors.Is(err, monitorbackend.ErrNotFound) {
			return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
		}

		return cloudJSONFail(c, http.StatusInternalServerError, "INTERNAL", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{azureMonitorKeyResult: items})
}

func (a *azureMonitorAPI) endpoints(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		azureMonitorKeyResult: a.b.ListDataCollectionEndpoints(requestHostname(c)),
	})
}

func (a *azureMonitorAPI) rules(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		azureMonitorKeyResult: a.b.ListDataCollectionRules(requestHostname(c)),
	})
}

// query answers with the data-plane status and body, so a rejected query keeps its BadArgumentError envelope.
func (a *azureMonitorAPI) query(c *echo.Context) error {
	var in azureMonitorQueryIn
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	if len(in.Query) > azureMonitorMaxQuery {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "query is too large")
	}

	status, body := a.b.QueryJSON(c.Param("ws"), monitorbackend.QueryRequest{Query: in.Query, Timespan: in.Timespan})
	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(status, body)
}
