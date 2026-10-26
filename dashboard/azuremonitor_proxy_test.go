package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

const azMonRoot = "/dashboard/api/v1/azure/monitor"

func seedAzureMonitor(t *testing.T) (*dashboard.DashboardHandler, string) {
	t.Helper()

	const (
		sub = "00000000-0000-0000-0000-000000000000"
		rg  = "rg"
	)

	ref := func(ns string, types []string, names ...string) azuremonitor.Ref {
		return azuremonitor.Ref{Sub: sub, RG: rg, Namespace: ns, Types: types, Names: names, Host: "localhost"}
	}

	const ops = "Microsoft.OperationalInsights"

	set := azuremonitor.DefaultSettings()
	mh := azuremonitor.NewHandler(azuremonitor.NewBackend(set), set)

	ws, err := mh.Backend.Put(ref(ops, []string{"workspaces"}, "dashws"), map[string]any{
		"location": "westus", "properties": map[string]any{"retentionInDays": float64(45)},
	})
	require.NoError(t, err)

	_, err = mh.Backend.Put(ref(ops, []string{"workspaces", "tables"}, "dashws", "AppEvents_CL"),
		map[string]any{"properties": map[string]any{"schema": map[string]any{
			"name": "AppEvents_CL",
			"columns": []any{
				map[string]any{"name": "TimeGenerated", "type": "dateTime"},
				map[string]any{"name": "Level", "type": "string"},
			},
		}}})
	require.NoError(t, err)

	_, err = mh.Backend.Put(ref("Microsoft.Insights", []string{"dataCollectionEndpoints"}, "dce-one"),
		map[string]any{"location": "westus"})
	require.NoError(t, err)

	wsID, _ := ws["id"].(string)
	_, err = mh.Backend.Put(ref("Microsoft.Insights", []string{"dataCollectionRules"}, "dcr-one"), map[string]any{
		"location": "westus", "kind": "Direct",
		"properties": map[string]any{
			"streamDeclarations": map[string]any{"Custom-AppEvents": map[string]any{"columns": []any{
				map[string]any{"name": "Level", "type": "string"},
			}}},
			"destinations": map[string]any{"logAnalytics": []any{
				map[string]any{"name": "dest", "workspaceResourceId": wsID},
			}},
			"dataFlows": []any{map[string]any{
				"streams": []any{"Custom-AppEvents"}, "destinations": []any{"dest"},
				"transformKql": "source | extend Seen = 1", "outputStream": "Custom-AppEvents_CL",
			}},
		},
	})
	require.NoError(t, err)

	props, _ := ws["properties"].(map[string]any)
	customerID, _ := props["customerId"].(string)

	return dashboard.NewHandler(dashboard.Config{AzureMonitorOps: mh}), customerID
}

func TestAzureMonitorProxy(t *testing.T) {
	t.Parallel()

	t.Run("browse", func(t *testing.T) {
		t.Parallel()

		h, cid := seedAzureMonitor(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, azMonRoot + "/workspaces", "", `"name":"dashws"`, 200},
			{http.MethodGet, azMonRoot + "/workspaces", "", `"retentionInDays":45`, 200},
			{http.MethodGet, azMonRoot + "/workspaces/" + cid + "/tables", "", `"name":"AppEvents_CL"`, 200},
			{http.MethodGet, azMonRoot + "/workspaces/dashws/tables", "", `"tableType":"Microsoft"`, 200},
			{http.MethodGet, azMonRoot + "/endpoints", "", `"name":"dce-one"`, 200},
			{http.MethodGet, azMonRoot + "/rules", "", `"transformKql":"source | extend Seen = 1"`, 200},
			{http.MethodGet, azMonRoot + "/rules", "", `"Custom-AppEvents"`, 200},
		})
	})

	t.Run("workspace keys never listed", func(t *testing.T) {
		t.Parallel()

		h, _ := seedAzureMonitor(t)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, azMonRoot+"/workspaces", nil)
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		assert.NotContains(t, strings.ToLower(rec.Body.String()), "primarysharedkey")
	})

	t.Run("query", func(t *testing.T) {
		t.Parallel()

		h, cid := seedAzureMonitor(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, azMonRoot + "/workspaces/" + cid + "/query", `{"query":"print x=1","timespan":"PT1H"}`, `"type":"long"`, 200},
			{http.MethodPost, azMonRoot + "/workspaces/dashws/query", `{"query":"AppEvents_CL | count"}`, `"PrimaryResult"`, 200},
		})
	})

	t.Run("query errors keep the bad argument body", func(t *testing.T) {
		t.Parallel()

		h, cid := seedAzureMonitor(t)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, azMonRoot+"/workspaces/"+cid+"/query",
			strings.NewReader(`{"query":"NoSuchTable | take 1"}`))
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

		var body struct {
			Error struct {
				Code       string `json:"code"`
				Innererror struct {
					Code       string `json:"code"`
					Innererror struct {
						Message string `json:"message"`
					} `json:"innererror"`
				} `json:"innererror"`
			} `json:"error"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "BadArgumentError", body.Error.Code)
		assert.Equal(t, "SemanticError", body.Error.Innererror.Code)
		assert.NotEmpty(t, body.Error.Innererror.Innererror.Message)
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		h, cid := seedAzureMonitor(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, azMonRoot + "/workspaces/nope/tables", "", "NOT_FOUND", 404},
			{http.MethodPost, azMonRoot + "/workspaces/nope/query", `{"query":"print 1"}`, "WorkspaceNotFoundError", 404},
			{http.MethodPost, azMonRoot + "/workspaces/" + cid + "/query", `{"query":""}`, "BadArgumentError", 400},
			{http.MethodPost, azMonRoot + "/workspaces/" + cid + "/query", `{"query":"print 1","timespan":"bogus"}`, "timespan", 400},
			{http.MethodPost, azMonRoot + "/workspaces/" + cid + "/query", `not json`, "invalid request body", 400},
		})
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, azMonRoot + "/workspaces", "", "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodPost, azMonRoot + "/workspaces/x/query", `{}`, "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodGet, azMonRoot + "/rules", "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}
