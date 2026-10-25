package azuremonitor_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/monitor/ingestion/azlogs"
	queryazlogs "github.com/Azure/azure-sdk-for-go/sdk/monitor/query/azlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

const (
	testSub = "00000000-0000-0000-0000-000000000000"
	testRG  = "rg"
)

type fakeCredential struct{}

func (fakeCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "fake.jwt.token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type env struct {
	handler *azuremonitor.Handler
	srv     *httptest.Server
	ingest  *azlogs.Client
	query   *queryazlogs.Client
}

func newEnv(t *testing.T, settings azuremonitor.Settings) *env {
	t.Helper()

	h := azuremonitor.NewHandler(azuremonitor.NewBackend(settings), settings)
	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	ing, err := azlogs.NewClient(srv.URL, fakeCredential{}, &azlogs.ClientOptions{
		Transport: srv.Client(), Retry: policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	qc, err := queryazlogs.NewClient(fakeCredential{}, &queryazlogs.ClientOptions{
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
		Cloud: cloud.Configuration{
			Services: map[cloud.ServiceName]cloud.ServiceConfiguration{ //nolint:exhaustive // only the query service is needed
				queryazlogs.ServiceName: {Endpoint: srv.URL, Audience: "https://api.loganalytics.io"},
			},
		},
	})
	require.NoError(t, err)

	return &env{handler: h, srv: srv, ingest: ing, query: qc}
}

func ref(namespace string, types []string, names ...string) azuremonitor.Ref {
	return azuremonitor.Ref{
		Sub:       testSub,
		RG:        testRG,
		Namespace: namespace,
		Types:     types,
		Names:     names,
		Host:      "localhost",
	}
}

func wsRef(name string) azuremonitor.Ref {
	return ref("Microsoft.OperationalInsights", []string{"workspaces"}, name)
}

func tableRef(ws, table string) azuremonitor.Ref {
	return ref("Microsoft.OperationalInsights", []string{"workspaces", "tables"}, ws, table)
}

func dcrRef(name string) azuremonitor.Ref {
	return ref("Microsoft.Insights", []string{"dataCollectionRules"}, name)
}

func wsID(name string) string {
	return "/subscriptions/" + testSub + "/resourceGroups/" + testRG +
		"/providers/Microsoft.OperationalInsights/workspaces/" + name
}

// seed creates workspace "testws", custom table AppEvents_CL and a Direct DCR streaming Custom-AppEvents into it.
type seeded struct {
	customerID string
	ruleID     string
}

func (e *env) seed(t *testing.T, transform string) seeded {
	t.Helper()

	b := e.handler.Backend

	ws, err := b.Put(wsRef("testws"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	_, err = b.Put(
		tableRef("testws", "AppEvents_CL"),
		map[string]any{"properties": map[string]any{"schema": map[string]any{
			"name": "AppEvents_CL",
			"columns": []any{
				map[string]any{"name": "TimeGenerated", "type": "dateTime"},
				map[string]any{"name": "Computer", "type": "string"},
				map[string]any{"name": "Level", "type": "string"},
				map[string]any{"name": "Count", "type": "long"},
				map[string]any{"name": "Value", "type": "real"},
				map[string]any{"name": "Ok", "type": "boolean"},
				map[string]any{"name": "Extra", "type": "dynamic"},
			},
		}}},
	)
	require.NoError(t, err)

	dcr, err := b.Put(dcrRef("dcr"), map[string]any{
		"location": "westus", "kind": "Direct",
		"properties": map[string]any{
			"streamDeclarations": map[string]any{"Custom-AppEvents": map[string]any{"columns": []any{
				map[string]any{"name": "Time", "type": "datetime"},
				map[string]any{"name": "Computer", "type": "string"},
				map[string]any{"name": "Level", "type": "string"},
				map[string]any{"name": "Count", "type": "long"},
				map[string]any{"name": "Value", "type": "real"},
				map[string]any{"name": "Ok", "type": "boolean"},
				map[string]any{"name": "Extra", "type": "dynamic"},
			}}},
			"destinations": map[string]any{"logAnalytics": []any{
				map[string]any{"name": "dest", "workspaceResourceId": wsID("testws")},
			}},
			"dataFlows": []any{map[string]any{
				"streams": []any{"Custom-AppEvents"}, "destinations": []any{"dest"},
				"transformKql": transform, "outputStream": "Custom-AppEvents_CL",
			}},
		},
	})
	require.NoError(t, err)

	return seeded{
		customerID: ws["properties"].(map[string]any)["customerId"].(string),   //nolint:forcetypeassert // test fixture
		ruleID:     dcr["properties"].(map[string]any)["immutableId"].(string), //nolint:forcetypeassert // test fixture
	}
}
