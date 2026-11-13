package dashboard_test

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	logicAPI    = "?api-version=2019-05-01"
	logicRoot   = "/dashboard/api/v1/azure/logic"
	logicAccess = "/dashboard/api/v1/azure/logicaccess"
	logicNS     = armBase + "Microsoft.Logic"
	logicWF     = logicNS + "/workflows/greeter"
	logicTicker = logicNS + "/workflows/ticker"
	logicIDs    = "/" + armSub + "/demo/greeter"

	logicSchema = `"$schema":"https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/` +
		`workflowdefinition.json#","contentVersion":"1.0.0.0"`
	logicGreeter = `{"location":"westus","properties":{"definition":{` + logicSchema + `,` +
		`"triggers":{"manual":{"type":"Request","kind":"Http","inputs":{"method":"POST"}}},` +
		`"actions":{"Compose":{"type":"Compose","inputs":"@concat('hello ', triggerBody()?['name'])","runAfter":{}},` +
		`"Response":{"type":"Response","kind":"Http","runAfter":{"Compose":["Succeeded"]},` +
		`"inputs":{"statusCode":200,"body":{"greeting":"@outputs('Compose')"}}}}}}}`
	logicRecurrence = `{"location":"westus","properties":{"definition":{` + logicSchema + `,` +
		`"triggers":{"tick":{"type":"Recurrence","recurrence":{"frequency":"Month","interval":1}}},` +
		`"actions":{"Compose":{"type":"Compose","inputs":"tick","runAfter":{}}}}}}`
)

func newLogicDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	return newARMDashboard(t, []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"westus"}`},
		{logicWF + logicAPI, logicGreeter},
		{logicTicker + logicAPI, logicRecurrence},
	})
}

func logicGet(t *testing.T, h *dashboard.DashboardHandler, path string) map[string]any {
	t.Helper()

	rec := lbDo(t, h, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, rec.Code, "%s: %s", path, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))

	return out
}

func logicValues(t *testing.T, doc map[string]any) []map[string]any {
	t.Helper()

	raw, ok := doc["value"].([]any)
	require.True(t, ok)

	out := make([]map[string]any, 0, len(raw))

	for _, r := range raw {
		m, isMap := r.(map[string]any)
		require.True(t, isMap)

		out = append(out, m)
	}

	return out
}

// logicFirstRun invokes the greeter and waits until its run has finished.
func logicFirstRun(t *testing.T, h *dashboard.DashboardHandler) string {
	t.Helper()

	rec := lbDo(t, h, http.MethodPost, logicAccess+"/invoke"+logicIDs+"/manual", `{"name":"bob"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var reply struct {
		Body   string `json:"body"`
		RunID  string `json:"runId"`
		Status int    `json:"status"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &reply, jsonx.V1()))
	require.Equal(t, http.StatusOK, reply.Status, rec.Body.String())
	require.NotEmpty(t, reply.RunID)
	assert.JSONEq(t, `{"greeting":"hello bob"}`, reply.Body)

	require.Eventually(t, func() bool {
		runs := logicGet(t, h, logicRoot+logicWF+"/runs"+logicAPI)
		for _, r := range logicValues(t, runs) {
			props, _ := r["properties"].(map[string]any)
			if r["name"] == reply.RunID && props["status"] == "Succeeded" {
				return true
			}
		}

		return false
	}, 5*time.Second, 10*time.Millisecond)

	return reply.RunID
}

func TestAzureLogicProxyWorkflowsAndTriggers(t *testing.T) {
	t.Parallel()

	h := newLogicDashboard(t)

	list := logicGet(t, h, logicRoot+armBase+"Microsoft.Logic/workflows"+logicAPI)
	assert.Len(t, logicValues(t, list), 2)

	one := logicGet(t, h, logicRoot+logicWF+logicAPI)
	props, _ := one["properties"].(map[string]any)
	assert.Equal(t, "Enabled", props["state"])
	assert.Contains(t, props, "definition")

	for state, action := range map[string]string{"Disabled": "disable", "Enabled": "enable"} {
		rec := lbDo(t, h, http.MethodPost, logicRoot+logicWF+"/"+action+logicAPI, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		got, _ := logicGet(t, h, logicRoot+logicWF+logicAPI)["properties"].(map[string]any)
		assert.Equal(t, state, got["state"])
	}

	trig := lbDo(t, h, http.MethodGet, logicRoot+logicWF+"/triggers"+logicAPI, "")
	require.Equal(t, http.StatusOK, trig.Code)
	assert.Contains(t, trig.Body.String(), `"name":"manual"`)
	assert.NotContains(t, trig.Body.String(), "sig=")
	assert.NotContains(t, trig.Body.String(), "callbackUrl")

	item := lbDo(t, h, http.MethodGet, logicRoot+logicWF+"/triggers/manual"+logicAPI, "")
	require.Equal(t, http.StatusOK, item.Code)
	assert.NotContains(t, item.Body.String(), "sig=")

	wf := lbDo(t, h, http.MethodGet, logicRoot+logicWF+logicAPI, "")
	assert.NotContains(t, wf.Body.String(), "sig=")

	tick := lbDo(t, h, http.MethodPost, logicRoot+logicTicker+"/triggers/tick/run"+logicAPI, "")
	require.Less(t, tick.Code, http.StatusMultipleChoices, tick.Body.String())

	require.Eventually(t, func() bool {
		runs := logicGet(t, h, logicRoot+logicTicker+"/runs"+logicAPI)

		return len(logicValues(t, runs)) == 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestAzureLogicProxyRunsHideSignedLinks(t *testing.T) {
	t.Parallel()

	h := newLogicDashboard(t)
	run := logicFirstRun(t, h)

	list := lbDo(t, h, http.MethodGet, logicRoot+logicWF+"/runs"+logicAPI, "")
	assert.NotContains(t, list.Body.String(), "sig=")
	assert.NotContains(t, list.Body.String(), "/_logic/")

	detail := lbDo(t, h, http.MethodGet, logicRoot+logicWF+"/runs/"+run+logicAPI, "")
	require.Equal(t, http.StatusOK, detail.Code)
	assert.NotContains(t, detail.Body.String(), "sig=")
	assert.NotContains(t, detail.Body.String(), `"uri"`)
	assert.Contains(t, detail.Body.String(), `"outputsLink":{"contentSize":`)

	actions := lbDo(t, h, http.MethodGet, logicRoot+logicWF+"/runs/"+run+"/actions"+logicAPI, "")
	require.Equal(t, http.StatusOK, actions.Code)
	assert.NotContains(t, actions.Body.String(), "sig=")
	assert.Contains(t, actions.Body.String(), `"name":"Compose"`)
	assert.Contains(t, actions.Body.String(), `"name":"Response"`)

	action := lbDo(t, h, http.MethodGet, logicRoot+logicWF+"/runs/"+run+"/actions/Compose"+logicAPI, "")
	require.Equal(t, http.StatusOK, action.Code)
	assert.NotContains(t, action.Body.String(), "sig=")

	finished := lbDo(t, h, http.MethodPost, logicRoot+logicWF+"/runs/"+run+"/cancel"+logicAPI, "")
	assert.Equal(t, http.StatusBadRequest, finished.Code, finished.Body.String())
	assert.Contains(t, finished.Body.String(), "RunNotRunning")
}

func TestAzureLogicProxyContentThroughSignedLinks(t *testing.T) {
	t.Parallel()

	h := newLogicDashboard(t)
	run := logicFirstRun(t, h)
	content := logicAccess + "/content" + logicIDs + "/" + run

	out := lbDo(t, h, http.MethodGet, content+"/action/Compose/outputs", "")
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())
	assert.JSONEq(t, `"hello bob"`, out.Body.String())
	assert.Equal(t, "no-store", out.Header().Get("Cache-Control"))

	in := lbDo(t, h, http.MethodGet, content+"/action/Compose/inputs", "")
	require.Equal(t, http.StatusOK, in.Code, in.Body.String())
	assert.Contains(t, in.Body.String(), "hello bob")

	trig := lbDo(t, h, http.MethodGet, content+"/trigger/outputs", "")
	require.Equal(t, http.StatusOK, trig.Code, trig.Body.String())
	assert.Contains(t, trig.Body.String(), `"name":"bob"`)
	assert.NotContains(t, trig.Body.String(), "sig=")

	for _, p := range []string{
		content + "/action/Compose/secrets", content + "/action/Missing/outputs", content + "/trigger/nope",
		logicAccess + "/content" + logicIDs + "/missing/trigger/outputs",
		logicAccess + "/content" + logicIDs + "/" + run + "/action/..%2f..%2fx/outputs",
	} {
		got := lbDo(t, h, http.MethodGet, p, "")
		assert.GreaterOrEqual(t, got.Code, http.StatusBadRequest, p)
		assert.NotContains(t, got.Body.String(), "sig=", p)
	}
}

func TestAzureLogicProxyCallbackIsExplicit(t *testing.T) {
	t.Parallel()

	h := newLogicDashboard(t)

	rec := lbDo(t, h, http.MethodPost, logicAccess+"/callback"+logicIDs+"/manual", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))

	var cb struct {
		Value  string `json:"value"`
		Method string `json:"method"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cb, jsonx.V1()))
	assert.Equal(t, http.MethodPost, cb.Method)
	assert.Contains(t, cb.Value, "/_logic/workflows/")
	assert.Contains(t, cb.Value, "sig=")

	hostReq := httptest.NewRequest(http.MethodPost, logicAccess+"/callback"+logicIDs+"/manual", http.NoBody)
	hostReq.Host = "files.test:18400"
	hostRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.NewContext(hostReq, hostRec)))
	require.Equal(t, http.StatusOK, hostRec.Code, hostRec.Body.String())
	assert.Contains(t, hostRec.Body.String(), `"value":"https://files.test:`)
	assert.NotContains(t, hostRec.Body.String(), "files.test:18400")

	generic := lbDo(t, h, http.MethodPost, logicRoot+logicWF+"/triggers/manual/listCallbackUrl"+logicAPI, "{}")
	assert.Equal(t, http.StatusBadRequest, generic.Code)
	assert.NotContains(t, generic.Body.String(), "sig=")

	invoke := lbDo(t, h, http.MethodPost, logicAccess+"/invoke"+logicIDs+"/manual", `{"name":"eve"}`)
	require.Equal(t, http.StatusOK, invoke.Code)
	assert.NotContains(t, invoke.Body.String(), "sig=")
	assert.NotContains(t, invoke.Body.String(), "/_logic/")
	assert.Equal(t, "no-store", invoke.Header().Get("Cache-Control"))

	missing := lbDo(t, h, http.MethodPost, logicAccess+"/callback"+logicIDs+"/nope", "")
	assert.GreaterOrEqual(t, missing.Code, http.StatusBadRequest)
	assert.NotContains(t, missing.Body.String(), "sig=")
}

func TestAzureLogicProxyRejectsOutsideAllowList(t *testing.T) {
	t.Parallel()

	h := newLogicDashboard(t)
	raw := logicRoot + logicWF

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"put workflow", http.MethodPut, raw + logicAPI, logicGreeter},
		{"delete workflow", http.MethodDelete, raw + logicAPI, ""},
		{"patch workflow", http.MethodPatch, raw + logicAPI, "{}"},
		{"regenerate key", http.MethodPost, raw + "/regenerateAccessKey" + logicAPI, `{"keyType":"Primary"}`},
		{"callback via generic", http.MethodPost, raw + "/triggers/manual/listCallbackUrl" + logicAPI, "{}"},
		{"version callback", http.MethodPost, raw + "/versions/v1/triggers/manual/listCallbackUrl" + logicAPI, "{}"},
		{"versions", http.MethodGet, raw + "/versions" + logicAPI, ""},
		{"histories", http.MethodGet, raw + "/triggers/manual/histories" + logicAPI, ""},
		{"set state", http.MethodPost, raw + "/triggers/manual/setState" + logicAPI, "{}"},
		{"reset trigger", http.MethodPost, raw + "/triggers/manual/reset" + logicAPI, ""},
		{"validate", http.MethodPost, raw + "/validate" + logicAPI, "{}"},
		{"get run action content", http.MethodGet, raw + "/runs/r/actions/a/contents/ActionOutputs" + logicAPI, ""},
		{"delete run", http.MethodDelete, raw + "/runs/r" + logicAPI, ""},
		{"other provider", http.MethodGet, logicRoot + armBase + "Microsoft.Network/virtualNetworks" + logicAPI, ""},
		{"other type", http.MethodGet, logicRoot + logicNS + "/integrationAccounts" + logicAPI, ""},
		{"subscription list", http.MethodGet, logicRoot + "/subscriptions/" + armSub +
			"/providers/Microsoft.Logic/workflows" + logicAPI, ""},
		{
			"resource group put",
			http.MethodPut,
			logicRoot + "/subscriptions/" + armSub + "/resourceGroups/x" + logicAPI,
			"{}",
		},
		{"traversal", http.MethodGet, logicRoot + armBase + "Microsoft.Logic/workflows/%2e%2e" + logicAPI, ""},
		{"encoded slash", http.MethodGet, logicRoot + armBase + "Microsoft.Logic/workflows/a%2Fb" + logicAPI, ""},
		{"empty", http.MethodGet, logicRoot + "/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lbDo(t, h, tt.method, tt.path, tt.body)
			assert.GreaterOrEqual(t, got.Code, http.StatusBadRequest, "%s %s", tt.method, tt.path)
			assert.NotContains(t, got.Body.String(), "sig=")
			assert.NotEqual(t, http.StatusOK, got.Code)
		})
	}

	state := logicGet(t, h, raw+logicAPI)
	assert.Equal(t, "greeter", state["name"], "rejected calls changed nothing")
}

func TestAzureLogicProxyAccessRejectsBadInput(t *testing.T) {
	t.Parallel()

	h := newLogicDashboard(t)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"invoke bad sub", http.MethodPost, logicAccess + "/invoke/a%20b/demo/greeter/manual", "{}"},
		{"invoke bad trigger", http.MethodPost, logicAccess + "/invoke" + logicIDs + "/a$b", "{}"},
		{"invoke not json", http.MethodPost, logicAccess + "/invoke" + logicIDs + "/manual", "not json"},
		{
			"invoke huge", http.MethodPost, logicAccess + "/invoke" + logicIDs + "/manual",
			`{"a":"` + strings.Repeat("x", 1<<17) + `"}`,
		},
		{"callback bad wf", http.MethodPost, logicAccess + "/callback/" + armSub + "/demo/a$b/manual", ""},
		{"content get on callback", http.MethodGet, logicAccess + "/callback" + logicIDs + "/manual", ""},
		{"invoke get", http.MethodGet, logicAccess + "/invoke" + logicIDs + "/manual", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lbDo(t, h, tt.method, tt.path, tt.body)
			assert.GreaterOrEqual(
				t,
				got.Code,
				http.StatusBadRequest,
				"%s %s: %s",
				tt.method,
				tt.path,
				got.Body.String(),
			)
			assert.NotContains(t, got.Body.String(), "sig=")
		})
	}
}

func TestAzureLogicProxyUnavailableWithoutService(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, p := range []struct{ method, path string }{
		{http.MethodGet, logicRoot + armBase + "Microsoft.Logic/workflows" + logicAPI},
		{http.MethodPost, logicRoot + logicWF + "/enable" + logicAPI},
		{http.MethodPost, logicAccess + "/callback" + logicIDs + "/manual"},
		{http.MethodPost, logicAccess + "/invoke" + logicIDs + "/manual"},
		{http.MethodGet, logicAccess + "/content" + logicIDs + "/r/trigger/outputs"},
	} {
		got := lbDo(t, h, p.method, p.path, `{}`)
		assert.Equal(t, http.StatusServiceUnavailable, got.Code, "%s %s", p.method, p.path)
	}
}
