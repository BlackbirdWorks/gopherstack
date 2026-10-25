package azurearm_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const monitorBase = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg1/providers/"

var errMonitorBoom = &azurearm.MonitorError{
	Code:    "InvalidParameter",
	Message: "bad column",
	Status:  http.StatusBadRequest,
}

type fakeMonitor struct {
	err   error
	calls []string
	mu    sync.Mutex
}

func (f *fakeMonitor) record(op string, r azurearm.MonitorRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, op+" "+r.Namespace+" "+joinPath(r))

	return f.err
}

func joinPath(r azurearm.MonitorRequest) string {
	var sb strings.Builder

	for i, t := range r.Types {
		sb.WriteString("/" + t)

		if i < len(r.Names) {
			sb.WriteString("/" + r.Names[i])
		}
	}

	return sb.String()
}

func (f *fakeMonitor) Put(r azurearm.MonitorRequest, _ map[string]any) (map[string]any, error) {
	return map[string]any{"name": r.Names[len(r.Names)-1]}, f.record("put", r)
}

func (f *fakeMonitor) Get(r azurearm.MonitorRequest) (map[string]any, error) {
	return map[string]any{"name": r.Names[len(r.Names)-1]}, f.record("get", r)
}

func (f *fakeMonitor) Patch(r azurearm.MonitorRequest, _ map[string]any) (map[string]any, error) {
	return map[string]any{"patched": true}, f.record("patch", r)
}

func (f *fakeMonitor) Delete(r azurearm.MonitorRequest) error { return f.record("delete", r) }

func (f *fakeMonitor) List(r azurearm.MonitorRequest) ([]map[string]any, error) {
	return []map[string]any{{"name": "x"}}, f.record("list", r)
}

func (f *fakeMonitor) Action(r azurearm.MonitorRequest, action string, _ map[string]any) (map[string]any, error) {
	return map[string]any{"primarySharedKey": "k"}, f.record("action:"+action, r)
}

func (f *fakeMonitor) DeleteInGroup(string) {}
func (f *fakeMonitor) Reset()               {}

func newMonitorHandler(t *testing.T, dp azurearm.MonitorLogs) *azurearm.Handler {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	if dp != nil {
		h.Registry.SetMonitorLogs(dp)
	}

	return h
}

func monitorDo(t *testing.T, h *azurearm.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Host = "host:10006"
	rec := httptest.NewRecorder()
	newEchoServer(h).ServeHTTP(rec, req)

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}

func TestMonitorRoutesReachDataPlane(t *testing.T) {
	t.Parallel()

	ws := monitorBase + "Microsoft.OperationalInsights/workspaces/ws1"
	dcr := monitorBase + "Microsoft.Insights/dataCollectionRules/r1"

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"workspace put", http.MethodPut, ws, "put Microsoft.OperationalInsights /workspaces/ws1"},
		{"workspace get", http.MethodGet, ws, "get Microsoft.OperationalInsights /workspaces/ws1"},
		{"workspace patch", http.MethodPatch, ws, "patch Microsoft.OperationalInsights /workspaces/ws1"},
		{"workspace delete", http.MethodDelete, ws, "delete Microsoft.OperationalInsights /workspaces/ws1"},
		{"workspace list", http.MethodGet, monitorBase + "Microsoft.OperationalInsights/workspaces",
			"list Microsoft.OperationalInsights /workspaces"},
		{
			"shared keys",
			http.MethodPost,
			ws + "/sharedKeys",
			"action:sharedKeys Microsoft.OperationalInsights /workspaces/ws1",
		},
		{"regenerate", http.MethodPost, ws + "/regenerateSharedKey",
			"action:regenerateSharedKey Microsoft.OperationalInsights /workspaces/ws1"},
		{
			"list keys",
			http.MethodPost,
			ws + "/listKeys",
			"action:listKeys Microsoft.OperationalInsights /workspaces/ws1",
		},
		{"table put", http.MethodPut, ws + "/tables/T_CL",
			"put Microsoft.OperationalInsights /workspaces/ws1/tables/T_CL"},
		{"table patch", http.MethodPatch, ws + "/tables/T_CL",
			"patch Microsoft.OperationalInsights /workspaces/ws1/tables/T_CL"},
		{"table list", http.MethodGet, ws + "/tables", "list Microsoft.OperationalInsights /workspaces/ws1/tables"},
		{"dcr put", http.MethodPut, dcr, "put Microsoft.Insights /dataCollectionRules/r1"},
		{"dcr patch", http.MethodPatch, dcr, "patch Microsoft.Insights /dataCollectionRules/r1"},
		{"dce get", http.MethodGet, monitorBase + "Microsoft.Insights/dataCollectionEndpoints/e1",
			"get Microsoft.Insights /dataCollectionEndpoints/e1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fm := &fakeMonitor{}
			h := newMonitorHandler(t, fm)

			code, _ := monitorDo(t, h, tt.method, tt.path, map[string]any{"location": "local"})
			assert.Equal(t, http.StatusOK, code)
			require.NotEmpty(t, fm.calls)
			assert.Equal(t, tt.want, fm.calls[len(fm.calls)-1])
		})
	}
}

func TestMonitorOtherTypesStayGeneric(t *testing.T) {
	t.Parallel()

	fm := &fakeMonitor{}
	h := newMonitorHandler(t, fm)

	for _, p := range []string{
		monitorBase + "Microsoft.Insights/actionGroups/ag1",
		monitorBase + "Microsoft.OperationalInsights/workspaces/ws1/savedSearches/s1",
	} {
		code, _ := monitorDo(t, h, http.MethodPut, p, map[string]any{"location": "local"})
		assert.Equal(t, http.StatusOK, code, p)

		code, body := monitorDo(t, h, http.MethodGet, p, nil)
		assert.Equal(t, http.StatusOK, code, p)
		assert.NotEmpty(t, body["id"], p)
	}

	assert.Empty(t, fm.calls)
}

func TestMonitorErrors(t *testing.T) {
	t.Parallel()

	ws := monitorBase + "Microsoft.OperationalInsights/workspaces/ws1"

	tests := []struct {
		name     string
		dp       azurearm.MonitorLogs
		method   string
		wantCode string
		want     int
	}{
		{name: "monitor error keeps its status and code", dp: &fakeMonitor{err: errMonitorBoom}, method: http.MethodPut,
			want: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "not found maps to ResourceNotFound", dp: &fakeMonitor{err: azurearm.ErrResourceNotFound},
			method: http.MethodGet, want: http.StatusNotFound, wantCode: "ResourceNotFound"},
		{name: "delete of a missing resource is 204", dp: &fakeMonitor{err: azurearm.ErrResourceNotFound},
			method: http.MethodDelete, want: http.StatusNoContent},
		{name: "no data plane wired answers 503", method: http.MethodPut,
			want: http.StatusServiceUnavailable, wantCode: "ServiceUnavailable"},
		{name: "no data plane wired get is 404", method: http.MethodGet, want: http.StatusNotFound,
			wantCode: "ResourceNotFound"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newMonitorHandler(t, tt.dp)

			code, body := monitorDo(t, h, tt.method, ws, map[string]any{"location": "local"})
			assert.Equal(t, tt.want, code)

			if tt.wantCode != "" {
				errBody, _ := body["error"].(map[string]any)
				assert.Equal(t, tt.wantCode, errBody["code"])
			}
		})
	}
}
