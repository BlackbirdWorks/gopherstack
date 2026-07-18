package networkmonitor_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

func newTestHandler(t *testing.T) *networkmonitor.Handler {
	t.Helper()

	b := networkmonitor.NewInMemoryBackend("us-east-1", "000000000000")
	h := networkmonitor.NewHandler(b)
	h.AccountID = "000000000000"
	h.DefaultRegion = "us-east-1"

	return h
}

func doNMRequest(
	t *testing.T,
	h *networkmonitor.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/networkmonitor/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	if err := h.Handler()(c); err != nil {
		t.Logf("handler returned error: %v", err)
	}

	return rec
}

// createMonitorP creates a monitor via the handler. Returns the monitor ARN.
func createMonitorP(t *testing.T, h *networkmonitor.Handler, name string) string {
	t.Helper()

	rec := doNMRequest(t, h, http.MethodPost, "/monitors", map[string]any{"monitorName": name})
	require.Equal(t, http.StatusOK, rec.Code, "create monitor: %s", rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	arn, _ := out["monitorArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// createProbeP creates a probe in the given monitor. Returns the probe ID.
func createProbeP(t *testing.T, h *networkmonitor.Handler, monitorName, destination, protocol string) string {
	t.Helper()

	rec := doNMRequest(t, h, http.MethodPost, "/monitors/"+monitorName+"/probes", map[string]any{
		"probe": map[string]any{
			"destination": destination,
			"protocol":    protocol,
			"sourceArn":   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create probe: %s", rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	id, _ := out["probeId"].(string)
	require.NotEmpty(t, id)

	return id
}

// TestHandler_RouteMatcher verifies RouteMatcher gates on both the signing
// service ("networkmonitor") and the REST path prefixes ("/monitors",
// "/tags/"), independent of HTTP method -- method-specific routing is
// ExtractOperation's job, tested separately below.
func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name    string
		method  string
		path    string
		service string
		want    bool
	}{
		{
			name: "monitors list with networkmonitor service", method: http.MethodGet,
			path: "/monitors", service: "networkmonitor", want: true,
		},
		{
			name: "monitor sub-path with PATCH", method: http.MethodPatch,
			path: "/monitors/mon-1", service: "networkmonitor", want: true,
		},
		{
			name: "probes sub-path", method: http.MethodPost,
			path: "/monitors/mon-1/probes", service: "networkmonitor", want: true,
		},
		{
			name: "tags path", method: http.MethodGet,
			path:    "/tags/arn:aws:networkmonitor:us-east-1:000000000000:monitor/mon-1",
			service: "networkmonitor", want: true,
		},
		{
			name: "unrelated path", method: http.MethodGet,
			path: "/unrelated", service: "networkmonitor", want: false,
		},
		{
			name: "monitors path with wrong signing service", method: http.MethodGet,
			path: "/monitors", service: "otherservice", want: false,
		},
		{
			name: "monitors path with no signing service", method: http.MethodGet,
			path: "/monitors", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.service != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/"+tt.service+"/aws4_request",
				)
			}

			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			if got := matcher(c); got != tt.want {
				t.Errorf("matcher(%s %s, service=%q): got %v, want %v",
					tt.method, tt.path, tt.service, got, tt.want)
			}
		})
	}
}

// TestHandler_ExtractOperation_MethodPathMatrix verifies the full
// method+path -> operation matrix, notably that UpdateMonitor/UpdateProbe
// are routed on PATCH (not PUT, which real networkmonitor does not use).
func TestHandler_ExtractOperation_MethodPathMatrix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const tagArn = "/tags/arn:aws:networkmonitor:us-east-1:000000000000:monitor/mon-1"

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{name: "create monitor", method: http.MethodPost, path: "/monitors", want: "CreateMonitor"},
		{name: "list monitors", method: http.MethodGet, path: "/monitors", want: "ListMonitors"},
		{name: "get monitor", method: http.MethodGet, path: "/monitors/mon-1", want: "GetMonitor"},
		{
			name: "update monitor is PATCH", method: http.MethodPatch,
			path: "/monitors/mon-1", want: "UpdateMonitor",
		},
		{
			name: "update monitor via PUT is unsupported", method: http.MethodPut,
			path: "/monitors/mon-1", want: "Unknown",
		},
		{name: "delete monitor", method: http.MethodDelete, path: "/monitors/mon-1", want: "DeleteMonitor"},
		{
			name: "create probe", method: http.MethodPost,
			path: "/monitors/mon-1/probes", want: "CreateProbe",
		},
		{
			name: "get probe", method: http.MethodGet,
			path: "/monitors/mon-1/probes/probe-1", want: "GetProbe",
		},
		{
			name: "update probe is PATCH", method: http.MethodPatch,
			path: "/monitors/mon-1/probes/probe-1", want: "UpdateProbe",
		},
		{
			name: "update probe via PUT is unsupported", method: http.MethodPut,
			path: "/monitors/mon-1/probes/probe-1", want: "Unknown",
		},
		{
			name: "delete probe", method: http.MethodDelete,
			path: "/monitors/mon-1/probes/probe-1", want: "DeleteProbe",
		},
		{name: "list tags", method: http.MethodGet, path: tagArn, want: "ListTagsForResource"},
		{name: "tag resource", method: http.MethodPost, path: tagArn, want: "TagResource"},
		{name: "untag resource", method: http.MethodDelete, path: tagArn, want: "UntagResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			if got := h.ExtractOperation(c); got != tt.want {
				t.Errorf("ExtractOperation(%s %s): got %q, want %q", tt.method, tt.path, got, tt.want)
			}
		})
	}
}
