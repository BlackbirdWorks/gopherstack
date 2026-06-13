package networkmonitor_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

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

func TestHandlerCreateMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "valid create",
			body:       map[string]any{"monitorName": "test-mon"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid period",
			body:       map[string]any{"monitorName": "x", "aggregationPeriod": 45},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doNMRequest(t, h, http.MethodPost, "/monitors", tc.body)

			if rr.Code != tc.wantStatus {
				t.Errorf(
					"status: got %d, want %d — body: %s",
					rr.Code,
					tc.wantStatus,
					rr.Body.String(),
				)
			}
		})
	}
}

func TestHandlerGetMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		monName    string
		wantStatus int
		create     bool
	}{
		{
			name:       "existing monitor",
			create:     true,
			monName:    "my-mon",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing monitor",
			create:     false,
			monName:    "ghost",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.create {
				rr := doNMRequest(
					t,
					h,
					http.MethodPost,
					"/monitors",
					map[string]any{"monitorName": tc.monName},
				)
				if rr.Code != http.StatusOK {
					t.Fatalf("create: status %d", rr.Code)
				}
			}

			rr := doNMRequest(t, h, http.MethodGet, "/monitors/"+tc.monName, nil)

			if rr.Code != tc.wantStatus {
				t.Errorf(
					"status: got %d, want %d — body: %s",
					rr.Code,
					tc.wantStatus,
					rr.Body.String(),
				)
			}
		})
	}
}

func TestHandlerDeleteMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		monName    string
		wantStatus int
		create     bool
	}{
		{
			name:       "delete existing",
			create:     true,
			monName:    "del-mon",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete missing",
			create:     false,
			monName:    "ghost",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.create {
				rr := doNMRequest(
					t,
					h,
					http.MethodPost,
					"/monitors",
					map[string]any{"monitorName": tc.monName},
				)
				if rr.Code != http.StatusOK {
					t.Fatalf("create: status %d", rr.Code)
				}
			}

			rr := doNMRequest(t, h, http.MethodDelete, "/monitors/"+tc.monName, nil)

			if rr.Code != tc.wantStatus {
				t.Errorf(
					"status: got %d, want %d — body: %s",
					rr.Code,
					tc.wantStatus,
					rr.Body.String(),
				)
			}
		})
	}
}

func TestHandlerListMonitors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"first-mon", "second-mon"} {
		rr := doNMRequest(t, h, http.MethodPost, "/monitors", map[string]any{"monitorName": name})
		if rr.Code != http.StatusOK {
			t.Fatalf("create %s: status %d", name, rr.Code)
		}
	}

	rr := doNMRequest(t, h, http.MethodGet, "/monitors", nil)

	if rr.Code != http.StatusOK {
		t.Fatalf("list: status %d — body: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	monitors, ok := resp["monitors"].([]any)
	if !ok {
		t.Fatalf("monitors field missing or wrong type in: %s", rr.Body.String())
	}

	if len(monitors) != 2 {
		t.Errorf("count: got %d, want 2", len(monitors))
	}
}

func TestHandlerUpdateMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		period     int64
		wantStatus int
	}{
		{
			name:       "update to 30",
			period:     30,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid period",
			period:     45,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doNMRequest(
				t,
				h,
				http.MethodPost,
				"/monitors",
				map[string]any{"monitorName": "upd-mon"},
			)

			if rr.Code != http.StatusOK {
				t.Fatalf("create: status %d", rr.Code)
			}

			rr = doNMRequest(
				t,
				h,
				http.MethodPatch,
				"/monitors/upd-mon",
				map[string]any{"aggregationPeriod": tc.period},
			)

			if rr.Code != tc.wantStatus {
				t.Errorf(
					"status: got %d, want %d — body: %s",
					rr.Code,
					tc.wantStatus,
					rr.Body.String(),
				)
			}
		})
	}
}

func TestHandlerProbeLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doNMRequest(
		t,
		h,
		http.MethodPost,
		"/monitors",
		map[string]any{"monitorName": "probe-mon"},
	)
	if rr.Code != http.StatusOK {
		t.Fatalf("create monitor: status %d", rr.Code)
	}

	rr = doNMRequest(t, h, http.MethodPost, "/monitors/probe-mon/probes", map[string]any{
		"probe": map[string]any{
			"destination": "10.0.0.2",
			"protocol":    "ICMP",
			"sourceArn":   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-abc",
		},
	})

	if rr.Code != http.StatusOK {
		t.Fatalf("create probe: status %d — body: %s", rr.Code, rr.Body.String())
	}

	var probeResp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &probeResp); err != nil {
		t.Fatalf("unmarshal probe: %v", err)
	}

	probeID, _ := probeResp["probeId"].(string)
	if probeID == "" {
		t.Fatal("expected non-empty probeId")
	}

	rr = doNMRequest(t, h, http.MethodGet, "/monitors/probe-mon/probes/"+probeID, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("get probe: status %d", rr.Code)
	}

	rr = doNMRequest(t, h, http.MethodPatch, "/monitors/probe-mon/probes/"+probeID, map[string]any{
		"destination": "10.0.0.3",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("update probe: status %d — %s", rr.Code, rr.Body.String())
	}

	rr = doNMRequest(t, h, http.MethodDelete, "/monitors/probe-mon/probes/"+probeID, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete probe: status %d", rr.Code)
	}

	rr = doNMRequest(t, h, http.MethodGet, "/monitors/probe-mon/probes/"+probeID, nil)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 after delete, got %d", rr.Code)
	}
}

func TestHandlerTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doNMRequest(t, h, http.MethodPost, "/monitors", map[string]any{
		"monitorName": "tagged",
		"tags":        map[string]any{"env": "prod"},
	})

	if rr.Code != http.StatusOK {
		t.Fatalf("create: status %d", rr.Code)
	}

	var mon map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &mon); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	monARN, _ := mon["monitorArn"].(string)
	if monARN == "" {
		t.Fatal("expected monitorArn in response")
	}

	rr = doNMRequest(t, h, http.MethodGet, "/tags/"+monARN, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list tags: status %d", rr.Code)
	}

	var tagResp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &tagResp); err != nil {
		t.Fatalf("unmarshal tags: %v", err)
	}

	tags, _ := tagResp["tags"].(map[string]any)
	if tags["env"] != "prod" {
		t.Errorf("tag env: got %v, want prod", tags["env"])
	}

	rr = doNMRequest(t, h, http.MethodPost, "/tags/"+monARN, map[string]any{
		"tags": map[string]any{"team": "sre"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("tag: status %d", rr.Code)
	}

	rr = doNMRequest(t, h, http.MethodDelete, "/tags/"+monARN+"?tagKeys=env", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("untag: status %d", rr.Code)
	}

	rr = doNMRequest(t, h, http.MethodGet, "/tags/"+monARN, nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &tagResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	tags, _ = tagResp["tags"].(map[string]any)

	if _, ok := tags["env"]; ok {
		t.Error("env tag should be removed")
	}

	if tags["team"] != "sre" {
		t.Errorf("team tag: got %v, want sre", tags["team"])
	}
}
