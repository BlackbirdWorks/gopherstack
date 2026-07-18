package networkmonitor_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandlerGetMonitorProbeTimestampsEpochFloat verifies that probes embedded
// in the GetMonitor response use epoch-second timestamps, not RFC3339
// strings. Real AWS networkmonitor wire format uses Iso8601Timestamp = JSON
// Number.
func TestHandlerGetMonitorProbeTimestampsEpochFloat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createMonitorP(t, h, "pm-ts-mon")
	createProbeP(t, h, "pm-ts-mon", "192.168.1.1", "ICMP")

	rec := doNMRequest(t, h, http.MethodGet, "/monitors/pm-ts-mon", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Probes []map[string]json.RawMessage `json:"probes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Probes, 1, "expected one probe in GetMonitor response")

	createdAtRaw, hasCreatedAt := out.Probes[0]["createdAt"]
	assert.True(t, hasCreatedAt, "probe createdAt must be present in GetMonitor")

	if hasCreatedAt {
		var asFloat float64
		require.NoError(t, json.Unmarshal(createdAtRaw, &asFloat),
			"probe createdAt in GetMonitor must be JSON number, got: %s", string(createdAtRaw))
		assert.Greater(t, asFloat, float64(0))
	}
}

// TestHandlerGetMonitorTimestampsEpochFloat verifies monitor-level timestamps
// are epoch floats.
func TestHandlerGetMonitorTimestampsEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field string
	}{
		{name: "created_at", field: "createdAt"},
		{name: "modified_at", field: "modifiedAt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createMonitorP(t, h, "epoch-mon")

			rec := doNMRequest(t, h, http.MethodGet, "/monitors/epoch-mon", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			fieldRaw, ok := raw[tt.field]
			assert.True(t, ok, "%s must be present", tt.field)

			if ok {
				var asFloat float64
				require.NoError(t, json.Unmarshal(fieldRaw, &asFloat),
					"%s must be JSON number, got: %s", tt.field, string(fieldRaw))
				assert.Greater(t, asFloat, float64(0))
			}
		})
	}
}

// TestHandlerListMonitorsPagination verifies that ListMonitors pagination
// works correctly.
func TestHandlerListMonitorsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		monitors   []string
		maxResults int
		wantPages  []int
	}{
		{
			name:       "two_pages_of_two",
			monitors:   []string{"aaa-mon", "bbb-mon", "ccc-mon", "ddd-mon"},
			maxResults: 2,
			wantPages:  []int{2, 2},
		},
		{
			name:       "exact_fit_no_token",
			monitors:   []string{"aaa-mon", "bbb-mon"},
			maxResults: 2,
			wantPages:  []int{2},
		},
		{
			name:       "three_items_page_two",
			monitors:   []string{"aaa-mon", "bbb-mon", "ccc-mon"},
			maxResults: 2,
			wantPages:  []int{2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, name := range tt.monitors {
				doNMRequest(t, h, http.MethodPost, "/monitors", map[string]any{"monitorName": name})
			}

			var token string
			var pageCounts []int

			for {
				path := fmt.Sprintf("/monitors?maxResults=%d", tt.maxResults)
				if token != "" {
					path += "&nextToken=" + token
				}

				rec := doNMRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct { //nolint:govet // fieldalignment: readability over micro-optimization
					Monitors  []any  `json:"monitors"`
					NextToken string `json:"nextToken"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				pageCounts = append(pageCounts, len(out.Monitors))
				token = out.NextToken

				if token == "" {
					break
				}
			}

			assert.Equal(t, tt.wantPages, pageCounts, "page counts mismatch")
		})
	}
}

// TestHandlerListMonitorsTokenPastEnd verifies that a nextToken past the last
// monitor returns an empty list, not the first page.
func TestHandlerListMonitorsTokenPastEnd(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doNMRequest(t, h, http.MethodPost, "/monitors", map[string]any{"monitorName": "only-mon"})

	rec := doNMRequest(t, h, http.MethodGet, "/monitors?nextToken=zzz-past-end", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct { //nolint:govet // fieldalignment: readability over micro-optimization
		Monitors  []any  `json:"monitors"`
		NextToken string `json:"nextToken"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Empty(t, out.Monitors, "token past last item must return empty list, not first page")
	assert.Empty(t, out.NextToken)
}
