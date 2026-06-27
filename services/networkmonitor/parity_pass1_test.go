package networkmonitor_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

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

// TestParity_ProbeTimestamps_EpochFloat verifies that GetProbe returns createdAt/modifiedAt
// as JSON numbers (epoch seconds), not RFC3339 strings. Real AWS networkmonitor wire format
// uses Iso8601Timestamp = JSON Number.
func TestParity_ProbeTimestamps_EpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		protocol    string
		destination string
	}{
		{name: "icmp_probe", protocol: "ICMP", destination: "10.0.0.1"},
		{name: "icmp_probe2", protocol: "ICMP", destination: "10.0.0.2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createMonitorP(t, h, "ts-mon")
			probeID := createProbeP(t, h, "ts-mon", tt.destination, tt.protocol)

			rec := doNMRequest(t, h, http.MethodGet, "/monitors/ts-mon/probes/"+probeID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			createdAtRaw, hasCreatedAt := raw["createdAt"]
			assert.True(t, hasCreatedAt, "createdAt must be present in GetProbe response")

			if hasCreatedAt {
				var asFloat float64
				require.NoError(t, json.Unmarshal(createdAtRaw, &asFloat),
					"createdAt must be a JSON number (epoch seconds), got: %s", string(createdAtRaw))
				assert.Greater(t, asFloat, float64(0), "createdAt epoch value must be positive")
			}
		})
	}
}

// TestParity_GetMonitor_ProbeTimestamps_EpochFloat verifies that probes embedded in the
// GetMonitor response also use epoch-second timestamps, not RFC3339 strings.
func TestParity_GetMonitor_ProbeTimestamps_EpochFloat(t *testing.T) {
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

// TestParity_GetMonitor_TimestampsEpochFloat verifies monitor-level timestamps are epoch floats.
func TestParity_GetMonitor_TimestampsEpochFloat(t *testing.T) {
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

// TestParity_ListMonitors_Pagination verifies that ListMonitors pagination works correctly.
func TestParity_ListMonitors_Pagination(t *testing.T) {
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

// TestParity_ListMonitors_TokenPastEnd verifies that a nextToken past the last monitor
// returns an empty list, not the first page.
func TestParity_ListMonitors_TokenPastEnd(t *testing.T) {
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

// TestParity_AddressFamily_AutoDetect verifies that addressFamily is IPV4 for IPv4 destinations
// and IPV6 for IPv6 destinations (colon-containing addresses).
func TestParity_AddressFamily_AutoDetect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination string
		wantFamily  string
	}{
		{name: "ipv4", destination: "10.0.0.1", wantFamily: "IPV4"},
		{name: "ipv6", destination: "2001:db8::1", wantFamily: "IPV6"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createMonitorP(t, h, "af-mon")
			createProbeP(t, h, "af-mon", tt.destination, "ICMP")

			rec := doNMRequest(t, h, http.MethodPost, "/monitors/af-mon/probes", map[string]any{
				"probe": map[string]any{
					"destination": tt.destination,
					"protocol":    "ICMP",
					"sourceArn":   "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-xyz",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantFamily, out["addressFamily"], "addressFamily for %s", tt.destination)
		})
	}
}
