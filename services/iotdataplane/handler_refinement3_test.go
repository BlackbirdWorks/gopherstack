package iotdataplane_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// doRequestCT sends a request with a specific Content-Type header.
func doRequestCT(
	t *testing.T, h *iotdataplane.Handler, method, path string, body []byte, ct string,
) *httptest.ResponseRecorder {
	t.Helper()

	var r *bytes.Reader
	if body != nil {
		r = bytes.NewReader(body)
	} else {
		r = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", ct)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ── Topic validation (issue #9) ───────────────────────────────────────────────

// TestRefinement3_TopicValidation_ControlChars validates per-segment control byte rejection.
// Control characters cannot be embedded in HTTP URLs, so we test validateTopic directly.
func TestRefinement3_TopicValidation_ControlChars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "nul_byte", topic: "sensor/\x00data", wantErr: true},
		{name: "tab", topic: "sensor/\tdata", wantErr: true},
		{name: "newline", topic: "sensor/\ndata", wantErr: true},
		{name: "carriage_return", topic: "a/\r/b", wantErr: true},
		{name: "del_0x7f", topic: "a/\x7f", wantErr: true},
		{name: "unit_separator_0x1f", topic: "a/\x1f/b", wantErr: true},
		{name: "printable_ascii", topic: "sensor/temp", wantErr: false},
		{name: "space_0x20_allowed", topic: "sensor/temp data", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateTopic(tt.topic)
			if tt.wantErr {
				require.Error(t, err, "topic %q should fail validation", tt.topic)
				assert.ErrorIs(t, err, iotdataplane.ErrValidation)
			} else {
				assert.NoError(t, err, "topic %q should pass validation", tt.topic)
			}
		})
	}
}

func TestRefinement3_TopicValidation_AwsShadowReserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{
			name:    "shadow_update_reserved",
			topic:   "$aws/things/mydevice/shadow/update",
			wantErr: true,
		},
		{
			name:    "shadow_accepted_reserved",
			topic:   "$aws/things/mydevice/shadow/update/accepted",
			wantErr: true,
		},
		{
			name:    "aws_non_shadow_allowed",
			topic:   "$aws/things/mydevice/jobs/get",
			wantErr: false,
		},
		{
			name:    "non_aws_dollar_allowed",
			topic:   "$local/sensor/data",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateTopic(tt.topic)
			if tt.wantErr {
				require.Error(t, err, "topic %q should fail validation", tt.topic)
				assert.ErrorIs(t, err, iotdataplane.ErrValidation)
			} else {
				assert.NoError(t, err, "topic %q should pass", tt.topic)
			}
		})
	}
}

// ── Shadow reserved names (issue #10) ────────────────────────────────────────

func TestRefinement3_ShadowName_ReservedKeywords(t *testing.T) {
	t.Parallel()

	reserved := []string{
		"update", "get", "delete", "accepted", "rejected", "delta", "documents",
	}

	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name="+name, nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "reserved name %q should be rejected", name)
			assert.Contains(t, rec.Body.String(), "InvalidRequestException")
		})
	}
}

func TestRefinement3_ShadowName_NonReservedAllowed(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "myConfig", []byte(`{"state":{"desired":{"k":"v"}}}`))
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/things/thing1/shadow?name=myConfig", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ── Shadow deep merge (issue #5) ─────────────────────────────────────────────

func TestRefinement3_ShadowMerge_DesiredPartialUpdate(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev1/shadow", []byte(`{
		"state": {"desired": {"temp": 68, "fan": "on"}}
	}`))

	doRequest(t, h, http.MethodPost, "/things/dev1/shadow", []byte(`{
		"state": {"desired": {"fan": "off"}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev1/shadow", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.InDelta(t, float64(68), desired["temp"], 0, "temp must be preserved after partial update")
	assert.Equal(t, "off", desired["fan"])
}

func TestRefinement3_ShadowMerge_NullDeletion(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev2/shadow", []byte(`{
		"state": {"desired": {"keep": "yes", "remove": "bye"}}
	}`))

	doRequest(t, h, http.MethodPost, "/things/dev2/shadow", []byte(`{
		"state": {"desired": {"remove": null}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev2/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	assert.Equal(t, "yes", desired["keep"])
	_, hasRemove := desired["remove"]
	assert.False(t, hasRemove, "null-deleted key must not appear in response")
}

func TestRefinement3_ShadowMerge_ReportedIndependent(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev3/shadow", []byte(`{
		"state": {"desired": {"target": 72}, "reported": {"current": 68}}
	}`))

	doRequest(t, h, http.MethodPost, "/things/dev3/shadow", []byte(`{
		"state": {"reported": {"current": 72}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev3/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	reported := state["reported"].(map[string]any)
	assert.InDelta(t, float64(72), desired["target"], 0, "desired must be unchanged")
	assert.InDelta(t, float64(72), reported["current"], 0)
}

func TestRefinement3_ShadowMerge_VersionIncrements(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	rec1 := doRequest(t, h, http.MethodPost, "/things/dev-ver/shadow",
		[]byte(`{"state":{"desired":{"k":"v1"}}}`))
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))

	rec2 := doRequest(t, h, http.MethodPost, "/things/dev-ver/shadow",
		[]byte(`{"state":{"desired":{"k":"v2"}}}`))
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))

	v1 := r1["version"].(float64)
	v2 := r2["version"].(float64)
	assert.InDelta(t, v1+1, v2, 0, "version must increment on each update")
}

// ── Shadow delta (issue #6) ───────────────────────────────────────────────────

func TestRefinement3_ShadowDelta_ComputedOnGet(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-delta/shadow", []byte(`{
		"state": {"desired": {"target": 72}, "reported": {"current": 68}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-delta/shadow", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	delta, hasDelta := state["delta"]
	require.True(t, hasDelta, "delta must be present when desired != reported")

	deltaMap := delta.(map[string]any)
	assert.InDelta(t, float64(72), deltaMap["target"], 0)
	_, hasUnrelated := deltaMap["current"]
	assert.False(t, hasUnrelated, "delta must not contain reported-only keys")
}

func TestRefinement3_ShadowDelta_AbsentWhenEqual(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-nodelta/shadow", []byte(`{
		"state": {"desired": {"temp": 70}, "reported": {"temp": 70}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-nodelta/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	_, hasDelta := state["delta"]
	assert.False(t, hasDelta, "delta must be absent when desired equals reported")
}

func TestRefinement3_ShadowDelta_UpdatedAfterReported(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// Initially desired=72, reported missing → delta contains target.
	doRequest(t, h, http.MethodPost, "/things/dev-delta2/shadow", []byte(`{
		"state": {"desired": {"target": 72}}
	}`))

	// Device reports it reached the target.
	doRequest(t, h, http.MethodPost, "/things/dev-delta2/shadow", []byte(`{
		"state": {"reported": {"target": 72}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-delta2/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	_, hasDelta := state["delta"]
	assert.False(t, hasDelta, "delta must clear once reported matches desired")
}

// ── Shadow metadata (issue #6) ────────────────────────────────────────────────

func TestRefinement3_ShadowMetadata_PresentAfterUpdate(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-meta/shadow", []byte(`{
		"state": {"desired": {"temp": 72}, "reported": {"current": 70}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-meta/shadow", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	meta, hasMeta := resp["metadata"]
	require.True(t, hasMeta, "metadata must be present after update")

	metaMap := meta.(map[string]any)
	desiredMeta := metaMap["desired"].(map[string]any)
	tempMeta := desiredMeta["temp"].(map[string]any)
	ts, hasTS := tempMeta["timestamp"]
	assert.True(t, hasTS, "metadata.desired.temp.timestamp must be present")
	assert.Greater(t, ts.(float64), float64(0))
}

func TestRefinement3_ShadowMetadata_UnchangedFieldTimestampPreserved(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-meta2/shadow", []byte(`{
		"state": {"desired": {"a": 1, "b": 2}}
	}`))

	rec := doRequest(t, h, http.MethodGet, "/things/dev-meta2/shadow", nil)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp1))
	ts1 := resp1["metadata"].(map[string]any)["desired"].(map[string]any)["a"].(map[string]any)["timestamp"].(float64)

	// Update only b.
	doRequest(t, h, http.MethodPost, "/things/dev-meta2/shadow", []byte(`{
		"state": {"desired": {"b": 99}}
	}`))

	rec = doRequest(t, h, http.MethodGet, "/things/dev-meta2/shadow", nil)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp2))
	ts2 := resp2["metadata"].(map[string]any)["desired"].(map[string]any)["a"].(map[string]any)["timestamp"].(float64)

	assert.InDelta(t, ts1, ts2, 0, "timestamp for unmodified field must not change")
}

// ── clientToken echo (issue #7) ──────────────────────────────────────────────

func TestRefinement3_ClientToken_Echoed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantToken   string
		wantPresent bool
	}{
		{
			name:        "token_echoed",
			body:        `{"state":{"desired":{"k":"v"}},"clientToken":"req-abc-123"}`,
			wantToken:   "req-abc-123",
			wantPresent: true,
		},
		{
			name:        "no_token_absent",
			body:        `{"state":{"desired":{"k":"v"}}}`,
			wantPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/things/dev-ct/shadow", []byte(tt.body))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			token, hasToken := resp["clientToken"]
			if tt.wantPresent {
				require.True(t, hasToken, "clientToken must be present in response")
				assert.Equal(t, tt.wantToken, token)
			} else {
				assert.False(t, hasToken, "clientToken must be absent when not provided")
			}
		})
	}
}

// ── Named shadow path routing (issue #8) ─────────────────────────────────────

func TestRefinement3_NamedShadowPath_PathStyle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(*iotdataplane.InMemoryBackend)
		path     string
		method   string
		body     []byte
		wantCode int
	}{
		{
			name: "get_via_query_param",
			setup: func(b *iotdataplane.InMemoryBackend) {
				b.AddShadowInternal("device1", "config", []byte(`{"state":{"desired":{"k":"v"}}}`))
			},
			path:     "/things/device1/shadow?name=config",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name: "get_via_path_style",
			setup: func(b *iotdataplane.InMemoryBackend) {
				b.AddShadowInternal("device1", "config", []byte(`{"state":{"desired":{"k":"v"}}}`))
			},
			path:     "/things/device1/shadow/name/config",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "get_path_style_not_found",
			path:     "/things/device2/shadow/name/nonexistent",
			method:   http.MethodGet,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "post_via_path_style",
			path:     "/things/device3/shadow/name/myConfig",
			method:   http.MethodPost,
			body:     []byte(`{"state":{"desired":{"k":"v"}}}`),
			wantCode: http.StatusOK,
		},
		{
			name: "delete_via_path_style",
			setup: func(b *iotdataplane.InMemoryBackend) {
				b.AddShadowInternal("device4", "todel", []byte(`{"state":{"desired":{"k":"v"}}}`))
			},
			path:     "/things/device4/shadow/name/todel",
			method:   http.MethodDelete,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			h := iotdataplane.NewHandler(b)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "path %q method %q", tt.path, tt.method)
		})
	}
}

func TestRefinement3_NamedShadowPath_PathTakesPrecedenceOverQuery(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("dev", "frompath", []byte(`{"state":{"desired":{"x":1}}}`))
	h := iotdataplane.NewHandler(b)

	// Path says "frompath", query says "fromquery" (doesn't exist). Path must win.
	rec := doRequest(t, h, http.MethodGet, "/things/dev/shadow/name/frompath?name=fromquery", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement3_NamedShadowPath_RouteMatcher(t *testing.T) {
	t.Parallel()

	body := []byte(`{"state":{"desired":{"k":"v"}}}`)

	tests := []struct {
		path     string
		method   string
		body     []byte
		wantCode int
	}{
		// POST creates the shadow → 200 confirms the path was routed.
		{"/things/mydevice/shadow", http.MethodPost, body, http.StatusOK},
		{"/things/mydevice/shadow?name=foo", http.MethodPost, body, http.StatusOK},
		{"/things/mydevice/shadow/name/bar", http.MethodPost, body, http.StatusOK},
		// These paths must NOT be matched by the shadow handler.
		{"/things/mydevice/shadow/extra", http.MethodGet, nil, http.StatusNotFound},
		{"/other/mydevice/shadow", http.MethodGet, nil, http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "path %q method %q", tt.path, tt.method)
		})
	}
}

// ── Pagination: pageSize + off-by-one fix (issues #13, #15) ──────────────────

func TestRefinement3_Pagination_PageSizeParam(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for i := range 30 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("t/%02d", i), []byte("x"), 0))
	}

	h := iotdataplane.NewHandler(b)

	tests := []struct {
		name          string
		query         string
		wantPageCount int
	}{
		{name: "pageSize_param", query: "?pageSize=5", wantPageCount: 5},
		{name: "maxResults_alias", query: "?maxResults=7", wantPageCount: 7},
		{name: "pageSize_wins_over_maxResults", query: "?pageSize=3&maxResults=10", wantPageCount: 3},
		{name: "default_is_25", query: "", wantPageCount: 25},
		{name: "cap_at_100", query: "?pageSize=200", wantPageCount: 30}, // only 30 messages
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet, "/retainedMessage"+tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			topics := resp["retainedTopics"].([]any)
			assert.Len(t, topics, tt.wantPageCount, "query: %s", tt.query)
		})
	}
}

func TestRefinement3_Pagination_OffByOneFixed(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for _, topic := range []string{"a/1", "b/2", "c/3", "d/4", "e/5"} {
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("x"), 0))
	}

	h := iotdataplane.NewHandler(b)

	// Page 1: pageSize=2 → a/1, b/2; nextToken = c/3 (first item of next page).
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage?pageSize=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	topics1 := page1["retainedTopics"].([]any)
	require.Len(t, topics1, 2)
	assert.Equal(t, "a/1", topics1[0].(map[string]any)["topic"])
	assert.Equal(t, "b/2", topics1[1].(map[string]any)["topic"])

	nextToken, hasNext := page1["nextToken"].(string)
	require.True(t, hasNext, "page 1 must have nextToken")
	// nextToken is the first item of the next page (AWS convention).
	assert.Equal(t, "c/3", nextToken)

	// Page 2: cursor=c/3 → page starts at c/3 (nextToken IS the first item of the page).
	rec = doRequest(t, h, http.MethodGet, "/retainedMessage?pageSize=2&nextToken="+nextToken, nil)
	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	topics2 := page2["retainedTopics"].([]any)
	require.Len(t, topics2, 2)
	assert.Equal(t, "c/3", topics2[0].(map[string]any)["topic"], "page 2 starts at cursor token")
	assert.Equal(t, "d/4", topics2[1].(map[string]any)["topic"])
}

func TestRefinement3_Pagination_ListThingsWithShadows_PageSize(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for i := range 30 {
		b.AddShadowInternal(fmt.Sprintf("thing-%02d", i), "", []byte(`{}`))
	}

	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows?pageSize=10", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	things := resp["things"].([]any)
	assert.Len(t, things, 10)
	assert.Contains(t, resp, "nextToken")
}

func TestRefinement3_Pagination_ListNamedShadows_PageSize(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for i := range 30 {
		b.AddShadowInternal("thing1", fmt.Sprintf("shadow-%02d", i), []byte(`{}`))
	}

	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListNamedShadowsForThing/thing1?pageSize=10", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results := resp["results"].([]any)
	assert.Len(t, results, 10)
	assert.Contains(t, resp, "nextToken")
}

func TestRefinement3_Pagination_ThingsOffByOneFixed(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for _, name := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		b.AddShadowInternal(name, "", []byte(`{}`))
	}

	h := iotdataplane.NewHandler(b)

	// Page 1: pageSize=2 → alpha, beta; nextToken = gamma (first of next page).
	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListThingsWithShadows?pageSize=2", nil)
	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	things1 := page1["things"].([]any)
	require.Len(t, things1, 2)
	// Sorted alphabetically: alpha, beta, delta, epsilon, gamma.
	assert.Equal(t, "alpha", things1[0])
	assert.Equal(t, "beta", things1[1])

	nextToken, hasNext := page1["nextToken"].(string)
	require.True(t, hasNext)
	// nextToken = "delta" (things[2] in sorted order, first item of next page).
	assert.Equal(t, "delta", nextToken, "nextToken is first item of next page")

	// Page 2: cursor=delta → page starts at delta.
	rec = doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListThingsWithShadows?pageSize=2&nextToken="+nextToken, nil)
	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	things2 := page2["things"].([]any)
	require.Len(t, things2, 2)
	assert.Equal(t, "delta", things2[0])
	assert.Equal(t, "epsilon", things2[1])
}

// ── qos stripped from ListRetainedMessages (issue #16) ───────────────────────

func TestRefinement3_ListRetainedMessages_NoQosInSummary(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("25"), 1))

	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/retainedMessage", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	topics := resp["retainedTopics"].([]any)
	require.Len(t, topics, 1)

	summary := topics[0].(map[string]any)
	_, hasQos := summary["qos"]
	assert.False(t, hasQos, "qos must NOT appear in RetainedMessageSummary (AWS parity #16)")
	assert.Contains(t, summary, "topic")
	assert.Contains(t, summary, "payloadSize")
	assert.Contains(t, summary, "lastModifiedTime")
}

func TestRefinement3_GetRetainedMessage_StillHasQos(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("25"), 1))

	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/retainedMessage/sensor/temp", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "qos", "GetRetainedMessage full response must include qos")
}

// ── LRU eviction for retained messages (issue #18) ───────────────────────────

func TestRefinement3_RetainedMessages_LRUEviction(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 1000 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("t/%04d", i), []byte("x"), 0))
	}

	require.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	// Adding a new topic must succeed via LRU eviction.
	require.NoError(t, b.StoreRetainedMessage("new/topic", []byte("new"), 0))

	// Count stays at cap.
	assert.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	msg, err := b.GetRetainedMessage("new/topic")
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), msg.Payload)
}

func TestRefinement3_RetainedMessages_LRUEviction_CountNeverExceedsCap(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 1000 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("t/%04d", i), []byte("v"), 0))
	}

	for i := range 10 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("new/%d", i), []byte("y"), 0))
		assert.LessOrEqual(t, iotdataplane.RetainedMessageCount(b), 1000)
	}
}

// ── Admin connections path (issue #19) ────────────────────────────────────────

func TestRefinement3_AdminConnectionsPath_BasicFlow(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	// List (empty initially).
	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Register a new connection.
	rec = doRequest(t, h, http.MethodPost, "/_admin/connections/dev-x", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	// List shows it.
	rec = doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["connections"].([]any), 1)

	// Delete.
	rec = doRequest(t, h, http.MethodDelete, "/_admin/connections/dev-x", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}

func TestRefinement3_AdminConnectionsPath_OldPathReturns404(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/connections"},
		{http.MethodPost, "/connections/device-x"},
		{http.MethodDelete, "/connections/device-x"},
	}

	for _, tt := range tests {
		t.Run(tt.method+tt.path, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"old /connections path must return 404 after move to /_admin")
		})
	}
}

// ── Content-Type gate for unwrapPublishPayload (issue #22) ───────────────────

func TestRefinement3_PublishPayload_ContentTypeGate(t *testing.T) {
	t.Parallel()

	// The JSON envelope {"payload":"..."} should only be unwrapped for JSON content types,
	// not for application/octet-stream (binary).
	tests := []struct {
		name     string
		ct       string
		body     []byte
		wantCode int
	}{
		{
			name:     "json_content_type_unwraps",
			ct:       "application/json",
			body:     []byte(`{"payload":"hello"}`),
			wantCode: http.StatusOK,
		},
		{
			name:     "no_content_type_unwraps",
			ct:       "",
			body:     []byte(`{"payload":"hello"}`),
			wantCode: http.StatusOK,
		},
		{
			name:     "octet_stream_passes_raw",
			ct:       "application/octet-stream",
			body:     []byte(`{"payload":"should-stay-raw"}`),
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequestCT(t, h, http.MethodPost, "/topics/test/topic", tt.body, tt.ct)
			assert.Equal(t, tt.wantCode, rec.Code, "ct: %q", tt.ct)
		})
	}
}

// ── Version conflict 409 body (issue #11) ────────────────────────────────────

func TestRefinement3_ShadowVersionConflict_BodyHasCode(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/things/dev-vc/shadow", []byte(`{"state":{"desired":{"k":"v"}}}`))

	rec := doRequest(t, h, http.MethodPost, "/things/dev-vc/shadow", []byte(`{
		"state": {"desired": {"k": "v2"}},
		"version": 0
	}`))
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	code, hasCode := resp["code"]
	require.True(t, hasCode, "409 body must include 'code' field")
	assert.InDelta(t, float64(http.StatusConflict), code, 0)
}

// ── Empty-payload × retain matrix (issue #26) ────────────────────────────────

func TestRefinement3_RetainMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retain    string
		payload   []byte
		wantCount int
		preseed   bool
	}{
		{
			name:      "non_empty_retain_true_stores",
			payload:   []byte("data"),
			retain:    "true",
			wantCount: 1,
		},
		{
			name:      "non_empty_retain_false_no_store",
			payload:   []byte("data"),
			retain:    "false",
			wantCount: 0,
		},
		{
			name:      "empty_retain_true_deletes",
			payload:   []byte{},
			retain:    "true",
			preseed:   true,
			wantCount: 0,
		},
		{
			name:      "empty_retain_false_no_store",
			payload:   []byte{},
			retain:    "false",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.preseed {
				require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("old"), 0))
			}

			h := iotdataplane.NewHandler(b)
			rec := doRequest(t, h, http.MethodPost, "/topics/sensor/temp?retain="+tt.retain, tt.payload)
			require.Equal(t, http.StatusOK, rec.Code)

			assert.Equal(t, tt.wantCount, iotdataplane.RetainedMessageCount(b))
		})
	}
}

// ── Shadow persistence: metadata survives snapshot/restore ───────────────────

func TestRefinement3_Persistence_ShadowWithMetadata(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/things/dev-persist/shadow", []byte(`{
		"state": {"desired": {"temp": 68}, "reported": {"current": 65}}
	}`))

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	h2 := iotdataplane.NewHandler(b2)

	rec := doRequest(t, h2, http.MethodGet, "/things/dev-persist/shadow", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	reported := state["reported"].(map[string]any)
	assert.InDelta(t, float64(68), desired["temp"], 0)
	assert.InDelta(t, float64(65), reported["current"], 0)

	_, hasDelta := state["delta"]
	assert.True(t, hasDelta, "delta must be recomputed after restore")

	_, hasMeta := resp["metadata"]
	assert.True(t, hasMeta, "metadata must survive snapshot/restore")
}

// ── Backend mergeStateFields unit tests ──────────────────────────────────────

func TestRefinement3_Backend_MergeStateFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		base     string
		patch    string
		wantKeys map[string]string
		deleted  []string
	}{
		{
			name:     "add_new_key",
			base:     `{"a":1}`,
			patch:    `{"b":2}`,
			wantKeys: map[string]string{"a": "1", "b": "2"},
		},
		{
			name:     "delete_with_null",
			base:     `{"a":1,"b":2}`,
			patch:    `{"b":null}`,
			wantKeys: map[string]string{"a": "1"},
			deleted:  []string{"b"},
		},
		{
			name:     "update_existing",
			base:     `{"a":1}`,
			patch:    `{"a":99}`,
			wantKeys: map[string]string{"a": "99"},
		},
		{
			name:     "empty_patch_unchanged",
			base:     `{"a":1}`,
			patch:    `{}`,
			wantKeys: map[string]string{"a": "1"},
		},
		{
			name:     "nil_base_uses_patch",
			base:     `{}`,
			patch:    `{"x":42}`,
			wantKeys: map[string]string{"x": "42"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var base, patch map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tt.base), &base))
			require.NoError(t, json.Unmarshal([]byte(tt.patch), &patch))

			result := iotdataplane.MergeStateFields(base, patch)

			for k, wantVal := range tt.wantKeys {
				got, ok := result[k]
				require.True(t, ok, "key %q must be present", k)
				assert.Equal(t, wantVal, string(got))
			}

			for _, k := range tt.deleted {
				_, ok := result[k]
				assert.False(t, ok, "key %q must be deleted", k)
			}
		})
	}
}
