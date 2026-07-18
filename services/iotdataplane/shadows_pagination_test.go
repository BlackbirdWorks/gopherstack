package iotdataplane_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParityAccuracy_ListNamedShadows_ExcludesClassicShadow verifies that the
// classic (unnamed) shadow is NOT listed in ListNamedShadowsForThing responses,
// matching real AWS IoT behavior.
func Test_ListNamedShadows_ExcludesClassicShadow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create classic shadow.
	updateShadow(t, h, parityThing, "", []byte(`{"state":{"desired":{"x":1}}}`))
	// Create named shadows.
	updateShadow(t, h, parityThing, "alpha", []byte(`{"state":{"desired":{"x":1}}}`))
	updateShadow(t, h, parityThing, "beta", []byte(`{"state":{"desired":{"x":1}}}`))

	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListNamedShadowsForThing/"+parityThing, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["results"].([]any)
	require.True(t, ok, "results must be an array")
	assert.Len(t, results, 2, "only named shadows appear in list")

	for _, r := range results {
		name, _ := r.(string)
		assert.NotEmpty(t, name, "listed shadow names must be non-empty")
	}
}

// TestParityAccuracy_ListNamedShadows_ResponseShape verifies that
// ListNamedShadowsForThing returns the exact AWS response structure:
// {"results": [...], "timestamp": N}. nextToken only appears when paginating.
func Test_ListNamedShadows_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		shadowNames     []string
		wantFields      []string
		wantResultCount int
	}{
		{
			name:            "no_named_shadows",
			shadowNames:     nil,
			wantFields:      []string{"results", "timestamp"},
			wantResultCount: 0,
		},
		{
			name:            "three_named_shadows",
			shadowNames:     []string{"gamma", "alpha", "beta"},
			wantFields:      []string{"results", "timestamp"},
			wantResultCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			thing := "shape-" + tt.name

			for _, name := range tt.shadowNames {
				updateShadow(t, h, thing, name, []byte(`{"state":{"desired":{"x":1}}}`))
			}

			rec := doRequest(t, h, http.MethodGet,
				"/api/things/shadow/ListNamedShadowsForThing/"+thing, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			for _, field := range tt.wantFields {
				_, ok := resp[field]
				assert.True(t, ok, "field %q must be present", field)
			}

			results, ok := resp["results"].([]any)
			require.True(t, ok, "results must be an array")
			assert.Len(t, results, tt.wantResultCount)

			// nextToken must be absent when results fit on one page.
			_, hasToken := resp["nextToken"]
			assert.False(t, hasToken, "nextToken must be absent when no pagination needed")
		})
	}
}

// TestParityAccuracy_ListNamedShadows_SortedAlphabetically verifies the returned
// shadow names are alphabetically sorted, matching real AWS IoT behavior.
func Test_ListNamedShadows_SortedAlphabetically(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra", "apple", "mango", "cherry"} {
		updateShadow(t, h, parityThing, name, []byte(`{"state":{"desired":{"x":1}}}`))
	}

	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListNamedShadowsForThing/"+parityThing, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["results"].([]any)
	require.True(t, ok)
	require.Len(t, results, 4)

	names := make([]string, len(results))
	for i, r := range results {
		names[i], _ = r.(string)
	}

	assert.Equal(t, []string{"apple", "cherry", "mango", "zebra"}, names,
		"named shadows must be returned in alphabetical order")
}
func Test_ListNamedShadows_IncludesTimestamp(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "shadow-a", []byte(`{}`))
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListNamedShadowsForThing/thing1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "results")
	assert.Contains(t, resp, "timestamp")
}
func Test_ListNamedShadows_SortedByBackend(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "z-shadow", []byte(`{}`))
	b.AddShadowInternal("thing1", "a-shadow", []byte(`{}`))
	b.AddShadowInternal("thing1", "m-shadow", []byte(`{}`))

	names, err := b.ListNamedShadowsForThing("thing1")
	require.NoError(t, err)
	require.Len(t, names, 3)
	assert.Equal(t, "a-shadow", names[0])
	assert.Equal(t, "m-shadow", names[1])
	assert.Equal(t, "z-shadow", names[2])
}
func Test_ListThingsWithShadows_Empty(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	things := resp["things"].([]any)
	assert.Empty(t, things)
}
func Test_ListThingsWithShadows_Multiple(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing-a", "", []byte(`{}`))
	b.AddShadowInternal("thing-b", "shadow1", []byte(`{}`))
	b.AddShadowInternal("thing-c", "shadow2", []byte(`{}`))

	h := iotdataplane.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	things := resp["things"].([]any)
	assert.Len(t, things, 3)
	// Must be sorted.
	assert.Equal(t, "thing-a", things[0])
	assert.Equal(t, "thing-b", things[1])
	assert.Equal(t, "thing-c", things[2])
}
func Test_ListThingsWithShadows_WrongMethod(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/api/things/shadow/ListThingsWithShadows", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
func Test_ListThingsWithShadows_Sorted(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("zebra", "", []byte(`{}`))
	b.AddShadowInternal("apple", "", []byte(`{}`))
	b.AddShadowInternal("mango", "", []byte(`{}`))

	things := b.ListThingsWithShadows()
	require.Len(t, things, 3)
	assert.Equal(t, "apple", things[0])
	assert.Equal(t, "mango", things[1])
	assert.Equal(t, "zebra", things[2])
}
func Test_ListThingsWithShadows_PageSize(t *testing.T) {
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
func Test_ListNamedShadows_PageSize(t *testing.T) {
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
func Test_ListThingsWithShadows_PaginationOffByOneFixed(t *testing.T) {
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
func Test_ListThingsWithShadows_IncludesTimestamp(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing-x", "", []byte(`{"state":{"desired":{"k":"v"}}}`))

	h := iotdataplane.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "things")
	assert.Contains(t, resp, "timestamp")

	ts := resp["timestamp"].(float64)
	assert.Greater(t, ts, float64(1e9), "timestamp must be epoch seconds > 1e9")
}
func Test_ListThingsWithShadows_Pagination(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 5 {
		name := fmt.Sprintf("thing-%02d", i)
		b.AddShadowInternal(name, "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	}

	h := iotdataplane.NewHandler(b)

	// First page: 2 items.
	rec := doRequest(t, h, http.MethodGet, "/api/things/shadow/ListThingsWithShadows?pageSize=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	things1 := page1["things"].([]any)
	assert.Len(t, things1, 2)
	nextToken := page1["nextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Second page using token.
	paginatedURL := "/api/things/shadow/ListThingsWithShadows?pageSize=2&nextToken=" + nextToken
	rec2 := doRequest(t, h, http.MethodGet, paginatedURL, nil)
	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	things2 := page2["things"].([]any)
	assert.Len(t, things2, 2)

	// Collect all and verify no duplicates.
	seen := map[string]bool{}
	for _, item := range append(things1, things2...) {
		key := item.(string)
		assert.False(t, seen[key], "duplicate thing %q across pages", key)
		seen[key] = true
	}
}
