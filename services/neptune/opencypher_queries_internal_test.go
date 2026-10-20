package neptune

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func statusDo(t *testing.T, f *cypherFront, method, target string) (int, map[string]any) {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), method, target, nil)
	f.serveQueryStatus(rec, req)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return rec.Code, out
}

func TestOpenCypherQueryStatus(t *testing.T) {
	t.Parallel()

	f := &cypherFront{graph: opencypher.NewGraph(), queries: newQueryTracker()}
	ctx, finish := f.queries.begin(t.Context(), "MATCH (n) RETURN n")

	var id string

	f.queries.running.Range(func(k string, _ *runningQuery) bool {
		id = k

		return false
	})
	require.NotEmpty(t, id)

	code, out := statusDo(t, f, http.MethodGet, "/opencypher/status")
	assert.Equal(t, http.StatusOK, code)
	assert.InDelta(t, 1, out["runningQueryCount"], 0)
	assert.InDelta(t, 1, out["acceptedQueryCount"], 0)

	queries, ok := out["queries"].([]any)
	require.True(t, ok)
	require.Len(t, queries, 1)

	code, out = statusDo(t, f, http.MethodGet, "/openCypher/status/"+id)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, id, out["queryId"])
	assert.Equal(t, "MATCH (n) RETURN n", out["queryString"])

	stats, ok := out["queryEvalStats"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, false, stats["cancelled"])

	code, out = statusDo(t, f, http.MethodDelete, "/opencypher/status/"+id)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "200 OK", out["status"])
	assert.Equal(t, true, out["payload"])
	require.ErrorIs(t, context.Cause(ctx), errQueryCancelled)

	rec := httptest.NewRecorder()
	writeCypherFailure(ctx, rec, ctx.Err())
	assert.Contains(t, rec.Body.String(), "CancelledByUserException")

	finish()

	for _, m := range []string{http.MethodGet, http.MethodDelete} {
		code, out = statusDo(t, f, m, "/opencypher/status/"+id)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "InvalidParameterException", out["code"])
	}

	code, out = statusDo(t, f, http.MethodGet, "/opencypher/status")
	assert.Equal(t, http.StatusOK, code)
	assert.InDelta(t, 0, out["runningQueryCount"], 0)
}

func TestOpenCypherExplain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		explain  string
		wantBody string
		wantCode int
	}{
		{name: "static", explain: "static", wantCode: http.StatusOK, wantBody: "PatternMatch"},
		{name: "dynamic", explain: "dynamic", wantCode: http.StatusOK, wantBody: "Rows returned: 0"},
		{name: "details", explain: "details", wantCode: http.StatusOK, wantBody: "Write query: false"},
		{name: "bad mode", explain: "nope", wantCode: http.StatusBadRequest, wantBody: "InvalidParameterException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &cypherFront{graph: opencypher.NewGraph(), queries: newQueryTracker()}
			f.up.Store(true)

			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/opencypher",
				strings.NewReader(`{"openCypherQuery":"MATCH (n) RETURN n","explainMode":"`+tt.explain+`"}`))
			req.Header.Set("Content-Type", "application/json")
			f.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}
