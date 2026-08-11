package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFunction_CRUD's subtests share one Handler and depend on running in
// order (put -> get -> list -> delete -> get-after-delete), so they cannot
// call t.Parallel().
func TestFunction_CRUD(t *testing.T) { //nolint:tparallel // subtests are order-dependent
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		method   string
		path     string
		body     any
		name     string
		wantCode int
	}{
		{
			name:   "put function returns id type arn",
			method: http.MethodPut,
			path:   "/function/fn1",
			body: map[string]any{
				"FunctionType": "CUSTOM_OUTPUT",
				"Description":  "my function",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "fn1", resp["FunctionId"])
				assert.Equal(t, "CUSTOM_OUTPUT", resp["FunctionType"])
				assert.Contains(t, resp["Arn"], ":mediatailor:")
				assert.Equal(t, "my function", resp["Description"])
			},
		},
		{
			name:     "get function returns details",
			method:   http.MethodGet,
			path:     "/function/fn1",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "fn1", resp["FunctionId"])
				assert.Equal(t, "my function", resp["Description"])
			},
		},
		{
			name:     "list functions returns one item",
			method:   http.MethodGet,
			path:     "/functions",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items, _ := resp["Items"].([]any)
				assert.Len(t, items, 1)
			},
		},
		{
			name:     "delete function returns 204",
			method:   http.MethodDelete,
			path:     "/function/fn1",
			wantCode: http.StatusNoContent,
		},
		{
			name:     "get after delete returns 404",
			method:   http.MethodGet,
			path:     "/function/fn1",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // subtests are order-dependent, see func doc
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}

func TestFunction_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:     "get missing function returns 404",
			method:   http.MethodGet,
			path:     "/function/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete missing function returns 404",
			method:   http.MethodDelete,
			path:     "/function/missing",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestFunction_MissingType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/function/fn1", map[string]any{
		"Description": "no type",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListFunctions_WithItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 2 {
		name := "fn-" + string(rune('a'+i))
		doRequest(t, h, http.MethodPut, "/function/"+name, map[string]any{
			"FunctionType": "HTTP_REQUEST",
			"Description":  "test fn",
		})
	}

	rec := doRequest(t, h, http.MethodGet, "/functions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 2)
}
