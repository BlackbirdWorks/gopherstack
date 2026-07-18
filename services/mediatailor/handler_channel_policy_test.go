package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChannelPolicy_FullCycle's subtests share one Handler and depend on
// running in order (put -> get -> delete -> get-after-delete), so they
// cannot call t.Parallel().
func TestChannelPolicy_FullCycle(t *testing.T) { //nolint:tparallel // subtests are order-dependent
	t.Parallel()

	h := newTestHandler(t)
	createTestChannel(t, h)

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		method   string
		path     string
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "put channel policy returns 200",
			method:   http.MethodPut,
			path:     "/channel/ch1/policy",
			body:     map[string]any{"Policy": `{"Version":"2012-10-17","Statement":[]}`},
			wantCode: http.StatusOK,
		},
		{
			name:     "get channel policy returns policy string",
			method:   http.MethodGet,
			path:     "/channel/ch1/policy",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["Policy"], "2012-10-17")
			},
		},
		{
			name:     "delete channel policy returns 200",
			method:   http.MethodDelete,
			path:     "/channel/ch1/policy",
			wantCode: http.StatusOK,
		},
		{
			name:     "get after delete returns 404",
			method:   http.MethodGet,
			path:     "/channel/ch1/policy",
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

func TestChannelPolicy_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method   string
		path     string
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "put policy on missing channel returns 404",
			method:   http.MethodPut,
			path:     "/channel/nope/policy",
			body:     map[string]any{"Policy": `{}`},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "get policy on missing channel returns 404",
			method:   http.MethodGet,
			path:     "/channel/nope/policy",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete policy on channel with no policy returns 200",
			method:   http.MethodDelete,
			path:     "/channel/ch1/policy",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestChannel(t, h)

			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandleDeleteChannelPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/channel/nonexistent/policy", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDeleteChannelPolicy_WithExistingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create channel
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
		"PlaybackMode": "LINEAR",
	})

	// Add policy
	doRequest(t, h, http.MethodPut, "/channel/ch1/policy", map[string]any{
		"Policy": `{"Version":"2012-10-17"}`,
	})

	// Delete it
	rec := doRequest(t, h, http.MethodDelete, "/channel/ch1/policy", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
