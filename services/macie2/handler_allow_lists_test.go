package macie2_test

import (
	"encoding/json"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestMacie2_AllowLists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *macie2.Handler) string
		pathFn   func(id string) string
		check    func(t *testing.T, body []byte)
		name     string
		method   string
		wantCode int
	}{
		{
			name:   "CreateAllowList returns arn and id",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/allow-lists" },
			body: map[string]any{
				"clientToken": "tok1",
				"name":        "test-list",
				"criteria":    map[string]any{"regex": "\\d+"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["arn"])
				assert.NotEmpty(t, resp["id"])
				assert.Contains(t, resp["arn"], "arn:aws:macie2:")
			},
		},
		{
			name:   "GetAllowList returns full detail with criteria",
			method: http.MethodGet,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
					"clientToken": "tok2",
					"name":        "my-list",
					"criteria":    map[string]any{"regex": "test-\\w+"},
					"description": "my desc",
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/allow-lists/" + id },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-list", resp["name"])
				assert.Equal(t, "my desc", resp["description"])
				assert.NotNil(t, resp["criteria"])
				assert.NotNil(t, resp["status"])
			},
		},
		{
			name:     "GetAllowList not found returns 404",
			method:   http.MethodGet,
			pathFn:   func(_ string) string { return "/allow-lists/nonexistent" },
			wantCode: http.StatusNotFound,
		},
		{
			// Real aws-sdk-go-v2 sends PUT for UpdateAllowList, not PATCH --
			// this exercises the route-matcher path+method combination a real
			// SDK client uses (see parseAllowListPath in handler.go).
			name:   "UpdateAllowList via PUT updates name and description",
			method: http.MethodPut,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
					"clientToken": "tok4",
					"name":        "orig-name",
					"criteria":    map[string]any{"regex": "orig"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn: func(id string) string { return "/allow-lists/" + id },
			body: map[string]any{
				"name":        "updated-name",
				"description": "updated desc",
				"criteria":    map[string]any{"regex": "updated"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["arn"])
				assert.NotEmpty(t, resp["id"])
			},
		},
		{
			name:     "ListAllowLists returns allowLists key",
			method:   http.MethodGet,
			pathFn:   func(_ string) string { return "/allow-lists" },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "allowLists")
			},
		},
		{
			name:   "DeleteAllowList returns 200",
			method: http.MethodDelete,
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
					"clientToken": "tok3",
					"name":        "del-list",
					"criteria":    map[string]any{"regex": ".+"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["id"]
			},
			pathFn:   func(id string) string { return "/allow-lists/" + id },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func createTestAllowListARN(t *testing.T, h *macie2.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "tag-test",
		"name":        "tag-test-list",
		"criteria":    map[string]any{"regex": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["arn"]
}

var reAllowListARN = regexp.MustCompile(
	`^arn:aws:macie2:[a-z0-9-]+:\d{12}:allow-list/[0-9a-f-]{36}$`,
)

func TestAllowListARNShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "tok-arn",
		"name":        "arn-test",
		"criteria":    map[string]any{"regex": "\\d+"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn := resp["arn"]
	require.NotEmpty(t, arn, "CreateAllowList must return arn")
	assert.True(t, reAllowListARN.MatchString(arn),
		"AllowList ARN must match arn:aws:macie2:{region}:{account}:allow-list/{uuid}, got %q", arn)
}

// 2 & 3. Session timestamps present and updatedAt advances

func TestListAllowListsEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/allow-lists", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["allowLists"]
	require.True(t, ok, "response must contain allowLists key")
	assert.NotNil(t, v, "allowLists must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "allowLists must be an array")
	assert.Empty(t, arr)
}
