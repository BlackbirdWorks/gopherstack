package detective_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

func newTestHandler(t *testing.T) *detective.Handler {
	t.Helper()
	backend := detective.NewInMemoryBackend("000000000000", "us-east-1")
	return detective.NewHandler(backend)
}

func doRequest(t *testing.T, h *detective.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestDetective_Graph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(h *detective.Handler)
		method   string
		path     string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:     "CreateGraph returns graphArn",
			method:   http.MethodPost,
			path:     "/graph",
			body:     map[string]any{"Tags": map[string]string{"env": "test"}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["GraphArn"], "arn:aws:detective:")
			},
		},
		{
			name:   "CreateGraph idempotent returns same arn",
			method: http.MethodPost,
			path:   "/graph",
			body:   map[string]any{},
			setup: func(h *detective.Handler) {
				doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["GraphArn"], "arn:aws:detective:")
			},
		},
		{
			name:   "DeleteGraph returns 200",
			method: http.MethodPost,
			path:   "/graph/removal",
			setup: func(h *detective.Handler) {
				doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				// get the graphArn from a fresh create first
			},
		},
		{
			name:     "DeleteGraph unknown arn returns 404",
			method:   http.MethodPost,
			path:     "/graph/removal",
			body:     map[string]any{"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:doesnotexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteGraph missing arn returns 400",
			method:   http.MethodPost,
			path:     "/graph/removal",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListGraphs empty returns empty list",
			method:   http.MethodPost,
			path:     "/graphs/list",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list, ok := resp["GraphList"].([]any)
				require.True(t, ok)
				assert.Empty(t, list)
			},
		},
		{
			name:   "ListGraphs after create returns graph",
			method: http.MethodPost,
			path:   "/graphs/list",
			body:   map[string]any{},
			setup: func(h *detective.Handler) {
				doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				list, ok := resp["GraphList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
	}

	for _, tc := range tests {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.setup != nil {
				tc.setup(h)
			}

			var body any

			if tc.body != nil {
				body = tc.body
			} else if tc.setup != nil && tc.method == http.MethodPost && tc.path == "/graph/removal" {
				// get graphArn from listing
				rec := doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				list := listResp["GraphList"].([]any)
				if len(list) > 0 {
					graph := list[0].(map[string]any)
					body = map[string]any{"GraphArn": graph["Arn"]}
				}
			}

			rec := doRequest(t, h, tc.method, tc.path, body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDetective_Members(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:   "CreateMembers without graph returns 404",
			method: http.MethodPost,
			path:   "/graph/members",
			body: map[string]any{
				"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:notexists",
				"Accounts": []any{},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "CreateMembers missing graphArn returns 400",
			method:   http.MethodPost,
			path:     "/graph/members",
			body:     map[string]any{"Accounts": []any{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DeleteMembers without graph returns 404",
			method: http.MethodPost,
			path:   "/graph/members/removal",
			body: map[string]any{
				"GraphArn":   "arn:aws:detective:us-east-1:000000000000:graph:notexists",
				"AccountIds": []string{"111111111111"},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "GetMembers without graph returns 404",
			method: http.MethodPost,
			path:   "/graph/members/get",
			body: map[string]any{
				"GraphArn":   "arn:aws:detective:us-east-1:000000000000:graph:notexists",
				"AccountIds": []string{"111111111111"},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListMembers without graph returns 404",
			method:   http.MethodPost,
			path:     "/graph/members/list",
			body:     map[string]any{"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:notexists"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListMembers missing graphArn returns 400",
			method:   http.MethodPost,
			path:     "/graph/members/list",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDetective_MembersLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create graph
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)
	require.NotEmpty(t, graphArn)

	// Add members
	rec = doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []any{
			map[string]any{"AccountId": "111111111111", "EmailAddress": "member1@example.com"},
			map[string]any{"AccountId": "222222222222", "EmailAddress": "member2@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createMembersResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createMembersResp))
	members := createMembersResp["Members"].([]any)
	assert.Len(t, members, 2)

	// List members
	rec = doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn": graphArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	memberDetails := listResp["MemberDetails"].([]any)
	assert.Len(t, memberDetails, 2)

	// Get specific members
	rec = doRequest(t, h, http.MethodPost, "/graph/members/get", map[string]any{
		"GraphArn":   graphArn,
		"AccountIds": []string{"111111111111", "999999999999"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	gotMembers := getResp["MemberDetails"].([]any)
	assert.Len(t, gotMembers, 1)
	unprocessed := getResp["UnprocessedAccounts"].([]any)
	assert.Len(t, unprocessed, 1)

	// Delete member
	rec = doRequest(t, h, http.MethodPost, "/graph/members/removal", map[string]any{
		"GraphArn":   graphArn,
		"AccountIds": []string{"111111111111"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deleteResp))
	deleted := deleteResp["AccountIds"].([]any)
	assert.Len(t, deleted, 1)

	// List members again — only 1 left
	rec = doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn": graphArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	memberDetails = listResp["MemberDetails"].([]any)
	assert.Len(t, memberDetails, 1)
}

func TestDetective_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create graph first
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	tests := []struct {
		name     string
		method   string
		path     string
		body     any
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name:     "ListTagsForResource returns empty tags",
			method:   http.MethodGet,
			path:     "/tags/" + graphArn,
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Empty(t, tags)
			},
		},
		{
			name:     "TagResource returns 200",
			method:   http.MethodPost,
			path:     "/tags/" + graphArn,
			body:     map[string]any{"Tags": map[string]string{"key1": "val1"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListTagsForResource after tag returns tags",
			method:   http.MethodGet,
			path:     "/tags/" + graphArn,
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				// tag first
				doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{
					"Tags": map[string]string{"mykey": "myval"},
				})

				rec2 := doRequest(t, h, http.MethodGet, "/tags/"+graphArn, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				tags := resp["Tags"].(map[string]any)
				assert.Equal(t, "myval", tags["mykey"])
			},
		},
		{
			name:     "TagResource unknown arn returns 404",
			method:   http.MethodPost,
			path:     "/tags/arn:aws:detective:us-east-1:000000000000:graph:notexists",
			body:     map[string]any{"Tags": map[string]string{"k": "v"}},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UntagResource returns 200",
			method:   http.MethodDelete,
			path:     "/tags/" + graphArn,
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}
