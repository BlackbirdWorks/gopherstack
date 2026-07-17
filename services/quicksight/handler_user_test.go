package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- User tests ----

func TestQuickSight_Users(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "RegisterUser returns user with ARN",
			method: http.MethodPost,
			path:   nsPath("/users"),
			body: map[string]any{
				"UserName":     "alice",
				"Email":        "alice@example.com",
				"UserRole":     "AUTHOR",
				"IdentityType": "QUICKSIGHT",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				u, ok := body["User"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "alice", u["UserName"])
				assert.Equal(t, "alice@example.com", u["Email"])
				assert.Equal(t, "AUTHOR", u["Role"])
				assert.Equal(t, "QUICKSIGHT", u["IdentityType"])
				assert.Contains(t, u["Arn"], "arn:aws:quicksight:us-east-1:000000000000:user/default/alice")
				assert.Equal(t, true, u["Active"])
				assert.NotEmpty(t, u["PrincipalId"])
			},
		},
		{
			name:   "RegisterUser duplicate returns 409",
			method: http.MethodPost,
			path:   nsPath("/users"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
					"UserName": "dup", "Email": "dup@example.com",
				})
			},
			body:     map[string]any{"UserName": "dup", "Email": "dup@example.com"},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DescribeUser returns user",
			method: http.MethodGet,
			path:   nsPath("/users/bob"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
					"UserName": "bob", "Email": "bob@example.com",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				u, ok := body["User"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "bob", u["UserName"])
			},
		},
		{
			name:     "DescribeUser missing returns 404",
			method:   http.MethodGet,
			path:     nsPath("/users/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateUser changes email and role",
			method: http.MethodPut,
			path:   nsPath("/users/carol"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
					"UserName": "carol", "Email": "carol@old.com", "UserRole": "READER",
				})
			},
			body:     map[string]any{"Email": "carol@new.com", "Role": "AUTHOR"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				u, ok := body["User"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "carol@new.com", u["Email"])
				assert.Equal(t, "AUTHOR", u["Role"])
			},
		},
		{
			name:   "DeleteUser removes user",
			method: http.MethodDelete,
			path:   nsPath("/users/dave"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
					"UserName": "dave", "Email": "dave@example.com",
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteUser missing returns 404",
			method:   http.MethodDelete,
			path:     nsPath("/users/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListUsers returns users",
			method: http.MethodGet,
			path:   nsPath("/users"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
					"UserName": "u1", "Email": "u1@example.com",
				})
				doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
					"UserName": "u2", "Email": "u2@example.com",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["UserList"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 2)
			},
		},
		{
			name:     "RegisterUser default role is READER",
			method:   http.MethodPost,
			path:     nsPath("/users"),
			body:     map[string]any{"UserName": "reader", "Email": "r@example.com"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				u, ok := body["User"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "READER", u["Role"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}
