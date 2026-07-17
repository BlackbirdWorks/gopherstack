package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Group tests ----

func TestQuickSight_Groups(t *testing.T) {
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
			name:     "CreateGroup returns ARN with namespace",
			method:   http.MethodPost,
			path:     nsPath("/groups"),
			body:     map[string]any{"GroupName": "eng", "Description": "Engineers"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				g, ok := body["Group"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, g["Arn"], "arn:aws:quicksight:us-east-1:000000000000:group/default/eng")
				assert.Equal(t, "eng", g["GroupName"])
				assert.Equal(t, "Engineers", g["Description"])
				assert.Equal(t, "default", g["Namespace"])
				assert.NotEmpty(t, g["PrincipalId"])
			},
		},
		{
			name:   "CreateGroup duplicate returns 409",
			method: http.MethodPost,
			path:   nsPath("/groups"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/groups"), map[string]any{"GroupName": "dup"})
			},
			body:     map[string]any{"GroupName": "dup"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "CreateGroup in missing namespace returns 404",
			method:   http.MethodPost,
			path:     fmt.Sprintf("/accounts/%s/namespaces/nosuchns/groups", testAccountID),
			body:     map[string]any{"GroupName": "g1"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DescribeGroup returns group",
			method: http.MethodGet,
			path:   nsPath("/groups/mygroup"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/groups"), map[string]any{"GroupName": "mygroup"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				g, ok := body["Group"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "mygroup", g["GroupName"])
			},
		},
		{
			name:     "DescribeGroup missing returns 404",
			method:   http.MethodGet,
			path:     nsPath("/groups/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateGroup changes description",
			method: http.MethodPut,
			path:   nsPath("/groups/g1"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/groups"), map[string]any{"GroupName": "g1"})
			},
			body:     map[string]any{"Description": "new-desc"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				g, ok := body["Group"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "new-desc", g["Description"])
			},
		},
		{
			name:   "DeleteGroup removes group",
			method: http.MethodDelete,
			path:   nsPath("/groups/todel"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/groups"), map[string]any{"GroupName": "todel"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteGroup missing returns 404",
			method:   http.MethodDelete,
			path:     nsPath("/groups/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListGroups returns groups",
			method: http.MethodGet,
			path:   nsPath("/groups"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, nsPath("/groups"), map[string]any{"GroupName": "ga"})
				doRequest(t, h, http.MethodPost, nsPath("/groups"), map[string]any{"GroupName": "gb"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["GroupList"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 2)
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

// ---- Group Membership tests ----

func TestQuickSight_GroupMemberships(t *testing.T) {
	t.Parallel()

	createGroup := func(h *quicksight.Handler, name string) {
		doRequest(t, h, http.MethodPost, nsPath("/groups"), map[string]any{"GroupName": name})
	}

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
			name:     "CreateGroupMembership returns member",
			method:   http.MethodPut,
			path:     nsPath("/groups/grp1/members/alice"),
			setup:    func(h *quicksight.Handler) { createGroup(h, "grp1") },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				m, ok := body["GroupMember"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "alice", m["MemberName"])
				assert.Contains(t, m["Arn"], "arn:aws:quicksight:")
			},
		},
		{
			name:   "CreateGroupMembership duplicate returns 409",
			method: http.MethodPut,
			path:   nsPath("/groups/grp2/members/bob"),
			setup: func(h *quicksight.Handler) {
				createGroup(h, "grp2")
				doRequest(t, h, http.MethodPut, nsPath("/groups/grp2/members/bob"), nil)
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "CreateGroupMembership missing group returns 404",
			method:   http.MethodPut,
			path:     nsPath("/groups/nogroup/members/alice"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteGroupMembership removes member",
			method: http.MethodDelete,
			path:   nsPath("/groups/grp3/members/carol"),
			setup: func(h *quicksight.Handler) {
				createGroup(h, "grp3")
				doRequest(t, h, http.MethodPut, nsPath("/groups/grp3/members/carol"), nil)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteGroupMembership missing returns 404",
			method:   http.MethodDelete,
			path:     nsPath("/groups/grp4/members/nobody"),
			setup:    func(h *quicksight.Handler) { createGroup(h, "grp4") },
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListGroupMemberships returns members",
			method: http.MethodGet,
			path:   nsPath("/groups/grp5/members"),
			setup: func(h *quicksight.Handler) {
				createGroup(h, "grp5")
				doRequest(t, h, http.MethodPut, nsPath("/groups/grp5/members/u1"), nil)
				doRequest(t, h, http.MethodPut, nsPath("/groups/grp5/members/u2"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["GroupMemberList"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 2)
			},
		},
		{
			name:   "DeleteGroup also removes its memberships",
			method: http.MethodGet,
			path:   nsPath("/groups/todel-m/members"),
			setup: func(h *quicksight.Handler) {
				createGroup(h, "todel-m")
				doRequest(t, h, http.MethodPut, nsPath("/groups/todel-m/members/m1"), nil)
				doRequest(t, h, http.MethodDelete, nsPath("/groups/todel-m"), nil)
				// Re-create so the ListGroupMemberships call can actually find the group
				createGroup(h, "todel-m")
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["GroupMemberList"].([]any)
				require.True(t, ok)
				assert.Empty(t, items)
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
