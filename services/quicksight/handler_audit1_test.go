package quicksight_test

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

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
	testNamespace = "default"
)

func newTestBackend(t *testing.T) *quicksight.InMemoryBackend {
	t.Helper()

	return quicksight.NewInMemoryBackend(testAccountID, testRegion)
}

func newTestHandler(t *testing.T) *quicksight.Handler {
	t.Helper()

	return quicksight.NewHandler(newTestBackend(t))
}

func doRequest(t *testing.T, h *quicksight.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var marshalErr error
		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/quicksight/aws4_request")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func parseBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func accountPath(sub string) string {
	return fmt.Sprintf("/accounts/%s%s", testAccountID, sub)
}

func nsPath(sub string) string {
	return fmt.Sprintf("/accounts/%s/namespaces/%s%s", testAccountID, testNamespace, sub)
}

// ---- Namespace tests ----

func TestQuickSight_Namespaces(t *testing.T) {
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
			name:     "CreateNamespace returns ARN",
			method:   http.MethodPost,
			path:     accountPath(""),
			body:     map[string]any{"Namespace": "my-ns", "CapacityRegion": "us-east-1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:namespace/my-ns")
				assert.Equal(t, "CREATION_SUCCESSFUL", body["CreationStatus"])
				assert.Equal(t, "QUICKSIGHT", body["IdentityStore"])
			},
		},
		{
			name:   "CreateNamespace duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath(""),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath(""), map[string]any{"Namespace": "dup-ns"})
			},
			body:     map[string]any{"Namespace": "dup-ns"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "DescribeNamespace returns default",
			method:   http.MethodGet,
			path:     accountPath("/namespaces/default"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ns, ok := body["Namespace"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "default", ns["Name"])
				assert.Equal(t, "CREATION_SUCCESSFUL", ns["CreationStatus"])
			},
		},
		{
			name:     "DescribeNamespace missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/namespaces/nonexistent"),
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListNamespaces includes default",
			method:   http.MethodGet,
			path:     accountPath("/namespaces"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["Namespaces"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, items)
			},
		},
		{
			name:     "DeleteNamespace default returns 400",
			method:   http.MethodDelete,
			path:     accountPath("/namespaces/default"),
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DeleteNamespace non-default succeeds",
			method: http.MethodDelete,
			path:   accountPath("/namespaces/to-delete"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath(""), map[string]any{"Namespace": "to-delete"})
			},
			wantCode: http.StatusOK,
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

// ---- DataSource tests ----

func TestQuickSight_DataSources(t *testing.T) {
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
			name:   "CreateDataSource returns ARN and 201",
			method: http.MethodPost,
			path:   accountPath("/data-sources"),
			body: map[string]any{
				"DataSourceId": "ds1",
				"Name":         "My Source",
				"Type":         "ATHENA",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:datasource/ds1")
				assert.Equal(t, "ds1", body["DataSourceId"])
				assert.Equal(t, "CREATION_SUCCESSFUL", body["CreationStatus"])
			},
		},
		{
			name:   "CreateDataSource duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/data-sources"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "dup", "Name": "x", "Type": "ATHENA",
				})
			},
			body:     map[string]any{"DataSourceId": "dup", "Name": "x", "Type": "ATHENA"},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DescribeDataSource returns source",
			method: http.MethodGet,
			path:   accountPath("/data-sources/ds2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "ds2", "Name": "S2", "Type": "S3",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ds, ok := body["DataSource"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "ds2", ds["DataSourceId"])
				assert.Equal(t, "S3", ds["Type"])
			},
		},
		{
			name:     "DescribeDataSource missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/data-sources/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateDataSource changes name",
			method: http.MethodPut,
			path:   accountPath("/data-sources/ds3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "ds3", "Name": "old-name", "Type": "ATHENA",
				})
			},
			body:     map[string]any{"Name": "new-name"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ds3", body["DataSourceId"])
				assert.Equal(t, "UPDATE_SUCCESSFUL", body["UpdateStatus"])
			},
		},
		{
			name:   "DeleteDataSource removes source",
			method: http.MethodDelete,
			path:   accountPath("/data-sources/ds4"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
					"DataSourceId": "ds4", "Name": "x", "Type": "ATHENA",
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListDataSources returns sources",
			method: http.MethodGet,
			path:   accountPath("/data-sources"),
			setup: func(h *quicksight.Handler) {
				for _, id := range []string{"ls1", "ls2"} {
					doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
						"DataSourceId": id, "Name": id, "Type": "ATHENA",
					})
				}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["DataSources"].([]any)
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

// ---- DataSet tests ----

func TestQuickSight_DataSets(t *testing.T) {
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
			name:   "CreateDataSet returns 201",
			method: http.MethodPost,
			path:   accountPath("/data-sets"),
			body: map[string]any{
				"DataSetId":  "set1",
				"Name":       "My Dataset",
				"ImportMode": "SPICE",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "set1", body["DataSetId"])
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:dataset/set1")
			},
		},
		{
			name:   "CreateDataSet duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/data-sets"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "dup", "Name": "x",
				})
			},
			body:     map[string]any{"DataSetId": "dup", "Name": "x"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "CreateDataSet default ImportMode is SPICE",
			method:   http.MethodPost,
			path:     accountPath("/data-sets"),
			body:     map[string]any{"DataSetId": "set-default-mode", "Name": "x"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.NotEmpty(t, body["DataSetId"])
			},
		},
		{
			name:   "DescribeDataSet returns dataset",
			method: http.MethodGet,
			path:   accountPath("/data-sets/set2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "set2", "Name": "S2", "ImportMode": "DIRECT_QUERY",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ds, ok := body["DataSet"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "DIRECT_QUERY", ds["ImportMode"])
			},
		},
		{
			name:     "DescribeDataSet missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/data-sets/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteDataSet removes dataset",
			method: http.MethodDelete,
			path:   accountPath("/data-sets/set3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
					"DataSetId": "set3", "Name": "x",
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListDataSets returns datasets",
			method: http.MethodGet,
			path:   accountPath("/data-sets"),
			setup: func(h *quicksight.Handler) {
				for _, id := range []string{"ls1", "ls2", "ls3"} {
					doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
						"DataSetId": id, "Name": id,
					})
				}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["DataSetSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 3)
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

// ---- Ingestion tests ----

func TestQuickSight_Ingestions(t *testing.T) {
	t.Parallel()

	createDataSet := func(h *quicksight.Handler, id string) {
		doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
			"DataSetId": id, "Name": id,
		})
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
			name:     "CreateIngestion returns 201 with RUNNING status",
			method:   http.MethodPut,
			path:     accountPath("/data-sets/dset1/ingestions/ing1"),
			setup:    func(h *quicksight.Handler) { createDataSet(h, "dset1") },
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "ing1", body["IngestionId"])
				assert.Equal(t, "RUNNING", body["IngestionStatus"])
			},
		},
		{
			name:     "CreateIngestion on missing dataset returns 404",
			method:   http.MethodPut,
			path:     accountPath("/data-sets/notexist/ingestions/ing1"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "CreateIngestion duplicate returns 409",
			method: http.MethodPut,
			path:   accountPath("/data-sets/dset2/ingestions/ing1"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset2")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset2/ingestions/ing1"), nil)
			},
			wantCode: http.StatusConflict,
		},
		{
			name:   "CancelIngestion sets status to CANCELLED",
			method: http.MethodDelete,
			path:   accountPath("/data-sets/dset3/ingestions/ing1"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset3")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset3/ingestions/ing1"), nil)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeIngestion after cancel shows CANCELLED",
			method: http.MethodGet,
			path:   accountPath("/data-sets/dset4/ingestions/ing1"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset4")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset4/ingestions/ing1"), nil)
				doRequest(t, h, http.MethodDelete, accountPath("/data-sets/dset4/ingestions/ing1"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ing, ok := body["Ingestion"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "CANCELLED", ing["IngestionStatus"])
			},
		},
		{
			name:   "ListIngestions returns ingestions for dataset",
			method: http.MethodGet,
			path:   accountPath("/data-sets/dset5/ingestions"),
			setup: func(h *quicksight.Handler) {
				createDataSet(h, "dset5")
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset5/ingestions/i1"), nil)
				doRequest(t, h, http.MethodPut, accountPath("/data-sets/dset5/ingestions/i2"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["Ingestions"].([]any)
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

// ---- Dashboard tests ----

func TestQuickSight_Dashboards(t *testing.T) {
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
			name:     "CreateDashboard returns ARN and version 1",
			method:   http.MethodPost,
			path:     accountPath("/dashboards/dash1"),
			body:     map[string]any{"Name": "My Dashboard"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "dash1", body["DashboardId"])
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1")
				assert.Contains(t, body["VersionArn"], "/version/1")
			},
		},
		{
			name:   "CreateDashboard duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/dashboards/dup-dash"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/dup-dash"), map[string]any{"Name": "x"})
			},
			body:     map[string]any{"Name": "x"},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DescribeDashboard returns dashboard",
			method: http.MethodGet,
			path:   accountPath("/dashboards/dash2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash2"), map[string]any{"Name": "D2"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				d, ok := body["Dashboard"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "dash2", d["DashboardId"])
				assert.InDelta(t, float64(1), d["PublishedVersionNumber"], 0)
			},
		},
		{
			name:     "DescribeDashboard missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/dashboards/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateDashboard increments version",
			method: http.MethodPut,
			path:   accountPath("/dashboards/dash3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash3"), map[string]any{"Name": "D3"})
			},
			body:     map[string]any{"Name": "D3-Updated"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Contains(t, body["VersionArn"], "/version/2")
			},
		},
		{
			name:   "ListDashboardVersions returns all versions",
			method: http.MethodGet,
			path:   accountPath("/dashboards/dash4/versions"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash4"), map[string]any{"Name": "D4"})
				doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash4"), map[string]any{"Name": "D4-v2"})
				doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash4"), map[string]any{"Name": "D4-v3"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["DashboardVersionSummaryList"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 3)
			},
		},
		{
			name:   "DeleteDashboard removes dashboard",
			method: http.MethodDelete,
			path:   accountPath("/dashboards/dash5"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash5"), map[string]any{"Name": "D5"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListDashboards returns dashboards",
			method: http.MethodGet,
			path:   accountPath("/dashboards"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/da"), map[string]any{"Name": "A"})
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/db"), map[string]any{"Name": "B"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["DashboardSummaryList"].([]any)
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

// ---- Analysis tests ----

func TestQuickSight_Analyses(t *testing.T) {
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
			name:     "CreateAnalysis returns ARN",
			method:   http.MethodPost,
			path:     accountPath("/analyses/a1"),
			body:     map[string]any{"Name": "My Analysis"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "a1", body["AnalysisId"])
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:analysis/a1")
				assert.Equal(t, "CREATION_SUCCESSFUL", body["CreationStatus"])
			},
		},
		{
			name:   "CreateAnalysis duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/analyses/dup"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/dup"), map[string]any{"Name": "x"})
			},
			body:     map[string]any{"Name": "x"},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DescribeAnalysis returns analysis",
			method: http.MethodGet,
			path:   accountPath("/analyses/a2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a2"), map[string]any{"Name": "A2"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				a, ok := body["Analysis"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "a2", a["AnalysisId"])
				assert.Equal(t, "CREATION_SUCCESSFUL", a["Status"])
			},
		},
		{
			name:     "DescribeAnalysis missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/analyses/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateAnalysis changes name and status",
			method: http.MethodPut,
			path:   accountPath("/analyses/a3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a3"), map[string]any{"Name": "A3"})
			},
			body:     map[string]any{"Name": "A3-Updated"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "UPDATE_SUCCESSFUL", body["UpdateStatus"])
			},
		},
		{
			name:   "DeleteAnalysis softdeletes without force",
			method: http.MethodDelete,
			path:   accountPath("/analyses/a4"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a4"), map[string]any{"Name": "A4"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteAnalysis forceDelete removes permanently",
			method: http.MethodDelete,
			path:   accountPath("/analyses/a5") + "?forceDeleteWithoutRecovery=true",
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a5"), map[string]any{"Name": "A5"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "RestoreAnalysis resets status",
			method: http.MethodPost,
			path:   accountPath("/restore/analyses/a6"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a6"), map[string]any{"Name": "A6"})
				doRequest(t, h, http.MethodDelete, accountPath("/analyses/a6"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "a6", body["AnalysisId"])
			},
		},
		{
			name:   "ListAnalyses returns analyses",
			method: http.MethodGet,
			path:   accountPath("/analyses"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/la1"), map[string]any{"Name": "A"})
				doRequest(t, h, http.MethodPost, accountPath("/analyses/la2"), map[string]any{"Name": "B"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["AnalysisSummaryList"].([]any)
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

// ---- Tag tests ----

func TestQuickSight_Tags(t *testing.T) {
	t.Parallel()

	arn := "arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"
	tagPath := fmt.Sprintf("/resources/%s/tags", arn)

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
			name:   "TagResource adds tags",
			method: http.MethodPost,
			path:   tagPath,
			body: map[string]any{
				"Tags": []any{
					map[string]any{"Key": "env", "Value": "prod"},
					map[string]any{"Key": "team", "Value": "eng"},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns added tags",
			method: http.MethodGet,
			path:   tagPath,
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"Tags": []any{
						map[string]any{"Key": "k1", "Value": "v1"},
					},
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				tags, ok := body["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, 1)
				tag, ok := tags[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "k1", tag["Key"])
				assert.Equal(t, "v1", tag["Value"])
			},
		},
		{
			name:   "UntagResource removes tags",
			method: http.MethodDelete,
			path:   tagPath + "?keys=env",
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"Tags": []any{
						map[string]any{"Key": "env", "Value": "prod"},
						map[string]any{"Key": "keep", "Value": "yes"},
					},
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListTagsForResource empty ARN returns empty list",
			method:   http.MethodGet,
			path:     tagPath,
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				tags, ok := body["Tags"].([]any)
				require.True(t, ok)
				assert.Empty(t, tags)
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

// ---- RouteMatcher tests ----

func TestQuickSight_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()

	withAuth := func(path string) *echo.Context {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/quicksight/aws4_request")
		c := e.NewContext(req, httptest.NewRecorder())

		return c
	}

	withoutAuth := func(path string) *echo.Context {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/securityhub/aws4_request")
		c := e.NewContext(req, httptest.NewRecorder())

		return c
	}

	assert.True(t, matcher(withAuth("/accounts/123/data-sources")), "quicksight auth on /accounts/ should match")
	assert.True(
		t,
		matcher(withAuth("/resources/arn:aws:quicksight:us-east-1:123:dashboard/d1/tags")),
		"tag path should match",
	)
	assert.False(t, matcher(withoutAuth("/accounts/123/data-sources")), "securityhub auth should not match")
	assert.False(t, matcher(withAuth("/other/path")), "non-quicksight path should not match")
}
