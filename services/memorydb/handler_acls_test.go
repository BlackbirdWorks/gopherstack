package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ACL_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(*memorydb.Handler)
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "create ACL",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create ACL missing name",
			op:         "CreateACL",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe ACLs",
			op:   "DescribeACLs",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "acl-x"})
			},
			body:       map[string]any{"ACLName": "acl-x"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe ACL not found",
			op:         "DescribeACLs",
			body:       map[string]any{"ACLName": "no-such-acl"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete ACL",
			op:   "DeleteACL",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "del-acl"})
			},
			body:       map[string]any{"ACLName": "del-acl"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeACLs_All tests DescribeACLs with no filter returns all ACLs.
func TestHandler_DescribeACLs_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 2 ACLs
	doRequest(t, h, "CreateACL", map[string]any{"ACLName": "acl-1"})
	doRequest(t, h, "CreateACL", map[string]any{"ACLName": "acl-2"})

	// Describe with no filter
	rec := doRequest(t, h, "DescribeACLs", map[string]any{})
	assert.Equal(t, 200, rec.Code)
}

// TestHandler_ACL_Tags tests that ACL tags are routed correctly.
func TestHandler_ACL_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create ACL with tags
	rec := doRequest(t, h, "CreateACL", map[string]any{
		"ACLName": "tagged-acl",
		"Tags":    []map[string]any{{"Key": "Env", "Value": "test"}},
	})
	require.Equal(t, 200, rec.Code)

	// Get ACL ARN
	var aclResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aclResp))
	aclMap := aclResp["ACL"].(map[string]any)
	aclARN := aclMap["ARN"].(string)

	// List tags
	tagsRec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": aclARN})
	assert.Equal(t, 200, tagsRec.Code)
}

// TestHandler_UpdateACL tests UpdateACL handler.
func TestHandler_UpdateACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates ACL",
			body: map[string]any{
				"ACLName":        "my-acl",
				"UserNamesToAdd": []string{"user1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing ACL name",
			body:       map[string]any{"UserNamesToAdd": []string{"user1"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ACL not found",
			body:       map[string]any{"ACLName": "no-such-acl"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "updates ACL" {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "user1",
					"AccessString":       "on ~* &* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password"},
				})
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			}

			rec := doRequest(t, h, "UpdateACL", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ACL_MinimumEngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{"open-access has MinimumEngineVersion", "open-access"},
		{"created ACL has MinimumEngineVersion", "my-acl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tt.aclName != "open-access" {
				rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": tt.aclName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			acls := doDescribeACLs(t, h, tt.aclName)
			require.Len(t, acls, 1)

			acl, _ := acls[0].(map[string]any)
			minVersion, _ := acl["MinimumEngineVersion"].(string)
			assert.Equal(t, "6.2", minVersion, "ACL %s should have MinimumEngineVersion=6.2", tt.aclName)
		})
	}
}

func TestHandler_ACL_PendingChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{"open-access has PendingChanges", "open-access"},
		{"custom ACL has PendingChanges", "custom-acl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tt.aclName != "open-access" {
				rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": tt.aclName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			acls := doDescribeACLs(t, h, tt.aclName)
			require.Len(t, acls, 1)

			acl, _ := acls[0].(map[string]any)
			pendingChanges, hasPending := acl["PendingChanges"]
			assert.True(t, hasPending, "ACL %s should have PendingChanges field", tt.aclName)
			pc, _ := pendingChanges.(map[string]any)
			assert.NotNil(t, pc)
		})
	}
}

// -- Finding 17: User fields -----------------------------------------------------

func TestHandler_ACL_ClusterMembership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clusterCount  int
		expectedCount int
	}{
		{"no clusters in ACL", 0, 0},
		{"one cluster in ACL", 1, 1},
		{"two clusters in ACL", 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create a named ACL.
			rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": "test-membership-acl"})
			require.Equal(t, http.StatusOK, rec.Code)

			// Create clusters referencing the ACL.
			for i := range tt.clusterCount {
				body := map[string]any{
					"ClusterName": "mem-cl-" + string(rune('a'+i)),
					"NodeType":    "db.r6g.large",
					"ACLName":     "test-membership-acl",
				}
				rec2 := doRequest(t, h, "CreateCluster", body)
				require.Equal(t, http.StatusOK, rec2.Code, "create cluster %d: %s", i, rec2.Body)
			}

			acls := doDescribeACLs(t, h, "test-membership-acl")
			require.Len(t, acls, 1)

			acl, _ := acls[0].(map[string]any)
			clusters, _ := acl["Clusters"].([]any)
			assert.Len(t, clusters, tt.expectedCount)
		})
	}
}

// -- User ACLNames accurate (finding 17) -----------------------------------------

func TestHandler_Pagination_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 ACLs beyond open-access.
	for _, name := range []string{"acl-page-a", "acl-page-b", "acl-page-c"} {
		rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page: MaxResults=2.
	rec1 := doRequest(t, h, "DescribeACLs", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	acls1, _ := resp1["ACLs"].([]any)
	assert.Len(t, acls1, 2)
	nextToken, _ := resp1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Second page using NextToken.
	rec2 := doRequest(t, h, "DescribeACLs", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	acls2, _ := resp2["ACLs"].([]any)
	assert.NotEmpty(t, acls2)

	// Ensure no overlap between pages.
	page1Names := make(map[string]bool)
	for _, a := range acls1 {
		am, _ := a.(map[string]any)
		page1Names[am["Name"].(string)] = true
	}
	for _, a := range acls2 {
		am, _ := a.(map[string]any)
		assert.False(t, page1Names[am["Name"].(string)], "page 2 returned item already in page 1")
	}
}

// -- FailoverShard emits event and returns cluster (finding 22) -----------------

// TestHandler_DeleteACL_InUse tests that deleting an ACL in use by a cluster returns 409.
func TestHandler_DeleteACL_InUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "delete ACL in use by cluster returns 409", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create ACL then cluster referencing it
			doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "cl-with-acl",
				"NodeType":    "db.r6g.large",
				"ACLName":     "my-acl",
			})

			rec := doRequest(t, h, "DeleteACL", map[string]any{"ACLName": "my-acl"})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ACL_CRUD_Extended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "create ACL",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create ACL with users",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "my-acl", "UserNames": []string{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create ACL missing name",
			op:         "CreateACL",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe ACL",
			op:   "DescribeACLs",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			},
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe ACL not found",
			op:         "DescribeACLs",
			body:       map[string]any{"ACLName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "update ACL add users",
			op:   "UpdateACL",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "acl-user",
					"AccessString":       "on ~* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			body: map[string]any{
				"ACLName":        "my-acl",
				"UserNamesToAdd": []string{"acl-user"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete ACL",
			op:   "DeleteACL",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			},
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- User CRUD -----------------------------------------------------------------
