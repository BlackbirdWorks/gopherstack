package detective_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetective_Members(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name     string
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
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

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDetective_MembersLifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
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

func TestDetective_CreateMembersValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		accounts []any
		wantCode int
	}{
		{
			name: "valid accounts accepted",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "non-numeric account ID rejected",
			accounts: []any{
				map[string]any{"AccountId": "abc123456789", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "short account ID rejected",
			accounts: []any{
				map[string]any{"AccountId": "12345", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "long account ID rejected",
			accounts: []any{
				map[string]any{"AccountId": "1234567890123", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid email rejected",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "notanemail"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "email missing local part rejected",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "email missing domain rejected",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "user@"},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			parseJSON(t, createRec.Body.Bytes(), &createResp)
			graphArn := createResp["GraphArn"].(string)

			rec := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
				"GraphArn": graphArn,
				"Accounts": tc.accounts,
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestDetective_CreateMembersBatchLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	parseJSON(t, createRec.Body.Bytes(), &createResp)
	graphArn := createResp["GraphArn"].(string)

	accounts := make([]any, 51)
	for i := range 51 {
		accounts[i] = map[string]any{
			"AccountId":    fmt.Sprintf("%012d", i+1),
			"EmailAddress": "a@example.com",
		}
	}

	rec := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": accounts,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ListMembers pagination
// ---------------------------------------------------------------------------

func TestListMembers_Pagination_NextToken_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	accounts := make([]any, 3)
	for i := range 3 {
		accounts[i] = map[string]any{
			"AccountId":    fmt.Sprintf("%012d", i+1),
			"EmailAddress": fmt.Sprintf("member%d@example.com", i+1),
		}
	}

	createMem := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": accounts,
	})
	require.Equal(t, http.StatusOK, createMem.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn":   graphArn,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page1))
	memberList1, _ := page1["MemberDetails"].([]any)
	assert.Len(t, memberList1, 2, "MaxResults=2 must return exactly 2 members")
	nextToken, hasToken := page1["NextToken"].(string)
	assert.True(t, hasToken && nextToken != "", "NextToken must be present when more results remain")

	rec3 := doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn":   graphArn,
		"MaxResults": 200,
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var all map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &all))
	_, hasAllToken := all["NextToken"]
	assert.False(t, hasAllToken, "NextToken must be absent when all members fit")
}

// ---------------------------------------------------------------------------
// MemberDetail: MasterId field (deprecated alias for AdministratorId)
// ---------------------------------------------------------------------------

func TestMemberDetail_MasterId_Equals_AdministratorId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []any{
			map[string]any{"AccountId": "111111111111", "EmailAddress": "member@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var membersResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &membersResp))
	members, _ := membersResp["Members"].([]any)
	require.Len(t, members, 1)

	m := members[0].(map[string]any)
	masterID, hasMaster := m["MasterId"]
	adminID := m["AdministratorId"]

	assert.True(t, hasMaster, "MemberDetail must include MasterId (deprecated alias for AdministratorId)")
	assert.Equal(t, adminID, masterID, "MasterId must equal AdministratorId")
}

// ---------------------------------------------------------------------------
// MemberDetail: all required fields present
// ---------------------------------------------------------------------------

func TestMemberDetail_AllRequiredFields_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []any{
			map[string]any{"AccountId": "222222222222", "EmailAddress": "user@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var membersResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &membersResp))
	members, _ := membersResp["Members"].([]any)
	require.Len(t, members, 1)

	m := members[0].(map[string]any)
	for _, field := range []string{
		"AccountId", "AdministratorId", "EmailAddress",
		"GraphArn", "InvitedTime", "MasterId", "Status", "UpdatedTime",
	} {
		_, ok := m[field]
		assert.True(t, ok, "MemberDetail must include %q field", field)
	}

	assert.Equal(t, "INVITED", m["Status"], "initial member status must be INVITED")
}

// ---------------------------------------------------------------------------
// DeleteMembers: AccountIds is [] not null when all accounts unprocessed
// ---------------------------------------------------------------------------

func TestDeleteMembers_AllUnprocessed_AccountIds_Empty_Not_Null(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members/removal", map[string]any{
		"GraphArn":   graphArn,
		"AccountIds": []string{"999999999999"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	accountIDs, hasKey := resp["AccountIds"]
	assert.True(t, hasKey, "DeleteMembers response must include AccountIds")

	ids, ok := accountIDs.([]any)
	assert.True(t, ok, "AccountIds must be an array (not null), got %T: %v", accountIDs, accountIDs)

	if ok {
		assert.Empty(t, ids, "AccountIds must be empty when all accounts are unprocessed")
	}

	unprocessed, _ := resp["UnprocessedAccounts"].([]any)
	assert.Len(t, unprocessed, 1, "unknown account must appear in UnprocessedAccounts")
}

// ---------------------------------------------------------------------------
// CreateMembers: UnprocessedAccounts always [] not null when none fail
// ---------------------------------------------------------------------------

func TestCreateMembers_UnprocessedAccounts_Empty_Not_Null(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []any{
			map[string]any{"AccountId": "333333333333", "EmailAddress": "ok@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	unprocessed, hasKey := resp["UnprocessedAccounts"]
	assert.True(t, hasKey, "CreateMembers must always include UnprocessedAccounts")
	_, ok := unprocessed.([]any)
	assert.True(t, ok, "UnprocessedAccounts must be an array (not null), got %T: %v", unprocessed, unprocessed)
}

// ---- ListMembers: opaque pagination token ----

func TestListMembersOpaqueToken(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	var gResp map[string]any
	parseJSON(t, rec.Body.Bytes(), &gResp)
	graphARN := gResp["GraphArn"].(string)

	// Invite three members.
	members := []map[string]any{
		{"AccountId": "111111111111", "EmailAddress": "a@example.com"},
		{"AccountId": "222222222222", "EmailAddress": "b@example.com"},
		{"AccountId": "333333333333", "EmailAddress": "c@example.com"},
	}
	rec2 := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphARN,
		"Accounts": members,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Page 1 of 1.
	rec3 := doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn":   graphARN,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec3.Code)
	var resp map[string]any
	parseJSON(t, rec3.Body.Bytes(), &resp)

	tok, hasTok := resp["NextToken"].(string)
	require.True(t, hasTok, "NextToken must be present when more results exist")

	// Token must be base64 — not a raw account ID.
	_, err := base64.StdEncoding.DecodeString(tok)
	require.NoError(t, err, "NextToken must be opaque base64, not a raw account ID")

	// Page 2 — no more results.
	rec4 := doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn":   graphARN,
		"MaxResults": 2,
		"NextToken":  tok,
	})
	require.Equal(t, http.StatusOK, rec4.Code)
	var resp2 map[string]any
	parseJSON(t, rec4.Body.Bytes(), &resp2)
	_, hasTok2 := resp2["NextToken"]
	assert.False(t, hasTok2, "NextToken must be absent on the last page")
}

func TestDetective_Invitations(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create a graph so invitations can reference it.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var graphResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graphResp))
	graphArn := graphResp["GraphArn"].(string)

	// Invite account 000000000000 (the backend's own account) will fail; use another.
	// Invite a different account so that account 000000000000 can be "member" by adding it.
	doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []map[string]any{
			{"AccountId": "111111111111", "EmailAddress": "member@example.com"},
		},
	})

	tests := []struct {
		name     string
		setup    func(h2 any)
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "ListInvitations empty returns empty list",
			method:   http.MethodPost,
			path:     "/invitations/list",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				invitations, ok := resp["Invitations"].([]any)
				require.True(t, ok)
				assert.Empty(t, invitations)
			},
		},
		{
			name:     "AcceptInvitation missing GraphArn returns 400",
			method:   http.MethodPut,
			path:     "/invitation",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "AcceptInvitation unknown graph returns 404",
			method:   http.MethodPut,
			path:     "/invitation",
			body:     map[string]any{"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:doesnotexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "RejectInvitation missing GraphArn returns 400",
			method:   http.MethodPost,
			path:     "/invitation/removal",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "RejectInvitation unknown graph returns 404",
			method:   http.MethodPost,
			path:     "/invitation/removal",
			body:     map[string]any{"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:doesnotexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DisassociateMembership missing GraphArn returns 400",
			method:   http.MethodPost,
			path:     "/membership/removal",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DisassociateMembership unknown graph returns 404",
			method:   http.MethodPost,
			path:     "/membership/removal",
			body:     map[string]any{"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:doesnotexist"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec2 := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec2.Code)

			if tc.check != nil {
				tc.check(t, rec2.Body.Bytes())
			}
		})
	}
}

func TestDetective_InvitationLifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	// Admin handler creates graph and invites member.
	adminH := newTestHandler(t)
	rec := doRequest(t, adminH, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var graphResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graphResp))
	graphArn := graphResp["GraphArn"].(string)

	// Invite the backend's own account ID so it can accept/reject via the same backend.
	// The backend account is 000000000000 — it can't invite itself.
	// Instead, invite 111111111111 and verify member status is INVITED.
	rec2 := doRequest(t, adminH, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []map[string]any{
			{"AccountId": "111111111111", "EmailAddress": "a@b.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// ListInvitations should return the invited member (if we were account 111111111111).
	// Since the backend is 000000000000, ListInvitations returns graphs where 000000000000 is a member.
	// Test directly: verify the member is INVITED in GetMembers.
	rec3 := doRequest(t, adminH, http.MethodPost, "/graph/members/get", map[string]any{
		"GraphArn":   graphArn,
		"AccountIds": []string{"111111111111"},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &getResp))
	details := getResp["MemberDetails"].([]any)
	require.Len(t, details, 1)
	member := details[0].(map[string]any)
	assert.Equal(t, "INVITED", member["Status"])
}
