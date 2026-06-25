package detective_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createGraphForTest creates a graph and returns its ARN.
func createGraphForTest(t *testing.T, h interface { //nolint:revive,unused,unparam // existing issue.
	Handler() interface {
		ServeHTTP(any, any)
	}
},
) string {
	t.Helper()

	rec := doRequest(t, newTestHandler(t), http.MethodPost, "/graph", map[string]any{})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["GraphArn"].(string)
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
func TestDetective_Datasources(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create graph.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var graphResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graphResp))
	graphArn := graphResp["GraphArn"].(string)

	// Add a member.
	doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []map[string]any{
			{"AccountId": "111111111111", "EmailAddress": "m@example.com"},
		},
	})

	tests := []struct {
		name     string
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "ListDatasourcePackages empty graph returns empty map",
			method:   http.MethodPost,
			path:     "/graph/datasources/list",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				pkgs, ok := resp["DatasourcePackages"].(map[string]any)
				require.True(t, ok)
				assert.Empty(t, pkgs)
			},
		},
		{
			name:     "UpdateDatasourcePackages adds packages",
			method:   http.MethodPost,
			path:     "/graph/datasources/update",
			body:     map[string]any{"GraphArn": graphArn, "DatasourcePackages": []string{"DETECTIVE_CORE"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListDatasourcePackages after update returns package",
			method:   http.MethodPost,
			path:     "/graph/datasources/list",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				pkgs, ok := resp["DatasourcePackages"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, pkgs, "DETECTIVE_CORE")
			},
		},
		{
			name:     "BatchGetGraphMemberDatasources returns member datasource info",
			method:   http.MethodPost,
			path:     "/graph/datasources/get",
			body:     map[string]any{"GraphArn": graphArn, "AccountIds": []string{"111111111111"}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				members, ok := resp["MemberDatasources"].([]any)
				require.True(t, ok)
				assert.Len(t, members, 1)
			},
		},
		{
			name:     "BatchGetGraphMemberDatasources missing account returns unprocessed",
			method:   http.MethodPost,
			path:     "/graph/datasources/get",
			body:     map[string]any{"GraphArn": graphArn, "AccountIds": []string{"999999999999"}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				unprocessed, ok := resp["UnprocessedAccounts"].([]any)
				require.True(t, ok)
				assert.Len(t, unprocessed, 1)
			},
		},
		{
			name:     "BatchGetMembershipDatasources returns graph datasource info",
			method:   http.MethodPost,
			path:     "/membership/datasources/get",
			body:     map[string]any{"GraphArns": []string{graphArn}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				members, ok := resp["MembershipDatasources"].([]any)
				require.True(t, ok)
				assert.Len(t, members, 1)
			},
		},
		{
			name:     "BatchGetMembershipDatasources unknown graph returns unprocessed",
			method:   http.MethodPost,
			path:     "/membership/datasources/get",
			body:     map[string]any{"GraphArns": []string{"arn:aws:detective:us-east-1:000000000000:graph:notexist"}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				unprocessed, ok := resp["UnprocessedGraphs"].([]any)
				require.True(t, ok)
				assert.Len(t, unprocessed, 1)
			},
		},
		{
			name:     "ListDatasourcePackages unknown graph returns 404",
			method:   http.MethodPost,
			path:     "/graph/datasources/list",
			body:     map[string]any{"GraphArn": "arn:aws:detective:us-east-1:000000000000:graph:notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateDatasourcePackages unknown graph returns 404",
			method: http.MethodPost,
			path:   "/graph/datasources/update",
			body: map[string]any{
				"GraphArn":           "arn:aws:detective:us-east-1:000000000000:graph:notexist",
				"DatasourcePackages": []string{"DETECTIVE_CORE"},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "BatchGetGraphMemberDatasources unknown graph returns 404",
			method: http.MethodPost,
			path:   "/graph/datasources/get",
			body: map[string]any{
				"GraphArn":   "arn:aws:detective:us-east-1:000000000000:graph:notexist",
				"AccountIds": []string{"111111111111"},
			},
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
func TestDetective_StartMonitoringMember(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create graph.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var graphResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graphResp))
	graphArn := graphResp["GraphArn"].(string)

	// Add a member (status INVITED, not ACCEPTED_BUT_DISABLED).
	doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []map[string]any{
			{"AccountId": "111111111111", "EmailAddress": "m@example.com"},
		},
	})

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "StartMonitoringMember missing GraphArn returns 400",
			body:     map[string]any{"AccountId": "111111111111"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "StartMonitoringMember missing AccountId returns 400",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "StartMonitoringMember unknown graph returns 404",
			body: map[string]any{
				"GraphArn":  "arn:aws:detective:us-east-1:000000000000:graph:notexist",
				"AccountId": "111111111111",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "StartMonitoringMember member not ACCEPTED_BUT_DISABLED returns 400",
			body: map[string]any{
				"GraphArn":  graphArn,
				"AccountId": "111111111111",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec2 := doRequest(t, h, http.MethodPost, "/graph/member/monitoringstate", tc.body)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}
func TestDetective_Investigations(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create graph.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var graphResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graphResp))
	graphArn := graphResp["GraphArn"].(string)

	unknownArn := "arn:aws:detective:us-east-1:000000000000:graph:notexist"

	tests := []struct {
		name     string
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "StartInvestigation missing GraphArn returns 400",
			method: http.MethodPost,
			path:   "/investigations/startInvestigation",
			body: map[string]any{
				"EntityArn":      "arn:aws:iam::000000000000:user/bob",
				"ScopeStartTime": "2024-01-01T00:00:00Z",
				"ScopeEndTime":   "2024-01-31T00:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "StartInvestigation missing EntityArn returns 400",
			method: http.MethodPost,
			path:   "/investigations/startInvestigation",
			body: map[string]any{
				"GraphArn":       graphArn,
				"ScopeStartTime": "2024-01-01T00:00:00Z",
				"ScopeEndTime":   "2024-01-31T00:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "StartInvestigation unknown graph returns 404",
			method: http.MethodPost,
			path:   "/investigations/startInvestigation",
			body: map[string]any{
				"GraphArn":       unknownArn,
				"EntityArn":      "arn:aws:iam::000000000000:user/bob",
				"ScopeStartTime": "2024-01-01T00:00:00Z",
				"ScopeEndTime":   "2024-01-31T00:00:00Z",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "StartInvestigation creates investigation and returns ID",
			method: http.MethodPost,
			path:   "/investigations/startInvestigation",
			body: map[string]any{
				"GraphArn":       graphArn,
				"EntityArn":      "arn:aws:iam::000000000000:user/alice",
				"ScopeStartTime": "2024-01-01T00:00:00Z",
				"ScopeEndTime":   "2024-01-31T00:00:00Z",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["InvestigationId"])
			},
		},
		{
			name:     "ListInvestigations returns started investigation",
			method:   http.MethodPost,
			path:     "/investigations/listInvestigations",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				details, ok := resp["InvestigationDetails"].([]any)
				require.True(t, ok)
				assert.Len(t, details, 1)
			},
		},
		{
			name:     "ListInvestigations unknown graph returns 404",
			method:   http.MethodPost,
			path:     "/investigations/listInvestigations",
			body:     map[string]any{"GraphArn": unknownArn},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "GetInvestigation missing GraphArn returns 400",
			method:   http.MethodPost,
			path:     "/investigations/getInvestigation",
			body:     map[string]any{"InvestigationId": "someid"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetInvestigation missing InvestigationId returns 400",
			method:   http.MethodPost,
			path:     "/investigations/getInvestigation",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetInvestigation unknown investigation returns 404",
			method:   http.MethodPost,
			path:     "/investigations/getInvestigation",
			body:     map[string]any{"GraphArn": graphArn, "InvestigationId": "doesnotexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UpdateInvestigationState missing GraphArn returns 400",
			method:   http.MethodPost,
			path:     "/investigations/updateInvestigationState",
			body:     map[string]any{"InvestigationId": "id", "State": "ARCHIVED"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "UpdateInvestigationState missing State returns 400",
			method:   http.MethodPost,
			path:     "/investigations/updateInvestigationState",
			body:     map[string]any{"GraphArn": graphArn, "InvestigationId": "id"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListIndicators missing GraphArn returns 400",
			method:   http.MethodPost,
			path:     "/investigations/listIndicators",
			body:     map[string]any{"InvestigationId": "id"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListIndicators missing InvestigationId returns 400",
			method:   http.MethodPost,
			path:     "/investigations/listIndicators",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListIndicators unknown investigation returns 404",
			method:   http.MethodPost,
			path:     "/investigations/listIndicators",
			body:     map[string]any{"GraphArn": graphArn, "InvestigationId": "doesnotexist"},
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
func TestDetective_InvestigationGetAndUpdate(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create graph.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var graphResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graphResp))
	graphArn := graphResp["GraphArn"].(string)

	// Start investigation.
	startRec := doRequest(t, h, http.MethodPost, "/investigations/startInvestigation", map[string]any{
		"GraphArn":       graphArn,
		"EntityArn":      "arn:aws:iam::000000000000:user/alice",
		"ScopeStartTime": "2024-01-01T00:00:00Z",
		"ScopeEndTime":   "2024-01-31T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	invID := startResp["InvestigationId"].(string)
	require.NotEmpty(t, invID)

	tests := []struct {
		name     string
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "GetInvestigation returns investigation details",
			method:   http.MethodPost,
			path:     "/investigations/getInvestigation",
			body:     map[string]any{"GraphArn": graphArn, "InvestigationId": invID},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, invID, resp["InvestigationId"])
				assert.Equal(t, graphArn, resp["GraphArn"])
				assert.Equal(t, "arn:aws:iam::000000000000:user/alice", resp["EntityArn"])
				assert.Equal(t, "ACTIVE", resp["State"])
				assert.Equal(t, "RUNNING", resp["Status"])
			},
		},
		{
			name:     "UpdateInvestigationState archives investigation",
			method:   http.MethodPost,
			path:     "/investigations/updateInvestigationState",
			body:     map[string]any{"GraphArn": graphArn, "InvestigationId": invID, "State": "ARCHIVED"},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetInvestigation after archive returns ARCHIVED state",
			method:   http.MethodPost,
			path:     "/investigations/getInvestigation",
			body:     map[string]any{"GraphArn": graphArn, "InvestigationId": invID},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ARCHIVED", resp["State"])
			},
		},
		{
			name:     "ListIndicators returns indicators for investigation",
			method:   http.MethodPost,
			path:     "/investigations/listIndicators",
			body:     map[string]any{"GraphArn": graphArn, "InvestigationId": invID},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				indicators, ok := resp["Indicators"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, indicators, "ListIndicators should return indicators for an investigation")
				for _, raw := range indicators {
					ind, isMap := raw.(map[string]any)
					require.True(t, isMap)
					assert.NotEmpty(t, ind["IndicatorType"])
					assert.NotEmpty(t, ind["Title"])
				}
			},
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
func TestDetective_OrgAdmin(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create graph so org config can reference it.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var graphResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graphResp))
	graphArn := graphResp["GraphArn"].(string)

	unknownArn := "arn:aws:detective:us-east-1:000000000000:graph:notexist"

	tests := []struct {
		name     string
		body     any
		check    func(t *testing.T, body []byte)
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "ListOrganizationAdminAccounts empty returns empty list",
			method:   http.MethodPost,
			path:     "/orgs/adminAccountslist",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				admins, ok := resp["Administrators"].([]any)
				require.True(t, ok)
				assert.Empty(t, admins)
			},
		},
		{
			name:     "EnableOrganizationAdminAccount missing AccountId returns 400",
			method:   http.MethodPost,
			path:     "/orgs/enableAdminAccount",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "EnableOrganizationAdminAccount invalid AccountId returns 400",
			method:   http.MethodPost,
			path:     "/orgs/enableAdminAccount",
			body:     map[string]any{"AccountId": "notanid"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "EnableOrganizationAdminAccount valid account returns 200",
			method:   http.MethodPost,
			path:     "/orgs/enableAdminAccount",
			body:     map[string]any{"AccountId": "123456789012"},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListOrganizationAdminAccounts after enable returns admin",
			method:   http.MethodPost,
			path:     "/orgs/adminAccountslist",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				admins, ok := resp["Administrators"].([]any)
				require.True(t, ok)
				require.Len(t, admins, 1)
				admin := admins[0].(map[string]any)
				assert.Equal(t, "123456789012", admin["AccountId"])
			},
		},
		{
			name:     "DisableOrganizationAdminAccount returns 200",
			method:   http.MethodPost,
			path:     "/orgs/disableAdminAccount",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListOrganizationAdminAccounts after disable returns empty",
			method:   http.MethodPost,
			path:     "/orgs/adminAccountslist",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				admins, ok := resp["Administrators"].([]any)
				require.True(t, ok)
				assert.Empty(t, admins)
			},
		},
		{
			name:     "DescribeOrganizationConfiguration missing GraphArn returns 400",
			method:   http.MethodPost,
			path:     "/orgs/describeOrganizationConfiguration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeOrganizationConfiguration unknown graph returns 404",
			method:   http.MethodPost,
			path:     "/orgs/describeOrganizationConfiguration",
			body:     map[string]any{"GraphArn": unknownArn},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DescribeOrganizationConfiguration defaults to false",
			method:   http.MethodPost,
			path:     "/orgs/describeOrganizationConfiguration",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, false, resp["AutoEnable"])
			},
		},
		{
			name:     "UpdateOrganizationConfiguration missing GraphArn returns 400",
			method:   http.MethodPost,
			path:     "/orgs/updateOrganizationConfiguration",
			body:     map[string]any{"AutoEnable": true},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "UpdateOrganizationConfiguration unknown graph returns 404",
			method:   http.MethodPost,
			path:     "/orgs/updateOrganizationConfiguration",
			body:     map[string]any{"GraphArn": unknownArn, "AutoEnable": true},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UpdateOrganizationConfiguration sets autoEnable true",
			method:   http.MethodPost,
			path:     "/orgs/updateOrganizationConfiguration",
			body:     map[string]any{"GraphArn": graphArn, "AutoEnable": true},
			wantCode: http.StatusOK,
		},
		{
			name:     "DescribeOrganizationConfiguration returns updated true",
			method:   http.MethodPost,
			path:     "/orgs/describeOrganizationConfiguration",
			body:     map[string]any{"GraphArn": graphArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, true, resp["AutoEnable"])
			},
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
