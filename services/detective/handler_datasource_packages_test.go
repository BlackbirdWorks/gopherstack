package detective_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestListDatasourcePackagesOpaqueToken verifies ListDatasourcePackages'
// NextToken is an opaque base64 offset, not the raw next package-name key.
// A prior audit flagged this list op (along with ListInvitations,
// ListInvestigations, and ListOrganizationAdminAccounts) as using the raw
// next item's identifier as the continuation token instead of the opaque
// base64 offset every other Detective list op already used.
func TestListDatasourcePackagesOpaqueToken(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	var gResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gResp))
	graphARN := gResp["GraphArn"].(string)

	updateRec := doRequest(t, h, http.MethodPost, "/graph/datasources/update", map[string]any{
		"GraphArn":           graphARN,
		"DatasourcePackages": []string{"DETECTIVE_CORE", "EKS_AUDIT", "ASFF_SECURITYHUB_FINDING"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/datasources/list", map[string]any{
		"GraphArn":   graphARN,
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	tok, hasTok := resp["NextToken"].(string)
	require.True(t, hasTok, "NextToken must be present when more results exist")

	_, err := base64.StdEncoding.DecodeString(tok)
	require.NoError(t, err, "NextToken must be opaque base64, not a raw package name")

	rec3 := doRequest(t, h, http.MethodPost, "/graph/datasources/list", map[string]any{
		"GraphArn":   graphARN,
		"MaxResults": 200,
		"NextToken":  tok,
	})
	require.Equal(t, http.StatusOK, rec3.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp2))
	_, hasTok2 := resp2["NextToken"]
	assert.False(t, hasTok2, "NextToken must be absent on the last page")
}
