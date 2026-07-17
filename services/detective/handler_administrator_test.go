package detective_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestOrganizationAdminAccounts covers the ListOrganizationAdminAccounts /
// EnableOrganizationAdminAccount / DisableOrganizationAdminAccount portion of
// what was formerly the combined TestDetective_OrgAdmin table. These cases
// are sequential/stateful (each depends on the prior case's effect on the
// same handler), so they run in table order without t.Parallel().
func TestOrganizationAdminAccounts(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create graph so org config can reference it.
	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

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
