package detective_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOrganizationConfiguration covers the DescribeOrganizationConfiguration /
// UpdateOrganizationConfiguration portion of what was formerly the combined
// TestDetective_OrgAdmin table. These cases are sequential/stateful (each
// depends on the prior case's effect on the same graph), so they run in
// table order without t.Parallel().
func TestOrganizationConfiguration(t *testing.T) { //nolint:paralleltest // existing issue.
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
