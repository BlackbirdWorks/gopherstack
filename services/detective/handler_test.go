package detective_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDetectiveHandler_RouteMatcher verifies the RouteMatcher against every
// wire path + HTTP method combination the real aws-sdk-go-v2 detective
// serializers produce, per pkgs/service/detective@v1.39.1's serializers.go.
// Unit tests that call h.Handler()(c) directly bypass RouteMatcher entirely,
// so this test goes through matcher(c) the way the router does before ever
// reaching Handler().
func TestDetectiveHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		wantMatch bool
	}{
		{name: "create_graph", method: http.MethodPost, path: "/graph", wantMatch: true},
		{name: "delete_graph", method: http.MethodPost, path: "/graph/removal", wantMatch: true},
		{name: "list_graphs", method: http.MethodPost, path: "/graphs/list", wantMatch: true},
		{name: "create_members", method: http.MethodPost, path: "/graph/members", wantMatch: true},
		{name: "delete_members", method: http.MethodPost, path: "/graph/members/removal", wantMatch: true},
		{name: "get_members", method: http.MethodPost, path: "/graph/members/get", wantMatch: true},
		{name: "list_members", method: http.MethodPost, path: "/graph/members/list", wantMatch: true},
		{name: "accept_invitation", method: http.MethodPut, path: "/invitation", wantMatch: true},
		{name: "reject_invitation", method: http.MethodPost, path: "/invitation/removal", wantMatch: true},
		{name: "list_invitations", method: http.MethodPost, path: "/invitations/list", wantMatch: true},
		{name: "disassociate_membership", method: http.MethodPost, path: "/membership/removal", wantMatch: true},
		{
			name: "batch_get_graph_member_datasources", method: http.MethodPost,
			path: "/graph/datasources/get", wantMatch: true,
		},
		{
			name: "batch_get_membership_datasources", method: http.MethodPost,
			path: "/membership/datasources/get", wantMatch: true,
		},
		{name: "list_datasource_packages", method: http.MethodPost, path: "/graph/datasources/list", wantMatch: true},
		{
			name: "update_datasource_packages", method: http.MethodPost,
			path: "/graph/datasources/update", wantMatch: true,
		},
		{
			name: "start_monitoring_member", method: http.MethodPost,
			path: "/graph/member/monitoringstate", wantMatch: true,
		},
		{
			name: "get_investigation", method: http.MethodPost,
			path: "/investigations/getInvestigation", wantMatch: true,
		},
		{
			name: "list_indicators", method: http.MethodPost,
			path: "/investigations/listIndicators", wantMatch: true,
		},
		{
			name: "list_investigations", method: http.MethodPost,
			path: "/investigations/listInvestigations", wantMatch: true,
		},
		{
			name: "start_investigation", method: http.MethodPost,
			path: "/investigations/startInvestigation", wantMatch: true,
		},
		{
			name: "update_investigation_state", method: http.MethodPost,
			path: "/investigations/updateInvestigationState", wantMatch: true,
		},
		{
			name: "describe_org_configuration", method: http.MethodPost,
			path: "/orgs/describeOrganizationConfiguration", wantMatch: true,
		},
		{name: "disable_admin_account", method: http.MethodPost, path: "/orgs/disableAdminAccount", wantMatch: true},
		{name: "enable_admin_account", method: http.MethodPost, path: "/orgs/enableAdminAccount", wantMatch: true},
		{name: "list_admin_accounts", method: http.MethodPost, path: "/orgs/adminAccountslist", wantMatch: true},
		{
			name: "update_org_configuration", method: http.MethodPost,
			path: "/orgs/updateOrganizationConfiguration", wantMatch: true,
		},
		{
			name: "tag_resource", method: http.MethodPost,
			path: "/tags/arn:aws:detective:us-east-1:000000000000:graph:abc123", wantMatch: true,
		},
		{
			name: "untag_resource", method: http.MethodDelete,
			path: "/tags/arn:aws:detective:us-east-1:000000000000:graph:abc123", wantMatch: true,
		},
		{
			name: "list_tags_for_resource", method: http.MethodGet,
			path: "/tags/arn:aws:detective:us-east-1:000000000000:graph:abc123", wantMatch: true,
		},

		// Non-Detective /tags/ ARNs must not be claimed (cross-service dispatch guard).
		{
			name: "tags_guardduty_arn", method: http.MethodPost,
			path: "/tags/arn:aws:guardduty:us-east-1:000000000000:detector/abc123", wantMatch: false,
		},
		{name: "tags_empty_arn", method: http.MethodPost, path: "/tags/", wantMatch: false},

		// Unrelated paths.
		{name: "root", method: http.MethodGet, path: "/", wantMatch: false},
		{name: "unrelated", method: http.MethodPost, path: "/graphql", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

// TestDetectiveHandler_ExtractOperation verifies operation classification for
// every routed path+method through the same matcher-facing entry point the
// router uses (ExtractOperation), including the PUT-vs-POST /invitation split
// and the GET/POST/DELETE split on /tags/{arn}.
func TestDetectiveHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "put_invitation_is_accept", method: http.MethodPut, path: "/invitation", wantOp: "AcceptInvitation"},
		{
			name: "post_invitation_removal_is_reject", method: http.MethodPost,
			path: "/invitation/removal", wantOp: "RejectInvitation",
		},
		{
			name: "post_tags_is_tag_resource", method: http.MethodPost,
			path: "/tags/arn:aws:detective:us-east-1:000000000000:graph:abc123", wantOp: "TagResource",
		},
		{
			name: "delete_tags_is_untag_resource", method: http.MethodDelete,
			path: "/tags/arn:aws:detective:us-east-1:000000000000:graph:abc123", wantOp: "UntagResource",
		},
		{
			name: "get_tags_is_list_tags", method: http.MethodGet,
			path: "/tags/arn:aws:detective:us-east-1:000000000000:graph:abc123", wantOp: "ListTagsForResource",
		},
		{name: "post_graph_is_create_graph", method: http.MethodPost, path: "/graph", wantOp: "CreateGraph"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// TestHandleStartInvestigation_RequiresScopeTimes verifies ScopeStartTime and
// ScopeEndTime are enforced as required (both are marked "This member is
// required" on StartInvestigationInput in the real SDK). Before this test,
// omitting them silently parsed as the zero time.Time instead of a
// ValidationException.
func TestHandleStartInvestigation_RequiresScopeTimes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing ScopeStartTime rejected",
			body: map[string]any{
				"EntityArn":    "arn:aws:iam::000000000000:user/bob",
				"ScopeEndTime": "2024-01-31T00:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing ScopeEndTime rejected",
			body: map[string]any{
				"EntityArn":      "arn:aws:iam::000000000000:user/bob",
				"ScopeStartTime": "2024-01-01T00:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "both scope times present accepted",
			body: map[string]any{
				"EntityArn":      "arn:aws:iam::000000000000:user/bob",
				"ScopeStartTime": "2024-01-01T00:00:00Z",
				"ScopeEndTime":   "2024-01-31T00:00:00Z",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, jsonUnmarshal(t, createRec.Body.Bytes(), &createResp))
			tc.body["GraphArn"] = createResp["GraphArn"]

			rec := doRequest(t, h, http.MethodPost, "/investigations/startInvestigation", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestHandleStartInvestigation_ClientSuppliedEntityTypeIgnored verifies a
// client-supplied EntityType field (not part of the real StartInvestigation
// wire request) has no effect: EntityType must always be derived from
// EntityArn, matching the real deserializer which has no such input member.
func TestHandleStartInvestigation_ClientSuppliedEntityTypeIgnored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, jsonUnmarshal(t, createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, http.MethodPost, "/investigations/startInvestigation", map[string]any{
		"GraphArn":       createResp["GraphArn"],
		"EntityArn":      "arn:aws:iam::000000000000:role/actual-role",
		"EntityType":     "IAM_USER", // wrong on purpose -- must be ignored.
		"ScopeStartTime": "2024-01-01T00:00:00Z",
		"ScopeEndTime":   "2024-01-31T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, jsonUnmarshal(t, rec.Body.Bytes(), &startResp))

	getRec := doRequest(t, h, http.MethodPost, "/investigations/getInvestigation", map[string]any{
		"GraphArn":        createResp["GraphArn"],
		"InvestigationId": startResp["InvestigationId"],
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, jsonUnmarshal(t, getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "IAM_ROLE", getResp["EntityType"],
		"EntityType must be derived from the role ARN, not the client field")
}
