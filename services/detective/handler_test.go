package detective_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

// newTestHandler constructs a Handler backed by a fresh InMemoryBackend, for
// use by every handler_*_test.go file in this package.
func newTestHandler(t *testing.T) *detective.Handler {
	t.Helper()
	backend := detective.NewInMemoryBackend("000000000000", "us-east-1")

	return detective.NewHandler(backend)
}

// doRequest issues an HTTP request directly against h.Handler(), bypassing
// RouteMatcher, and returns the recorded response.
func doRequest(t *testing.T, h *detective.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

// parseJSON is a test helper that unmarshals JSON into v.
func parseJSON(t *testing.T, data []byte, v any) {
	t.Helper()

	require.NoError(t, json.Unmarshal(data, v))
}

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

// TestDetective_UnknownOperation_Returns400 verifies handleREST's dispatch
// table falls through to a 400 InvalidInputException for any method+path
// combination not present in the dispatch map.
func TestDetective_UnknownOperation_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "unknown POST path returns 400 InvalidInputException",
			method:   http.MethodPost,
			path:     "/unknown/operation",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DELETE on non-tag path returns 400",
			method:   http.MethodDelete,
			path:     "/graph",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tc.method, tc.path, map[string]any{})
			assert.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})
	}
}
