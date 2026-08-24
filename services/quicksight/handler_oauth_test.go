package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- OAuth client application CRUD round-trip and errors ----

func TestQuickSight_OAuthClientAppCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), map[string]any{
		"OAuthClientApplicationId":      "app1",
		"Name":                          "App One",
		"ClientId":                      "idp-client-id",
		"ClientSecret":                  "idp-client-secret",
		"OAuthClientAuthenticationType": "TOKEN",
		"OAuthTokenEndpointUrl":         "https://idp.example.com/token",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "app1", createBody["OAuthClientApplicationId"])
	assert.Equal(t, "CREATION_SUCCESSFUL", createBody["CreationStatus"])
	assert.Contains(t, createBody["Arn"], "application/app1")

	dupRec := doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), oauthAppBody("app1", "dup"))
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/oauth-client-applications/app1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	app := parseBody(t, describeRec)["OAuthClientApplication"].(map[string]any)
	assert.Equal(t, "App One", app["Name"])
	assert.Equal(t, "https://idp.example.com/token", app["OAuthTokenEndpointUrl"])
	assert.NotContains(t, app, "ClientSecret", "client secret must not be echoed back on describe")

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/oauth-client-applications/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)

	updateRec := doRequest(t, h, http.MethodPut, accountPath("/oauth-client-applications/app1"), map[string]any{
		"Name": "App One Renamed",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	assert.Equal(t, "UPDATE_SUCCESSFUL", parseBody(t, updateRec)["UpdateStatus"])

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/oauth-client-applications/app1"), nil)
	afterApp := parseBody(t, describeAfterUpdate)["OAuthClientApplication"].(map[string]any)
	assert.Equal(t, "App One Renamed", afterApp["Name"])

	updateMissingRec := doRequest(
		t, h, http.MethodPut, accountPath("/oauth-client-applications/notexist"),
		map[string]any{"Name": "x"},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/oauth-client-applications/app1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "app1", parseBody(t, deleteRec)["OAuthClientApplicationId"])

	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/oauth-client-applications/app1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- CreateOAuthClientApplication required-field presence (gopherstack-wl0s) ----

// TestQuickSight_CreateOAuthClientApp_PresenceValidation covers
// gopherstack-wl0s: CreateOAuthClientApplication's ClientId, ClientSecret,
// OAuthClientAuthenticationType, and OAuthTokenEndpointUrl round-trip (or,
// for ClientId/ClientSecret, are legitimately write-only -- see
// isOAuthAppModeledField's doc comment) through the oauthAppExtraFields
// passthrough, but nothing rejected a request that omitted them, matching
// aws-sdk-go-v2/service/quicksight@v1.123.1/validators.go's
// validateOpCreateOAuthClientApplicationInput. This covers two more required
// fields (ClientId, ClientSecret) than the originating audit named
// (OAuthClientAuthenticationType, OAuthTokenEndpointUrl) -- both are
// required there too. OAuthClientAuthenticationType is additionally enum-
// validated against types.OAuthClientAuthenticationType.Values() (currently
// just "TOKEN").
func TestQuickSight_CreateOAuthClientApp_PresenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(body map[string]any)
		name   string
	}{
		{name: "missing_client_id", mutate: func(body map[string]any) { delete(body, "ClientId") }},
		{name: "missing_client_secret", mutate: func(body map[string]any) { delete(body, "ClientSecret") }},
		{
			name:   "missing_oauth_client_authentication_type",
			mutate: func(body map[string]any) { delete(body, "OAuthClientAuthenticationType") },
		},
		{
			name:   "unrecognized_oauth_client_authentication_type",
			mutate: func(body map[string]any) { body["OAuthClientAuthenticationType"] = "BOGUS" },
		},
		{
			name:   "missing_oauth_token_endpoint_url",
			mutate: func(body map[string]any) { delete(body, "OAuthTokenEndpointUrl") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := oauthAppBody("presence-"+tt.name, "presence app")
			tt.mutate(body)

			rec := doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, "InvalidParameterValueException", parseBody(t, rec)["Code"])
		})
	}

	t.Run("all_present_accepted_and_round_trips", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		body := oauthAppBody("presence-all", "presence app")

		createRec := doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), body)
		require.Equal(t, http.StatusOK, createRec.Code)

		describeRec := doRequest(t, h, http.MethodGet, accountPath("/oauth-client-applications/presence-all"), nil)
		require.Equal(t, http.StatusOK, describeRec.Code)
		app := parseBody(t, describeRec)["OAuthClientApplication"].(map[string]any)
		assert.Equal(t, "TOKEN", app["OAuthClientAuthenticationType"])
		assert.Equal(t, body["OAuthTokenEndpointUrl"], app["OAuthTokenEndpointUrl"])
	})
}

// ---- CreateOAuthClientApplication.Tags: applied to tag state, not echoed ----

func TestQuickSight_OAuthClientApp_CreateTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := oauthAppBody("app1", "App One")
	body["Tags"] = []any{
		map[string]any{"Key": "env", "Value": "prod"},
	}
	createRec := doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), body)
	require.Equal(t, http.StatusOK, createRec.Code)
	arn, ok := parseBody(t, createRec)["Arn"].(string)
	require.True(t, ok)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/oauth-client-applications/app1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	app := parseBody(t, describeRec)["OAuthClientApplication"].(map[string]any)
	// OAuthClientApplication (the Describe response type) has no Tags member;
	// tags travel only via ListTagsForResource.
	assert.NotContains(t, app, "Tags")

	tagsRec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/resources/%s/tags", arn), nil)
	require.Equal(t, http.StatusOK, tagsRec.Code)
	tagList, ok := parseBody(t, tagsRec)["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tagList, 1)
	tag := tagList[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])
}

// ---- ListOAuthClientApplications pagination ----

func TestQuickSight_ListOAuthClientApps_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), oauthAppBody(id, id))
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/oauth-client-applications?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["OAuthClientApplications"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next := body["NextToken"].(string)
	require.NotEmpty(t, next)

	page2 := doRequest(
		t, h, http.MethodGet,
		accountPath(fmt.Sprintf("/oauth-client-applications?max-results=2&next-token=%s", next)), nil,
	)
	require.Equal(t, http.StatusOK, page2.Code)
	assert.Len(t, parseBody(t, page2)["OAuthClientApplications"].([]any), 2)
}

// ListOAuthClientApplications must return the OAuthClientApplicationSummary
// shape, not OAuthClientApplication -- types.OAuthClientApplicationSummary
// (types.go) has no OAuthAuthorizationEndpointUrl/OAuthScopes/
// OAuthTokenEndpointUrl, unlike DescribeOAuthClientApplication's response
// type. Raw-body assertion: an SDK client's deserializer silently drops
// unrecognized members, so it can't prove the leak.
func TestQuickSight_ListOAuthClientApps_OmitsDescribeOnlyFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), oauthAppBody("app1", "App One"))

	rec := doRequest(t, h, http.MethodGet, accountPath("/oauth-client-applications"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	items, ok := parseBody(t, rec)["OAuthClientApplications"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, items)
	for _, it := range items {
		m, isMap := it.(map[string]any)
		require.True(t, isMap)
		for _, k := range []string{"OAuthAuthorizationEndpointUrl", "OAuthScopes", "OAuthTokenEndpointUrl"} {
			_, has := m[k]
			assert.False(t, has, "OAuthClientApplicationSummary must not carry "+k)
		}
	}
}

// ---- OAuth Client App tests ---- //nolint:godot // existing issue.
func TestQuickSight_OAuthClientApps(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create oauth app",
			method:     http.MethodPost,
			path:       accountPath("/oauth-client-applications"),
			body:       oauthAppBody("app1", "App1"),
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplicationId",
		},
		{
			name:       "describe oauth app",
			method:     http.MethodGet,
			path:       accountPath("/oauth-client-applications/app1"),
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplication",
		},
		{
			name:       "update oauth app",
			method:     http.MethodPut,
			path:       accountPath("/oauth-client-applications/app1"),
			body:       map[string]any{"Name": "App1Renamed"},
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplicationId",
		},
		{
			name:       "list oauth apps",
			method:     http.MethodGet,
			path:       accountPath("/oauth-client-applications"),
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplications",
		},
		{
			name:       "delete oauth app",
			method:     http.MethodDelete,
			path:       accountPath("/oauth-client-applications/app1"),
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}
