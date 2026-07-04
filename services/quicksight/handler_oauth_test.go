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

	dupRec := doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), map[string]any{
		"OAuthClientApplicationId": "app1", "Name": "dup",
	})
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

// ---- ListOAuthClientApplications pagination ----

func TestQuickSight_ListOAuthClientApps_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/oauth-client-applications"), map[string]any{
			"OAuthClientApplicationId": id, "Name": id,
		})
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
