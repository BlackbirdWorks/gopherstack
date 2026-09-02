package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// TestListApplications_Pagination_DeletedMidPage proves that deleting the
// application a cursor names does not restart pagination at page one. Prior
// pagination coverage only exercised the happy path where every named cursor
// still resolves.
func TestListApplications_Pagination_DeletedMidPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"app-a", "app-b", "app-c", "app-d", "app-e"} {
		_, err := h.Backend.CreateApplication(name, "desc", "author", "", "", nil, "", "", "")
		require.NoError(t, err)
	}

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r1))
	nextToken, ok := r1["nextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, nextToken)

	require.NoError(t, h.Backend.DeleteApplication(nextToken))

	rec = doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r2))
	apps2, _ := r2["applications"].([]any)

	restarted := false

	for _, item := range apps2 {
		entry, _ := item.(map[string]any)
		if entry["name"] == "app-a" || entry["name"] == "app-b" {
			restarted = true
		}
	}

	assert.False(t, restarted, "cursor must not restart pagination at page one after its item is deleted")
}

// TestListApplicationVersions_Pagination_StaleTokenDoesNotRestart proves that
// an unresolvable nextToken does not restart ListApplicationVersions at page
// one. Real AWS SAR has no per-version delete operation, so the hostile
// scenario is a forged/unresolvable token rather than deletion.
func TestListApplicationVersions_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("stale-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0", "4.0.0", "5.0.0"} {
		_, err = h.Backend.CreateApplicationVersion("stale-app", v, "https://example.com", "")
		require.NoError(t, err)
	}

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/stale-app/versions?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r1))
	v1, _ := r1["versions"].([]any)
	require.Len(t, v1, 2)

	page1 := map[string]bool{}
	for _, item := range v1 {
		entry, _ := item.(map[string]any)
		page1[entry["semanticVersion"].(string)] = true
	}

	rec2 := doServerlessRepoRequest(
		t, h, http.MethodGet,
		"/applications/stale-app/versions?maxItems=2&nextToken=99.99.99",
		nil,
	)
	require.Equal(t, http.StatusOK, rec2.Code)

	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
	v2, _ := r2["versions"].([]any)

	for _, item := range v2 {
		entry, _ := item.(map[string]any)
		sv := entry["semanticVersion"].(string)
		assert.False(t, page1[sv], "an unresolvable nextToken must not restart pagination at page one")
	}
}

// TestListApplicationDependencies_Pagination_StaleTokenDoesNotRestart proves
// that an unresolvable nextToken does not restart
// ListApplicationDependencies at page one. Dependency entries are derived,
// not independently deletable, so the hostile scenario is a forged token.
func TestListApplicationDependencies_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("dep-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	deps := []serverlessrepo.ApplicationDependency{
		{ApplicationID: "arn:aws:serverlessrepo:us-east-1:000000000000:applications/app-a", SemanticVersion: "1.0.0"},
		{ApplicationID: "arn:aws:serverlessrepo:us-east-1:000000000000:applications/app-b", SemanticVersion: "1.0.0"},
		{ApplicationID: "arn:aws:serverlessrepo:us-east-1:000000000000:applications/app-c", SemanticVersion: "1.0.0"},
	}

	for _, dep := range deps {
		require.NoError(t, b.AddApplicationDependencyInternal("dep-app", "1.0.0", dep))
	}

	h := serverlessrepo.NewHandler(b)

	rec := doServerlessRepoRequest(
		t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=1.0.0&maxItems=2",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r1))
	page1, _ := r1["dependencies"].([]any)
	require.Len(t, page1, 2)

	page1IDs := map[string]bool{}
	for _, item := range page1 {
		entry, _ := item.(map[string]any)
		page1IDs[entry["applicationId"].(string)] = true
	}

	rec2 := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=1.0.0&maxItems=2&nextToken="+
			"arn:aws:serverlessrepo:us-east-1:000000000000:applications/does-not-exist", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
	page2, _ := r2["dependencies"].([]any)

	for _, item := range page2 {
		entry, _ := item.(map[string]any)
		id := entry["applicationId"].(string)
		assert.False(t, page1IDs[id], "an unresolvable nextToken must not restart pagination at page one")
	}
}
