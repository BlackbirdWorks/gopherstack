package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// ---- CreateCloudFormationTemplate semanticVersion omission ----

func TestParity_CreateCloudFormationTemplate_OmitsSemanticVersionWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("tmpl-no-sv", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/tmpl-no-sv/templates",
		map[string]any{}) // no semanticVersion
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasSV := resp["semanticVersion"]
	assert.False(t, hasSV, "semanticVersion must be absent from response when not provided")
	assert.NotEmpty(t, resp["templateId"])
	assert.NotEmpty(t, resp["templateUrl"])
}

func TestParity_CreateCloudFormationTemplate_IncludesSemanticVersionWhenProvided(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("tmpl-sv", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/tmpl-sv/templates",
		map[string]any{"semanticVersion": "2.0.0"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "2.0.0", resp["semanticVersion"])
}

// ---- GetCloudFormationTemplate semanticVersion omission ----

func TestParity_GetCloudFormationTemplate_OmitsSemanticVersionWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("get-tmpl-no-sv", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	createRec := doServerlessRepoRequest(t, h, http.MethodPost,
		"/applications/get-tmpl-no-sv/templates", map[string]any{})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	templateID := createResp["templateId"].(string)

	getRec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/get-tmpl-no-sv/templates/"+templateID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	_, hasSV := getResp["semanticVersion"]
	assert.False(t, hasSV, "semanticVersion must be absent from GetCloudFormationTemplate when not set")
}

// ---- GetCloudFormationTemplate EXPIRED status ----

func TestParity_GetCloudFormationTemplate_ExpiredStatus(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("exp-tmpl-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	expired := serverlessrepo.AddExpiredTemplateInternal(b, "exp-tmpl-app", "1.0.0")
	require.NotNil(t, expired)

	h := serverlessrepo.NewHandler(b)
	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/exp-tmpl-app/templates/"+expired.TemplateID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "EXPIRED", resp["status"])
	assert.Equal(t, "1.0.0", resp["semanticVersion"])
}

// ---- CreateCloudFormationChangeSet semanticVersion omission ----

func TestParity_CreateCloudFormationChangeSet_OmitsSemanticVersionWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("cs-no-sv", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/cs-no-sv/changesets",
		map[string]any{"stackName": "my-stack"}) // no semanticVersion
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasSV := resp["semanticVersion"]
	assert.False(t, hasSV, "semanticVersion must be absent from response when not provided")
	assert.NotEmpty(t, resp["changeSetId"])
	assert.NotEmpty(t, resp["stackId"])
}

func TestParity_CreateCloudFormationChangeSet_IncludesSemanticVersionWhenProvided(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("cs-sv", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/cs-sv/changesets",
		map[string]any{"stackName": "stack", "semanticVersion": "3.1.4"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "3.1.4", resp["semanticVersion"])
}

// ---- GetApplication with explicit semanticVersion not found ----

func TestParity_GetApplication_ExplicitSemanticVersionNotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("ver-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion("ver-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/ver-app?semanticVersion=9.9.9", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- CreateApplication with templateUrl only (no sourceCodeUrl) ----

func TestParity_CreateApplication_TemplateURLOnly_CreatesVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":            "tmpl-only-app",
		"description":     "desc",
		"author":          "author",
		"semanticVersion": "1.0.0",
		"templateUrl":     "s3://bucket/template.yaml",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	version := resp["version"].(map[string]any)
	assert.Equal(t, "s3://bucket/template.yaml", version["templateUrl"])
	assert.Equal(t, "1.0.0", version["semanticVersion"])
}

// ---- CreateApplicationVersion with templateUrl only ----

func TestParity_CreateApplicationVersion_TemplateURLOnly(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("tv-only-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut,
		"/applications/tv-only-app/versions/1.0.0",
		map[string]any{"templateUrl": "s3://bucket/tmpl.yaml"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "s3://bucket/tmpl.yaml", resp["templateUrl"])
}

// ---- CreateApplicationVersion duplicate returns 409 ----

func TestParity_CreateApplicationVersion_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("dup-ver-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	path := "/applications/dup-ver-app/versions/1.0.0"
	body := map[string]any{"sourceCodeUrl": "https://example.com"}

	rec1 := doServerlessRepoRequest(t, h, http.MethodPut, path, body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doServerlessRepoRequest(t, h, http.MethodPut, path, body)
	assert.Equal(t, http.StatusConflict, rec2.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errResp))
	assert.Equal(t, "ConflictException", errResp["__type"])
}

// ---- Application ARN format ----

func TestParity_Application_ARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "arn-check-app",
		"description": "desc",
		"author":      "author",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	appID := resp["applicationId"].(string)
	assert.Contains(t, appID, "arn:aws:serverlessrepo:")
	assert.Contains(t, appID, ":applications/arn-check-app")
}

// ---- DeleteApplication returns 204 ----

func TestParity_DeleteApplication_Returns204(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("del-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodDelete, "/applications/del-app", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ---- UnshareApplication missing organizationId returns 400 ----

func TestParity_UnshareApplication_MissingOrgID_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("unshare-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost,
		"/applications/unshare-app/unshare",
		map[string]any{}) // no organizationId
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListApplicationVersions sorted order ----

func TestParity_ListApplicationVersions_SortedBySemanticVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("sorted-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	for _, v := range []string{"3.0.0", "1.0.0", "2.0.0"} {
		_, err = h.Backend.CreateApplicationVersion("sorted-app", v, "https://example.com", "")
		require.NoError(t, err)
	}

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/sorted-app/versions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions := resp["versions"].([]any)
	require.Len(t, versions, 3)

	semVers := make([]string, 3)
	for i, v := range versions {
		semVers[i] = v.(map[string]any)["semanticVersion"].(string)
	}
	assert.Equal(t, []string{"1.0.0", "2.0.0", "3.0.0"}, semVers)
}

// ---- GetApplication semanticVersion embed from version store ----

func TestParity_GetApplication_VersionEmbedFromStore_IncludesTemplateURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("tmpl-embed-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersionWithOptions("tmpl-embed-app", "1.5.0",
		serverlessrepo.CreateApplicationVersionOptions{
			TemplateURL: "s3://my-bucket/tmpl.yaml",
		})
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/tmpl-embed-app", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	version := resp["version"].(map[string]any)
	assert.Equal(t, "1.5.0", version["semanticVersion"])
	assert.Equal(t, "s3://my-bucket/tmpl.yaml", version["templateUrl"])
}

// ---- GetApplicationPolicy on app with no policy ----

func TestParity_GetApplicationPolicy_EmptyPolicy_ReturnsEmptyStatements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("no-policy-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/no-policy-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts := resp["statements"].([]any)
	assert.Empty(t, stmts)
}

// ---- ListApplicationDependencies with no version filter ----

func TestParity_ListApplicationDependencies_NoSemanticVersion_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("dep-no-sv-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	require.NoError(t, b.AddApplicationDependencyInternal("dep-no-sv-app", "1.0.0",
		serverlessrepo.ApplicationDependency{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/child",
			SemanticVersion: "1.0.0",
		}))

	h := serverlessrepo.NewHandler(b)

	// Query for a different version — should return empty
	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/dep-no-sv-app/dependencies?semanticVersion=2.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	deps := resp["dependencies"].([]any)
	assert.Empty(t, deps)
}
