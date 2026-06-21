package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

const testAccountID = "000000000000"

// ----------------------------------------
// Refinement 1 tests
// ----------------------------------------

func TestRefinement1_CreateApplication_Returns201(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "A test application",
		"author":      "test-author",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestRefinement1_CreateApplication_MissingAuthor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "author is required")
}

func TestRefinement1_CreateApplication_MissingDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":   "my-app",
		"author": "author",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "description is required")
}

func TestRefinement1_CreateApplication_WithLabels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      "author",
		"labels":      []string{"tag1", "tag2"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	labels, ok := resp["labels"].([]any)
	require.True(t, ok)
	assert.Len(t, labels, 2)
}

func TestRefinement1_CreateApplication_WithHomePageURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      "author",
		"homePageUrl": "https://github.com/example/my-app",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "https://github.com/example/my-app", resp["homePageUrl"])
}

func TestRefinement1_CreateApplicationVersion_Returns201(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "0.1.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/versions/1.0.0", map[string]any{
		"sourceCodeUrl": "https://github.com/example",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestRefinement1_CreateApplicationVersion_HasRequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "0.1.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/versions/1.0.0", map[string]any{
		"sourceCodeUrl": "https://github.com/example",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["parameterDefinitions"], "parameterDefinitions must be present")
	assert.NotNil(t, resp["requiredCapabilities"], "requiredCapabilities must be present")
	assert.Equal(t, true, resp["resourcesSupported"])
}

func TestRefinement1_CreateApplicationVersion_MissingURLs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "0.1.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/versions/1.0.0", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateCloudFormationTemplate_Returns201(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/my-app/templates", map[string]any{
		"semanticVersion": "1.0.0",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestRefinement1_CreateCloudFormationTemplate_HasTemplateURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/my-app/templates", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["templateUrl"])
	assert.NotEmpty(t, resp["expirationTime"])
}

func TestRefinement1_CreateCloudFormationChangeSet_Returns201(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/my-app/changesets", map[string]any{
		"stackName": "my-stack",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestRefinement1_CreateCloudFormationChangeSet_CustomName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/my-app/changesets", map[string]any{
		"stackName":     "my-stack",
		"changeSetName": "my-custom-changeset",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["changeSetId"].(string), "my-custom-changeset")
}

func TestRefinement1_PutApplicationPolicy_ValidationError_EmptyActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":    []string{},
				"principals": []string{"*"},
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_PutApplicationPolicy_ValidationError_EmptyPrincipals(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":    []string{"deploy"},
				"principals": []string{},
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_PutApplicationPolicy_AutoGeneratesStatementID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":    []string{"deploy"},
				"principals": []string{"*"},
				// no statementId
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts, ok := resp["statements"].([]any)
	require.True(t, ok)
	require.Len(t, stmts, 1)

	stmt := stmts[0].(map[string]any)
	assert.NotEmpty(t, stmt["statementId"], "statementId should be auto-generated")
}

func TestRefinement1_PolicyResponse_NonNilSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"deploy"}, Principals: []string{"*"}},
	})
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts, ok := resp["statements"].([]any)
	require.True(t, ok)
	require.Len(t, stmts, 1)

	stmt := stmts[0].(map[string]any)
	actions, ok := stmt["actions"].([]any)
	require.True(t, ok, "actions should be non-nil array")
	assert.NotNil(t, actions)
	principals, ok := stmt["principals"].([]any)
	require.True(t, ok, "principals should be non-nil array")
	assert.NotNil(t, principals)
	principalOrgIDs := stmt["principalOrgIDs"]
	assert.IsType(t, []any{}, principalOrgIDs, "principalOrgIDs should be an empty array, not nil")
}

func TestRefinement1_ApplicationResponse_LabelsField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      "author",
		"labels":      []string{"serverless", "demo"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// SAR uses "labels" ([]string), not "tags" (map[string]string)
	labels, ok := resp["labels"].([]any)
	require.True(t, ok)
	assert.Len(t, labels, 2)
	assert.Equal(t, "serverless", labels[0])
}

func TestRefinement1_ListApplications_IncludesLabels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", []string{"label1"}, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	apps, ok := resp["applications"].([]any)
	require.True(t, ok)
	require.Len(t, apps, 1)

	app := apps[0].(map[string]any)
	labels, ok := app["labels"].([]any)
	require.True(t, ok)
	assert.Len(t, labels, 1)
}

func TestRefinement1_AddApplicationInternal(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	app := b.AddApplicationInternal("seed-app", "seeded description", "seed-author")
	require.NotNil(t, app)
	assert.Equal(t, "seed-app", app.Name)
	assert.NotEmpty(t, app.ApplicationID)
	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b))
}

func TestRefinement1_AddVersionInternal(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	b.AddApplicationInternal("my-app", "desc", "author")
	v := b.AddVersionInternal("my-app", "1.0.0")
	require.NotNil(t, v)
	assert.Equal(t, "1.0.0", v.SemanticVersion)
	assert.Equal(t, 1, serverlessrepo.VersionCount(b, "my-app"))
}

func TestRefinement1_AddVersionInternal_MissingApp(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	v := b.AddVersionInternal("non-existent", "1.0.0")
	assert.Nil(t, v)
}

func TestRefinement1_ErrValidation_MapsTo400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	// PutApplicationPolicy with no actions triggers ErrValidation
	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{}, "principals": []string{"*"}},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "BadRequestException", resp["__type"])
}

func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &serverlessrepo.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, serverlessrepo.ErrNilAppContext)
}

func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("app1", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b))
	b.Reset()
	assert.Equal(t, 0, serverlessrepo.ApplicationCount(b))
}

func TestRefinement1_DeleteApplication_CascadesVersions(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplicationVersion("my-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	assert.Equal(t, 1, serverlessrepo.VersionCount(b, "my-app"))

	err = b.DeleteApplication("my-app")
	require.NoError(t, err)

	assert.Equal(t, 0, serverlessrepo.ApplicationCount(b))
	assert.Equal(t, 0, serverlessrepo.VersionCount(b, "my-app"))
	assert.Equal(t, 0, serverlessrepo.TemplateCount(b, "my-app"))
	assert.Equal(t, 0, serverlessrepo.PolicyStatementCount(b, "my-app"))
}

func TestRefinement1_AccountIDRegion(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("111111111111", "eu-west-1")
	assert.Equal(t, "111111111111", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestRefinement1_VersionResponse_ResourcesSupported(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, _ = b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	v, err := b.CreateApplicationVersion("my-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	assert.True(t, v.ResourcesSupported)
	assert.NotNil(t, v.ParameterDefinitions)
	assert.NotNil(t, v.RequiredCapabilities)
}

func TestRefinement1_Snapshot_DeepCopiesPolicies(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"deploy"}, Principals: []string{"*"}},
	})
	require.NoError(t, err)

	// Snapshot, then mutate original, restore should have original
	snap := b.Snapshot(t.Context())

	// Overwrite policy
	_, err = b.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"Deploy"}, Principals: []string{"111"}},
	})
	require.NoError(t, err)

	// Restore into a new backend
	b2 := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	stmts, err := b2.GetApplicationPolicy("my-app")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "deploy", stmts[0].Actions[0])
}

func TestRefinement1_UpdateApplication_HomePageURL(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	updated, err := b.UpdateApplication("my-app", "", "", "https://example.com", "")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com", updated.HomePageURL)
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 14, serverlessrepo.HandlerOpsLen(h))
}

func TestRefinement1_ExportCounts(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("app1", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b))
	assert.Equal(t, 0, serverlessrepo.VersionCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.TemplateCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.ChangeSetCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.PolicyStatementCount(b, "app1"))
}
