package serverlessrepo_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// doServerlessRepoRequestEncoded sends a request using a pre-encoded URL path. Unlike
// doServerlessRepoRequest, the path is not re-encoded: callers may pass paths that contain
// percent-encoded characters (e.g. %2F, %3A) that must be preserved as-is so that the handler
// correctly routes ARN-form application IDs.
func doServerlessRepoRequestEncoded(
	t *testing.T,
	h *serverlessrepo.Handler,
	method string,
	rawPath string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, rawPath, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/serverlessrepo/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// arnPathFor returns the percent-encoded URL path for a SAR request whose applicationId is
// the ARN form: arn:aws:serverlessrepo:us-east-1:testAccountID:applications/{name}.
// The ARN is percent-encoded with path-safe encoding so that %2F is preserved as an opaque
// token and does not break path routing.
func arnPathFor(name string) string {
	arnStr := "arn:aws:serverlessrepo:us-east-1:" + testAccountID + ":applications/" + name

	return "/applications/" + url.PathEscape(arnStr)
}

// ----------------------------------------
// Refinement 2 tests
// ----------------------------------------

func TestRefinement2_GetApplication_ARNForm_Routing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	// Use the ARN-encoded path that the AWS SDK sends.
	path := arnPathFor("my-app")
	rec := doServerlessRepoRequestEncoded(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code, "GET with ARN path should route to GetApplication")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-app", resp["name"])
}

func TestRefinement2_UpdateApplication_ARNForm_Routing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("update-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	path := arnPathFor("update-app")
	rec := doServerlessRepoRequestEncoded(t, h, http.MethodPatch, path, map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code, "PATCH with ARN path should route to UpdateApplication")
}

func TestRefinement2_DeleteApplication_ARNForm_Routing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("del-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	path := arnPathFor("del-app")
	rec := doServerlessRepoRequestEncoded(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code, "DELETE with ARN path should route to DeleteApplication")
}

func TestRefinement2_GetApplicationPolicy_ARNForm_Routing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("policy-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	// PUT policy using ARN form
	path := arnPathFor("policy-app") + "/policy"
	rec := doServerlessRepoRequestEncoded(t, h, http.MethodPut, path, map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"Deploy"}, "principals": []string{"*"}},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "PUT with ARN path should route to PutApplicationPolicy")

	// GET policy using ARN form
	rec = doServerlessRepoRequestEncoded(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts, ok := resp["statements"].([]any)
	require.True(t, ok)
	assert.Len(t, stmts, 1)
}

func TestRefinement2_GetApplication_WithSemanticVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("versioned-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion("versioned-app", "1.2.3", "https://github.com/example", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/versioned-app?semanticVersion=1.2.3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versionObj, ok := resp["version"].(map[string]any)
	require.True(t, ok, "version field must be present when semanticVersion is requested")
	assert.Equal(t, "1.2.3", versionObj["semanticVersion"])
	assert.NotEmpty(t, versionObj["templateUrl"])
}

func TestRefinement2_GetApplication_WithSemanticVersion_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app?semanticVersion=9.9.9", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement2_ListApplications_Pagination_MaxItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"app-a", "app-b", "app-c"} {
		_, err := h.Backend.CreateApplication(name, "desc", "author", "", "", nil, "", "", "")
		require.NoError(t, err)
	}

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	apps, ok := resp["applications"].([]any)
	require.True(t, ok)
	assert.Len(t, apps, 2, "maxItems=2 should return 2 items")
	assert.NotNil(t, resp["nextToken"], "nextToken must be set when more items remain")
}

func TestRefinement2_ListApplications_Pagination_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"app-a", "app-b", "app-c"} {
		_, err := h.Backend.CreateApplication(name, "desc", "author", "", "", nil, "", "", "")
		require.NoError(t, err)
	}

	// First page: maxItems=2
	rec1 := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	nextToken, ok := resp1["nextToken"].(string)
	require.True(t, ok, "nextToken should be a string")

	// Second page
	rec2 := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	apps, ok := resp2["applications"].([]any)
	require.True(t, ok)
	assert.Len(t, apps, 1, "second page should have 1 remaining item")
	assert.Nil(t, resp2["nextToken"], "no more pages - nextToken should be absent")
}

func TestRefinement2_ListApplicationVersions_SemanticVersionFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		_, err = h.Backend.CreateApplicationVersion("my-app", v, "https://github.com/example", "")
		require.NoError(t, err)
	}

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app/versions?semanticVersion=2.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, ok := resp["versions"].([]any)
	require.True(t, ok)
	assert.Len(t, versions, 1, "filter should return only 1 matching version")
	assert.Equal(t, "2.0.0", versions[0].(map[string]any)["semanticVersion"])
}

func TestRefinement2_ListApplicationVersions_MaxItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		_, err = h.Backend.CreateApplicationVersion("my-app", v, "https://github.com/example", "")
		require.NoError(t, err)
	}

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app/versions?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, ok := resp["versions"].([]any)
	require.True(t, ok)
	assert.Len(t, versions, 2)
	assert.NotNil(t, resp["nextToken"])
}

func TestRefinement2_GetCloudFormationTemplate_ActiveStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/my-app/templates", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	templateID := createResp["templateId"].(string)

	rec2 := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/my-app/templates/"+templateID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))
	assert.Equal(t, "ACTIVE", getResp["status"])
}

func TestRefinement2_CreateApplication_InvalidName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
	}{
		{name: "spaces_in_name", appName: "my app"},
		{name: "underscore_in_name", appName: "my_app"},
		{name: "slash_in_name", appName: "my/app"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":        tt.appName,
				"description": "desc",
				"author":      "author",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code, "invalid name should be rejected")
		})
	}
}

func TestRefinement2_CreateApplication_ValidName_WithHyphens(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-valid-app-123",
		"description": "desc",
		"author":      "author",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestRefinement2_CreateApplicationVersion_GeneratesTemplateURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/versions/1.0.0", map[string]any{
		"sourceCodeUrl": "https://github.com/example",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["templateUrl"], "templateUrl should be auto-generated from sourceCodeUrl")
}

func TestRefinement2_PutApplicationPolicy_InvalidAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"s3:GetObject"}, "principals": []string{"*"}},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "unsupported policy action should return 400")
}

func TestRefinement2_PutApplicationPolicy_CaseSensitive_Deploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "lowercase_deploy", action: "deploy"},
		{name: "mixed_case_Deploy", action: "Deploy"},
		{name: "lowercase_GetApplication", action: "getapplication"},
		{name: "mixed_case_GetApplication", action: "GetApplication"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
				"statements": []map[string]any{
					{"actions": []string{tt.action}, "principals": []string{"*"}},
				},
			})
			assert.Equal(t, http.StatusOK, rec.Code, "action %q should be accepted", tt.action)
		})
	}
}

func TestRefinement2_GetApplicationPolicy_EmptyForNewApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts, ok := resp["statements"].([]any)
	require.True(t, ok, "statements must be an array")
	assert.Empty(t, stmts, "new app should have empty policy statements")
}

func TestRefinement2_CreateCloudFormationChangeSet_ARNForm(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("cs-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	path := arnPathFor("cs-app") + "/changesets"
	rec := doServerlessRepoRequestEncoded(t, h, http.MethodPost, path, map[string]any{
		"stackName": "test-stack",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["changeSetId"].(string), "cloudformation", "changeSetId should be a CloudFormation ARN")
	assert.Contains(t, resp["stackId"].(string), "cloudformation", "stackId should be a CloudFormation ARN")
}

func TestRefinement2_ListApplicationDependencies_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("dep-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/dep-app/dependencies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	deps, ok := resp["dependencies"].([]any)
	require.True(t, ok, "dependencies must be an array (not nil)")
	assert.Empty(t, deps)
}

func TestRefinement2_UnshareApplication_Returns204(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("share-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/share-app/unshare", map[string]any{
		"organizationId": "o-abc123",
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestRefinement2_GetApplicationVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.GetApplicationVersion("my-app", "99.99.99")
	assert.Error(t, err, "GetApplicationVersion for unknown version should return error")
}

func TestRefinement2_AddVersionInternal_SeedHelper(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	v := b.AddVersionInternal("my-app", "3.0.0")
	require.NotNil(t, v)
	assert.Equal(t, "3.0.0", v.SemanticVersion)
	assert.Equal(t, 1, serverlessrepo.VersionCount(b, "my-app"))
}

func TestRefinement2_RawPathOrPath_StrictModeNotNeeded(t *testing.T) {
	t.Parallel()

	// Verify that a plain name (no encoding) still works normally.
	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("plain-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/plain-app", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "plain-app", resp["name"])
}

func TestRefinement2_CreateApplication_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 2 {
		rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
			"name":        "dup-app",
			"description": "desc",
			"author":      "author",
		})

		if i == 0 {
			assert.Equal(t, http.StatusCreated, rec.Code)
		} else {
			assert.Equal(t, http.StatusConflict, rec.Code, "duplicate create should return 409")
		}
	}
}

func TestRefinement2_ListApplicationVersions_ARNForm(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("ver-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion("ver-app", "1.0.0", "https://github.com/example", "")
	require.NoError(t, err)

	path := arnPathFor("ver-app") + "/versions"
	rec := doServerlessRepoRequestEncoded(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, ok := resp["versions"].([]any)
	require.True(t, ok)
	assert.Len(t, versions, 1)
}
