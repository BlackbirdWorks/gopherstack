package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// ---- CreateApplication validation ----

func TestParity_CreateApplication_NameTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        strings.Repeat("a", 141),
		"description": "desc",
		"author":      "author",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "name must be at most")
}

func TestParity_CreateApplication_NameMaxLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        strings.Repeat("a", 140),
		"description": "desc",
		"author":      "author",
	})
	assert.Equal(t, http.StatusCreated, rec.Code, "exactly 140 chars should be accepted")
}

func TestParity_CreateApplication_AuthorTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      strings.Repeat("a", 128),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "author must be at most")
}

func TestParity_CreateApplication_AuthorMaxLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      strings.Repeat("a", 127),
	})
	assert.Equal(t, http.StatusCreated, rec.Code, "exactly 127 chars should be accepted")
}

func TestParity_CreateApplication_DescriptionTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": strings.Repeat("a", 257),
		"author":      "author",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "description must be at most")
}

func TestParity_CreateApplication_DescriptionMaxLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": strings.Repeat("a", 256),
		"author":      "author",
	})
	assert.Equal(t, http.StatusCreated, rec.Code, "exactly 256 chars should be accepted")
}

func TestParity_CreateApplication_InvalidSemanticVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
	}{
		{name: "no_dots", version: "1"},
		{name: "one_dot", version: "1.0"},
		{name: "alpha", version: "v1.0.0"},
		{name: "empty_parts", version: ".0.0"},
		{name: "trailing_dot", version: "1.0."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":            "my-app",
				"description":     "desc",
				"author":          "author",
				"semanticVersion": tt.version,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestParity_CreateApplication_ValidSemanticVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		label   string
		appName string
		version string
	}{
		{label: "basic", appName: "app-sv-basic", version: "1.0.0"},
		{label: "prerelease", appName: "app-sv-pre", version: "1.0.0-alpha.1"},
		{label: "build_metadata", appName: "app-sv-build", version: "1.0.0+build.1"},
		{label: "large_numbers", appName: "app-sv-large", version: "10.20.30"},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
				"name":            tt.appName,
				"description":     "desc",
				"author":          "author",
				"semanticVersion": tt.version,
			})
			assert.Equal(t, http.StatusCreated, rec.Code)
		})
	}
}

func TestParity_CreateApplication_TooManyLabels(t *testing.T) {
	t.Parallel()

	labels := make([]string, 11)
	for i := range labels {
		labels[i] = "label"
	}

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      "author",
		"labels":      labels,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "at most 10 labels")
}

func TestParity_CreateApplication_MaxLabels(t *testing.T) {
	t.Parallel()

	labels := make([]string, 10)
	for i := range labels {
		labels[i] = "label"
	}

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      "author",
		"labels":      labels,
	})
	assert.Equal(t, http.StatusCreated, rec.Code, "exactly 10 labels should be accepted")
}

func TestParity_CreateApplication_LabelTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      "author",
		"labels":      []string{strings.Repeat("x", 128)},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["message"], "at most 127 characters")
}

func TestParity_CreateApplication_EmptyLabel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "desc",
		"author":      "author",
		"labels":      []string{"ok", ""},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- UpdateApplication validation ----

func TestParity_UpdateApplication_DescriptionTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/my-app", map[string]any{
		"description": strings.Repeat("x", 257),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestParity_UpdateApplication_AuthorTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/my-app", map[string]any{
		"author": strings.Repeat("x", 128),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestParity_UpdateApplication_LabelsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		labels   []string
		wantCode int
	}{
		{
			name:     "too_many_labels",
			labels:   []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "label_too_long",
			labels:   []string{strings.Repeat("x", 128)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_label",
			labels:   []string{"ok", ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "max_10_labels",
			labels:   []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
			wantCode: http.StatusOK,
		},
		{
			name:     "clear_labels",
			labels:   []string{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/my-app", map[string]any{
				"labels": tt.labels,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- CreateApplicationVersion validation ----

func TestParity_CreateApplicationVersion_InvalidSemanticVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
	}{
		{name: "no_dots", version: "1"},
		{name: "one_dot", version: "1.0"},
		{name: "v_prefix", version: "v1.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			path := "/applications/my-app/versions/" + tt.version
			rec := doServerlessRepoRequest(t, h, http.MethodPut, path, map[string]any{
				"sourceCodeUrl": "https://github.com/example",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---- Latest version tracking ----

func TestParity_CreateApplicationVersion_UpdatesLatestVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("track-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	// Create initial version with source URL so it's stored in appVersions
	_, err = h.Backend.CreateApplicationVersion("track-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	// Create newer version
	_, err = h.Backend.CreateApplicationVersion("track-app", "2.0.0", "https://example.com", "")
	require.NoError(t, err)

	// GetApplication without semanticVersion query should return the latest (2.0.0)
	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/track-app", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	version, ok := resp["version"].(map[string]any)
	require.True(t, ok, "version field must be present")
	assert.Equal(t, "2.0.0", version["semanticVersion"], "should return latest created version")
}

func TestParity_CreateApplicationVersion_FullVersionDataInGetApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("fv-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion("fv-app", "3.1.4", "https://github.com/example/repo", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/fv-app", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	version, ok := resp["version"].(map[string]any)
	require.True(t, ok, "version must be embedded in GetApplication response")
	assert.Equal(t, "3.1.4", version["semanticVersion"])
	assert.NotEmpty(t, version["templateUrl"], "templateUrl must be present after version creation")
	assert.Equal(t, "https://github.com/example/repo", version["sourceCodeUrl"])
	assert.True(t, version["resourcesSupported"].(bool))
}

// ---- UpdateApplication version embed ----

func TestParity_UpdateApplication_EmbedCurrentVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("upd-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion("upd-app", "1.2.3", "https://example.com", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/upd-app", map[string]any{
		"description": "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	version, ok := resp["version"].(map[string]any)
	require.True(t, ok, "version must be embedded in UpdateApplication response when current version exists")
	assert.Equal(t, "1.2.3", version["semanticVersion"])
	assert.NotEmpty(t, version["templateUrl"])
}

func TestParity_UpdateApplication_NoVersionWhenNoneCreated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("no-ver-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/no-ver-app", map[string]any{
		"description": "updated",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Nil(t, resp["version"], "version should be absent when no version has been created")
}

// ---- ListApplicationDependencies pagination ----

func TestParity_ListApplicationDependencies_Pagination(t *testing.T) {
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

	// First page: maxItems=2
	rec := doServerlessRepoRequest(
		t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=1.0.0&maxItems=2",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp1))
	page1, ok := resp1["dependencies"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	nextToken, ok := resp1["nextToken"].(string)
	require.True(t, ok, "nextToken must be present when more items remain")
	assert.NotEmpty(t, nextToken)

	// Second page
	rec2 := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=1.0.0&maxItems=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	page2, ok := resp2["dependencies"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)
	assert.Nil(t, resp2["nextToken"], "no more pages")
}

func TestParity_ListApplicationDependencies_MaxItemsDefault(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("dep-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	for i := range 3 {
		dep := serverlessrepo.ApplicationDependency{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-" + string(rune('a'+i)),
			SemanticVersion: "1.0.0",
		}
		require.NoError(t, b.AddApplicationDependencyInternal("dep-app", "2.0.0", dep))
	}

	h := serverlessrepo.NewHandler(b)
	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=2.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	page, ok := resp["dependencies"].([]any)
	require.True(t, ok)
	assert.Len(t, page, 3)
	assert.Nil(t, resp["nextToken"], "all 3 fit within default maxItems=100")
}

// ---- ListApplicationVersions summary fields ----

func TestParity_ListApplicationVersions_ResourcesSupported(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("vs-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion("vs-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/vs-app/versions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, ok := resp["versions"].([]any)
	require.True(t, ok)
	require.Len(t, versions, 1)

	v := versions[0].(map[string]any)
	resourcesSupported, exists := v["resourcesSupported"]
	assert.True(t, exists, "resourcesSupported must be present in version list summary")
	assert.Equal(t, true, resourcesSupported)
}

// ---- Deterministic dependency ordering ----

func TestParity_ListApplicationDependencies_DeterministicOrder(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("order-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	// Insert in reverse alphabetical order.
	for _, name := range []string{"zzz-app", "aaa-app", "mmm-app"} {
		dep := serverlessrepo.ApplicationDependency{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/" + name,
			SemanticVersion: "1.0.0",
		}
		require.NoError(t, b.AddApplicationDependencyInternal("order-app", "1.0.0", dep))
	}

	h := serverlessrepo.NewHandler(b)
	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/order-app/dependencies?semanticVersion=1.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	page, ok := resp["dependencies"].([]any)
	require.True(t, ok)
	require.Len(t, page, 3)

	ids := make([]string, 3)
	for i, d := range page {
		ids[i] = d.(map[string]any)["applicationId"].(string)
	}

	assert.True(t, ids[0] < ids[1] && ids[1] < ids[2], "dependencies must be sorted alphabetically by applicationId")
}

// ---- Error shape compliance ----

func TestParity_ErrorShape_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/missing-app", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "NotFoundException", resp["__type"])
	assert.NotEmpty(t, resp["message"])
}

func TestParity_ErrorShape_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"name": "dup", "description": "d", "author": "a"}

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doServerlessRepoRequest(t, h, http.MethodPost, "/applications", body)
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ConflictException", resp["__type"])
}

func TestParity_ErrorShape_BadRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":   "x",
		"author": "a",
		// description missing
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "BadRequestException", resp["__type"])
}

// ---- CreateApplication inline version fidelity ----

func TestParity_CreateApplication_InlineVersion_FullResponseFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":            "inline-ver-app",
		"description":     "desc",
		"author":          "author",
		"semanticVersion": "1.0.0",
		"sourceCodeUrl":   "https://github.com/example/repo",
		"templateUrl":     "s3://bucket/template.yaml",
		"spdxLicenseId":   "MIT",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, "MIT", resp["spdxLicenseId"])
	version, ok := resp["version"].(map[string]any)
	require.True(t, ok, "version must be embedded when semanticVersion + URLs provided")
	assert.Equal(t, "1.0.0", version["semanticVersion"])
	assert.Equal(t, "s3://bucket/template.yaml", version["templateUrl"])
	assert.Equal(t, "https://github.com/example/repo", version["sourceCodeUrl"])
	assert.NotNil(t, version["parameterDefinitions"])
	assert.NotNil(t, version["requiredCapabilities"])
	assert.Equal(t, true, version["resourcesSupported"])
}

// ---- Policy principal org IDs ----

func TestParity_PutApplicationPolicy_PrincipalOrgIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("org-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/org-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":         []string{"Deploy"},
				"principals":      []string{"*"},
				"principalOrgIDs": []string{"o-abc123", "o-def456"},
				"statementId":     "stmt-1",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	stmts := putResp["statements"].([]any)
	stmt := stmts[0].(map[string]any)
	orgIDs := stmt["principalOrgIDs"].([]any)
	assert.Len(t, orgIDs, 2)
	assert.Equal(t, "o-abc123", orgIDs[0])

	// GET should return orgIDs too
	rec2 := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/org-app/policy", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))
	stmts2 := getResp["statements"].([]any)
	stmt2 := stmts2[0].(map[string]any)
	orgIDs2 := stmt2["principalOrgIDs"].([]any)
	assert.Len(t, orgIDs2, 2)
}

// ---- CloudFormation ChangeSet response fields ----

func TestParity_CreateCloudFormationChangeSet_ResponseFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("cf-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/cf-app/changesets", map[string]any{
		"stackName":       "my-stack",
		"semanticVersion": "1.0.0",
		"capabilities":    []string{"CAPABILITY_IAM"},
		"tags":            []map[string]string{{"key": "env", "value": "test"}},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["changeSetId"])
	assert.NotEmpty(t, resp["stackId"])
	assert.Equal(t, "1.0.0", resp["semanticVersion"])
	assert.Contains(t, resp["changeSetId"].(string), "cloudformation")
	assert.Contains(t, resp["stackId"].(string), "cloudformation")
}

// ---- Delete cascade ----

func TestParity_DeleteApplication_CascadesAllResources(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("full-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplicationVersion("full-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationTemplate("full-app", "1.0.0")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationChangeSet("full-app", "stack", "cs", "1.0.0")
	require.NoError(t, err)

	_, err = b.PutApplicationPolicy("full-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"Deploy"}, Principals: []string{"*"}},
	})
	require.NoError(t, err)

	require.NoError(t, b.DeleteApplication("full-app"))

	assert.Equal(t, 0, serverlessrepo.ApplicationCount(b))
	assert.Equal(t, 0, serverlessrepo.VersionCount(b, "full-app"))
	assert.Equal(t, 0, serverlessrepo.TemplateCount(b, "full-app"))
	assert.Equal(t, 0, serverlessrepo.ChangeSetCount(b, "full-app"))
	assert.Equal(t, 0, serverlessrepo.PolicyStatementCount(b, "full-app"))
}

// ---- Snapshot/restore round-trip with all resources ----

func TestParity_Snapshot_RestoreAllResourceTypes(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("snap-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplicationVersion("snap-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationTemplate("snap-app", "1.0.0")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationChangeSet("snap-app", "stack", "cs", "1.0.0")
	require.NoError(t, err)

	require.NoError(t, b.AddApplicationDependencyInternal("snap-app", "1.0.0", serverlessrepo.ApplicationDependency{
		ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/child",
		SemanticVersion: "2.0.0",
	}))

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b2))
	assert.Equal(t, 1, serverlessrepo.VersionCount(b2, "snap-app"))
	assert.Equal(t, 1, serverlessrepo.TemplateCount(b2, "snap-app"))
	assert.Equal(t, 1, serverlessrepo.ChangeSetCount(b2, "snap-app"))

	deps, depErr := b2.ListApplicationDependencies("snap-app", "1.0.0")
	require.NoError(t, depErr)
	assert.Len(t, deps, 1)
	assert.Equal(t, "2.0.0", deps[0].SemanticVersion)
}

// ---- GetApplication semanticVersion embed is full-fidelity ----

func TestParity_GetApplication_ExplicitSemanticVersion_FullData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("ev-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion(
		"ev-app", "5.0.0", "https://github.com/example", "s3://bucket/tmpl.yaml",
	)
	require.NoError(t, err)

	_, err = h.Backend.CreateApplicationVersion("ev-app", "6.0.0", "https://github.com/example/v2", "")
	require.NoError(t, err)

	// Explicitly request older version
	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/ev-app?semanticVersion=5.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	version, ok := resp["version"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "5.0.0", version["semanticVersion"])
	assert.Equal(t, "s3://bucket/tmpl.yaml", version["templateUrl"])
}

// ---- ListApplications pagination correctness ----

func TestParity_ListApplications_PageBoundaryExact(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"app-1", "app-2", "app-3", "app-4"} {
		_, err := h.Backend.CreateApplication(name, "desc", "author", "", "", nil, "", "", "")
		require.NoError(t, err)
	}

	// Page 1: 2 items
	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r1))
	apps1 := r1["applications"].([]any)
	assert.Len(t, apps1, 2)
	nt := r1["nextToken"].(string)

	// Page 2: 2 items
	rec = doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2&nextToken="+nt, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r2))
	apps2 := r2["applications"].([]any)
	assert.Len(t, apps2, 2)
	assert.Nil(t, r2["nextToken"], "no more pages when exactly on boundary")
}

// ---- CFTemplate expiry ----

func TestParity_GetCloudFormationTemplate_Fields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("tmpl-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/tmpl-app/templates", map[string]any{
		"semanticVersion": "1.0.0",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	templateID := createResp["templateId"].(string)

	rec2 := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/tmpl-app/templates/"+templateID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))
	assert.Equal(t, "ACTIVE", getResp["status"])
	assert.Equal(t, "1.0.0", getResp["semanticVersion"])
	assert.NotEmpty(t, getResp["templateUrl"])
	assert.NotEmpty(t, getResp["creationTime"])
	assert.NotEmpty(t, getResp["expirationTime"])
	assert.NotEmpty(t, getResp["applicationId"])
}

// ---- Policy replacement semantics ----

func TestParity_PutApplicationPolicy_ReplacesExistingStatements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("policy-replace-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	// Set initial policy with 2 statements
	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/policy-replace-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"Deploy"}, "principals": []string{"111111111111"}, "statementId": "s1"},
			{"actions": []string{"Deploy"}, "principals": []string{"222222222222"}, "statementId": "s2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Replace with 1 statement
	rec = doServerlessRepoRequest(t, h, http.MethodPut, "/applications/policy-replace-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"SearchApplications"}, "principals": []string{"*"}, "statementId": "s3"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessRepoRequest(t, h, http.MethodGet, "/applications/policy-replace-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts := resp["statements"].([]any)
	require.Len(t, stmts, 1, "policy should have exactly 1 statement after replacement")
	assert.Equal(t, "s3", stmts[0].(map[string]any)["statementId"])
}

// ---- Application response field completeness ----

func TestParity_GetApplication_ResponseFieldCompleteness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":          "full-resp-app",
		"description":   "A full response app",
		"author":        "test-author",
		"homePageUrl":   "https://example.com",
		"licenseUrl":    "https://example.com/license",
		"readmeUrl":     "https://example.com/readme",
		"spdxLicenseId": "Apache-2.0",
		"sourceCodeUrl": "https://github.com/example",
		"labels":        []string{"test", "demo"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doServerlessRepoRequest(t, h, http.MethodGet, "/applications/full-resp-app", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "full-resp-app", resp["name"])
	assert.Equal(t, "A full response app", resp["description"])
	assert.Equal(t, "test-author", resp["author"])
	assert.Equal(t, "https://example.com", resp["homePageUrl"])
	assert.Equal(t, "https://example.com/license", resp["licenseUrl"])
	assert.Equal(t, "https://example.com/readme", resp["readmeUrl"])
	assert.Equal(t, "Apache-2.0", resp["spdxLicenseId"])
	assert.NotEmpty(t, resp["applicationId"])
	assert.NotEmpty(t, resp["creationTime"])
	assert.Equal(t, false, resp["isVerifiedAuthor"])

	labels := resp["labels"].([]any)
	assert.Len(t, labels, 2)
}

// ---- Version summary pagination ----

func TestParity_ListApplicationVersions_PaginationNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("pag-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0", "4.0.0"} {
		_, err = h.Backend.CreateApplicationVersion("pag-app", v, "https://example.com", "")
		require.NoError(t, err)
	}

	// Page 1
	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/pag-app/versions?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r1))
	v1 := r1["versions"].([]any)
	assert.Len(t, v1, 2)
	nt, ok := r1["nextToken"].(string)
	require.True(t, ok)

	// Page 2
	rec2 := doServerlessRepoRequest(
		t, h, http.MethodGet,
		"/applications/pag-app/versions?maxItems=2&nextToken="+nt,
		nil,
	)
	require.Equal(t, http.StatusOK, rec2.Code)

	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
	v2 := r2["versions"].([]any)
	assert.Len(t, v2, 2)
	assert.Nil(t, r2["nextToken"])
}
