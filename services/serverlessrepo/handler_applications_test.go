package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "creates application successfully",
			body: map[string]any{
				"name":            "my-app",
				"description":     "A test application",
				"author":          "test-author",
				"semanticVersion": "1.0.0",
			},
			wantCode: http.StatusCreated,
			wantARN:  true,
		},
		{
			name:     "missing name returns bad request",
			body:     map[string]any{"description": "No name", "author": "a"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing author returns bad request",
			body:     map[string]any{"name": "my-app", "description": "desc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing description returns bad request",
			body:     map[string]any{"name": "my-app", "author": "author"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate application returns conflict",
			body: map[string]any{
				"name":        "existing-app",
				"description": "desc",
				"author":      "author",
			},
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantCode == http.StatusConflict {
				_, err := h.Backend.CreateApplication("existing-app", "desc", "author", "", "", nil, "", "", "")
				require.NoError(t, err)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["applicationId"])
				assert.Equal(t, tt.body["name"], resp["name"])
			}
		})
	}
}

func TestCreateApplication_Returns201(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":        "my-app",
		"description": "A test application",
		"author":      "test-author",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestCreateApplication_MissingAuthor(t *testing.T) {
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

func TestCreateApplication_MissingDescription(t *testing.T) {
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

func TestCreateApplication_WithLabels(t *testing.T) {
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

func TestCreateApplication_WithHomePageURL(t *testing.T) {
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

func TestCreateApplication_AlreadyExists(t *testing.T) {
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

func TestApplicationResponse_LabelsField(t *testing.T) {
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

func TestApplication_ARNFormat(t *testing.T) {
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

func TestCreateApplication_TemplateURLOnly_CreatesVersion(t *testing.T) {
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

func TestCreateApplication_InlineVersion_FullResponseFields(t *testing.T) {
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

func TestApplicationMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		check  func(*testing.T, map[string]any)
		name   string
		method string
		path   string
		seed   bool
	}{
		{
			name:   "create returns readme and verification status",
			method: http.MethodPost,
			path:   "/applications",
			body: map[string]any{
				"name":                 "metadata-app",
				"author":               "author",
				"description":          "description",
				"readmeUrl":            "s3://bucket/readme.md",
				"semanticVersion":      "1.0.0",
				"sourceCodeArchiveUrl": "s3://bucket/source.zip",
			},
			check: func(t *testing.T, response map[string]any) {
				t.Helper()
				assert.Equal(t, "s3://bucket/readme.md", response["readmeUrl"])
				assert.Equal(t, false, response["isVerifiedAuthor"])
				version := response["version"].(map[string]any)
				assert.Equal(t, "s3://bucket/source.zip", version["sourceCodeArchiveUrl"])
			},
		},
		{
			name:   "update replaces labels",
			method: http.MethodPatch,
			path:   "/applications/metadata-app",
			seed:   true,
			body: map[string]any{
				"labels": []string{"deploy", "public"},
			},
			check: func(t *testing.T, response map[string]any) {
				t.Helper()
				assert.Equal(t, []any{"deploy", "public"}, response["labels"])
			},
		},
		{
			name:   "get returns stored current version metadata",
			method: http.MethodGet,
			path:   "/applications/metadata-app",
			seed:   true,
			check: func(t *testing.T, response map[string]any) {
				t.Helper()
				version := response["version"].(map[string]any)
				assert.Equal(t, "s3://bucket/source.zip", version["sourceCodeArchiveUrl"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				createMetadataApplication(t, h)
			}

			rec := doServerlessRepoRequest(t, h, tt.method, tt.path, tt.body)
			assert.Contains(t, []int{http.StatusCreated, http.StatusOK}, rec.Code)

			var response map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			tt.check(t, response)
		})
	}
}

func createMetadataApplication(t *testing.T, h *serverlessrepo.Handler) {
	t.Helper()

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":                 "metadata-app",
		"author":               "author",
		"description":          "description",
		"readmeUrl":            "s3://bucket/readme.md",
		"semanticVersion":      "1.0.0",
		"sourceCodeArchiveUrl": "s3://bucket/source.zip",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestHandler_GetApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		appName  string
		wantCode int
	}{
		{
			name:     "get existing application",
			appName:  "existing-app",
			wantCode: http.StatusOK,
		},
		{
			name:     "get non-existent application",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			_, err := h.Backend.CreateApplication("existing-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/"+tt.appName, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "existing-app", resp["name"])
			}
		})
	}
}

func TestGetApplication_ExplicitSemanticVersionNotFound_Returns404(t *testing.T) {
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

func TestGetApplication_WithSemanticVersion(t *testing.T) {
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

func TestGetApplication_WithSemanticVersion_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app?semanticVersion=9.9.9", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetApplication_VersionEmbedFromStore_IncludesTemplateURL(t *testing.T) {
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

func TestGetApplication_ResponseFieldCompleteness(t *testing.T) {
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

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantCode int
	}{
		{
			name:    "update existing application",
			appName: "my-app",
			body: map[string]any{
				"description": "Updated description",
			},
			wantCode: http.StatusOK,
		},
		{
			name:    "update non-existent application",
			appName: "not-found",
			body: map[string]any{
				"description": "Updated description",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "original", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/"+tt.appName, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestUpdateApplication_HomePageURL(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	updated, err := b.UpdateApplication("my-app", "", "", "https://example.com", "")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com", updated.HomePageURL)
}

func TestUpdateApplication_EmbedCurrentVersion(t *testing.T) {
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

func TestUpdateApplication_NoVersionWhenNoneCreated(t *testing.T) {
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

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		appName  string
		wantCode int
	}{
		{
			name:     "delete existing application",
			appName:  "my-app",
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete non-existent application",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodDelete, "/applications/"+tt.appName, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestDeleteApplication_CascadesVersions(t *testing.T) {
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

func TestHandler_UnshareApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantCode int
	}{
		{
			name:    "unshares application successfully",
			appName: "my-app",
			body: map[string]any{
				"organizationId": "o-abc123",
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "missing organizationId returns bad request",
			appName:  "my-app",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:    "app not found returns 404",
			appName: "not-found",
			body: map[string]any{
				"organizationId": "o-abc123",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/"+tt.appName+"/unshare", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAddApplicationInternal(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	app := b.AddApplicationInternal("seed-app", "seeded description", "seed-author")
	require.NotNil(t, app)
	assert.Equal(t, "seed-app", app.Name)
	assert.NotEmpty(t, app.ApplicationID)
	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b))
}

// TestApplication_ARNFormRouting verifies that Get/Update/Delete on an
// application accept the ARN-encoded path form the AWS SDK sends
// (arn:aws:serverlessrepo:...:applications/{name}, percent-encoded), not just
// the plain application name.
func TestApplication_ARNFormRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		method   string
		appName  string
		wantCode int
	}{
		{
			name:     "GET routes to GetApplication",
			method:   http.MethodGet,
			appName:  "my-app",
			wantCode: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, "my-app", resp["name"])
			},
		},
		{
			name:     "PATCH routes to UpdateApplication",
			method:   http.MethodPatch,
			appName:  "update-app",
			body:     map[string]any{"description": "updated"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DELETE routes to DeleteApplication",
			method:   http.MethodDelete,
			appName:  "del-app",
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication(tt.appName, "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			path := arnPathFor(tt.appName)
			rec := doServerlessRepoRequestEncoded(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}
