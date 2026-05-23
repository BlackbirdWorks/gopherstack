package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestBatch1_ApplicationMetadata(t *testing.T) {
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

func TestBatch1_CreateApplicationVersion_ArchiveURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		semanticVersion string
	}{
		{
			name:            "archive only",
			semanticVersion: "1.0.0",
			body:            map[string]any{"sourceCodeArchiveUrl": "s3://bucket/archive.zip"},
		},
		{
			name:            "archive with provided template",
			semanticVersion: "1.0.1",
			body: map[string]any{
				"sourceCodeArchiveUrl": "s3://bucket/archive.zip",
				"templateUrl":          "s3://bucket/template.yaml",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("versions-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			path := "/applications/versions-app/versions/" + tt.semanticVersion
			rec := doServerlessRepoRequest(t, h, http.MethodPut, path, tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var response map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			assert.Equal(t, "s3://bucket/archive.zip", response["sourceCodeArchiveUrl"])
			assert.NotEmpty(t, response["templateUrl"])
		})
	}
}

func TestBatch1_PutApplicationPolicy_AWSActions(t *testing.T) {
	t.Parallel()

	tests := []string{"Deploy", "SearchAndDeploy", "UnshareApplication", "UnSubscribeFromApplication"}
	for _, action := range tests {
		t.Run(action, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("policy-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/policy-app/policy", map[string]any{
				"statements": []map[string]any{{"actions": []string{action}, "principals": []string{"*"}}},
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestBatch1_CreateCloudFormationChangeSet_CapabilitiesAndTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		capability string
		wantStatus int
	}{
		{name: "iam", capability: "CAPABILITY_IAM", wantStatus: http.StatusCreated},
		{name: "named iam", capability: "CAPABILITY_NAMED_IAM", wantStatus: http.StatusCreated},
		{name: "auto expand", capability: "CAPABILITY_AUTO_EXPAND", wantStatus: http.StatusCreated},
		{name: "resource policy", capability: "CAPABILITY_RESOURCE_POLICY", wantStatus: http.StatusCreated},
		{name: "invalid", capability: "CAPABILITY_UNKNOWN", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("deploy-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/deploy-app/changesets", map[string]any{
				"stackName":    "stack",
				"capabilities": []string{tt.capability},
				"tags":         []map[string]string{{"key": "stage", "value": "test"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				assert.Contains(t, string(h.Snapshot()), `"capabilities":["`+tt.capability+`"]`)
				assert.Contains(t, string(h.Snapshot()), `"tags":[{"key":"stage","value":"test"}]`)
			}
		})
	}
}

func TestBatch1_ListApplicationDependencies_PersistedState(t *testing.T) {
	t.Parallel()

	tests := []serverlessrepo.ApplicationDependency{
		{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-a",
			SemanticVersion: "1.0.0",
		},
		{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-b",
			SemanticVersion: "2.1.0",
		},
		{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-c",
			SemanticVersion: "4.0.0",
		},
	}

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("root-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)
	for _, dependency := range tests[:2] {
		require.NoError(t, b.AddApplicationDependencyInternal("root-app", "3.0.0", dependency))
	}
	_, err = b.CreateApplication("nested-a", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)
	require.NoError(t, b.AddApplicationDependencyInternal("nested-a", "1.0.0", tests[2]))

	restored := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	require.NoError(t, restored.Restore(b.Snapshot()))
	h := serverlessrepo.NewHandler(restored)
	rec := doServerlessRepoRequest(
		t,
		h,
		http.MethodGet,
		"/applications/root-app/dependencies?semanticVersion=3.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var response struct {
		Dependencies []serverlessrepo.ApplicationDependency `json:"dependencies"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.ElementsMatch(t, tests, response.Dependencies)
}
