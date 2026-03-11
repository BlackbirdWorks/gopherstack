package codecommit_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

func newTestHandler(t *testing.T) *codecommit.Handler {
	t.Helper()

	return codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
}

func doRequest(t *testing.T, h *codecommit.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CodeCommit_20150413."+action)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CodeCommit", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "codecommit", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateRepository",
		"GetRepository",
		"DeleteRepository",
		"ListRepositories",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "codecommit_target",
			target:    "CodeCommit_20150413.CreateRepository",
			wantMatch: true,
		},
		{
			name:      "codecommit_get_target",
			target:    "CodeCommit_20150413.GetRepository",
			wantMatch: true,
		},
		{
			name:      "other_service_target",
			target:    "AmazonDynamoDB.GetItem",
			wantMatch: false,
		},
		{
			name:      "empty_target",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.RouteMatcher()(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestHandler_CreateRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input       map[string]any
		name        string
		wantRepoKey string
		wantStatus  int
	}{
		{
			name: "success",
			input: map[string]any{
				"repositoryName":        "my-repo",
				"repositoryDescription": "A test repository",
				"tags":                  map[string]string{"env": "test"},
			},
			wantStatus:  http.StatusOK,
			wantRepoKey: "my-repo",
		},
		{
			name: "missing_name",
			input: map[string]any{
				"repositoryName": "",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateRepository", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				meta, ok := resp["repositoryMetadata"].(map[string]any)
				require.True(t, ok, "repositoryMetadata should be present")
				assert.Equal(t, tt.wantRepoKey, meta["repositoryName"])
				assert.NotEmpty(t, meta["repositoryId"])
				assert.NotEmpty(t, meta["Arn"])
			}
		})
	}
}

func TestHandler_GetRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "existing_repository",
			repoName:   "my-repo",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_repository",
			repoName:   "missing-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Seed a repository for the existing case.
			if tt.repoName == "my-repo" {
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "my-repo",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "GetRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "existing_repository",
			repoName:   "to-delete",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_repository",
			repoName:   "missing-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.repoName == "to-delete" {
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "to-delete",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		repos     []string
		wantCount int
	}{
		{
			name:      "empty",
			repos:     nil,
			wantCount: 0,
		},
		{
			name:      "two_repositories",
			repos:     []string{"repo-a", "repo-b"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, n := range tt.repos {
				rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": n})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListRepositories", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			repos, ok := resp["repositories"].([]any)
			require.True(t, ok)
			assert.Len(t, repos, tt.wantCount)
		})
	}
}

func TestHandler_TagAndUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codecommit.Handler) string
		name       string
		action     string
		wantStatus int
	}{
		{
			name:   "tag_existing_resource",
			action: "TagResource",
			setup: func(t *testing.T, h *codecommit.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "tag-repo",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				metaRaw, ok := resp["repositoryMetadata"]
				require.True(t, ok)

				meta, ok := metaRaw.(map[string]any)
				require.True(t, ok)

				arnRaw, ok := meta["Arn"]
				require.True(t, ok)

				arn, ok := arnRaw.(string)
				require.True(t, ok)

				return arn
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "untag_existing_resource",
			action: "UntagResource",
			setup: func(t *testing.T, h *codecommit.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "untag-repo",
					"tags":           map[string]string{"key1": "val1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				metaRaw, ok := resp["repositoryMetadata"]
				require.True(t, ok)

				meta, ok := metaRaw.(map[string]any)
				require.True(t, ok)

				arnRaw, ok := meta["Arn"]
				require.True(t, ok)

				arn, ok := arnRaw.(string)
				require.True(t, ok)

				return arn
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			var body map[string]any
			if tt.action == "TagResource" {
				body = map[string]any{
					"resourceArn": resourceARN,
					"tags":        map[string]string{"new-key": "new-val"},
				}
			} else {
				body = map[string]any{
					"resourceArn": resourceARN,
					"tagKeys":     []string{"key1"},
				}
			}

			rec := doRequest(t, h, tt.action, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codecommit.Handler) string
		wantTags   map[string]string
		name       string
		wantStatus int
	}{
		{
			name: "repository_with_tags",
			setup: func(t *testing.T, h *codecommit.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "tagged-repo",
					"tags":           map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				metaRaw, ok := resp["repositoryMetadata"]
				require.True(t, ok)

				meta, ok := metaRaw.(map[string]any)
				require.True(t, ok)

				arnRaw, ok := meta["Arn"]
				require.True(t, ok)

				arn, ok := arnRaw.(string)
				require.True(t, ok)

				return arn
			},
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"env": "test"},
		},
		{
			name: "missing_arn",
			setup: func(_ *testing.T, _ *codecommit.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": resourceARN,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				tagsRaw, ok := resp["tags"].(map[string]any)
				require.True(t, ok)

				for k, v := range tt.wantTags {
					assert.Equal(t, v, tagsRaw[k])
				}
			}
		})
	}
}

func TestHandler_DuplicateRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName": "dupe-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName": "dupe-repo",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		wantAction string
	}{
		{
			name:       "create_repository",
			target:     "CodeCommit_20150413.CreateRepository",
			wantAction: "CreateRepository",
		},
		{
			name:       "get_repository",
			target:     "CodeCommit_20150413.GetRepository",
			wantAction: "GetRepository",
		},
		{
			name:       "unknown_target",
			target:     "SomeOtherService.Action",
			wantAction: "Unknown",
		},
		{
			name:       "empty_target",
			target:     "",
			wantAction: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractOperation(c)
			assert.Equal(t, tt.wantAction, got)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantName string
	}{
		{
			name:     "with_repository_name",
			body:     `{"repositoryName":"my-repo"}`,
			wantName: "my-repo",
		},
		{
			name:     "empty_repository_name",
			body:     `{"repositoryName":""}`,
			wantName: "",
		},
		{
			name:     "invalid_json",
			body:     `not-json`,
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractResource(c)
			assert.Equal(t, tt.wantName, got)
		})
	}
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.NotEmpty(t, regions[0])
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	assert.Equal(t, config.DefaultRegion, b.Region())
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "create_repository_invalid_json", action: "CreateRepository"},
		{name: "get_repository_invalid_json", action: "GetRepository"},
		{name: "delete_repository_invalid_json", action: "DeleteRepository"},
		{name: "tag_resource_invalid_json", action: "TagResource"},
		{name: "untag_resource_invalid_json", action: "UntagResource"},
		{name: "list_tags_invalid_json", action: "ListTagsForResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString("not-valid-json{"))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "CodeCommit_20150413."+tt.action)

			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_NotFoundErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "tag_nonexistent_resource",
			action: "TagResource",
			body: map[string]any{
				"resourceArn": "arn:aws:codecommit:us-east-1:123456789012:nonexistent",
				"tags":        map[string]string{"key": "val"},
			},
		},
		{
			name:   "untag_nonexistent_resource",
			action: "UntagResource",
			body: map[string]any{
				"resourceArn": "arn:aws:codecommit:us-east-1:123456789012:nonexistent",
				"tagKeys":     []string{"key"},
			},
		},
		{
			name:   "list_tags_nonexistent_resource",
			action: "ListTagsForResource",
			body: map[string]any{
				"resourceArn": "arn:aws:codecommit:us-east-1:123456789012:nonexistent",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
