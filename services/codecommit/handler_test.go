package codecommit_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// ---- New operation tests ----

func TestHandler_CreateApprovalRuleTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			input: map[string]any{
				"approvalRuleTemplateName": "my-template",
				"approvalRuleTemplateContent": `{"Version":"2018-11-08",` +
					`"Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":1}]}`,
				"approvalRuleTemplateDescription": "A test template",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_name",
			input: map[string]any{
				"approvalRuleTemplateName":    "",
				"approvalRuleTemplateContent": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_content",
			input: map[string]any{
				"approvalRuleTemplateName":    "no-content",
				"approvalRuleTemplateContent": "",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_name",
			input: map[string]any{
				"approvalRuleTemplateName":    "dup-template",
				"approvalRuleTemplateContent": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_name" {
				rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName":    "dup-template",
					"approvalRuleTemplateContent": `{}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateApprovalRuleTemplate", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				tmpl, ok := resp["approvalRuleTemplate"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-template", tmpl["approvalRuleTemplateName"])
				assert.NotEmpty(t, tmpl["approvalRuleTemplateId"])
				assert.NotEmpty(t, tmpl["ruleContentSha256"])
			}
		})
	}
}

func TestHandler_AssociateApprovalRuleTemplateWithRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		templateName string
		repoName     string
		seedTemplate bool
		seedRepo     bool
		wantStatus   int
	}{
		{
			name:         "success",
			templateName: "tmpl",
			repoName:     "repo",
			seedTemplate: true,
			seedRepo:     true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "template_not_found",
			templateName: "missing-tmpl",
			repoName:     "repo",
			seedRepo:     true,
			wantStatus:   http.StatusNotFound,
		},
		{
			name:         "repo_not_found",
			templateName: "tmpl",
			repoName:     "missing-repo",
			seedTemplate: true,
			wantStatus:   http.StatusNotFound,
		},
		{
			name:         "missing_template_name",
			templateName: "",
			repoName:     "repo",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seedTemplate {
				rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName":    "tmpl",
					"approvalRuleTemplateContent": `{}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.seedRepo {
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "repo",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
				"approvalRuleTemplateName": tt.templateName,
				"repositoryName":           tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchAssociateApprovalRuleTemplateWithRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		templateName   string
		repos          []string
		wantAssocCount int
		wantErrorCount int
		wantStatus     int
		seedTemplate   bool
	}{
		{
			name:           "all_found",
			templateName:   "tmpl",
			repos:          []string{"repo-a", "repo-b"},
			seedTemplate:   true,
			wantAssocCount: 2,
			wantErrorCount: 0,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "partial_found",
			templateName:   "tmpl",
			repos:          []string{"repo-a", "missing-repo"},
			seedTemplate:   true,
			wantAssocCount: 1,
			wantErrorCount: 1,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "template_not_found",
			templateName:   "no-tmpl",
			repos:          []string{"repo-a"},
			wantAssocCount: 0,
			wantErrorCount: 1,
			wantStatus:     http.StatusOK,
		},
		{
			name:         "missing_template_name",
			templateName: "",
			repos:        []string{"repo-a"},
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seedTemplate {
				rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName":    "tmpl",
					"approvalRuleTemplateContent": `{}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo-a"})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo-b"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "BatchAssociateApprovalRuleTemplateWithRepositories", map[string]any{
				"approvalRuleTemplateName": tt.templateName,
				"repositoryNames":          tt.repos,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				assoc, _ := resp["associatedRepositoryNames"].([]any)
				assert.Len(t, assoc, tt.wantAssocCount)

				errs, _ := resp["errors"].([]any)
				assert.Len(t, errs, tt.wantErrorCount)
			}
		})
	}
}

func TestHandler_BatchDisassociateApprovalRuleTemplateFromRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		templateName      string
		repos             []string
		wantDisassocCount int
		wantErrorCount    int
		wantStatus        int
		seedAndAssociate  bool
	}{
		{
			name:              "all_found",
			templateName:      "tmpl",
			repos:             []string{"repo-a"},
			seedAndAssociate:  true,
			wantDisassocCount: 1,
			wantErrorCount:    0,
			wantStatus:        http.StatusOK,
		},
		{
			name:              "repo_not_found",
			templateName:      "tmpl",
			repos:             []string{"missing-repo"},
			seedAndAssociate:  true,
			wantDisassocCount: 0,
			wantErrorCount:    1,
			wantStatus:        http.StatusOK,
		},
		{
			name:              "template_not_found",
			templateName:      "no-tmpl",
			repos:             []string{"repo-a"},
			wantDisassocCount: 0,
			wantErrorCount:    1,
			wantStatus:        http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seedAndAssociate {
				rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName":    "tmpl",
					"approvalRuleTemplateContent": `{}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo-a"})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
					"approvalRuleTemplateName": "tmpl",
					"repositoryName":           "repo-a",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "BatchDisassociateApprovalRuleTemplateFromRepositories", map[string]any{
				"approvalRuleTemplateName": tt.templateName,
				"repositoryNames":          tt.repos,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				disassoc, _ := resp["disassociatedRepositoryNames"].([]any)
				assert.Len(t, disassoc, tt.wantDisassocCount)

				errs, _ := resp["errors"].([]any)
				assert.Len(t, errs, tt.wantErrorCount)
			}
		})
	}
}

func TestHandler_BatchGetRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		seedRepos      []string
		requestRepos   []string
		wantFoundCount int
		wantMissCount  int
		wantStatus     int
	}{
		{
			name:           "all_found",
			seedRepos:      []string{"repo-a", "repo-b"},
			requestRepos:   []string{"repo-a", "repo-b"},
			wantFoundCount: 2,
			wantMissCount:  0,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "partial_found",
			seedRepos:      []string{"repo-a"},
			requestRepos:   []string{"repo-a", "missing-b"},
			wantFoundCount: 1,
			wantMissCount:  1,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "none_found",
			seedRepos:      nil,
			requestRepos:   []string{"missing-a"},
			wantFoundCount: 0,
			wantMissCount:  1,
			wantStatus:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.seedRepos {
				rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": name})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "BatchGetRepositories", map[string]any{
				"repositoryNames": tt.requestRepos,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			repos, _ := resp["repositories"].([]any)
			assert.Len(t, repos, tt.wantFoundCount)

			notFound, _ := resp["repositoriesNotFound"].([]any)
			assert.Len(t, notFound, tt.wantMissCount)
		})
	}
}

func TestHandler_CreateBranch(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

		// Seed a real commit via CreateCommit.
		commitRec := doRequest(t, h, "CreateCommit", map[string]any{
			"repositoryName": "repo",
			"branchName":     "main",
			"authorName":     "test",
			"email":          "test@example.com",
			"commitMessage":  "init",
		})
		require.Equal(t, http.StatusOK, commitRec.Code)

		var commitResp map[string]any
		require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
		commitID := commitResp["commitId"].(string)

		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       commitID,
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("commit_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       "does-not-exist",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("repo_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "missing-repo",
			"branchName":     "feature",
			"commitId":       "abc123",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("missing_branch_name", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "",
			"commitId":       "abc123",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing_commit_id", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       "",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("duplicate_branch", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

		commitRec := doRequest(t, h, "CreateCommit", map[string]any{
			"repositoryName": "repo",
			"branchName":     "main",
			"authorName":     "test",
			"email":          "test@example.com",
			"commitMessage":  "init",
		})
		require.Equal(t, http.StatusOK, commitRec.Code)

		var commitResp map[string]any
		require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
		commitID := commitResp["commitId"].(string)

		doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       commitID,
		})
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       commitID,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_CreateCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		branchName string
		authorName string
		wantStatus int
	}{
		{
			name:       "success",
			repoName:   "repo",
			branchName: "main",
			authorName: "Alice",
			wantStatus: http.StatusOK,
		},
		{
			name:       "repo_not_found",
			repoName:   "missing-repo",
			branchName: "main",
			authorName: "Alice",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_repo_name",
			repoName:   "",
			branchName: "main",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_branch_name",
			repoName:   "repo",
			branchName: "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.repoName == "repo" {
				rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateCommit", map[string]any{
				"repositoryName": tt.repoName,
				"branchName":     tt.branchName,
				"authorName":     tt.authorName,
				"commitMessage":  "initial commit",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["commitId"])
				assert.NotEmpty(t, resp["treeId"])
			}
		})
	}
}

func TestHandler_BatchGetCommits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantFoundCount int
		wantErrCount   int
		wantStatus     int
		seedCommits    int
		requestMissing bool
	}{
		{
			name:           "all_found",
			seedCommits:    2,
			wantFoundCount: 2,
			wantErrCount:   0,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "partial_found",
			seedCommits:    1,
			requestMissing: true,
			wantFoundCount: 1,
			wantErrCount:   1,
			wantStatus:     http.StatusOK,
		},
		{
			name:       "missing_repo",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "missing_repo" {
				rec := doRequest(t, h, "BatchGetCommits", map[string]any{
					"repositoryName": "",
					"commitIds":      []string{"abc"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
			require.Equal(t, http.StatusOK, rec.Code)

			var commitIDs []string
			for range tt.seedCommits {
				r := doRequest(t, h, "CreateCommit", map[string]any{
					"repositoryName": "repo",
					"branchName":     "main",
					"commitMessage":  "commit",
				})
				require.Equal(t, http.StatusOK, r.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(r.Body.Bytes(), &resp))
				commitIDs = append(commitIDs, resp["commitId"].(string))
			}

			if tt.requestMissing {
				commitIDs = append(commitIDs, "nonexistent-commit-id")
			}

			rec = doRequest(t, h, "BatchGetCommits", map[string]any{
				"repositoryName": "repo",
				"commitIds":      commitIDs,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			commits, _ := resp["commits"].([]any)
			assert.Len(t, commits, tt.wantFoundCount)

			errs, _ := resp["errors"].([]any)
			assert.Len(t, errs, tt.wantErrCount)
		})
	}
}

func TestHandler_BatchDescribeMergeConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success_no_file_paths",
			input: map[string]any{
				"repositoryName":             "repo",
				"destinationCommitSpecifier": "main",
				"sourceCommitSpecifier":      "feature",
				"mergeOption":                "FAST_FORWARD_MERGE",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "success_with_file_paths",
			input: map[string]any{
				"repositoryName":             "repo",
				"destinationCommitSpecifier": "main",
				"sourceCommitSpecifier":      "feature",
				"mergeOption":                "THREE_WAY_MERGE",
				"filePaths":                  []string{"file1.txt", "file2.txt"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "repo_not_found",
			input: map[string]any{
				"repositoryName":             "missing",
				"destinationCommitSpecifier": "main",
				"sourceCommitSpecifier":      "feature",
				"mergeOption":                "FAST_FORWARD_MERGE",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_repo_name",
			input: map[string]any{
				"repositoryName":             "",
				"destinationCommitSpecifier": "main",
				"sourceCommitSpecifier":      "feature",
				"mergeOption":                "FAST_FORWARD_MERGE",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_merge_option",
			input: map[string]any{
				"repositoryName":             "repo",
				"destinationCommitSpecifier": "main",
				"sourceCommitSpecifier":      "feature",
				"mergeOption":                "",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "BatchDescribeMergeConflicts", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "conflicts")
				assert.Contains(t, resp, "destinationCommitId")
				assert.Contains(t, resp, "sourceCommitId")
			}
		})
	}
}

func TestHandler_CreatePullRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			input: map[string]any{
				"title":       "My PR",
				"description": "A test PR",
				"targets": []map[string]any{
					{
						"repositoryName":       "repo",
						"sourceReference":      "refs/heads/feature",
						"destinationReference": "refs/heads/main",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_title",
			input: map[string]any{
				"title": "",
				"targets": []map[string]any{
					{
						"repositoryName":  "repo",
						"sourceReference": "refs/heads/feature",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "no_targets",
			input: map[string]any{
				"title":   "My PR",
				"targets": []map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "sequential_ids",
			input: map[string]any{
				"title": "Second PR",
				"targets": []map[string]any{
					{
						"repositoryName":  "repo",
						"sourceReference": "refs/heads/feature",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreatePullRequest", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				pr, ok := resp["pullRequest"].(map[string]any)
				require.True(t, ok, "pullRequest key should be present")
				assert.NotEmpty(t, pr["pullRequestId"])
				assert.Equal(t, "OPEN", pr["pullRequestStatus"])
				assert.NotEmpty(t, pr["revisionId"])
			}
		})
	}
}

func TestHandler_NewOperations_GetSupportedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"AssociateApprovalRuleTemplateWithRepository",
		"BatchAssociateApprovalRuleTemplateWithRepositories",
		"BatchDescribeMergeConflicts",
		"BatchDisassociateApprovalRuleTemplateFromRepositories",
		"BatchGetCommits",
		"BatchGetRepositories",
		"CreateApprovalRuleTemplate",
		"CreateBranch",
		"CreateCommit",
		"CreatePullRequest",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}

// ---- Refinement check 1 tests ----

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	// Seed data
	_, err := b.CreateRepository("repo-a", "", nil)
	require.NoError(t, err)
	_, err = b.CreateApprovalRuleTemplate("tmpl", "", "{}")
	require.NoError(t, err)
	_, err = b.CreateCommit("repo-a", "main", "Alice", "alice@test.com", "init")
	require.NoError(t, err)
	_, err = b.CreatePullRequest("My PR", "", "", []codecommit.PullRequestTarget{
		{RepositoryName: "repo-a", SourceReference: "refs/heads/feature"},
	})
	require.NoError(t, err)

	b.Reset()

	// All state cleared
	_, err = b.GetRepository("repo-a")
	require.Error(t, err)

	repos := b.ListRepositories()
	assert.Empty(t, repos)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo-a"})
	require.Equal(t, http.StatusOK, rec.Code)

	h.Reset()

	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo-a"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestProvider_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codecommit.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	require.ErrorIs(t, err, codecommit.ErrNilAppContext)
}

func TestHandler_SortedListRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListRepositories", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, ok := resp["repositories"].([]any)
	require.True(t, ok)
	require.Len(t, items, 3)

	names := make([]string, 0, 3)
	for _, item := range items {
		m := item.(map[string]any)
		names = append(names, m["repositoryName"].(string))
	}

	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names)
}

func TestHandler_DeleteRepository_Cascade(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create repo + branch + commit
	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"commitMessage":  "init",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete the repo
	rec = doRequest(t, h, "DeleteRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Repo is gone
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// BatchGetCommits on deleted repo returns 404
	rec = doRequest(t, h, "BatchGetCommits", map[string]any{
		"repositoryName": "repo",
		"commitIds":      []string{"some-id"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CommitParentTracking(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	// First commit — no parents
	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"commitMessage":  "initial",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var firstResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &firstResp))
	firstCommitID := firstResp["commitId"].(string)

	// Second commit on same branch — should have firstCommitID as parent
	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"commitMessage":  "second",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var secondResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &secondResp))
	secondCommitID := secondResp["commitId"].(string)

	// Fetch both commits via BatchGetCommits and check parent linkage
	rec = doRequest(t, h, "BatchGetCommits", map[string]any{
		"repositoryName": "repo",
		"commitIds":      []string{firstCommitID, secondCommitID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var batchResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchResp))

	commits, ok := batchResp["commits"].([]any)
	require.True(t, ok)
	require.Len(t, commits, 2)

	// Find the second commit and verify it has the first as parent
	for _, raw := range commits {
		c := raw.(map[string]any)
		if c["commitId"].(string) == secondCommitID {
			parents, _ := c["parents"].([]any)
			require.Len(t, parents, 1)
			assert.Equal(t, firstCommitID, parents[0].(string))
		}
	}
}

func TestHandler_BatchGetCommits_RepoNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchGetCommits", map[string]any{
		"repositoryName": "nonexistent-repo",
		"commitIds":      []string{"abc123"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_BatchGetCommits_EmptyIDsRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "BatchGetCommits", map[string]any{
		"repositoryName": "repo",
		"commitIds":      []string{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_BatchAssociate_EmptySlicesNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Template doesn't exist → errors list has entries, associated is empty []
	rec := doRequest(t, h, "BatchAssociateApprovalRuleTemplateWithRepositories", map[string]any{
		"approvalRuleTemplateName": "no-such-tmpl",
		"repositoryNames":          []string{"repo-a"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// Must be JSON array, not null
	associated := resp["associatedRepositoryNames"]
	assert.NotNil(t, associated, "associatedRepositoryNames must not be null")

	errs := resp["errors"]
	assert.NotNil(t, errs, "errors must not be null")
}

func TestHandler_BatchDisassociate_EmptySlicesNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchDisassociateApprovalRuleTemplateFromRepositories", map[string]any{
		"approvalRuleTemplateName": "no-such-tmpl",
		"repositoryNames":          []string{"repo-a"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	disassoc := resp["disassociatedRepositoryNames"]
	assert.NotNil(t, disassoc, "disassociatedRepositoryNames must not be null")
}

func TestHandler_RepoMetadataTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	meta, ok := resp["repositoryMetadata"].(map[string]any)
	require.True(t, ok)

	assert.NotNil(t, meta["creationDate"], "creationDate should be present")
	assert.NotNil(t, meta["lastModifiedDate"], "lastModifiedDate should be present")
	assert.Greater(t, meta["creationDate"].(float64), float64(0))
}

func TestHandler_BatchGetCommits_FullCommitMap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"authorName":     "Alice",
		"email":          "alice@example.com",
		"commitMessage":  "test commit",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	commitID := createResp["commitId"].(string)

	rec = doRequest(t, h, "BatchGetCommits", map[string]any{
		"repositoryName": "repo",
		"commitIds":      []string{commitID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	commits, ok := resp["commits"].([]any)
	require.True(t, ok)
	require.Len(t, commits, 1)

	c := commits[0].(map[string]any)
	assert.Equal(t, commitID, c["commitId"])
	assert.NotNil(t, c["author"], "author sub-object should be present")
	assert.NotNil(t, c["committer"], "committer sub-object should be present")

	author, ok := c["author"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Alice", author["name"])
	assert.Equal(t, "alice@example.com", author["email"])

	// parents should be a JSON array (not null) for first commit
	parents, ok := c["parents"].([]any)
	require.True(t, ok)
	assert.Empty(t, parents)
}

func TestHandler_MergeOption_InvalidValue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "BatchDescribeMergeConflicts", map[string]any{
		"repositoryName":             "repo",
		"destinationCommitSpecifier": "main",
		"sourceCommitSpecifier":      "feature",
		"mergeOption":                "INVALID_MERGE_OPTION",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreatePullRequest_TargetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targets    []map[string]any
		wantStatus int
	}{
		{
			name: "missing_repo_name",
			targets: []map[string]any{
				{"repositoryName": "", "sourceReference": "refs/heads/feature"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_source_reference",
			targets: []map[string]any{
				{"repositoryName": "repo", "sourceReference": ""},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreatePullRequest", map[string]any{
				"title":   "My PR",
				"targets": tt.targets,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ARN_TagOps_O1(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create repo and get its ARN
	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	meta := createResp["repositoryMetadata"].(map[string]any)
	repoARN := meta["Arn"].(string)
	require.NotEmpty(t, repoARN)

	// Tag using ARN
	rec = doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": repoARN,
		"tags":        map[string]string{"env": "prod"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags to verify
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": repoARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))

	tagMap, ok := tagsResp["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tagMap["env"])

	// Untag
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": repoARN,
		"tagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify removed
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": repoARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tagMap, _ = tagsResp["tags"].(map[string]any)
	assert.Empty(t, tagMap)
}

func TestHandler_PullRequest_FieldsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreatePullRequest", map[string]any{
		"title":              "My PR",
		"description":        "Details here",
		"clientRequestToken": "tok-123",
		"targets": []map[string]any{
			{
				"repositoryName":       "repo",
				"sourceReference":      "refs/heads/feature",
				"destinationReference": "refs/heads/main",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	pr, ok := resp["pullRequest"].(map[string]any)
	require.True(t, ok)

	assert.NotEmpty(t, pr["pullRequestId"])
	assert.Equal(t, "My PR", pr["title"])
	assert.Equal(t, "Details here", pr["description"])
	assert.Equal(t, "OPEN", pr["pullRequestStatus"])
	assert.NotNil(t, pr["authorArn"])
	assert.NotNil(t, pr["clientRequestToken"])
	assert.NotNil(t, pr["revisionId"])
	assert.NotNil(t, pr["creationDate"])
	assert.NotNil(t, pr["lastActivityDate"])

	targets, ok := pr["pullRequestTargets"].([]any)
	require.True(t, ok)
	require.Len(t, targets, 1)

	target := targets[0].(map[string]any)
	assert.Equal(t, "repo", target["repositoryName"])
	assert.Equal(t, "refs/heads/feature", target["sourceReference"])
	assert.Equal(t, "refs/heads/main", target["destinationReference"])
}

func TestHandler_ApprovalRuleTemplate_LastModifiedUser(t *testing.T) {
	t.Parallel()

	b := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	h := codecommit.NewHandler(b)

	// Seed template directly with LastModifiedUser set
	b.AddApprovalRuleTemplateInternal(&codecommit.ApprovalRuleTemplate{
		ApprovalRuleTemplateID:      "tmpl-id",
		ApprovalRuleTemplateName:    "my-tmpl",
		ApprovalRuleTemplateARN:     "arn:aws:codecommit:us-east-1:123456789012:approval-rule-template/my-tmpl",
		ApprovalRuleTemplateContent: `{}`,
		RuleContentSha256:           "abc",
		LastModifiedUser:            "arn:aws:iam::123456789012:user/Alice",
	})

	// Create a repo and associate
	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "my-tmpl",
		"repositoryName":           "repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	// Seed a repository
	repoARN := "arn:aws:codecommit:us-east-1:123456789012:seed-repo"
	b.AddRepositoryInternal(&codecommit.Repository{
		RepositoryName: "seed-repo",
		RepositoryID:   "seed-id",
		ARN:            repoARN,
		AccountID:      config.DefaultAccountID,
	})

	r, err := b.GetRepository("seed-repo")
	require.NoError(t, err)
	assert.Equal(t, "seed-repo", r.RepositoryName)

	// Seed a commit
	b.AddCommitInternal("seed-repo", &codecommit.Commit{
		CommitID:       "commit-seed",
		TreeID:         "tree-seed",
		Message:        "seeded",
		RepositoryName: "seed-repo",
	})

	found, errs, err := b.BatchGetCommits("seed-repo", []string{"commit-seed"})
	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.Empty(t, errs)
	assert.Equal(t, "commit-seed", found[0].CommitID)

	// Seed a branch
	b.AddBranchInternal("seed-repo", &codecommit.Branch{
		BranchName:     "seed-branch",
		CommitID:       "commit-seed",
		RepositoryName: "seed-repo",
	})

	// Seed a pull request
	b.AddPullRequestInternal(&codecommit.PullRequest{
		PullRequestID:     "99",
		Title:             "Seeded PR",
		PullRequestStatus: "OPEN",
		PullRequestTargets: []codecommit.PullRequestTarget{
			{RepositoryName: "seed-repo", SourceReference: "refs/heads/feat"},
		},
	})
}

// --- Parity gap tests added in go-7lrz audit ---

func TestHandler_CreateRepository_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "valid_name",
			repoName:   "my-repo",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_with_dots",
			repoName:   "my.repo.name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_with_underscores",
			repoName:   "my_repo_name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_name",
			repoName:   "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name_with_spaces",
			repoName:   "my repo",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name_with_slash",
			repoName:   "my/repo",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name_too_long",
			repoName:   string(make([]byte, 101)),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_RepoMetadata_DefaultBranchAndKmsKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Initially no defaultBranch or kmsKeyId in metadata.
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta := resp["repositoryMetadata"].(map[string]any)
	_, hasDefault := meta["defaultBranch"]
	_, hasKms := meta["kmsKeyId"]
	assert.False(t, hasDefault, "defaultBranch should not appear when unset")
	assert.False(t, hasKms, "kmsKeyId should not appear when unset")

	// Set defaultBranch.
	rec = doRequest(t, h, "UpdateDefaultBranch", map[string]any{
		"repositoryName":    "repo",
		"defaultBranchName": "main",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set kmsKeyId.
	rec = doRequest(t, h, "UpdateRepositoryEncryptionKey", map[string]any{
		"repositoryName": "repo",
		"kmsKeyId":       "arn:aws:kms:us-east-1:123456789012:key/my-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now GetRepository should include both.
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta = resp["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "main", meta["defaultBranch"])
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", meta["kmsKeyId"])
}

func TestHandler_BatchGetRepositories_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 26 repos.
	names := make([]string, 26)
	for i := range names {
		names[i] = fmt.Sprintf("repo-%02d", i)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": names[i]})
	}

	// Request 26 — should fail.
	rec := doRequest(t, h, "BatchGetRepositories", map[string]any{
		"repositoryNames": names,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "MaximumRepositoryNamesExceededException", errResp["__type"])

	// Request 25 — should succeed.
	rec = doRequest(t, h, "BatchGetRepositories", map[string]any{
		"repositoryNames": names[:25],
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListPullRequests_StatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterStatus string
		wantCount    int
		wantHTTPCode int
	}{
		{
			name:         "all_no_filter",
			filterStatus: "",
			wantCount:    3,
			wantHTTPCode: http.StatusOK,
		},
		{
			name:         "open_only",
			filterStatus: "OPEN",
			wantCount:    2,
			wantHTTPCode: http.StatusOK,
		},
		{
			name:         "closed_only",
			filterStatus: "CLOSED",
			wantCount:    1,
			wantHTTPCode: http.StatusOK,
		},
		{
			name:         "invalid_status",
			filterStatus: "INVALID",
			wantHTTPCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			// Seed 2 OPEN + 1 CLOSED PR.
			createPR := func() string {
				rec := doRequest(t, h, "CreatePullRequest", map[string]any{
					"title": "PR",
					"targets": []map[string]any{
						{"repositoryName": "repo", "sourceReference": "refs/heads/feat"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				return resp["pullRequest"].(map[string]any)["pullRequestId"].(string)
			}

			createPR()
			createPR()
			closedID := createPR()

			doRequest(t, h, "UpdatePullRequestStatus", map[string]any{
				"pullRequestId":     closedID,
				"pullRequestStatus": "CLOSED",
			})

			body := map[string]any{"repositoryName": "repo"}
			if tt.filterStatus != "" {
				body["pullRequestStatus"] = tt.filterStatus
			}
			rec := doRequest(t, h, "ListPullRequests", body)
			assert.Equal(t, tt.wantHTTPCode, rec.Code)

			if tt.wantHTTPCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ids := resp["pullRequestIds"].([]any)
				assert.Len(t, ids, tt.wantCount)
			}
		})
	}
}

func TestHandler_ListPullRequests_NumericDescendingOrder(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	// Create 3 PRs — expect IDs 1, 2, 3.
	for i := range 3 {
		doRequest(t, h, "CreatePullRequest", map[string]any{
			"title": fmt.Sprintf("PR %d", i),
			"targets": []map[string]any{
				{"repositoryName": "repo", "sourceReference": "refs/heads/feat"},
			},
		})
	}

	rec := doRequest(t, h, "ListPullRequests", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ids := resp["pullRequestIds"].([]any)
	require.Len(t, ids, 3)

	// Should be descending: "3", "2", "1".
	assert.Equal(t, "3", ids[0])
	assert.Equal(t, "2", ids[1])
	assert.Equal(t, "1", ids[2])
}

func TestHandler_MergePullRequest_AlreadyMerged(t *testing.T) {
	t.Parallel()

	strategies := []string{
		"MergePullRequestByFastForward",
		"MergePullRequestBySquash",
		"MergePullRequestByThreeWay",
	}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			rec := doRequest(t, h, "CreatePullRequest", map[string]any{
				"title": "PR",
				"targets": []map[string]any{
					{"repositoryName": "repo", "sourceReference": "refs/heads/feat"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			prID := resp["pullRequest"].(map[string]any)["pullRequestId"].(string)

			// First merge — should succeed.
			rec = doRequest(t, h, strategy, map[string]any{"pullRequestId": prID})
			require.Equal(t, http.StatusOK, rec.Code)

			// Second merge — should fail with PullRequestAlreadyClosedException.
			rec = doRequest(t, h, strategy, map[string]any{"pullRequestId": prID})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "PullRequestAlreadyClosedException", errResp["__type"])
		})
	}
}

func TestHandler_UpdatePullRequestStatus_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		wantStatus int
	}{
		{
			name:       "open_valid",
			status:     "OPEN",
			wantStatus: http.StatusOK,
		},
		{
			name:       "closed_valid",
			status:     "CLOSED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "merged_invalid",
			status:     "MERGED",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_invalid",
			status:     "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "arbitrary_invalid",
			status:     "DONE",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			rec := doRequest(t, h, "CreatePullRequest", map[string]any{
				"title": "PR",
				"targets": []map[string]any{
					{"repositoryName": "repo", "sourceReference": "refs/heads/feat"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			prID := resp["pullRequest"].(map[string]any)["pullRequestId"].(string)

			rec = doRequest(t, h, "UpdatePullRequestStatus", map[string]any{
				"pullRequestId":     prID,
				"pullRequestStatus": tt.status,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateBranch_CommitValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	// Create a real commit.
	commitRec := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"authorName":     "test",
		"email":          "test@example.com",
		"commitMessage":  "init",
	})
	require.Equal(t, http.StatusOK, commitRec.Code)

	var commitResp map[string]any
	require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
	realCommitID := commitResp["commitId"].(string)

	tests := []struct {
		name       string
		commitID   string
		wantStatus int
	}{
		{
			name:       "valid_commit",
			commitID:   realCommitID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_commit",
			commitID:   "00000000-0000-0000-0000-000000000000",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hh := newTestHandler(t)
			doRequest(t, hh, "CreateRepository", map[string]any{"repositoryName": "repo"})

			// Seed the commit into this handler's backend if needed.
			if tt.commitID == realCommitID {
				// Create our own real commit for this subtest handler.
				cr := doRequest(t, hh, "CreateCommit", map[string]any{
					"repositoryName": "repo",
					"branchName":     "main",
					"authorName":     "test",
					"email":          "test@example.com",
					"commitMessage":  "init",
				})
				require.Equal(t, http.StatusOK, cr.Code)
				var cr2 map[string]any
				require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &cr2))
				tt.commitID = cr2["commitId"].(string)
			}

			rec := doRequest(t, hh, "CreateBranch", map[string]any{
				"repositoryName": "repo",
				"branchName":     "feature",
				"commitId":       tt.commitID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetRepository_FullMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName":        "full-meta-repo",
		"repositoryDescription": "desc here",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "full-meta-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta := resp["repositoryMetadata"].(map[string]any)

	assert.NotEmpty(t, meta["repositoryId"])
	assert.Equal(t, "full-meta-repo", meta["repositoryName"])
	assert.NotEmpty(t, meta["Arn"])
	assert.NotEmpty(t, meta["accountId"])
	assert.NotEmpty(t, meta["cloneUrlHttp"])
	assert.NotEmpty(t, meta["cloneUrlSsh"])
	assert.NotNil(t, meta["creationDate"])
	assert.NotNil(t, meta["lastModifiedDate"])
	assert.Equal(t, "desc here", meta["repositoryDescription"])
}

func TestHandler_BatchGetRepositories_PartialFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "exists"})

	rec := doRequest(t, h, "BatchGetRepositories", map[string]any{
		"repositoryNames": []string{"exists", "missing1", "missing2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	repos := resp["repositories"].([]any)
	notFound := resp["repositoriesNotFound"].([]any)

	assert.Len(t, repos, 1)
	assert.Len(t, notFound, 2)
}

func TestHandler_ListPullRequests_RepoNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListPullRequests", map[string]any{"repositoryName": "no-such-repo"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreatePullRequest_RepoMetadataInResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	rec := doRequest(t, h, "CreatePullRequest", map[string]any{
		"title":       "My PR",
		"description": "A test pull request",
		"targets": []map[string]any{
			{
				"repositoryName":       "repo",
				"sourceReference":      "refs/heads/feat",
				"destinationReference": "refs/heads/main",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)

	assert.NotEmpty(t, pr["pullRequestId"])
	assert.Equal(t, "My PR", pr["title"])
	assert.Equal(t, "A test pull request", pr["description"])
	assert.Equal(t, "OPEN", pr["pullRequestStatus"])
	assert.NotEmpty(t, pr["revisionId"])
	assert.NotNil(t, pr["creationDate"])
	assert.NotNil(t, pr["lastActivityDate"])

	targets := pr["pullRequestTargets"].([]any)
	require.Len(t, targets, 1)
	target := targets[0].(map[string]any)
	assert.Equal(t, "repo", target["repositoryName"])
	assert.Equal(t, "refs/heads/feat", target["sourceReference"])
	assert.Equal(t, "refs/heads/main", target["destinationReference"])
}

func TestHandler_MergePullRequest_StatusBecomesmerged(t *testing.T) {
	t.Parallel()

	strategies := []string{
		"MergePullRequestByFastForward",
		"MergePullRequestBySquash",
		"MergePullRequestByThreeWay",
	}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			rec := doRequest(t, h, "CreatePullRequest", map[string]any{
				"title": "PR",
				"targets": []map[string]any{
					{"repositoryName": "repo", "sourceReference": "refs/heads/feat"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			prID := resp["pullRequest"].(map[string]any)["pullRequestId"].(string)

			rec = doRequest(t, h, strategy, map[string]any{"pullRequestId": prID})
			require.Equal(t, http.StatusOK, rec.Code)

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			mergedPR := resp["pullRequest"].(map[string]any)
			assert.Equal(t, "MERGED", mergedPR["pullRequestStatus"])
		})
	}
}

func TestHandler_UpdateRepositoryDescription_Reflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	doRequest(t, h, "UpdateRepositoryDescription", map[string]any{
		"repositoryName":        "repo",
		"repositoryDescription": "new description",
	})

	rec := doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta := resp["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "new description", meta["repositoryDescription"])
}

func TestHandler_CreateRepository_InvalidName_ErrorType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "bad name with spaces"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidRepositoryNameException", errResp["__type"])
}

func TestHandler_ErrorTypes_Comprehensive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name:     "repo_not_found",
			action:   "GetRepository",
			body:     map[string]any{"repositoryName": "no-such"},
			wantCode: http.StatusNotFound,
			wantType: "RepositoryDoesNotExistException",
		},
		{
			name:     "branch_not_found",
			action:   "GetBranch",
			body:     map[string]any{"repositoryName": "no-such", "branchName": "main"},
			wantCode: http.StatusNotFound,
			wantType: "RepositoryDoesNotExistException",
		},
		{
			name:     "pr_not_found",
			action:   "GetPullRequest",
			body:     map[string]any{"pullRequestId": "9999"},
			wantCode: http.StatusNotFound,
			wantType: "PullRequestDoesNotExistException",
		},
		{
			name:     "invalid_repo_name",
			action:   "CreateRepository",
			body:     map[string]any{"repositoryName": "bad/name"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidRepositoryNameException",
		},
		{
			name:     "approval_template_not_found",
			action:   "GetApprovalRuleTemplate",
			body:     map[string]any{"approvalRuleTemplateName": "no-such-tmpl"},
			wantCode: http.StatusNotFound,
			wantType: "ApprovalRuleTemplateDoesNotExistException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}
