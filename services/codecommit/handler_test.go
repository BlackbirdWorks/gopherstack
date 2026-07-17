package codecommit_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func setupRepoAndBranch(t *testing.T, h *codecommit.Handler, repoName string) {
	t.Helper()

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": repoName})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a commit so branch can exist
	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": repoName,
		"branchName":     "main",
		"authorName":     "test",
		"email":          "test@example.com",
		"commitMessage":  "initial",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func setupPR(t *testing.T, h *codecommit.Handler, repoName string) string {
	t.Helper()
	setupRepoAndBranch(t, h, repoName)
	rec := doRequest(t, h, "CreatePullRequest", map[string]any{
		"title": "Test PR",
		"targets": []map[string]any{
			{"repositoryName": repoName, "sourceReference": "refs/heads/feature"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	pr := resp["pullRequest"].(map[string]any)

	return pr["pullRequestId"].(string)
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

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	// Seed data
	_, err := b.CreateRepository("repo-a", "", nil)
	require.NoError(t, err)
	_, err = b.CreateApprovalRuleTemplate("tmpl", "", "{}")
	require.NoError(t, err)
	_, _, err = b.CreateCommit("repo-a", "main", "Alice", "alice@test.com", "init", "", nil, nil)
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

func TestHandler_ErrorTypes(t *testing.T) {
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

// TestHandler_NotFoundErrorTypes_AreResourceSpecific verifies that "not found" errors for
// files, blobs, comments, and PR approval rules surface their own AWS exception type
// (e.g. FileDoesNotExistException) instead of collapsing to RepositoryDoesNotExistException,
// which is what the shared ErrNotFound sentinel produced before these resources got their
// own sentinel errors.
func TestHandler_NotFoundErrorTypes_AreResourceSpecific(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		body        func(t *testing.T, h *codecommit.Handler) map[string]any
		wantErrType string
	}{
		{
			name:   "GetFile_file_not_found",
			action: "GetFile",
			body: func(t *testing.T, h *codecommit.Handler) map[string]any {
				t.Helper()
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "nf-file-repo"})

				return map[string]any{"repositoryName": "nf-file-repo", "filePath": "missing.txt"}
			},
			wantErrType: "FileDoesNotExistException",
		},
		{
			name:   "GetBlob_blob_not_found",
			action: "GetBlob",
			body: func(t *testing.T, h *codecommit.Handler) map[string]any {
				t.Helper()
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "nf-blob-repo"})

				return map[string]any{"repositoryName": "nf-blob-repo", "blobId": "no-such-blob"}
			},
			wantErrType: "BlobIdDoesNotExistException",
		},
		{
			name:   "GetComment_comment_not_found",
			action: "GetComment",
			body: func(t *testing.T, _ *codecommit.Handler) map[string]any {
				t.Helper()

				return map[string]any{"commentId": "no-such-comment"}
			},
			wantErrType: "CommentDoesNotExistException",
		},
		{
			name:   "DeletePullRequestApprovalRule_rule_not_found",
			action: "DeletePullRequestApprovalRule",
			body: func(t *testing.T, h *codecommit.Handler) map[string]any {
				t.Helper()
				prID := setupPR(t, h, "nf-rule-repo")

				return map[string]any{"pullRequestId": prID, "approvalRuleName": "no-such-rule"}
			},
			wantErrType: "ApprovalRuleDoesNotExistException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body(t, h)

			rec := doRequest(t, h, tt.action, body)
			require.Equal(t, http.StatusNotFound, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantErrType, resp["__type"])
		})
	}
}
