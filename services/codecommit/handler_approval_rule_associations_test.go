package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHandler_ListAssociatedApprovalRuleTemplatesForRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "assoc-repo"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "assoc-tmpl",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "assoc-tmpl",
		"repositoryName":           "assoc-repo",
	})

	rec := doRequest(t, h, "ListAssociatedApprovalRuleTemplatesForRepository", map[string]any{
		"repositoryName": "assoc-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	names := resp["approvalRuleTemplateNames"].([]any)
	assert.Contains(t, names, "assoc-tmpl")
}

func TestHandler_ListRepositoriesForApprovalRuleTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo-for-tmpl"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-for-repo",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "tmpl-for-repo",
		"repositoryName":           "repo-for-tmpl",
	})

	rec := doRequest(t, h, "ListRepositoriesForApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-for-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repos := resp["repositoryNames"].([]any)
	assert.Contains(t, repos, "repo-for-tmpl")
}

func TestHandler_ListRepositoriesForApprovalRuleTemplate_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl",
		"approvalRuleTemplateContent": `{}`,
	})

	rec := doRequest(t, h, "ListRepositoriesForApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repos := resp["repositoryNames"].([]any)
	assert.Empty(t, repos)
}

func TestHandler_ListAssociatedApprovalRuleTemplatesForRepository_AfterAssoc(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl1",
		"approvalRuleTemplateContent": `{}`,
	})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl2",
		"approvalRuleTemplateContent": `{}`,
	})

	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "tmpl1",
		"repositoryName":           "repo",
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "tmpl2",
		"repositoryName":           "repo",
	})

	rec := doRequest(t, h, "ListAssociatedApprovalRuleTemplatesForRepository", map[string]any{
		"repositoryName": "repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	names := resp["approvalRuleTemplateNames"].([]any)
	assert.Len(t, names, 2)
}

func TestHandler_DisassociateApprovalRuleTemplateFromRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "disassoc-repo"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "disassoc-tmpl",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "disassoc-tmpl",
		"repositoryName":           "disassoc-repo",
	})

	rec := doRequest(t, h, "DisassociateApprovalRuleTemplateFromRepository", map[string]any{
		"approvalRuleTemplateName": "disassoc-tmpl",
		"repositoryName":           "disassoc-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DisassociateApprovalRuleTemplateFromRepository_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tmplName   string
		repoName   string
		seedAssoc  bool
		wantStatus int
	}{
		{
			name:       "success",
			tmplName:   "tmpl",
			repoName:   "repo",
			seedAssoc:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "template_not_found",
			tmplName:   "no-tmpl",
			repoName:   "repo",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "repo_not_found",
			tmplName:   "tmpl",
			repoName:   "no-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
			doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName":    "tmpl",
				"approvalRuleTemplateContent": `{}`,
			})

			if tt.seedAssoc {
				doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
					"approvalRuleTemplateName": "tmpl",
					"repositoryName":           "repo",
				})
			}

			rec := doRequest(t, h, "DisassociateApprovalRuleTemplateFromRepository", map[string]any{
				"approvalRuleTemplateName": tt.tmplName,
				"repositoryName":           tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchAssociateAndDisassociate_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		repoNames    []string
		wantAssocLen int
		wantErrLen   int
	}{
		{
			name:         "all_exist",
			repoNames:    []string{"repo1", "repo2", "repo3"},
			wantAssocLen: 3,
			wantErrLen:   0,
		},
		{
			name:         "some_missing",
			repoNames:    []string{"repo1", "no-such", "repo2"},
			wantAssocLen: 2,
			wantErrLen:   1,
		},
		{
			name:         "all_missing",
			repoNames:    []string{"no1", "no2"},
			wantAssocLen: 0,
			wantErrLen:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create only repo1 and repo2.
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo1"})
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo2"})
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo3"})
			doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName":    "tmpl",
				"approvalRuleTemplateContent": `{}`,
			})

			rec := doRequest(t, h, "BatchAssociateApprovalRuleTemplateWithRepositories", map[string]any{
				"approvalRuleTemplateName": "tmpl",
				"repositoryNames":          tt.repoNames,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assoc := resp["associatedRepositoryNames"].([]any)
			errs := resp["errors"].([]any)
			assert.Len(t, assoc, tt.wantAssocLen)
			assert.Len(t, errs, tt.wantErrLen)

			// Now disassociate.
			if tt.wantAssocLen > 0 {
				rec = doRequest(t, h, "BatchDisassociateApprovalRuleTemplateFromRepositories", map[string]any{
					"approvalRuleTemplateName": "tmpl",
					"repositoryNames":          tt.repoNames,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				disassoc := resp["disassociatedRepositoryNames"].([]any)
				assert.Len(t, disassoc, tt.wantAssocLen)
			}
		})
	}
}

// TestListAssociatedApprovalRuleTemplatesForRepository_RepoNotFound verifies that
// listing templates for a nonexistent repository returns 404. AWS throws
// RepositoryDoesNotExistException instead of silently returning an empty list.
func TestListAssociatedApprovalRuleTemplatesForRepository_RepoNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		seed       bool
		wantStatus int
	}{
		{
			name:       "repo_exists_no_templates",
			repoName:   "repo",
			seed:       true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "repo_not_found",
			repoName:   "no-such-repo",
			seed:       false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
			}

			rec := doRequest(t, h, "ListAssociatedApprovalRuleTemplatesForRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				names, ok := resp["approvalRuleTemplateNames"].([]any)
				require.True(t, ok, "approvalRuleTemplateNames must be a JSON array")
				assert.Empty(t, names)
			} else {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "RepositoryDoesNotExistException", errResp["__type"])
			}
		})
	}
}

// TestListRepositoriesForApprovalRuleTemplate_TemplateNotFound verifies that listing
// repositories for a nonexistent template returns 404. AWS throws
// ApprovalRuleTemplateDoesNotExistException instead of silently returning empty.
func TestListRepositoriesForApprovalRuleTemplate_TemplateNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		templateName string
		seed         bool
		wantStatus   int
	}{
		{
			name:         "template_exists_no_repos",
			templateName: "tmpl",
			seed:         true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "template_not_found",
			templateName: "no-such-tmpl",
			seed:         false,
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName":    "tmpl",
					"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
				})
			}

			rec := doRequest(t, h, "ListRepositoriesForApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName": tt.templateName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				repos, ok := resp["repositoryNames"].([]any)
				require.True(t, ok, "repositoryNames must be a JSON array")
				assert.Empty(t, repos)
			} else {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "ApprovalRuleTemplateDoesNotExistException", errResp["__type"])
			}
		})
	}
}
