package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

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

func TestHandler_DeleteApprovalRuleTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl1",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "DeleteApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteApprovalRuleTemplate is idempotent in real AWS: "If the
	// template has been previously deleted, the only response is a 200 OK"
	// (codecommit@v1.36.4 api_op_DeleteApprovalRuleTemplate.go:39).
	rec = doRequest(t, h, "DeleteApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetApprovalRuleTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-get",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-get",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tmpl := resp["approvalRuleTemplate"].(map[string]any)
	assert.Equal(t, "tmpl-get", tmpl["approvalRuleTemplateName"])

	// not found
	rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "no-tmpl",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListApprovalRuleTemplates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-a",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-b",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "ListApprovalRuleTemplates", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	names := resp["approvalRuleTemplateNames"].([]any)
	assert.Len(t, names, 2)
}

func TestHandler_UpdateApprovalRuleTemplateContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-content",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	newContent := `{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":1}]}`
	rec := doRequest(t, h, "UpdateApprovalRuleTemplateContent", map[string]any{
		"approvalRuleTemplateName": "tmpl-content",
		"newRuleContent":           newContent,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// not found
	rec = doRequest(t, h, "UpdateApprovalRuleTemplateContent", map[string]any{
		"approvalRuleTemplateName": "no-tmpl",
		"newRuleContent":           "{}",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateApprovalRuleTemplateContent_Reflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create template.
	rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	newContent := `{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":2}]}`
	rec = doRequest(t, h, "UpdateApprovalRuleTemplateContent", map[string]any{
		"approvalRuleTemplateName": "tmpl",
		"newRuleContent":           newContent,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tmpl := resp["approvalRuleTemplate"].(map[string]any)
	assert.Equal(t, newContent, tmpl["approvalRuleTemplateContent"])
}

func TestHandler_UpdateApprovalRuleTemplateDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-desc",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "UpdateApprovalRuleTemplateDescription", map[string]any{
		"approvalRuleTemplateName":        "tmpl-desc",
		"approvalRuleTemplateDescription": "new description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateApprovalRuleTemplateName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-old",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "UpdateApprovalRuleTemplateName", map[string]any{
		"oldApprovalRuleTemplateName": "tmpl-old",
		"newApprovalRuleTemplateName": "tmpl-new",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// old name no longer exists
	rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-old",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// new name exists
	rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-new",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
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

func TestHandler_ApprovalRuleTemplate_CRUD_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tmplName    string
		content     string
		description string
		wantCode    int
	}{
		{
			name:     "minimal_content",
			tmplName: "tmpl-min",
			content:  `{"Version":"2018-11-08","Statements":[]}`,
			wantCode: http.StatusOK,
		},
		{
			name:        "with_description",
			tmplName:    "tmpl-desc",
			content:     `{"Version":"2018-11-08","Statements":[]}`,
			description: "Requires 2 approvers",
			wantCode:    http.StatusOK,
		},
		{
			name:     "missing_name",
			tmplName: "",
			content:  `{}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_content",
			tmplName: "tmpl-no-content",
			content:  "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName":        tt.tmplName,
				"approvalRuleTemplateContent":     tt.content,
				"approvalRuleTemplateDescription": tt.description,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tmpl := resp["approvalRuleTemplate"].(map[string]any)
				assert.Equal(t, tt.tmplName, tmpl["approvalRuleTemplateName"])
				assert.Equal(t, tt.content, tmpl["approvalRuleTemplateContent"])
				assert.NotEmpty(t, tmpl["approvalRuleTemplateId"])
				assert.NotContains(t, tmpl, "approvalRuleTemplateArn",
					"types.ApprovalRuleTemplate has no ARN member")
				assert.NotEmpty(t, tmpl["ruleContentSha256"])
				assert.NotNil(t, tmpl["creationDate"])
				assert.NotNil(t, tmpl["lastModifiedDate"])
				if tt.description != "" {
					assert.Equal(t, tt.description, tmpl["approvalRuleTemplateDescription"])
				}

				// Get it back.
				rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName": tt.tmplName,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Delete it.
				rec = doRequest(t, h, "DeleteApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName": tt.tmplName,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify deleted.
				rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName": tt.tmplName,
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}
