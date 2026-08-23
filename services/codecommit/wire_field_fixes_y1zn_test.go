package codecommit_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateApprovalRuleTemplate_NoArnKey_RealClient covers gopherstack-y1zn.
// approvalRuleTemplateToMap emitted "approvalRuleTemplateArn";
// types.ApprovalRuleTemplate (codecommit@v1.36.4 types/types.go) has no ARN
// member -- templates are identified by approvalRuleTemplateId only. A typed
// client silently ignores the unknown key, so the proof is the raw body.
func TestCreateApprovalRuleTemplate_NoArnKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "y1zn-template",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"approvalRuleTemplateArn"`,
		"types.ApprovalRuleTemplate has no ARN member")
}

// TestTestRepositoryTriggers_SuccessfulExecutionsIsStringArray_RealClient
// covers gopherstack-y1zn. handleTestRepositoryTriggers wrapped each
// successful trigger name in a {"triggerName": ...} object;
// TestRepositoryTriggersOutput.SuccessfulExecutions (codecommit@v1.36.4
// api_op_TestRepositoryTriggers.go) is []string.
func TestTestRepositoryTriggers_SuccessfulExecutionsIsStringArray_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "y1zn-trigger-repo"})
	doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "y1zn-trigger-repo",
		"triggers": []map[string]any{
			{
				"name":           "y1zn-trigger",
				"destinationArn": "arn:aws:sns:us-east-1:123456789012:topic1",
				"events":         []string{"all"},
			},
		},
	})

	rec := doRequest(t, h, "TestRepositoryTriggers", map[string]any{
		"repositoryName": "y1zn-trigger-repo",
		"triggers":       []map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"triggerName"`,
		"SuccessfulExecutions is []string, not an array of {triggerName} objects")
	assert.Contains(t, body, `"successfulExecutions":["y1zn-trigger"]`,
		"SuccessfulExecutions must be a flat string array")
}
