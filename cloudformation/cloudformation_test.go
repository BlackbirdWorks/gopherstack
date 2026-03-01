package cloudformation_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudformation"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ---- helpers ----------------------------------------------------------------

func newBackend() *cloudformation.InMemoryBackend {
	return cloudformation.NewInMemoryBackendWithConfig(
		"000000000000",
		"us-east-1",
		cloudformation.NewResourceCreator(nil),
	)
}

func newHandler() *cloudformation.Handler {
	return cloudformation.NewHandler(newBackend(), slog.Default())
}

func postForm(t *testing.T, h *cloudformation.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, req.ParseForm())
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

const simpleTemplate = `{"AWSTemplateFormatVersion":"2010-09-09",` +
	`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`

const templateWithParams = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "BucketName": {
      "Type": "String",
      "Default": "default-bucket"
    }
  },
  "Resources": {
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {
        "BucketName": {"Ref": "BucketName"}
      }
    }
  },
  "Outputs": {
    "BucketOut": {
      "Value": {"Ref": "BucketName"},
      "Description": "The bucket name"
    }
  }
}`

const yamlTemplate = `
AWSTemplateFormatVersion: "2010-09-09"
Description: "YAML template"
Resources:
  MyQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: my-yaml-queue
`

// ---- Template parsing -------------------------------------------------------

func TestParseTemplate_JSON(t *testing.T) {
	t.Parallel()
	tmpl, err := cloudformation.ParseTemplate(simpleTemplate)
	require.NoError(t, err)
	assert.NotNil(t, tmpl)
	assert.Contains(t, tmpl.Resources, "MyBucket")
}

func TestParseTemplate_YAML(t *testing.T) {
	t.Parallel()
	tmpl, err := cloudformation.ParseTemplate(yamlTemplate)
	require.NoError(t, err)
	assert.Equal(t, "YAML template", tmpl.Description)
	assert.Contains(t, tmpl.Resources, "MyQueue")
}

func TestParseTemplate_Empty(t *testing.T) {
	t.Parallel()
	_, err := cloudformation.ParseTemplate("")
	require.Error(t, err)
}

func TestParseTemplate_InvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := cloudformation.ParseTemplate("{invalid json}")
	require.Error(t, err)
}

func TestParseTemplate_InvalidYAML(t *testing.T) {
	t.Parallel()
	_, err := cloudformation.ParseTemplate(":\n  bad: [unclosed")
	require.Error(t, err)
}

func TestResolveParameters(t *testing.T) {
	t.Parallel()
	tmpl, err := cloudformation.ParseTemplate(templateWithParams)
	require.NoError(t, err)

	// defaults only
	resolved := cloudformation.ResolveParameters(tmpl, nil)
	assert.Equal(t, "default-bucket", resolved["BucketName"])

	// override
	resolved2 := cloudformation.ResolveParameters(tmpl, []cloudformation.Parameter{
		{ParameterKey: "BucketName", ParameterValue: "my-bucket"},
	})
	assert.Equal(t, "my-bucket", resolved2["BucketName"])
}

func TestResolveValue(t *testing.T) {
	t.Parallel()
	params := map[string]string{"Env": "prod"}
	ids := map[string]string{"MyQueue": "https://queue-url"}

	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"string", "hello", "hello"},
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},
		{"int", 42, "42"},
		{"float", 3.14, "3.14"},
		{"nil", nil, ""},
		{"ref_param", map[string]any{"Ref": "Env"}, "prod"},
		{"ref_physical", map[string]any{"Ref": "MyQueue"}, "https://queue-url"},
		{"ref_missing", map[string]any{"Ref": "Unknown"}, "Unknown"},
		{"fn_sub", map[string]any{"Fn::Sub": "env-${Env}"}, "env-prod"},
		{"fn_join", map[string]any{"Fn::Join": []any{"-", []any{"a", "b", "c"}}}, "a-b-c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := cloudformation.ResolveValue(tt.input, params, ids)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// ---- Backend ----------------------------------------------------------------

func TestBackend_CreateStack(t *testing.T) {
	t.Parallel()
	b := newBackend()
	stack, err := b.CreateStack(t.Context(), "my-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "my-stack", stack.StackName)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
	assert.Contains(t, stack.StackID, "my-stack")
}

func TestBackend_CreateStack_AlreadyExists(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "dup-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateStack(t.Context(), "dup-stack", simpleTemplate, nil, nil)
	require.ErrorIs(t, err, cloudformation.ErrStackAlreadyExists)
}

func TestBackend_CreateStack_InvalidTemplate(t *testing.T) {
	t.Parallel()
	b := newBackend()
	stack, err := b.CreateStack(t.Context(), "bad-stack", "{bad}", nil, nil)
	require.NoError(t, err) // does not return error; sets status
	assert.Equal(t, "CREATE_FAILED", stack.StackStatus)
}

func TestBackend_CreateStack_WithParams(t *testing.T) {
	t.Parallel()
	b := newBackend()
	params := []cloudformation.Parameter{{ParameterKey: "BucketName", ParameterValue: "test-bucket"}}
	stack, err := b.CreateStack(t.Context(), "param-stack", templateWithParams, params, nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
	require.Len(t, stack.Outputs, 1)
	assert.Equal(t, "test-bucket", stack.Outputs[0].OutputValue)
}

func TestBackend_CreateStack_WithTags(t *testing.T) {
	t.Parallel()
	b := newBackend()
	tags := []cloudformation.Tag{{Key: "env", Value: "test"}}
	stack, err := b.CreateStack(t.Context(), "tagged-stack", simpleTemplate, nil, tags)
	require.NoError(t, err)
	require.Len(t, stack.Tags, 1)
	assert.Equal(t, "env", stack.Tags[0].Key)
}

func TestBackend_CreateStack_YAML(t *testing.T) {
	t.Parallel()
	b := newBackend()
	stack, err := b.CreateStack(t.Context(), "yaml-stack", yamlTemplate, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "YAML template", stack.Description)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

func TestBackend_DescribeStack(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "desc-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	stack, err := b.DescribeStack("desc-stack")
	require.NoError(t, err)
	assert.Equal(t, "desc-stack", stack.StackName)
}

func TestBackend_DescribeStack_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.DescribeStack("nonexistent")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

func TestBackend_UpdateStack(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "upd-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdateStack(t.Context(), "upd-stack", simpleTemplate, nil)
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.StackStatus)
}

func TestBackend_UpdateStack_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.UpdateStack(t.Context(), "no-stack", simpleTemplate, nil)
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

func TestBackend_DeleteStack(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "del-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	err = b.DeleteStack(t.Context(), "del-stack")
	require.NoError(t, err)

	stack, err := b.DescribeStack("del-stack")
	require.NoError(t, err)
	assert.Equal(t, "DELETE_COMPLETE", stack.StackStatus)
}

func TestBackend_DeleteStack_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	err := b.DeleteStack(t.Context(), "missing")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

func TestBackend_ListStacks(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "list-s1", simpleTemplate, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateStack(t.Context(), "list-s2", simpleTemplate, nil, nil)
	require.NoError(t, err)

	all := b.ListStacks(nil)
	assert.Len(t, all, 2)

	filtered := b.ListStacks([]string{"CREATE_COMPLETE"})
	assert.Len(t, filtered, 2)

	noMatch := b.ListStacks([]string{"ROLLBACK_COMPLETE"})
	assert.Empty(t, noMatch)
}

func TestBackend_DescribeStackEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "evt-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	events, err := b.DescribeStackEvents("evt-stack")
	require.NoError(t, err)
	assert.NotEmpty(t, events)
}

func TestBackend_DescribeStackEvents_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.DescribeStackEvents("missing")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

func TestBackend_GetTemplate(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "tmpl-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	body, err := b.GetTemplate("tmpl-stack")
	require.NoError(t, err)
	assert.JSONEq(t, simpleTemplate, body)
}

func TestBackend_GetTemplate_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.GetTemplate("no-stack")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

func TestBackend_ListAll(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "a", simpleTemplate, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateStack(t.Context(), "b", simpleTemplate, nil, nil)
	require.NoError(t, err)

	all := b.ListAll()
	assert.Len(t, all, 2)
}

func TestBackend_ChangeSet_CreateDescribeDelete(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "cs-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	cs, err := b.CreateChangeSet(t.Context(), "cs-stack", "my-cs", simpleTemplate, "desc", nil)
	require.NoError(t, err)
	assert.Equal(t, "my-cs", cs.ChangeSetName)
	assert.Equal(t, "CREATE_COMPLETE", cs.Status)

	got, err := b.DescribeChangeSet("cs-stack", "my-cs")
	require.NoError(t, err)
	assert.Equal(t, cs.ChangeSetID, got.ChangeSetID)

	err = b.DeleteChangeSet("cs-stack", "my-cs")
	require.NoError(t, err)

	_, err = b.DescribeChangeSet("cs-stack", "my-cs")
	require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound)
}

func TestBackend_ChangeSet_AlreadyExists(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateChangeSet(t.Context(), "cs-stack", "dup-cs", simpleTemplate, "", nil)
	require.NoError(t, err)
	_, err = b.CreateChangeSet(t.Context(), "cs-stack", "dup-cs", simpleTemplate, "", nil)
	require.ErrorIs(t, err, cloudformation.ErrChangeSetExists)
}

func TestBackend_ExecuteChangeSet_NewStack(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateChangeSet(t.Context(), "new-cs-stack", "exec-cs", simpleTemplate, "", nil)
	require.NoError(t, err)

	err = b.ExecuteChangeSet(t.Context(), "new-cs-stack", "exec-cs")
	require.NoError(t, err)

	stack, err := b.DescribeStack("new-cs-stack")
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

func TestBackend_ExecuteChangeSet_ExistingStack(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateStack(t.Context(), "existing-stack", simpleTemplate, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChangeSet(t.Context(), "existing-stack", "upd-cs", simpleTemplate, "", nil)
	require.NoError(t, err)

	err = b.ExecuteChangeSet(t.Context(), "existing-stack", "upd-cs")
	require.NoError(t, err)
}

func TestBackend_ExecuteChangeSet_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	err := b.ExecuteChangeSet(t.Context(), "s", "missing-cs")
	require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound)
}

func TestBackend_ListChangeSets(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateChangeSet(t.Context(), "list-cs-stack", "cs1", simpleTemplate, "", nil)
	require.NoError(t, err)
	_, err = b.CreateChangeSet(t.Context(), "list-cs-stack", "cs2", simpleTemplate, "", nil)
	require.NoError(t, err)

	summaries, err := b.ListChangeSets("list-cs-stack")
	require.NoError(t, err)
	assert.Len(t, summaries, 2)
}

func TestBackend_DeleteChangeSet_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	err := b.DeleteChangeSet("no-stack", "no-cs")
	require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound)
}

// ---- Handler ----------------------------------------------------------------

func TestHandler_Name(t *testing.T) {
	t.Parallel()
	h := newHandler()
	assert.Equal(t, "CloudFormation", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()
	h := newHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()
	h := newHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateStack")
	assert.Contains(t, ops, "DescribeStacks")
	assert.Contains(t, ops, "DeleteStack")
	assert.Contains(t, ops, "ListStacks")
	assert.Contains(t, ops, "CreateChangeSet")
	assert.Contains(t, ops, "ExecuteChangeSet")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()
	h := newHandler()
	e := echo.New()
	matcher := h.RouteMatcher()

	// Match: correct method + content-type + action
	body := strings.NewReader("Action=CreateStack")
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.True(t, matcher(e.NewContext(req, httptest.NewRecorder())))

	// No match: wrong method
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assert.False(t, matcher(e.NewContext(req2, httptest.NewRecorder())))

	// No match: wrong content-type
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateStack"))
	req3.Header.Set("Content-Type", "application/json")
	assert.False(t, matcher(e.NewContext(req3, httptest.NewRecorder())))

	// No match: unsupported action
	req4 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=UnknownAction"))
	req4.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, req4.ParseForm())
	assert.False(t, matcher(e.NewContext(req4, httptest.NewRecorder())))
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()
	h := newHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeStacks"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, req.ParseForm())
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "DescribeStacks", h.ExtractOperation(c))
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()
	h := newHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeStacks&StackName=my-stack"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, req.ParseForm())
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-stack", h.ExtractResource(c))
}

func TestHandler_CreateStack(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=CreateStack&StackName=test-stack&TemplateBody="+
		`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{}}}}`)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateStackResponse")
	assert.Contains(t, rec.Body.String(), "StackId")
}

func TestHandler_CreateStack_MissingName(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=CreateStack")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationError")
}

func TestHandler_DescribeStacks_All(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=desc-all&TemplateBody=")
	rec := postForm(t, h, "Action=DescribeStacks")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeStacksResponse")
}

func TestHandler_DescribeStacks_ByName(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=named-stack&TemplateBody=")
	rec := postForm(t, h, "Action=DescribeStacks&StackName=named-stack")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "named-stack")
}

func TestHandler_DescribeStacks_NotFound(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=DescribeStacks&StackName=no-such-stack")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateStack(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=upd-stack&TemplateBody=")
	rec := postForm(t, h, "Action=UpdateStack&StackName=upd-stack&TemplateBody=")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdateStackResponse")
}

func TestHandler_UpdateStack_MissingName(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=UpdateStack")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteStack(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=del-stack&TemplateBody=")
	rec := postForm(t, h, "Action=DeleteStack&StackName=del-stack")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteStackResponse")
}

func TestHandler_DeleteStack_MissingName(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=DeleteStack")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListStacks(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=ls-stack&TemplateBody=")
	rec := postForm(t, h, "Action=ListStacks")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListStacksResponse")
}

func TestHandler_ListStacks_WithFilter(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=filt-stack&TemplateBody=")
	rec := postForm(t, h, "Action=ListStacks&StackStatusFilter.member.1=CREATE_COMPLETE")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "filt-stack")
}

func TestHandler_DescribeStackEvents(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=evt-stack&TemplateBody=")
	rec := postForm(t, h, "Action=DescribeStackEvents&StackName=evt-stack")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeStackEventsResponse")
}

func TestHandler_DescribeStackEvents_MissingName(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=DescribeStackEvents")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetTemplate(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=tmpl-stack&TemplateBody={}")
	rec := postForm(t, h, "Action=GetTemplate&StackName=tmpl-stack")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetTemplateResponse")
}

func TestHandler_GetTemplate_MissingName(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=GetTemplate")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateChangeSet(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=cs-stack&TemplateBody=")
	rec := postForm(t, h, "Action=CreateChangeSet&StackName=cs-stack&ChangeSetName=my-cs&TemplateBody=")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateChangeSetResponse")
}

func TestHandler_CreateChangeSet_MissingFields(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=CreateChangeSet&StackName=cs-stack")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeChangeSet(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateChangeSet&StackName=cs-desc&ChangeSetName=cs1&TemplateBody=")
	rec := postForm(t, h, "Action=DescribeChangeSet&StackName=cs-desc&ChangeSetName=cs1")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeChangeSetResponse")
}

func TestHandler_DescribeChangeSet_NotFound(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=DescribeChangeSet&StackName=no-stack&ChangeSetName=no-cs")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ExecuteChangeSet(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateChangeSet&StackName=exec-cs-stack&ChangeSetName=exec-cs&TemplateBody=")
	rec := postForm(t, h, "Action=ExecuteChangeSet&StackName=exec-cs-stack&ChangeSetName=exec-cs")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ExecuteChangeSetResponse")
}

func TestHandler_DeleteChangeSet(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateChangeSet&StackName=del-cs-stack&ChangeSetName=del-cs&TemplateBody=")
	rec := postForm(t, h, "Action=DeleteChangeSet&StackName=del-cs-stack&ChangeSetName=del-cs")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteChangeSetResponse")
}

func TestHandler_ListChangeSets(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateChangeSet&StackName=lcs-stack&ChangeSetName=cs1&TemplateBody=")
	rec := postForm(t, h, "Action=ListChangeSets&StackName=lcs-stack")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListChangeSetsResponse")
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=UnknownAction")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidAction")
}

// ---- Provider ---------------------------------------------------------------

func TestProvider_Name(t *testing.T) {
	t.Parallel()
	p := &cloudformation.Provider{}
	assert.Equal(t, "CloudFormation", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()
	p := &cloudformation.Provider{}
	appCtx := &service.AppContext{
		Logger: slog.Default(),
		Config: nil,
	}
	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "CloudFormation", svc.Name())
}

// ---- XML response verification ----------------------------------------------

func TestHandler_CreateStack_XMLResponse(t *testing.T) {
	t.Parallel()
	h := newHandler()
	rec := postForm(t, h, "Action=CreateStack&StackName=xml-stack&TemplateBody=")
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"CreateStackResponse"`
		Result  struct {
			StackID string `xml:"StackId"`
		} `xml:"CreateStackResult"`
		RequestID string `xml:"ResponseMetadata>RequestId"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.StackID)
	assert.NotEmpty(t, resp.RequestID)
}

func TestHandler_DescribeStacks_XMLResponse(t *testing.T) {
	t.Parallel()
	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=desc-xml-stack&TemplateBody=")
	rec := postForm(t, h, "Action=DescribeStacks&StackName=desc-xml-stack")
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeStacksResponse"`
		Result  struct {
			Stacks []struct {
				StackName   string `xml:"StackName"`
				StackStatus string `xml:"StackStatus"`
			} `xml:"Stacks>member"`
		} `xml:"DescribeStacksResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Stacks, 1)
	assert.Equal(t, "desc-xml-stack", resp.Result.Stacks[0].StackName)
}
