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
	return cloudformation.NewHandler(newBackend())
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

func TestParseTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		wantDesc     string
		wantResource string
		wantErr      bool
	}{
		{
			name:         "json_valid",
			input:        simpleTemplate,
			wantResource: "MyBucket",
		},
		{
			name:         "yaml_valid",
			input:        yamlTemplate,
			wantDesc:     "YAML template",
			wantResource: "MyQueue",
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid_json",
			input:   "{invalid json}",
			wantErr: true,
		},
		{
			name:    "invalid_yaml",
			input:   ":\n  bad: [unclosed",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := cloudformation.ParseTemplate(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, tmpl)

			if tt.wantDesc != "" {
				assert.Equal(t, tt.wantDesc, tmpl.Description)
			}

			if tt.wantResource != "" {
				assert.Contains(t, tmpl.Resources, tt.wantResource)
			}
		})
	}
}

// ---- Parameter resolution ---------------------------------------------------

func TestResolveParameters(t *testing.T) {
	t.Parallel()

	tmpl, err := cloudformation.ParseTemplate(templateWithParams)
	require.NoError(t, err)

	tests := []struct {
		name    string
		wantVal string
		params  []cloudformation.Parameter
	}{
		{
			name:    "defaults_only",
			params:  nil,
			wantVal: "default-bucket",
		},
		{
			name: "override",
			params: []cloudformation.Parameter{
				{ParameterKey: "BucketName", ParameterValue: "my-bucket"},
			},
			wantVal: "my-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolved := cloudformation.ResolveParameters(tmpl, tt.params)
			assert.Equal(t, tt.wantVal, resolved["BucketName"])
		})
	}
}

// ---- Value resolution -------------------------------------------------------

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

// ---- Backend: CreateStack ---------------------------------------------------

func TestBackend_CreateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		stackName   string
		template    string
		wantStatus  string
		wantDesc    string
		params      []cloudformation.Parameter
		tags        []cloudformation.Tag
		wantOutputs int
		wantTags    int
		setupDup    bool
		checkID     bool
	}{
		{
			name:       "success",
			stackName:  "my-stack",
			template:   simpleTemplate,
			wantStatus: "CREATE_COMPLETE",
			checkID:    true,
		},
		{
			name:      "already_exists",
			stackName: "dup-stack",
			template:  simpleTemplate,
			setupDup:  true,
			wantErr:   cloudformation.ErrStackAlreadyExists,
		},
		{
			name:       "invalid_template",
			stackName:  "bad-stack",
			template:   "{bad}",
			wantStatus: "CREATE_FAILED",
		},
		{
			name:      "with_params",
			stackName: "param-stack",
			template:  templateWithParams,
			params: []cloudformation.Parameter{
				{ParameterKey: "BucketName", ParameterValue: "test-bucket"},
			},
			wantStatus:  "CREATE_COMPLETE",
			wantOutputs: 1,
		},
		{
			name:       "with_tags",
			stackName:  "tagged-stack",
			template:   simpleTemplate,
			tags:       []cloudformation.Tag{{Key: "env", Value: "test"}},
			wantStatus: "CREATE_COMPLETE",
			wantTags:   1,
		},
		{
			name:       "yaml_template",
			stackName:  "yaml-stack",
			template:   yamlTemplate,
			wantStatus: "CREATE_COMPLETE",
			wantDesc:   "YAML template",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setupDup {
				_, err := b.CreateStack(t.Context(), tt.stackName, tt.template, nil, nil)
				require.NoError(t, err)
			}

			stack, err := b.CreateStack(t.Context(), tt.stackName, tt.template, tt.params, tt.tags)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStatus != "" {
				assert.Equal(t, tt.wantStatus, stack.StackStatus)
			}

			if tt.checkID {
				assert.Equal(t, tt.stackName, stack.StackName)
				assert.Contains(t, stack.StackID, tt.stackName)
			}

			if tt.wantDesc != "" {
				assert.Equal(t, tt.wantDesc, stack.Description)
			}

			if tt.wantOutputs > 0 {
				require.Len(t, stack.Outputs, tt.wantOutputs)
				assert.Equal(t, "test-bucket", stack.Outputs[0].OutputValue)
			}

			if tt.wantTags > 0 {
				require.Len(t, stack.Tags, tt.wantTags)
				assert.Equal(t, "env", stack.Tags[0].Key)
			}
		})
	}
}

// ---- Backend: DescribeStack -------------------------------------------------

func TestBackend_DescribeStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName string
		wantErr   error
		wantName  string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "desc-stack", simpleTemplate, nil, nil)
				require.NoError(t, err)
			},
			stackName: "desc-stack",
			wantName:  "desc-stack",
		},
		{
			name:      "not_found",
			stackName: "nonexistent",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			stack, err := b.DescribeStack(tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, stack.StackName)
		})
	}
}

// ---- Backend: UpdateStack ---------------------------------------------------

func TestBackend_UpdateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName  string
		wantErr    error
		wantStatus string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "upd-stack", simpleTemplate, nil, nil)
				require.NoError(t, err)
			},
			stackName:  "upd-stack",
			wantStatus: "UPDATE_COMPLETE",
		},
		{
			name:      "not_found",
			stackName: "no-stack",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			updated, err := b.UpdateStack(t.Context(), tt.stackName, simpleTemplate, nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, updated.StackStatus)
		})
	}
}

// ---- Backend: DeleteStack ---------------------------------------------------

func TestBackend_DeleteStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName  string
		wantErr    error
		wantStatus string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "del-stack", simpleTemplate, nil, nil)
				require.NoError(t, err)
			},
			stackName:  "del-stack",
			wantStatus: "DELETE_COMPLETE",
		},
		{
			name:      "not_found",
			stackName: "missing",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteStack(t.Context(), tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			stack, err := b.DescribeStack(tt.stackName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, stack.StackStatus)
		})
	}
}

// ---- Backend: ListStacks ----------------------------------------------------

func TestBackend_ListStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  []string
		wantLen int
	}{
		{
			name:    "no_filter",
			filter:  nil,
			wantLen: 2,
		},
		{
			name:    "filter_create_complete",
			filter:  []string{"CREATE_COMPLETE"},
			wantLen: 2,
		},
		{
			name:    "filter_no_match",
			filter:  []string{"ROLLBACK_COMPLETE"},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(t.Context(), "list-s1", simpleTemplate, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateStack(t.Context(), "list-s2", simpleTemplate, nil, nil)
			require.NoError(t, err)

			result := b.ListStacks(tt.filter)
			assert.Len(t, result, tt.wantLen)
		})
	}
}

// ---- Backend: DescribeStackEvents -------------------------------------------

func TestBackend_DescribeStackEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		name      string
		stackName string
		wantEmpty bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "evt-stack", simpleTemplate, nil, nil)
				require.NoError(t, err)
			},
			stackName: "evt-stack",
		},
		{
			name:      "not_found",
			stackName: "missing",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			events, err := b.DescribeStackEvents(tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, events)
		})
	}
}

// ---- Backend: GetTemplate ---------------------------------------------------

func TestBackend_GetTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName string
		wantErr   error
		wantBody  string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "tmpl-stack", simpleTemplate, nil, nil)
				require.NoError(t, err)
			},
			stackName: "tmpl-stack",
			wantBody:  simpleTemplate,
		},
		{
			name:      "not_found",
			stackName: "no-stack",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			body, err := b.GetTemplate(tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.JSONEq(t, tt.wantBody, body)
		})
	}
}

// ---- Backend: ListAll -------------------------------------------------------

func TestBackend_ListAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		wantLen int
	}{
		{
			name:    "two_stacks",
			count:   2,
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			for i := range tt.count {
				name := "stack-" + string(rune('a'+i))
				_, err := b.CreateStack(t.Context(), name, simpleTemplate, nil, nil)
				require.NoError(t, err)
			}

			all := b.ListAll()
			assert.Len(t, all, tt.wantLen)
		})
	}
}

// ---- Backend: ChangeSet (create, describe, delete) --------------------------

func TestBackend_ChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		name      string
		stackName string
		csName    string
		template  string
		wantCS    bool
	}{
		{
			name: "create_describe_delete_workflow",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "cs-stack", simpleTemplate, nil, nil)
				require.NoError(t, err)
			},
			stackName: "cs-stack",
			csName:    "my-cs",
			template:  simpleTemplate,
			wantCS:    true,
		},
		{
			name: "already_exists",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateChangeSet(t.Context(), "cs-stack", "dup-cs", simpleTemplate, "", nil)
				require.NoError(t, err)
			},
			stackName: "cs-stack",
			csName:    "dup-cs",
			template:  simpleTemplate,
			wantErr:   cloudformation.ErrChangeSetExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			cs, err := b.CreateChangeSet(t.Context(), tt.stackName, tt.csName, tt.template, "desc", nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.csName, cs.ChangeSetName)
			assert.Equal(t, "CREATE_COMPLETE", cs.Status)

			got, err := b.DescribeChangeSet(tt.stackName, tt.csName)
			require.NoError(t, err)
			assert.Equal(t, cs.ChangeSetID, got.ChangeSetID)

			err = b.DeleteChangeSet(tt.stackName, tt.csName)
			require.NoError(t, err)

			_, err = b.DescribeChangeSet(tt.stackName, tt.csName)
			require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound)
		})
	}
}

// ---- Backend: ExecuteChangeSet ----------------------------------------------

func TestBackend_ExecuteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName  string
		csName     string
		wantErr    error
		wantStatus string
	}{
		{
			name: "new_stack",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateChangeSet(t.Context(), "new-cs-stack", "exec-cs", simpleTemplate, "", nil)
				require.NoError(t, err)
			},
			stackName:  "new-cs-stack",
			csName:     "exec-cs",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name: "existing_stack",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "existing-stack", simpleTemplate, nil, nil)
				require.NoError(t, err)
				_, err = b.CreateChangeSet(t.Context(), "existing-stack", "upd-cs", simpleTemplate, "", nil)
				require.NoError(t, err)
			},
			stackName: "existing-stack",
			csName:    "upd-cs",
		},
		{
			name:      "not_found",
			stackName: "s",
			csName:    "missing-cs",
			wantErr:   cloudformation.ErrChangeSetNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.ExecuteChangeSet(t.Context(), tt.stackName, tt.csName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStatus != "" {
				stack, descErr := b.DescribeStack(tt.stackName)
				require.NoError(t, descErr)
				assert.Equal(t, tt.wantStatus, stack.StackStatus)
			}
		})
	}
}

// ---- Backend: ListChangeSets ------------------------------------------------

func TestBackend_ListChangeSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stackName string
		csNames   []string
		wantLen   int
	}{
		{
			name:      "two_changesets",
			stackName: "list-cs-stack",
			csNames:   []string{"cs1", "cs2"},
			wantLen:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			for _, cs := range tt.csNames {
				_, err := b.CreateChangeSet(t.Context(), tt.stackName, cs, simpleTemplate, "", nil)
				require.NoError(t, err)
			}

			summaries, err := b.ListChangeSets(tt.stackName)
			require.NoError(t, err)
			assert.Len(t, summaries, tt.wantLen)
		})
	}
}

// ---- Backend: DeleteChangeSet -----------------------------------------------

func TestBackend_DeleteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		stackName string
		csName    string
	}{
		{
			name:      "not_found",
			stackName: "no-stack",
			csName:    "no-cs",
			wantErr:   cloudformation.ErrChangeSetNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			err := b.DeleteChangeSet(tt.stackName, tt.csName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ---- Handler: metadata ------------------------------------------------------

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_cloudformation", want: "CloudFormation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want int
	}{
		{name: "returns_80", want: 80},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			assert.Equal(t, tt.want, h.MatchPriority())
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantOperations []string
	}{
		{
			name: "contains_expected_operations",
			wantOperations: []string{
				"CreateStack",
				"DescribeStacks",
				"DeleteStack",
				"ListStacks",
				"CreateChangeSet",
				"ExecuteChangeSet",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			ops := h.GetSupportedOperations()

			for _, op := range tt.wantOperations {
				assert.Contains(t, ops, op)
			}
		})
	}
}

// ---- Handler: RouteMatcher --------------------------------------------------

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		contentType string
		body        string
		parseForm   bool
		wantMatch   bool
	}{
		{
			name:        "match",
			method:      http.MethodPost,
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateStack",
			wantMatch:   true,
		},
		{
			name:        "wrong_method",
			method:      http.MethodGet,
			contentType: "application/x-www-form-urlencoded",
			body:        "",
			wantMatch:   false,
		},
		{
			name:        "wrong_content_type",
			method:      http.MethodPost,
			contentType: "application/json",
			body:        "Action=CreateStack",
			wantMatch:   false,
		},
		{
			name:        "unsupported_action",
			method:      http.MethodPost,
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=UnknownAction",
			parseForm:   true,
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()
			matcher := h.RouteMatcher()

			req := httptest.NewRequest(tt.method, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)

			if tt.parseForm {
				require.NoError(t, req.ParseForm())
			}

			got := matcher(e.NewContext(req, httptest.NewRecorder()))
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

// ---- Handler: ExtractOperation / ExtractResource ----------------------------

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantOp string
	}{
		{
			name:   "describe_stacks",
			body:   "Action=DescribeStacks",
			wantOp: "DescribeStacks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			require.NoError(t, req.ParseForm())

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
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
			name:     "stack_name",
			body:     "Action=DescribeStacks&StackName=my-stack",
			wantName: "my-stack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			require.NoError(t, req.ParseForm())

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

// ---- Handler: CreateStack ---------------------------------------------------

func TestHandler_CreateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkResponse func(t *testing.T, body []byte)
		name          string
		form          string
		wantBody      string
		wantCode      int
	}{
		{
			name: "success",
			form: "Action=CreateStack&StackName=test-stack&TemplateBody=" +
				`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{}}}}`,
			wantCode: http.StatusOK,
			wantBody: "CreateStackResponse",
			checkResponse: func(t *testing.T, body []byte) {
				t.Helper()
				var resp struct {
					XMLName xml.Name `xml:"CreateStackResponse"`
					Result  struct {
						StackID string `xml:"StackId"`
					} `xml:"CreateStackResult"`
					RequestID string `xml:"ResponseMetadata>RequestId"`
				}
				require.NoError(t, xml.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp.Result.StackID)
				assert.NotEmpty(t, resp.RequestID)
			},
		},
		{
			name:     "missing_name",
			form:     "Action=CreateStack",
			wantCode: http.StatusBadRequest,
			wantBody: "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}

			if tt.checkResponse != nil {
				tt.checkResponse(t, rec.Body.Bytes())
			}
		})
	}
}

// ---- Handler: DescribeStacks ------------------------------------------------

func TestHandler_DescribeStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *cloudformation.Handler)
		checkResponse func(t *testing.T, body []byte)
		name          string
		form          string
		wantBody      string
		wantCode      int
	}{
		{
			name: "all",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=desc-all&TemplateBody=")
			},
			form:     "Action=DescribeStacks",
			wantCode: http.StatusOK,
			wantBody: "DescribeStacksResponse",
		},
		{
			name: "by_name",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=named-stack&TemplateBody=")
			},
			form:     "Action=DescribeStacks&StackName=named-stack",
			wantCode: http.StatusOK,
			wantBody: "named-stack",
		},
		{
			name:     "not_found",
			form:     "Action=DescribeStacks&StackName=no-such-stack",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "xml_response",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=desc-xml-stack&TemplateBody=")
			},
			form:     "Action=DescribeStacks&StackName=desc-xml-stack",
			wantCode: http.StatusOK,
			checkResponse: func(t *testing.T, body []byte) {
				t.Helper()
				var resp struct {
					XMLName xml.Name `xml:"DescribeStacksResponse"`
					Result  struct {
						Stacks []struct {
							StackName   string `xml:"StackName"`
							StackStatus string `xml:"StackStatus"`
						} `xml:"Stacks>member"`
					} `xml:"DescribeStacksResult"`
				}
				require.NoError(t, xml.Unmarshal(body, &resp))
				require.Len(t, resp.Result.Stacks, 1)
				assert.Equal(t, "desc-xml-stack", resp.Result.Stacks[0].StackName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}

			if tt.checkResponse != nil {
				tt.checkResponse(t, rec.Body.Bytes())
			}
		})
	}
}

// ---- Handler: UpdateStack ---------------------------------------------------

func TestHandler_UpdateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=upd-stack&TemplateBody=")
			},
			form:     "Action=UpdateStack&StackName=upd-stack&TemplateBody=",
			wantCode: http.StatusOK,
			wantBody: "UpdateStackResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=UpdateStack",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: DeleteStack ---------------------------------------------------

func TestHandler_DeleteStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=del-stack&TemplateBody=")
			},
			form:     "Action=DeleteStack&StackName=del-stack",
			wantCode: http.StatusOK,
			wantBody: "DeleteStackResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=DeleteStack",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: ListStacks ----------------------------------------------------

func TestHandler_ListStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "all",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=ls-stack&TemplateBody=")
			},
			form:     "Action=ListStacks",
			wantCode: http.StatusOK,
			wantBody: "ListStacksResponse",
		},
		{
			name: "with_filter",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=filt-stack&TemplateBody=")
			},
			form:     "Action=ListStacks&StackStatusFilter.member.1=CREATE_COMPLETE",
			wantCode: http.StatusOK,
			wantBody: "filt-stack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: DescribeStackEvents -------------------------------------------

func TestHandler_DescribeStackEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=evt-stack&TemplateBody=")
			},
			form:     "Action=DescribeStackEvents&StackName=evt-stack",
			wantCode: http.StatusOK,
			wantBody: "DescribeStackEventsResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=DescribeStackEvents",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: GetTemplate ---------------------------------------------------

func TestHandler_GetTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=tmpl-stack&TemplateBody={}")
			},
			form:     "Action=GetTemplate&StackName=tmpl-stack",
			wantCode: http.StatusOK,
			wantBody: "GetTemplateResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=GetTemplate",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: CreateChangeSet -----------------------------------------------

func TestHandler_CreateChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=cs-stack&TemplateBody=")
			},
			form:     "Action=CreateChangeSet&StackName=cs-stack&ChangeSetName=my-cs&TemplateBody=",
			wantCode: http.StatusOK,
			wantBody: "CreateChangeSetResponse",
		},
		{
			name:     "missing_fields",
			form:     "Action=CreateChangeSet&StackName=cs-stack",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: DescribeChangeSet ---------------------------------------------

func TestHandler_DescribeChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=cs-desc&ChangeSetName=cs1&TemplateBody=")
			},
			form:     "Action=DescribeChangeSet&StackName=cs-desc&ChangeSetName=cs1",
			wantCode: http.StatusOK,
			wantBody: "DescribeChangeSetResponse",
		},
		{
			name:     "not_found",
			form:     "Action=DescribeChangeSet&StackName=no-stack&ChangeSetName=no-cs",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: ExecuteChangeSet ----------------------------------------------

func TestHandler_ExecuteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=exec-cs-stack&ChangeSetName=exec-cs&TemplateBody=")
			},
			form:     "Action=ExecuteChangeSet&StackName=exec-cs-stack&ChangeSetName=exec-cs",
			wantCode: http.StatusOK,
			wantBody: "ExecuteChangeSetResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: DeleteChangeSet -----------------------------------------------

func TestHandler_DeleteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=del-cs-stack&ChangeSetName=del-cs&TemplateBody=")
			},
			form:     "Action=DeleteChangeSet&StackName=del-cs-stack&ChangeSetName=del-cs",
			wantCode: http.StatusOK,
			wantBody: "DeleteChangeSetResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: ListChangeSets ------------------------------------------------

func TestHandler_ListChangeSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=lcs-stack&ChangeSetName=cs1&TemplateBody=")
			},
			form:     "Action=ListChangeSets&StackName=lcs-stack",
			wantCode: http.StatusOK,
			wantBody: "ListChangeSetsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- Handler: UnknownAction -------------------------------------------------

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name:     "invalid_action",
			form:     "Action=UnknownAction",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidAction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// ---- Provider ---------------------------------------------------------------

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_cloudformation", want: "CloudFormation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudformation.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  any
		wantSvc string
	}{
		{
			name:    "nil_config",
			config:  nil,
			wantSvc: "CloudFormation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudformation.Provider{}
			appCtx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}
			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantSvc, svc.Name())
		})
	}
}
