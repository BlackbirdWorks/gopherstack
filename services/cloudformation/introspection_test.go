package cloudformation_test

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- Template: new intrinsics -----------------------------------------------

func TestResolveValue_NewIntrinsics(t *testing.T) {
	t.Parallel()

	params := map[string]string{"Env": "prod", "Region": "us-east-1"}
	ids := map[string]string{"MyBucket": "my-bucket-physical"}

	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "fn_select_int",
			input:    map[string]any{"Fn::Select": []any{1, []any{"a", "b", "c"}}},
			expected: "b",
		},
		{
			name:     "fn_select_float",
			input:    map[string]any{"Fn::Select": []any{float64(2), []any{"a", "b", "c"}}},
			expected: "c",
		},
		{
			name:     "fn_select_out_of_range",
			input:    map[string]any{"Fn::Select": []any{float64(5), []any{"a", "b"}}},
			expected: "",
		},
		{
			name:     "fn_split_produces_joined",
			input:    map[string]any{"Fn::Split": []any{":", "a:b:c"}},
			expected: "a,b,c",
		},
		{
			name:  "fn_importvalue_no_exports",
			input: map[string]any{"Fn::ImportValue": "shared-vpc-id"},
			// When no exports are available, returns the name itself.
			expected: "shared-vpc-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ResolveValue(tt.input, params, ids)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestResolveValue_FindInMap(t *testing.T) {
	t.Parallel()

	// We test via ParseTemplate and CreateStack since FindInMap needs template context.
	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Mappings": {
			"RegionMap": {
				"us-east-1": {"AMI": "ami-12345"},
				"eu-west-1": {"AMI": "ami-67890"}
			}
		},
		"Parameters": {
			"AWS::Region": {"Type": "String", "Default": "us-east-1"}
		},
		"Resources": {
			"MyInstance": {
				"Type": "AWS::EC2::Instance",
				"Properties": {}
			}
		},
		"Outputs": {
			"AMIId": {
				"Value": {"Fn::FindInMap": ["RegionMap", "us-east-1", "AMI"]}
			}
		}
	}`

	b := newBackend()
	stack, err := b.CreateStack(t.Context(), "map-test", template, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, stack)

	var amiOutput string
	for _, out := range stack.Outputs {
		if out.OutputKey == "AMIId" {
			amiOutput = out.OutputValue
		}
	}

	assert.Equal(t, "ami-12345", amiOutput)
}

func TestResolveValue_ConditionsAndFnIf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		template    string
		wantOutput  string
		wantOutputK string
	}{
		{
			name: "condition_true_selects_first_branch",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Parameters": {
					"Env": {"Type": "String", "Default": "prod"}
				},
				"Conditions": {
					"IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
				},
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"BucketSize": {
						"Value": {"Fn::If": ["IsProd", "large", "small"]}
					}
				}
			}`,
			wantOutputK: "BucketSize",
			wantOutput:  "large",
		},
		{
			name: "condition_false_selects_second_branch",
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Parameters": {
					"Env": {"Type": "String", "Default": "dev"}
				},
				"Conditions": {
					"IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
				},
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"BucketSize": {
						"Value": {"Fn::If": ["IsProd", "large", "small"]}
					}
				}
			}`,
			wantOutputK: "BucketSize",
			wantOutput:  "small",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stack, err := b.CreateStack(t.Context(), tt.name, tt.template, nil, nil)
			require.NoError(t, err)
			require.NotNil(t, stack)

			var got string
			for _, out := range stack.Outputs {
				if out.OutputKey == tt.wantOutputK {
					got = out.OutputValue
				}
			}

			assert.Equal(t, tt.wantOutput, got)
		})
	}
}

// ---- Backend: DescribeStackResource -----------------------------------------

func TestBackend_DescribeStackResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudformation.InMemoryBackend, string)
		name       string
		stackInput string
		logicalID  string
		wantErr    error
		wantType   string
	}{
		{
			name:       "resource_found",
			stackInput: simpleTemplate,
			logicalID:  "MyBucket",
			wantType:   "AWS::S3::Bucket",
		},
		{
			name:       "resource_not_found",
			stackInput: simpleTemplate,
			logicalID:  "NonExistent",
			wantErr:    cloudformation.ErrResourceNotFound,
		},
		{
			name:      "stack_not_found",
			logicalID: "MyBucket",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stackName := "test-stack-" + tt.name

			if tt.stackInput != "" {
				_, err := b.CreateStack(t.Context(), stackName, tt.stackInput, nil, nil)
				require.NoError(t, err)
			}

			lookupName := stackName
			if errors.Is(tt.wantErr, cloudformation.ErrStackNotFound) {
				lookupName = "no-such-stack"
			}

			res, err := b.DescribeStackResource(lookupName, tt.logicalID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tt.logicalID, res.LogicalID)
			assert.Equal(t, tt.wantType, res.Type)
			assert.NotEmpty(t, res.PhysicalID)
			assert.Equal(t, "CREATE_COMPLETE", res.Status)
			assert.False(t, res.Timestamp.IsZero())
		})
	}
}

// ---- Backend: ListStackResources --------------------------------------------

func TestBackend_ListStackResources(t *testing.T) {
	t.Parallel()

	multiTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"BucketA": {"Type": "AWS::S3::Bucket", "Properties": {}},
			"BucketB": {"Type": "AWS::S3::Bucket", "Properties": {}}
		}
	}`

	tests := []struct {
		wantErr       error
		name          string
		template      string
		wantLogicalID string
		wantCount     int
	}{
		{
			name:      "lists_all_resources",
			template:  multiTemplate,
			wantCount: 2,
		},
		{
			name:     "stack_not_found",
			template: "",
			wantErr:  cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stackName := "res-list-" + tt.name

			if tt.template != "" {
				_, err := b.CreateStack(t.Context(), stackName, tt.template, nil, nil)
				require.NoError(t, err)
			}

			lookupName := stackName
			if errors.Is(tt.wantErr, cloudformation.ErrStackNotFound) {
				lookupName = "no-such-stack"
			}

			p, err := b.ListStackResources(lookupName, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)

			for _, s := range p.Data {
				assert.NotEmpty(t, s.LogicalResourceID)
				assert.NotEmpty(t, s.ResourceType)
				assert.NotEmpty(t, s.ResourceStatus)
				assert.False(t, s.Timestamp.IsZero())
			}
		})
	}
}

// ---- Backend: DescribeStackResources ----------------------------------------

func TestBackend_DescribeStackResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		template  string
		wantCount int
	}{
		{
			name:      "returns_resources",
			template:  simpleTemplate,
			wantCount: 1,
		},
		{
			name:    "stack_not_found",
			wantErr: cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stackName := "desc-res-" + tt.name

			if tt.template != "" {
				_, err := b.CreateStack(t.Context(), stackName, tt.template, nil, nil)
				require.NoError(t, err)
			}

			lookupName := stackName
			if errors.Is(tt.wantErr, cloudformation.ErrStackNotFound) {
				lookupName = "no-such-stack"
			}

			resources, err := b.DescribeStackResources(lookupName)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, resources, tt.wantCount)
		})
	}
}

// ---- Backend: Exports (ListExports, ListImports) ----------------------------

const exportTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {}}
	},
	"Outputs": {
		"BucketName": {
			"Value": {"Ref": "MyBucket"},
			"Export": {"Name": "shared-bucket"}
		}
	}
}`

const importTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyTopic": {"Type": "AWS::SNS::Topic", "Properties": {}}
	},
	"Outputs": {
		"ImportedBucket": {
			"Value": {"Fn::ImportValue": "shared-bucket"}
		}
	}
}`

func TestBackend_ListExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stacks    []string
		wantNames []string
	}{
		{
			name:      "no_exports",
			stacks:    nil,
			wantNames: nil,
		},
		{
			name:      "single_export",
			stacks:    []string{exportTemplate},
			wantNames: []string{"shared-bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			for i, tmpl := range tt.stacks {
				stackName := fmt.Sprintf("%s-stack-%d", tt.name, i)
				_, err := b.CreateStack(t.Context(), stackName, tmpl, nil, nil)
				require.NoError(t, err)
			}

			p, err := b.ListExports("")
			require.NoError(t, err)

			names := make([]string, 0, len(p.Data))
			for _, exp := range p.Data {
				names = append(names, exp.Name)
			}

			if len(tt.wantNames) == 0 {
				assert.Empty(t, names)
			} else {
				assert.Equal(t, tt.wantNames, names)
			}
		})
	}
}

func TestBackend_ExportsRemovedOnDelete(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "export-stack", exportTemplate, nil, nil)
	require.NoError(t, err)

	p, err := b.ListExports("")
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "shared-bucket", p.Data[0].Name)

	err = b.DeleteStack(t.Context(), "export-stack")
	require.NoError(t, err)

	p2, err := b.ListExports("")
	require.NoError(t, err)
	assert.Empty(t, p2.Data)
}

func TestBackend_ListImports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		exportName string
		setup      func(*cloudformation.InMemoryBackend)
		wantErr    error
		wantStacks []string
	}{
		{
			name:       "export_not_found",
			exportName: "does-not-exist",
			setup:      func(_ *cloudformation.InMemoryBackend) {},
			wantErr:    cloudformation.ErrExportNotFound,
		},
		{
			name:       "no_importers",
			exportName: "shared-bucket",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, err := b.CreateStack(t.Context(), "exporter", exportTemplate, nil, nil)
				require.NoError(t, err)
			},
			wantStacks: nil,
		},
		{
			name:       "one_importer",
			exportName: "shared-bucket",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, err := b.CreateStack(t.Context(), "exporter", exportTemplate, nil, nil)
				require.NoError(t, err)
				_, err = b.CreateStack(t.Context(), "importer", importTemplate, nil, nil)
				require.NoError(t, err)
			},
			wantStacks: []string{"importer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			p, err := b.ListImports(tt.exportName, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStacks == nil {
				assert.Empty(t, p.Data)
			} else {
				assert.Equal(t, tt.wantStacks, p.Data)
			}
		})
	}
}

func TestBackend_ExportOutput_IncludesExportName(t *testing.T) {
	t.Parallel()

	b := newBackend()
	stack, err := b.CreateStack(t.Context(), "export-stack", exportTemplate, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, stack)

	require.Len(t, stack.Outputs, 1)
	assert.Equal(t, "BucketName", stack.Outputs[0].OutputKey)
	assert.Equal(t, "shared-bucket", stack.Outputs[0].ExportName)
}

// ---- Handler: DescribeStackResource -----------------------------------------

func TestHandler_DescribeStackResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*cloudformation.Handler) string
		name      string
		action    string
		logicalID string
		wantElem  string
		wantCode  int
	}{
		{
			name:      "success",
			action:    "DescribeStackResource",
			logicalID: "MyBucket",
			wantCode:  200,
			wantElem:  "DescribeStackResourceResult",
			setup: func(h *cloudformation.Handler) string {
				body := url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"handler-res-stack"},
					"TemplateBody": {simpleTemplate},
				}.Encode()
				postForm(t, h, body)

				return "handler-res-stack"
			},
		},
		{
			name:      "missing_params",
			action:    "DescribeStackResource",
			logicalID: "",
			wantCode:  400,
			wantElem:  "ErrorResponse",
			setup:     func(_ *cloudformation.Handler) string { return "irrelevant" },
		},
		{
			name:      "resource_not_found",
			action:    "DescribeStackResource",
			logicalID: "NonExistent",
			wantCode:  400,
			wantElem:  "ErrorResponse",
			setup: func(h *cloudformation.Handler) string {
				body := url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"handler-res-stack-miss"},
					"TemplateBody": {simpleTemplate},
				}.Encode()
				postForm(t, h, body)

				return "handler-res-stack-miss"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			stackName := tt.setup(h)

			var formBody string
			if tt.logicalID == "" {
				formBody = url.Values{
					"Action":    {tt.action},
					"StackName": {stackName},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":            {tt.action},
					"StackName":         {stackName},
					"LogicalResourceId": {tt.logicalID},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}

// ---- Handler: ListStackResources -------------------------------------------

func TestHandler_ListStackResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantElem string
		wantCode int
		noStack  bool
	}{
		{
			name:     "success",
			wantCode: 200,
			wantElem: "ListStackResourcesResult",
		},
		{
			name:     "missing_stack_name",
			wantCode: 400,
			wantElem: "ErrorResponse",
			noStack:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			stackName := "lsr-stack-" + tt.name

			if !tt.noStack {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {stackName},
					"TemplateBody": {simpleTemplate},
				}.Encode())
			}

			var formBody string
			if tt.noStack {
				formBody = url.Values{
					"Action": {"ListStackResources"},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":    {"ListStackResources"},
					"StackName": {stackName},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}

// ---- Handler: DescribeStackResources ----------------------------------------

func TestHandler_DescribeStackResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantElem string
		wantCode int
		noStack  bool
	}{
		{
			name:     "success",
			wantCode: 200,
			wantElem: "DescribeStackResourcesResult",
		},
		{
			name:     "missing_stack_name",
			wantCode: 400,
			wantElem: "ErrorResponse",
			noStack:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			stackName := "dsr-stack-" + tt.name

			if !tt.noStack {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {stackName},
					"TemplateBody": {simpleTemplate},
				}.Encode())
			}

			var formBody string
			if tt.noStack {
				formBody = url.Values{
					"Action": {"DescribeStackResources"},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":    {"DescribeStackResources"},
					"StackName": {stackName},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}

// ---- Handler: ListExports ---------------------------------------------------

func TestHandler_ListExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudformation.Handler)
		name       string
		wantElem   string
		wantExport string
		wantCode   int
	}{
		{
			name:     "no_exports",
			setup:    func(_ *cloudformation.Handler) {},
			wantCode: 200,
			wantElem: "ListExportsResult",
		},
		{
			name: "with_export",
			setup: func(h *cloudformation.Handler) {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"export-stack"},
					"TemplateBody": {exportTemplate},
				}.Encode())
			},
			wantCode:   200,
			wantElem:   "ListExportsResult",
			wantExport: "shared-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			tt.setup(h)

			rec := postForm(t, h, url.Values{
				"Action": {"ListExports"},
			}.Encode())

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)

			if tt.wantExport != "" {
				assert.Contains(t, rec.Body.String(), tt.wantExport)
			}
		})
	}
}

// ---- Handler: ListImports ---------------------------------------------------

func TestHandler_ListImports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudformation.Handler)
		name       string
		exportName string
		wantElem   string
		wantCode   int
	}{
		{
			name:       "missing_export_name",
			setup:      func(_ *cloudformation.Handler) {},
			exportName: "",
			wantCode:   400,
			wantElem:   "ErrorResponse",
		},
		{
			name: "export_not_found",
			setup: func(_ *cloudformation.Handler) {
				// no stacks created
			},
			exportName: "nonexistent-export",
			wantCode:   400,
			wantElem:   "ErrorResponse",
		},
		{
			name: "found_importers",
			setup: func(h *cloudformation.Handler) {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"exporter"},
					"TemplateBody": {exportTemplate},
				}.Encode())
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"importer"},
					"TemplateBody": {importTemplate},
				}.Encode())
			},
			exportName: "shared-bucket",
			wantCode:   200,
			wantElem:   "ListImportsResult",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			tt.setup(h)

			var formBody string
			if tt.exportName == "" {
				formBody = url.Values{
					"Action": {"ListImports"},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":     {"ListImports"},
					"ExportName": {tt.exportName},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}

// ---- Handler: DescribeStacks shows ExportName in outputs -------------------

func TestHandler_DescribeStacks_ExportName(t *testing.T) {
	t.Parallel()

	h := newHandler()

	postForm(t, h, url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"export-ds-stack"},
		"TemplateBody": {exportTemplate},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":    {"DescribeStacks"},
		"StackName": {"export-ds-stack"},
	}.Encode())

	require.Equal(t, 200, rec.Code)

	type outputXML struct {
		OutputKey   string `xml:"OutputKey"`
		OutputValue string `xml:"OutputValue"`
		ExportName  string `xml:"ExportName"`
	}
	type stackXML struct {
		Outputs []outputXML `xml:"Outputs>member"`
	}
	type result struct {
		Stacks []stackXML `xml:"Stacks>member"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeStacksResponse"`
		Result  result   `xml:"DescribeStacksResult"`
	}

	var decoded resp
	require.NoError(t, xml.NewDecoder(strings.NewReader(rec.Body.String())).Decode(&decoded))
	require.Len(t, decoded.Result.Stacks, 1)
	require.Len(t, decoded.Result.Stacks[0].Outputs, 1)
	assert.Equal(t, "shared-bucket", decoded.Result.Stacks[0].Outputs[0].ExportName)
}
