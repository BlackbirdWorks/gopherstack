package cloudformation_test

import (
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
			name:  "fn_split_produces_joined",
			input: map[string]any{"Fn::Split": []any{":", "a:b:c"}},
			// Fn::Split returns a null-byte-delimited list (internal encoding).
			expected: "a\x00b\x00c",
		},
		{
			// Fn::Select picking from the null-byte-encoded output of a previous Fn::Split
			name: "fn_select_from_split_encoded",
			input: map[string]any{
				"Fn::Select": []any{float64(1), "a\x00b\x00c"},
			},
			expected: "b",
		},
		{
			// The canonical pattern: Fn::Select wrapping Fn::Split inline
			name: "fn_select_inline_split",
			input: map[string]any{
				"Fn::Select": []any{
					float64(1),
					map[string]any{"Fn::Split": []any{":", "x:y:z"}},
				},
			},
			expected: "y",
		},
		{
			name:  "fn_importvalue_no_exports",
			input: map[string]any{"Fn::ImportValue": "shared-vpc-id"},
			// When no exports are available, returns an unresolved-import marker.
			expected: "\x01unresolved-import:shared-vpc-id",
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
	stack, err := b.CreateStack(t.Context(), "map-test", template, nil, cloudformation.StackOptions{})
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
			stack, err := b.CreateStack(t.Context(), tt.name, tt.template, nil, cloudformation.StackOptions{})
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
