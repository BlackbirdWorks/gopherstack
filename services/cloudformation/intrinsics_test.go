package cloudformation_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFnGetAtt_S3Bucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		attrName string
		wantPart string
	}{
		{
			name:     "arn",
			attrName: "Arn",
			wantPart: "arn:aws:s3:::",
		},
		{
			name:     "domain_name",
			attrName: "DomainName",
			wantPart: ".s3.amazonaws.com",
		},
		{
			name:     "regional_domain_name",
			attrName: "RegionalDomainName",
			wantPart: ".s3.us-east-1.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			template := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "test-bucket-getatt"}}
				},
				"Outputs": {
					"BucketAttr": {
						"Value": {"Fn::GetAtt": ["MyBucket", "` + tt.attrName + `"]}
					}
				}
			}`

			b := newBackend()
			stack, err := b.CreateStack(context.Background(), "getatt-s3-"+tt.name, template, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			var got string
			for _, o := range stack.Outputs {
				if o.OutputKey == "BucketAttr" {
					got = o.OutputValue
				}
			}
			assert.NotEmpty(t, got)
			assert.Contains(t, got, tt.wantPart)
		})
	}
}

func TestFnGetAtt_DynamoDBTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		attrName string
		wantPart string
	}{
		{
			name:     "arn",
			attrName: "Arn",
			wantPart: "arn:aws:dynamodb:us-east-1:000000000000:table/",
		},
		{
			name:     "stream_arn",
			attrName: "StreamArn",
			wantPart: "/stream/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			template := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyTable": {
						"Type": "AWS::DynamoDB::Table",
						"Properties": {
							"TableName": "parity-test-table",
							"AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
							"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
							"BillingMode": "PAY_PER_REQUEST"
						}
					}
				},
				"Outputs": {
					"TableAttr": {
						"Value": {"Fn::GetAtt": ["MyTable", "` + tt.attrName + `"]}
					}
				}
			}`

			b := newBackend()
			stack, err := b.CreateStack(context.Background(), "getatt-ddb-"+tt.name, template, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			var got string
			for _, o := range stack.Outputs {
				if o.OutputKey == "TableAttr" {
					got = o.OutputValue
				}
			}
			assert.NotEmpty(t, got)
			assert.Contains(t, got, tt.wantPart)
		})
	}
}

func TestFnGetAtt_LambdaFunction(t *testing.T) {
	t.Parallel()

	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyFunction": {
				"Type": "AWS::Lambda::Function",
				"Properties": {
					"FunctionName": "parity-test-fn",
					"Runtime": "python3.9",
					"Handler": "index.handler",
					"Role": "arn:aws:iam::000000000000:role/exec"
				}
			}
		},
		"Outputs": {
			"FnArn": {"Value": {"Fn::GetAtt": ["MyFunction", "Arn"]}}
		}
	}`

	b := newBackend()
	stack, err := b.CreateStack(context.Background(), "getatt-lambda", template, nil,
		cloudformation.StackOptions{Capabilities: []string{"CAPABILITY_IAM"}})
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	var got string
	for _, o := range stack.Outputs {
		if o.OutputKey == "FnArn" {
			got = o.OutputValue
		}
	}
	assert.Contains(t, got, "arn:aws:lambda:us-east-1:000000000000:function:")
}

func TestFnGetAtt_SQSQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		attrName    string
		wantContain string
		wantNot     string
	}{
		{
			name:        "arn",
			attrName:    "Arn",
			wantContain: "arn:aws:sqs:us-east-1:000000000000:",
			wantNot:     "Fn::GetAtt",
		},
		{
			name:        "queue_name",
			attrName:    "QueueName",
			wantContain: "MyQueue", // logicalID used when SQS backend is nil
			wantNot:     "Fn::GetAtt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			template := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyQueue": {
						"Type": "AWS::SQS::Queue",
						"Properties": {}
					}
				},
				"Outputs": {
					"QueueAttr": {
						"Value": {"Fn::GetAtt": ["MyQueue", "` + tt.attrName + `"]}
					}
				}
			}`

			b := newBackend()
			stack, err := b.CreateStack(context.Background(), "getatt-sqs-"+tt.name, template, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			var got string
			for _, o := range stack.Outputs {
				if o.OutputKey == "QueueAttr" {
					got = o.OutputValue
				}
			}
			assert.NotEmpty(t, got)
			assert.Contains(t, got, tt.wantContain)
			assert.NotContains(t, got, tt.wantNot)
		})
	}
}

func TestPseudoParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pseudoRef string
		wantVal   string
	}{
		{
			name:      "aws_region",
			pseudoRef: "AWS::Region",
			wantVal:   "us-east-1",
		},
		{
			name:      "aws_account_id",
			pseudoRef: "AWS::AccountId",
			wantVal:   "000000000000",
		},
		{
			name:      "aws_stack_name",
			pseudoRef: "AWS::StackName",
			wantVal:   "pseudo-param-test",
		},
		{
			name:      "aws_partition",
			pseudoRef: "AWS::Partition",
			wantVal:   "aws",
		},
		{
			name:      "aws_url_suffix",
			pseudoRef: "AWS::URLSuffix",
			wantVal:   "amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			template := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"PseudoVal": {"Value": {"Ref": "` + tt.pseudoRef + `"}}
				}
			}`

			b := newBackend()
			stack, err := b.CreateStack(context.Background(), "pseudo-param-test", template, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			var got string
			for _, o := range stack.Outputs {
				if o.OutputKey == "PseudoVal" {
					got = o.OutputValue
				}
			}
			assert.Equal(t, tt.wantVal, got)
		})
	}
}

func TestPseudoParameters_InSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subStr   string
		wantPart string
	}{
		{
			name:     "region_in_arn",
			subStr:   "arn:aws:s3:::my-bucket-${AWS::Region}",
			wantPart: "arn:aws:s3:::my-bucket-us-east-1",
		},
		{
			name:     "account_in_arn",
			subStr:   "arn:aws:iam::${AWS::AccountId}:role/MyRole",
			wantPart: "arn:aws:iam::000000000000:role/MyRole",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			template := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"SubOut": {"Value": {"Fn::Sub": "` + tt.subStr + `"}}
				}
			}`

			b := newBackend()
			stack, err := b.CreateStack(context.Background(), "pseudo-sub-"+tt.name, template, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			var got string
			for _, o := range stack.Outputs {
				if o.OutputKey == "SubOut" {
					got = o.OutputValue
				}
			}
			assert.Equal(t, tt.wantPart, got)
		})
	}
}

func TestFnSub_TwoArgForm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tmplStr string
		vars    string
		wantVal string
	}{
		{
			name:    "simple_var_map",
			tmplStr: "Hello ${Who}!",
			vars:    `{"Who": "World"}`,
			wantVal: "Hello World!",
		},
		{
			name:    "var_overrides_param",
			tmplStr: "${Domain}",
			vars:    `{"Domain": "custom.example.com"}`,
			wantVal: "custom.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			template := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
				},
				"Outputs": {
					"SubOut": {
						"Value": {"Fn::Sub": ["` + tt.tmplStr + `", ` + tt.vars + `]}
					}
				}
			}`

			b := newBackend()
			stack, err := b.CreateStack(context.Background(), "sub-twoarg-"+tt.name, template, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			var got string
			for _, o := range stack.Outputs {
				if o.OutputKey == "SubOut" {
					got = o.OutputValue
				}
			}
			assert.Equal(t, tt.wantVal, got)
		})
	}
}

func TestFnSub_GetAttStyle(t *testing.T) {
	t.Parallel()

	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {}}
		},
		"Outputs": {
			"BucketARN": {
				"Value": {"Fn::Sub": "The ARN is ${MyBucket.Arn}"}
			}
		}
	}`

	b := newBackend()
	stack, err := b.CreateStack(context.Background(), "sub-getatt-style", template, nil,
		cloudformation.StackOptions{})
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	var got string
	for _, o := range stack.Outputs {
		if o.OutputKey == "BucketARN" {
			got = o.OutputValue
		}
	}
	// Verify GetAtt-style sub resolves to an S3 ARN (not the garbage ${MyBucket.Arn} literal)
	assert.Contains(t, got, "The ARN is arn:aws:s3:::")
	assert.NotContains(t, got, "${MyBucket.Arn}")
}

func TestFnBase64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantVal string
	}{
		{
			name:    "simple_string",
			input:   "hello world",
			wantVal: base64.StdEncoding.EncodeToString([]byte("hello world")),
		},
		{
			name:    "empty_string",
			input:   "",
			wantVal: "",
		},
		{
			name:    "multiline",
			input:   "line1\nline2",
			wantVal: base64.StdEncoding.EncodeToString([]byte("line1\nline2")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ResolveValue(
				map[string]any{"Fn::Base64": tt.input},
				nil, nil,
			)
			assert.Equal(t, tt.wantVal, got)
		})
	}
}

func TestFnGetAZs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		region    string
		wantFirst string
		wantCount int
	}{
		{
			name:      "us_east_1",
			region:    "us-east-1",
			wantFirst: "us-east-1a",
			wantCount: 6,
		},
		{
			name:      "eu_west_1",
			region:    "eu-west-1",
			wantFirst: "eu-west-1a",
			wantCount: 3,
		},
		{
			name:      "unknown_region",
			region:    "ap-custom-1",
			wantFirst: "ap-custom-1a",
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ResolveValue(
				map[string]any{"Fn::GetAZs": tt.region},
				nil, nil,
			)
			parts := strings.Split(got, "\x00")
			assert.Len(t, parts, tt.wantCount)
			assert.Equal(t, tt.wantFirst, parts[0])
		})
	}
}

func TestFnGetAZs_EmptyUsesContext(t *testing.T) {
	t.Parallel()

	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Placeholder": {"Type": "AWS::CloudFormation::WaitConditionHandle", "Properties": {}}
		},
		"Outputs": {
			"AZList": {"Value": {"Fn::Select": [0, {"Fn::GetAZs": ""}]}}
		}
	}`

	b := newBackend()
	stack, err := b.CreateStack(context.Background(), "getazs-ctx", template, nil,
		cloudformation.StackOptions{})
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	var got string
	for _, o := range stack.Outputs {
		if o.OutputKey == "AZList" {
			got = o.OutputValue
		}
	}
	assert.Equal(t, "us-east-1a", got)
}

func TestFnLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   any
		wantVal string
	}{
		{
			name:    "list_of_three",
			input:   map[string]any{"Fn::Length": []any{"a", "b", "c"}},
			wantVal: "3",
		},
		{
			name:    "empty_list",
			input:   map[string]any{"Fn::Length": []any{}},
			wantVal: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ResolveValue(tt.input, nil, nil)
			assert.Equal(t, tt.wantVal, got)
		})
	}
}

func TestFnToJsonString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   any
		wantVal string
	}{
		{
			name:    "string_value",
			input:   map[string]any{"Fn::ToJsonString": "hello"},
			wantVal: `"hello"`,
		},
		{
			name:    "object",
			input:   map[string]any{"Fn::ToJsonString": map[string]any{"key": "value"}},
			wantVal: `{"key":"value"}`,
		},
		{
			name:    "number",
			input:   map[string]any{"Fn::ToJsonString": 42},
			wantVal: "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ResolveValue(tt.input, nil, nil)
			assert.Equal(t, tt.wantVal, got)
		})
	}
}

func TestFnCidr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		args      any
		wantFirst string
		wantCount int
	}{
		{
			name:      "four_subnets",
			args:      map[string]any{"Fn::Cidr": []any{"10.0.0.0/24", "4", "4"}},
			wantCount: 4,
			wantFirst: "10.0.0.0/28",
		},
		{
			name:      "two_subnets_slash16",
			args:      map[string]any{"Fn::Cidr": []any{"192.168.0.0/16", "2", "8"}},
			wantCount: 2,
			wantFirst: "192.168.0.0/24",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ResolveValue(tt.args, nil, nil)
			parts := strings.Split(got, "\x00")
			assert.Len(t, parts, tt.wantCount)
			assert.Equal(t, tt.wantFirst, parts[0])
		})
	}
}

func TestIntegration_GetAttInSub_WithPseudoParams(t *testing.T) {
	t.Parallel()

	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {}
			}
		},
		"Outputs": {
			"FullARN": {
				"Value": {"Fn::Sub": "arn:${AWS::Partition}:s3:::${MyBucket}"}
			},
			"ArnViaGetAtt": {
				"Value": {"Fn::Sub": "${MyBucket.Arn}"}
			},
			"RegionRef": {
				"Value": {"Ref": "AWS::Region"}
			},
			"AccountRef": {
				"Value": {"Ref": "AWS::AccountId"}
			}
		}
	}`

	b := newBackend()
	stack, err := b.CreateStack(context.Background(), "integration-pseudo", template, nil,
		cloudformation.StackOptions{})
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	outputs := make(map[string]string)
	for _, o := range stack.Outputs {
		outputs[o.OutputKey] = o.OutputValue
	}

	// FullARN uses ${MyBucket} (Ref) in Fn::Sub — physicalID is random but partition is aws
	assert.True(t, strings.HasPrefix(outputs["FullARN"], "arn:aws:s3:::"),
		"FullARN should start with arn:aws:s3::: but got %q", outputs["FullARN"])
	// ArnViaGetAtt uses ${MyBucket.Arn} — resolves to S3 ARN
	assert.True(t, strings.HasPrefix(outputs["ArnViaGetAtt"], "arn:aws:s3:::"),
		"ArnViaGetAtt should start with arn:aws:s3::: but got %q", outputs["ArnViaGetAtt"])
	assert.Equal(t, "us-east-1", outputs["RegionRef"])
	assert.Equal(t, "000000000000", outputs["AccountRef"])
}
