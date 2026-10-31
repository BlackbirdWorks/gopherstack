package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// Each case asserts a synchronous ValidationError whose text matches real
// CloudFormation's wording.
func TestCreateStack_TemplateStrictness_SyncErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		wantErr  string
		params   []cloudformation.Parameter
	}{
		{
			name:     "unresolved_condition_on_resource",
			template: `{"Resources":{"B":{"Type":"AWS::S3::Bucket","Condition":"Nope"}}}`,
			wantErr:  "Template format error: Unresolved condition dependencies [Nope] in the Resources block of the template",
		},
		{
			name: "unresolved_condition_in_fn_if",
			template: `{"Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":` +
				`{"Fn::If":["Missing","a","b"]}}}}}`,
			wantErr: "Template format error: Unresolved condition dependencies [Missing] in the Resources block of the template",
		},
		{
			name: "unresolved_condition_on_output",
			template: `{"Resources":{"B":{"Type":"AWS::S3::Bucket"}},` +
				`"Outputs":{"O":{"Condition":"Gone","Value":"x"}}}`,
			wantErr: "Template format error: Unresolved condition dependencies [Gone] in the Outputs block of the template",
		},
		{
			name: "unresolved_condition_in_conditions_block",
			template: `{"Conditions":{"A":{"Fn::Not":[{"Condition":"Ghost"}]}},` +
				`"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			wantErr: "Template format error: Unresolved condition dependencies [Ghost] in the Conditions block of the template",
		},
		{
			name: "unresolved_ref_in_outputs_block",
			template: `{"Resources":{"B":{"Type":"AWS::S3::Bucket"}},` +
				`"Outputs":{"O":{"Value":{"Ref":"Absent"}}}}`,
			wantErr: "Template format error: Unresolved resource dependencies [Absent] in the Outputs block of the template",
		},
		{
			name:     "parameter_without_type",
			template: `{"Parameters":{"P":{"Default":"x"}},"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			wantErr:  "Template format error: [/Parameters/P] Every Parameters object must contain a Type member.",
		},
		{
			name:     "parameter_unrecognized_type",
			template: `{"Parameters":{"P":{"Type":"Integer"}},"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			wantErr:  "Template format error: Unrecognized parameter type: Integer",
		},
		{
			name:     "output_without_value",
			template: `{"Resources":{"B":{"Type":"AWS::S3::Bucket"}},"Outputs":{"O":{"Description":"d"}}}`,
			wantErr:  "Template format error: Every Outputs member must contain a Value object.",
		},
		{
			name: "unrecognized_resource_types_sorted",
			template: `{"Resources":{"A":{"Type":"AWS::Bogus::Zed"},"B":{"Type":"AWS::Bogus::Alpha"},` +
				`"C":{"Type":"AWS::S3::Bucket"}}}`,
			wantErr: "Template format error: Unrecognized resource types: [AWS::Bogus::Alpha, AWS::Bogus::Zed]",
		},
		{
			name: "no_echo_value_masked",
			template: `{"Parameters":{"P":{"Type":"String","NoEcho":true,"MinLength":8}},` +
				`"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			params:  []cloudformation.Parameter{{ParameterKey: "P", ParameterValue: "short"}},
			wantErr: "Parameter validation failed: parameter value **** for parameter name P is shorter than MinLength 8",
		},
		{
			name: "max_length",
			template: `{"Parameters":{"P":{"Type":"String","MaxLength":3}},` +
				`"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			params:  []cloudformation.Parameter{{ParameterKey: "P", ParameterValue: "abcd"}},
			wantErr: "Parameter validation failed: parameter value abcd for parameter name P is longer than MaxLength 3",
		},
		{
			name: "max_value",
			template: `{"Parameters":{"P":{"Type":"Number","MaxValue":10}},` +
				`"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			params:  []cloudformation.Parameter{{ParameterKey: "P", ParameterValue: "11"}},
			wantErr: "Parameter validation failed: parameter value 11 for parameter name P is higher than MaxValue 10",
		},
		{
			name: "allowed_pattern_is_full_match",
			template: `{"Parameters":{"P":{"Type":"String","AllowedPattern":"[a-z]+"}},` +
				`"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			params: []cloudformation.Parameter{{ParameterKey: "P", ParameterValue: "abc1"}},
			wantErr: "Parameter validation failed: parameter value abc1 for parameter name P " +
				"does not match AllowedPattern [a-z]+",
		},
		{
			name: "rule_assertion_fails",
			template: `{"Parameters":{"Env":{"Type":"String"},"Size":{"Type":"String"}},` +
				`"Rules":{"ProdBig":{"RuleCondition":{"Fn::Equals":[{"Ref":"Env"},"prod"]},` +
				`"Assertions":[{"Assert":{"Fn::Contains":[["m5.large","m5.xlarge"],{"Ref":"Size"}]},` +
				`"AssertDescription":"prod needs m5"}]}},"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`,
			params: []cloudformation.Parameter{
				{ParameterKey: "Env", ParameterValue: "prod"}, {ParameterKey: "Size", ParameterValue: "t3.micro"},
			},
			wantErr: "Rule 'ProdBig' assertion failed: prod needs m5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(
				t.Context(),
				"strict-"+tc.name,
				tc.template,
				tc.params,
				cloudformation.StackOptions{},
			)
			require.EqualError(t, err, tc.wantErr)

			_, err = b.ValidateTemplate(tc.template)
			if tc.params == nil {
				require.EqualError(t, err, tc.wantErr, "ValidateTemplate must agree with CreateStack")
			}
		})
	}
}

func TestCreateStack_RulesAssertion_PassesWhenConditionFalse(t *testing.T) {
	t.Parallel()

	tmpl := `{"Parameters":{"Env":{"Type":"String"}},` +
		`"Rules":{"R":{"RuleCondition":{"Fn::Equals":[{"Ref":"Env"},"prod"]},` +
		`"Assertions":[{"Assert":{"Fn::Equals":[{"Ref":"Env"},"never"]}}]}},` +
		`"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`

	stack, err := newBackend().CreateStack(t.Context(), "rule-skip", tmpl,
		[]cloudformation.Parameter{{ParameterKey: "Env", ParameterValue: "dev"}}, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// Real CloudFormation skips a resource or output whose Condition is false.
func TestCreateStack_ResourceAndOutputCondition(t *testing.T) {
	t.Parallel()

	tmpl := `{"Parameters":{"Env":{"Type":"String","Default":"dev"}},` +
		`"Conditions":{"IsProd":{"Fn::Equals":[{"Ref":"Env"},"prod"]},"NotProd":{"Fn::Not":[{"Condition":"IsProd"}]}},` +
		`"Resources":{"Always":{"Type":"AWS::S3::Bucket"},` +
		`"ProdOnly":{"Type":"AWS::S3::Bucket","Condition":"IsProd"},` +
		`"DevOnly":{"Type":"AWS::S3::Bucket","Condition":"NotProd"}},` +
		`"Outputs":{"ProdOut":{"Condition":"IsProd","Value":"p"},"Out":{"Value":"o"}}}`

	tests := []struct {
		name          string
		env           string
		wantResources []string
		wantOutputs   []string
	}{
		{name: "dev", env: "dev", wantResources: []string{"Always", "DevOnly"}, wantOutputs: []string{"Out"}},
		{
			name:          "prod",
			env:           "prod",
			wantResources: []string{"Always", "ProdOnly"},
			wantOutputs:   []string{"Out", "ProdOut"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stack, err := b.CreateStack(
				t.Context(),
				"cond-"+tc.name,
				tmpl,
				[]cloudformation.Parameter{
					{ParameterKey: "Env", ParameterValue: tc.env},
				},
				cloudformation.StackOptions{},
			)
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			res, err := b.DescribeStackResources("cond-" + tc.name)
			require.NoError(t, err)

			var gotRes, gotOut []string
			for _, r := range res {
				gotRes = append(gotRes, r.LogicalID)
			}
			for _, o := range stack.Outputs {
				gotOut = append(gotOut, o.OutputKey)
			}

			assert.ElementsMatch(t, tc.wantResources, gotRes)
			assert.ElementsMatch(t, tc.wantOutputs, gotOut)
		})
	}
}

func TestCreateStack_RequiredResourceProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resource   string
		wantStatus string
		wantReason string
	}{
		{
			name:       "lambda_missing_code",
			resource:   `{"Type":"AWS::Lambda::Function","Properties":{"Role":"arn:aws:iam::000000000000:role/r"}}`,
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "Properties validation failed for resource R with message:\n#: required key [Code] not found",
		},
		{
			name:       "iam_role_missing_trust_policy",
			resource:   `{"Type":"AWS::IAM::Role","Properties":{}}`,
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "#: required key [AssumeRolePolicyDocument] not found",
		},
		{
			name:       "no_value_counts_as_missing",
			resource:   `{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"AWS::NoValue"}}}`,
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "#: required key [VpcId] not found",
		},
		{
			name:       "multiple_missing_listed",
			resource:   `{"Type":"AWS::SSM::Parameter","Properties":{}}`,
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "#: required key [Type] not found\n#: required key [Value] not found",
		},
		{
			name:       "present_via_intrinsic",
			resource:   `{"Type":"AWS::SSM::Parameter","Properties":{"Type":"String","Value":{"Ref":"AWS::StackName"}}}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "conditional_false_resource_not_checked",
			resource:   `{"Type":"AWS::IAM::Role","Condition":"Off","Properties":{}}`,
			wantStatus: "CREATE_COMPLETE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpl := `{"Conditions":{"Off":{"Fn::Equals":["a","b"]}},"Resources":{"R":` + tc.resource + `,` +
				`"Keep":{"Type":"AWS::S3::Bucket"}}}`

			stack, err := newBackend().CreateStack(t.Context(), "req-"+tc.name, tmpl, nil,
				cloudformation.StackOptions{Capabilities: []string{"CAPABILITY_IAM"}})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, stack.StackStatus)
			assert.Contains(t, stack.StackStatusReason, tc.wantReason)
		})
	}
}

func TestCreateStack_DisableValidationSkipsTypeAndPropertyChecks(t *testing.T) {
	t.Parallel()

	tmpl := `{"Resources":{"X":{"Type":"AWS::Bogus::Thing"},"R":{"Type":"AWS::SSM::Parameter","Properties":{}}}}`

	stack, err := newBackend().CreateStack(t.Context(), "novalidate", tmpl, nil,
		cloudformation.StackOptions{DisableValidation: true})
	require.NoError(t, err)
	assert.NotEmpty(t, stack.StackID)
}

func TestCreateStack_RegisteredTypeIsRecognized(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{"Resources":{"X":{"Type":"MyOrg::Svc::Widget","Properties":{}}}}`

	_, err := b.CreateStack(t.Context(), "unreg", tmpl, nil, cloudformation.StackOptions{})
	require.EqualError(t, err, "Template format error: Unrecognized resource types: [MyOrg::Svc::Widget]")

	_, err = b.ActivateType("MyOrg::Svc::Widget", "", cloudformation.ActivateTypeOptions{})
	require.NoError(t, err)

	_, err = b.CreateStack(t.Context(), "reg", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
}

func TestCreateStack_DuplicateExportMessage(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{"Resources":{"B":{"Type":"AWS::S3::Bucket"}},"Outputs":{"O":{"Value":"v","Export":{"Name":"shared"}}}}`

	_, err := b.CreateStack(t.Context(), "owner", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	stack, err := b.CreateStack(t.Context(), "second", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "Export with name shared is already exported by stack owner", stack.StackStatusReason)
}

func TestCreateStack_PropertyValidationToggle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resource   string
		wantStatus string
		wantReason string
		disabled   bool
	}{
		{
			name:       "unknown_property",
			resource:   `{"Type":"AWS::SQS::Queue","Properties":{"Bogus":"x"}}`,
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "Encountered unsupported property Bogus",
		},
		{
			name:       "wrong_type",
			resource:   `{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":"abc"}}`,
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "#/DelaySeconds: expected type: number, found: String",
		},
		{
			name:       "missing_required_from_spec",
			resource:   `{"Type":"AWS::EKS::Cluster","Properties":{"RoleArn":"arn:aws:iam::000000000000:role/r"}}`,
			wantStatus: "ROLLBACK_COMPLETE",
			wantReason: "#: required key [ResourcesVpcConfig] not found",
		},
		{
			name:       "intrinsic_skips_type_check",
			resource:   `{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":{"Ref":"AWS::StackName"}}}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "valid",
			resource:   `{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":5}}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "disabled_unknown_property",
			resource:   `{"Type":"AWS::SQS::Queue","Properties":{"Bogus":"x"}}`,
			disabled:   true,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "disabled_missing_required",
			resource:   `{"Type":"AWS::EKS::Cluster","Properties":{}}`,
			disabled:   true,
			wantStatus: "CREATE_COMPLETE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			b.SetPropertyValidation(!tc.disabled)

			stack, err := b.CreateStack(t.Context(), "pv-"+tc.name, `{"Resources":{"R":`+tc.resource+`}}`, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, stack.StackStatus)
			assert.Contains(t, stack.StackStatusReason, tc.wantReason)
		})
	}
}
