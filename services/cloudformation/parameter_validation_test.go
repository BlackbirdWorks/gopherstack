package cloudformation_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// constrainedParamTemplate builds a JSON template with a single constrained
// parameter P and one S3 bucket that creates without provisioning real resources.
func constrainedParamTemplate(paramDef string) string {
	return `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"P": ` + paramDef + `},
		"Resources": {"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {}}}
	}`
}

// createWithParam is a helper that creates a stack with a single parameter P=value.
// Returns the stack (never nil on no-error) and error.
func createWithParam(
	t *testing.T,
	b *cloudformation.InMemoryBackend,
	stackName, paramDef, value string,
) *cloudformation.Stack {
	t.Helper()
	stack, err := b.CreateStack(
		context.Background(),
		stackName,
		constrainedParamTemplate(paramDef),
		[]cloudformation.Parameter{{ParameterKey: "P", ParameterValue: value}},
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)

	return stack
}

// TestValidateParameters_AllowedPattern_Valid verifies that values matching
// AllowedPattern produce a CREATE_COMPLETE stack.
func TestValidateParameters_AllowedPattern_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		value   string
	}{
		{name: "alpha_only", pattern: `^[a-zA-Z]+$`, value: "Hello"},
		{name: "numeric", pattern: `^[0-9]+$`, value: "12345"},
		{name: "prefix_match", pattern: `^prod-.*`, value: "prod-bucket"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			paramDef := `{"Type": "String", "AllowedPattern": "` + tc.pattern + `"}`
			stack := createWithParam(t, b, "pat-"+tc.name, paramDef, tc.value)
			assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus,
				"valid value %q should match pattern %q", tc.value, tc.pattern)
		})
	}
}

// TestValidateParameters_AllowedPattern_Invalid verifies that values not matching
// AllowedPattern produce a ROLLBACK_COMPLETE stack.
func TestValidateParameters_AllowedPattern_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		value   string
	}{
		{name: "alpha_rejects_digit", pattern: `^[a-zA-Z]+$`, value: "Bad1"},
		{name: "numeric_rejects_alpha", pattern: `^[0-9]+$`, value: "abc"},
		{name: "prefix_rejects_wrong", pattern: `^prod-.*`, value: "dev-bucket"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			paramDef := `{"Type": "String", "AllowedPattern": "` + tc.pattern + `"}`
			stack := createWithParam(t, b, "pat-"+tc.name, paramDef, tc.value)
			assert.Equal(t, "ROLLBACK_COMPLETE", stack.StackStatus,
				"value %q should be rejected by pattern %q", tc.value, tc.pattern)
		})
	}
}

// TestValidateParameters_AllowedPattern_ConstraintDescription verifies that the
// custom ConstraintDescription is surfaced in StackStatusReason when the pattern fails.
func TestValidateParameters_AllowedPattern_ConstraintDescription(t *testing.T) {
	t.Parallel()

	b := newBackend()
	body := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"P": {
				"Type": "String",
				"AllowedPattern": "^[a-z]+$",
				"ConstraintDescription": "must be lowercase letters only"
			}
		},
		"Resources": {"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {}}}
	}`
	stack, err := b.CreateStack(context.Background(), "cd-stack", body,
		[]cloudformation.Parameter{{ParameterKey: "P", ParameterValue: "Bad1"}},
		cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "ROLLBACK_COMPLETE", stack.StackStatus)
	assert.Contains(t, stack.StackStatusReason, "must be lowercase letters only")
}

// TestValidateParameters_MinValue_MaxValue validates Number type range constraints.
// Failures produce ROLLBACK_COMPLETE; successes produce CREATE_COMPLETE.
func TestValidateParameters_MinValue_MaxValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		paramDef   string
		value      string
		wantStatus string
	}{
		{
			name:       "value_within_range",
			paramDef:   `{"Type": "Number", "MinValue": 1, "MaxValue": 100}`,
			value:      "50",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "at_min_boundary",
			paramDef:   `{"Type": "Number", "MinValue": 1, "MaxValue": 100}`,
			value:      "1",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "at_max_boundary",
			paramDef:   `{"Type": "Number", "MinValue": 1, "MaxValue": 100}`,
			value:      "100",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "below_min",
			paramDef:   `{"Type": "Number", "MinValue": 1, "MaxValue": 100}`,
			value:      "0",
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "above_max",
			paramDef:   `{"Type": "Number", "MinValue": 1, "MaxValue": 100}`,
			value:      "101",
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "only_min_passes",
			paramDef:   `{"Type": "Number", "MinValue": 5}`,
			value:      "10",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "only_min_fails",
			paramDef:   `{"Type": "Number", "MinValue": 5}`,
			value:      "4",
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "only_max_passes",
			paramDef:   `{"Type": "Number", "MaxValue": 10}`,
			value:      "10",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "only_max_fails",
			paramDef:   `{"Type": "Number", "MaxValue": 10}`,
			value:      "11",
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "float_value_in_range",
			paramDef:   `{"Type": "Number", "MinValue": 0, "MaxValue": 1}`,
			value:      "0.5",
			wantStatus: "CREATE_COMPLETE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stack := createWithParam(t, b, "num-"+tc.name, tc.paramDef, tc.value)
			assert.Equal(t, tc.wantStatus, stack.StackStatus,
				"value=%q paramDef=%s", tc.value, tc.paramDef)
		})
	}
}

// TestValidateParameters_MinLength_MaxLength validates String type length constraints.
func TestValidateParameters_MinLength_MaxLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		paramDef   string
		value      string
		wantStatus string
	}{
		{
			name:       "within_range",
			paramDef:   `{"Type": "String", "MinLength": 2, "MaxLength": 5}`,
			value:      "abc",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "at_min",
			paramDef:   `{"Type": "String", "MinLength": 2, "MaxLength": 5}`,
			value:      "ab",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "at_max",
			paramDef:   `{"Type": "String", "MinLength": 2, "MaxLength": 5}`,
			value:      "abcde",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "below_min",
			paramDef:   `{"Type": "String", "MinLength": 2, "MaxLength": 5}`,
			value:      "a",
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "above_max",
			paramDef:   `{"Type": "String", "MinLength": 2, "MaxLength": 5}`,
			value:      "abcdef",
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "only_min_passes",
			paramDef:   `{"Type": "String", "MinLength": 3}`,
			value:      "hello",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "only_min_fails",
			paramDef:   `{"Type": "String", "MinLength": 3}`,
			value:      "ab",
			wantStatus: "ROLLBACK_COMPLETE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stack := createWithParam(t, b, "len-"+tc.name, tc.paramDef, tc.value)
			assert.Equal(t, tc.wantStatus, stack.StackStatus,
				"len(%q)=%d paramDef=%s", tc.value, len(tc.value), tc.paramDef)
		})
	}
}

// TestValidateParameters_UpdateStack_Constraints verifies that constraints are
// enforced during UpdateStack (failure → UPDATE_ROLLBACK_COMPLETE).
func TestValidateParameters_UpdateStack_Constraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		paramDef   string
		createVal  string
		updateVal  string
		wantStatus string
	}{
		{
			name:       "pattern_enforced_on_update",
			paramDef:   `{"Type": "String", "AllowedPattern": "^[a-z]+$"}`,
			createVal:  "valid",
			updateVal:  "INVALID",
			wantStatus: "UPDATE_ROLLBACK_COMPLETE",
		},
		{
			name:       "min_value_enforced_on_update",
			paramDef:   `{"Type": "Number", "MinValue": 10}`,
			createVal:  "20",
			updateVal:  "5",
			wantStatus: "UPDATE_ROLLBACK_COMPLETE",
		},
		{
			name:       "valid_update_accepted",
			paramDef:   `{"Type": "String", "AllowedPattern": "^[a-z]+$"}`,
			createVal:  "valid",
			updateVal:  "alsovalid",
			wantStatus: "UPDATE_COMPLETE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			body := constrainedParamTemplate(tc.paramDef)

			_, err := b.CreateStack(context.Background(), "upd-"+tc.name,
				body,
				[]cloudformation.Parameter{{ParameterKey: "P", ParameterValue: tc.createVal}},
				cloudformation.StackOptions{})
			require.NoError(t, err)

			stack, err := b.UpdateStack(context.Background(), "upd-"+tc.name,
				body,
				[]cloudformation.Parameter{{ParameterKey: "P", ParameterValue: tc.updateVal}},
				cloudformation.StackOptions{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, stack.StackStatus,
				"update with value=%q", tc.updateVal)
		})
	}
}

// TestValidateParameters_DefaultValue_ConstraintApplied verifies that parameter
// constraints are applied even when only the default value is used.
func TestValidateParameters_DefaultValue_ConstraintApplied(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		paramDef   string
		wantStatus string
	}{
		{
			name:       "default_violates_allowed_pattern",
			paramDef:   `{"Type": "String", "Default": "UPPERCASE", "AllowedPattern": "^[a-z]+$"}`,
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "default_satisfies_allowed_values",
			paramDef:   `{"Type": "String", "Default": "prod", "AllowedValues": ["dev", "prod", "staging"]}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:       "default_violates_min_value",
			paramDef:   `{"Type": "Number", "Default": 0, "MinValue": 1}`,
			wantStatus: "ROLLBACK_COMPLETE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			// No Parameters supplied — default is used.
			stack, err := b.CreateStack(context.Background(), "def-"+tc.name,
				constrainedParamTemplate(tc.paramDef),
				nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, stack.StackStatus)
		})
	}
}
