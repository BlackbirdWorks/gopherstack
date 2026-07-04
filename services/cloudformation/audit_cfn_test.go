package cloudformation_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- DeletionPolicy tests --------------------------------------------------

// deleteTracker records resource types passed to ResourceCreator.Delete.
type deleteTracker struct {
	deleted []string
	mu      sync.Mutex
}

func (dt *deleteTracker) record(resType string) {
	dt.mu.Lock()
	dt.deleted = append(dt.deleted, resType)
	dt.mu.Unlock()
}

func (dt *deleteTracker) snapshot() []string {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	out := make([]string, len(dt.deleted))
	copy(out, dt.deleted)

	return out
}

func newTrackedBackend(dt *deleteTracker) *cloudformation.InMemoryBackend {
	b := newBackend()
	b.GetCreator().InjectDeleteHook(dt.record)

	return b
}

// templateWithDeletionPolicy builds a JSON template with one S3 bucket resource
// with the given DeletionPolicy value (empty string omits the field).
func templateWithDeletionPolicy(policy string) string {
	res := map[string]any{
		"Type":       "AWS::S3::Bucket",
		"Properties": map[string]any{},
	}
	if policy != "" {
		res["DeletionPolicy"] = policy
	}
	body, _ := json.Marshal(map[string]any{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources":                map[string]any{"Bucket": res},
	})

	return string(body)
}

// TestDeletionPolicy_Delete_CallsDeleteOnResource verifies that the default
// (no DeletionPolicy) and explicit "Delete" cause Delete to be called.
func TestDeletionPolicy_Delete_CallsDeleteOnResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "no_policy_default_delete", policy: ""},
		{name: "explicit_delete", policy: "Delete"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dt := &deleteTracker{}
			b := newTrackedBackend(dt)

			_, err := b.CreateStack(context.Background(), "test-stack",
				templateWithDeletionPolicy(tc.policy), nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			err = b.DeleteStack(context.Background(), "test-stack")
			require.NoError(t, err)

			assert.Contains(t, dt.snapshot(), "AWS::S3::Bucket",
				"Delete should be called for policy=%q", tc.policy)
		})
	}
}

// TestDeletionPolicy_Retain_SkipsDelete verifies that DeletionPolicy=Retain
// prevents Delete from being called when the stack is deleted, while the stack
// still reaches DELETE_COMPLETE.
func TestDeletionPolicy_Retain_SkipsDelete(t *testing.T) {
	t.Parallel()

	dt := &deleteTracker{}
	b := newTrackedBackend(dt)

	_, err := b.CreateStack(context.Background(), "retain-stack",
		templateWithDeletionPolicy("Retain"), nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	err = b.DeleteStack(context.Background(), "retain-stack")
	require.NoError(t, err)

	assert.NotContains(t, dt.snapshot(), "AWS::S3::Bucket",
		"Delete must NOT be called for DeletionPolicy=Retain")
}

// TestDeletionPolicy_Snapshot_SkipsDelete verifies that DeletionPolicy=Snapshot
// (not yet fully emulated) is treated like Retain and does not call Delete.
func TestDeletionPolicy_Snapshot_SkipsDelete(t *testing.T) {
	t.Parallel()

	dt := &deleteTracker{}
	b := newTrackedBackend(dt)

	_, err := b.CreateStack(context.Background(), "snap-stack",
		templateWithDeletionPolicy("Snapshot"), nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	err = b.DeleteStack(context.Background(), "snap-stack")
	require.NoError(t, err)

	assert.NotContains(t, dt.snapshot(), "AWS::S3::Bucket",
		"Delete must NOT be called for DeletionPolicy=Snapshot")
}

// TestDeletionPolicy_Retain_OnUpdate_SkipsDelete verifies that when a resource
// with DeletionPolicy=Retain is removed from the template during UpdateStack,
// the underlying Delete call is skipped (while resources with no policy are deleted).
func TestDeletionPolicy_Retain_OnUpdate_SkipsDelete(t *testing.T) {
	t.Parallel()

	dt := &deleteTracker{}
	b := newTrackedBackend(dt)

	initial := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Bucket": {
				"Type": "AWS::S3::Bucket",
				"DeletionPolicy": "Retain",
				"Properties": {}
			},
			"Queue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {}
			}
		}
	}`

	// Update removes Bucket (Retain) and Queue (no policy). Adds Topic.
	updated := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Topic": {
				"Type": "AWS::SNS::Topic",
				"Properties": {}
			}
		}
	}`

	_, err := b.CreateStack(context.Background(), "upd-retain", initial, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	_, err = b.UpdateStack(context.Background(), "upd-retain", updated, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	deleted := dt.snapshot()
	assert.Contains(t, deleted, "AWS::SQS::Queue",
		"Queue (no DeletionPolicy) should be deleted on update")
	assert.NotContains(t, deleted, "AWS::S3::Bucket",
		"Bucket (DeletionPolicy=Retain) must NOT be deleted on update")
}

// TestDeletionPolicy_ParsedFromTemplate verifies DeletionPolicy is parsed from
// both JSON and YAML template bodies.
func TestDeletionPolicy_ParsedFromTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantPolicy string
	}{
		{
			name: "json_retain",
			body: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Bucket": {
						"Type": "AWS::S3::Bucket",
						"DeletionPolicy": "Retain",
						"Properties": {}
					}
				}
			}`,
			wantPolicy: "Retain",
		},
		{
			name: "yaml_retain",
			body: `
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain
    Properties: {}
`,
			wantPolicy: "Retain",
		},
		{
			name: "json_delete",
			body: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Bucket": {
						"Type": "AWS::S3::Bucket",
						"DeletionPolicy": "Delete",
						"Properties": {}
					}
				}
			}`,
			wantPolicy: "Delete",
		},
		{
			name: "json_omitted",
			body: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {}}
				}
			}`,
			wantPolicy: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := cloudformation.ParseTemplate(tc.body)
			require.NoError(t, err)
			assert.Equal(t, tc.wantPolicy, tmpl.Resources["Bucket"].DeletionPolicy)
		})
	}
}

// ---- Parameter constraint tests --------------------------------------------

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
