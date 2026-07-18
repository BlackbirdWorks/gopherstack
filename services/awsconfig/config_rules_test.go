package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_DeleteConfigRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
			},
			delName: "my-rule",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteConfigRule(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteEvaluationResults(t *testing.T) {
	t.Parallel()

	t.Run("existing_rule_clears_evaluations", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
		require.NoError(t, b.PutEvaluations([]awsconfig.EvaluationResult{
			{
				ConfigRuleName: "my-rule", ResourceType: "AWS::S3::Bucket",
				ResourceID: "b1", ComplianceType: "NON_COMPLIANT",
			},
		}))
		require.Len(t, b.GetComplianceDetailsByConfigRule("my-rule", nil), 1)

		require.NoError(t, b.DeleteEvaluationResults("my-rule"))
		assert.Empty(t, b.GetComplianceDetailsByConfigRule("my-rule", nil))
		assert.Empty(t, b.GetConfigRuleComplianceType("my-rule"))
	})

	t.Run("nonexistent_rule_errors", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		err := b.DeleteEvaluationResults("nonexistent")
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchConfigRule)
	})

	t.Run("empty_name_is_validation_error", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		err := b.DeleteEvaluationResults("")
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrValidation)
	})
}

func TestAWSConfigBackend_DescribeConfigRules_WithStoredRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		filter    []string
		wantNames []string
	}{
		{
			name: "returns_all_sorted",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-b"}))
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-a"}))
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-c"}))
			},
			wantNames: []string{"rule-a", "rule-b", "rule-c"},
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-a"}))
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-b"}))
			},
			filter:    []string{"rule-a"},
			wantNames: []string{"rule-a"},
		},
		{
			name:      "empty_backend",
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			rules := b.DescribeConfigRules(tt.filter)
			names := make([]string, len(rules))
			for i, r := range rules {
				names[i] = r.ConfigRuleName
			}
			assert.Equal(t, tt.wantNames, names)
		})
	}
}

func TestDescribeConfigRuleEvaluationStatus(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule1"})
	_ = b.StartConfigRulesEvaluation()

	statuses := b.DescribeConfigRuleEvaluationStatus([]string{"rule1"})
	if len(statuses) != 1 || statuses[0].ConfigRuleName != "rule1" {
		t.Fatalf("DescribeConfigRuleEvaluationStatus: %v", statuses)
	}
}

func TestDescribeConfigRuleEvaluationStatus_All(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "r1"})
	_ = b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "r2"})
	_ = b.StartConfigRulesEvaluation()

	statuses := b.DescribeConfigRuleEvaluationStatus(nil)
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d: %v", len(statuses), statuses)
	}
}

func TestPutEvaluations(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "rule1", ComplianceType: "COMPLIANT"},
	})
	if err != nil {
		t.Fatalf("PutEvaluations: %v", err)
	}

	ct := b.GetConfigRuleComplianceType("rule1")
	if ct != "COMPLIANT" {
		t.Fatalf("expected COMPLIANT, got %q", ct)
	}
}

func TestPutExternalEvaluation(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutExternalEvaluation(awsconfig.EvaluationResult{
		ConfigRuleName: "rule1",
		ComplianceType: "NON_COMPLIANT",
	})
	if err != nil {
		t.Fatalf("PutExternalEvaluation: %v", err)
	}

	ct := b.GetConfigRuleComplianceType("rule1")
	if ct != "NON_COMPLIANT" {
		t.Fatalf("expected NON_COMPLIANT, got %q", ct)
	}
}

func TestDescribeComplianceByConfigRule(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "r1", ComplianceType: "COMPLIANT"},
	})

	out := b.DescribeComplianceByConfigRule([]string{"r1"})
	if len(out) != 1 || out[0].Compliance.ComplianceType != "COMPLIANT" {
		t.Fatalf("DescribeComplianceByConfigRule: %v", out)
	}
}

func TestGetComplianceSummaryByConfigRule(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	out := b.GetComplianceSummaryByConfigRule()
	if out == nil {
		t.Fatal("expected non-nil slice")
	}

	if len(out) != 0 {
		t.Fatalf("expected empty summary for fresh backend, got %v", out)
	}
}

func TestGetComplianceSummaryByConfigRule_Aggregates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantType         string
		evaluations      []awsconfig.EvaluationResult
		wantCompliant    int32
		wantNonCompliant int32
	}{
		{
			name: "all compliant",
			evaluations: []awsconfig.EvaluationResult{
				{ConfigRuleName: "r1", ComplianceType: "COMPLIANT"},
				{ConfigRuleName: "r2", ComplianceType: "COMPLIANT"},
			},
			wantCompliant:    2,
			wantNonCompliant: 0,
			wantType:         "COMPLIANT",
		},
		{
			name: "mixed becomes non-compliant",
			evaluations: []awsconfig.EvaluationResult{
				{ConfigRuleName: "r1", ComplianceType: "COMPLIANT"},
				{ConfigRuleName: "r2", ComplianceType: "NON_COMPLIANT"},
			},
			wantCompliant:    1,
			wantNonCompliant: 1,
			wantType:         "NON_COMPLIANT",
		},
		{
			name: "not applicable ignored in counts",
			evaluations: []awsconfig.EvaluationResult{
				{ConfigRuleName: "r1", ComplianceType: "NOT_APPLICABLE"},
			},
			wantCompliant:    0,
			wantNonCompliant: 0,
			wantType:         "COMPLIANT",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if err := b.PutEvaluations(tc.evaluations); err != nil {
				t.Fatalf("PutEvaluations: %v", err)
			}

			out := b.GetComplianceSummaryByConfigRule()
			if len(out) != 1 {
				t.Fatalf("expected one summary, got %v", out)
			}

			got := out[0]
			if got.ComplianceType != tc.wantType {
				t.Errorf("ComplianceType = %q, want %q", got.ComplianceType, tc.wantType)
			}

			if got.ComplianceSummary.CompliantResourceCount.CappedCount != tc.wantCompliant {
				t.Errorf("CompliantResourceCount = %d, want %d",
					got.ComplianceSummary.CompliantResourceCount.CappedCount, tc.wantCompliant)
			}

			if got.ComplianceSummary.NonCompliantResourceCount.CappedCount != tc.wantNonCompliant {
				t.Errorf("NonCompliantResourceCount = %d, want %d",
					got.ComplianceSummary.NonCompliantResourceCount.CappedCount, tc.wantNonCompliant)
			}
		})
	}
}

func TestGetCustomRulePolicy_DefaultEmpty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	policy := b.GetCustomRulePolicy("my-rule")
	if policy != "" {
		t.Fatalf("expected empty policy, got %q", policy)
	}
}

func TestDescribeAggregateComplianceByConfigRules(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "r1", ComplianceType: "COMPLIANT"},
	})

	out := b.DescribeAggregateComplianceByConfigRules()
	if len(out) != 1 {
		t.Fatalf("expected 1, got %d", len(out))
	}
}
